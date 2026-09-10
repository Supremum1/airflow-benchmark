#!/usr/bin/env python3
"""Low-frequency, read-only Linux /proc observer for Airflow processes."""

from __future__ import annotations

import argparse
import csv
import json
import os
import resource
import signal
import time
from pathlib import Path


FIELDS = (
    "timestamp", "pid", "process_start_ticks", "ppid", "component", "role",
    "cpu_ticks", "rss_bytes", "read_bytes", "write_bytes",
)
STOP = False


def stop(_signum, _frame) -> None:
    global STOP
    STOP = True


def component(command: str) -> str:
    text = command.lower()
    if "localexecutor" in text or "task sdk" in text:
        return "worker"
    if "dag-processor" in text or "dag_processor" in text:
        return "dag_processor"
    if "scheduler" in text:
        return "scheduler"
    if "api_server" in text or "api-server" in text:
        return "api_server"
    if "triggerer" in text:
        return "triggerer"
    return "service"


def process_role(command: str) -> str:
    return "worker_pool" if "localexecutor" in command.lower() else "component"


def io_bytes(pid: str) -> tuple[int, int]:
    values = {"read_bytes": 0, "write_bytes": 0}
    try:
        for line in Path(f"/proc/{pid}/io").read_text().splitlines():
            key, _, value = line.partition(":")
            if key in values:
                values[key] = int(value.strip())
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        pass
    return values["read_bytes"], values["write_bytes"]


def find_roots(airflow_home: str) -> set[int]:
    marker = f"AIRFLOW_HOME={airflow_home}".encode()
    roots = set()
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            environ = (entry / "environ").read_bytes().split(b"\0")
            command = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace")
            if marker in environ and "airflow standalone" in command:
                roots.add(int(entry.name))
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
    return roots


def descendants(roots: set[int]) -> set[int]:
    selected = set(roots)
    pending = list(roots)
    while pending:
        pid = pending.pop()
        children = []
        for child_file in Path(f"/proc/{pid}/task").glob("*/children"):
            try:
                children.extend(child_file.read_text().split())
            except (FileNotFoundError, PermissionError, ProcessLookupError):
                continue
        for child in map(int, children):
            if child not in selected:
                selected.add(child)
                pending.append(child)
    return selected


def snapshot(roots: set[int]) -> list[dict[str, int | float | str]]:
    processes = {}
    page_size = os.sysconf("SC_PAGE_SIZE")
    selected = descendants(roots)
    for pid in selected:
        if pid == os.getpid():
            continue
        entry = Path(f"/proc/{pid}")
        try:
            args = (entry / "cmdline").read_bytes().replace(b"\0", b" ").decode(errors="replace")
            comm = (entry / "comm").read_text().strip()
            raw = (entry / "stat").read_text()
            tail = raw[raw.rfind(")") + 2 :].split()
            processes[pid] = {
                "pid": pid,
                "process_start_ticks": int(tail[19]),
                "ppid": int(tail[1]),
                "command": args,
                "comm": comm,
                "component": component(args),
                "role": process_role(args),
                "cpu_ticks": int(tail[11]) + int(tail[12]),
                "rss_bytes": int(tail[21]) * page_size,
                "read_bytes": 0,
                "write_bytes": 0,
            }
        except (FileNotFoundError, PermissionError, ProcessLookupError, ValueError):
            continue

    # Airflow workers deliberately make /proc/<pid>/environ unreadable. They are
    # still safely attributable as descendants of the matching standalone tree.
    rows = []
    for pid in selected:
        row = processes.get(pid)
        if not row:
            continue
        if row["comm"].startswith("airflow") or any(Path(part).name == "airflow" for part in row["command"].split()):
            ancestor = row["ppid"]
            while ancestor in processes:
                if processes[ancestor]["component"] == "worker":
                    row["component"] = "worker"
                    row["role"] = "task"
                    break
                ancestor = processes[ancestor]["ppid"]
            row["read_bytes"], row["write_bytes"] = io_bytes(str(pid))
            row.pop("command")
            row.pop("comm")
            rows.append(row)
    return rows


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path)
    parser.add_argument("--airflow-home", required=True)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--check-workers", type=int)
    args = parser.parse_args()
    roots = find_roots(args.airflow_home)
    if not roots:
        raise SystemExit(f"Не найден процесс 'airflow standalone' для AIRFLOW_HOME={args.airflow_home}")
    if args.check_workers is not None:
        workers = sum(row["role"] == "worker_pool" for row in snapshot(roots))
        if workers != args.check_workers:
            raise SystemExit(
                f"Конфигурации расходятся: scheduler имеет {workers} workers, "
                f"а текущий env задаёт parallelism={args.check_workers}. Перезапустите Airflow."
            )
        print(f"Проверено: scheduler использует {workers} workers")
        return
    if args.output is None:
        parser.error("--output обязателен для режима наблюдения")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    started = time.monotonic()
    samples = 0
    with args.output.open("w", newline="", encoding="utf-8") as target:
        writer = csv.DictWriter(target, fieldnames=FIELDS)
        writer.writeheader()
        while not STOP:
            timestamp = time.time()
            for row in snapshot(roots):
                writer.writerow({"timestamp": f"{timestamp:.6f}", **row})
                samples += 1
            target.flush()
            time.sleep(max(0.2, args.interval))
    usage = resource.getrusage(resource.RUSAGE_SELF)
    stats = {
        "wall_seconds": time.monotonic() - started,
        "cpu_seconds": usage.ru_utime + usage.ru_stime,
        "samples": samples,
        "interval_seconds": args.interval,
    }
    args.output.with_name("observer.json").write_text(json.dumps(stats, indent=2), encoding="utf-8")


if __name__ == "__main__":
    main()
