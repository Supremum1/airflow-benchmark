#!/usr/bin/env python3
"""Wait for a run and build a self-contained Russian HTML report."""

from __future__ import annotations

import argparse
import csv
import html
import json
import os
import sqlite3
import statistics
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path


PHASES = {
    "cpu": ("CPU-bound", "Вычисления", "Показывает, сколько ядер реально заняли worker-процессы."),
    "io": ("I/O-bound", "Ожидание I/O", "Показывает цену ожидания внешнего сервиса в слотах Airflow."),
    "sleep": ("Sleep", "Контрольное ожидание", "Показывает чистую пропускную способность оркестрации и слотов."),
}
COMPONENTS = {
    "worker": "Исполнение задач",
    "scheduler": "Планировщик",
    "dag_processor": "Разбор DAG",
    "api_server": "API и интерфейс",
    "triggerer": "Triggerer",
    "service": "Служебные процессы",
}
TERMINAL = {"success", "failed"}


def db_connect(path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=10)
    connection.row_factory = sqlite3.Row
    return connection


def wait_for_run(db: Path, run_id: str, timeout: float, poll: float) -> int:
    deadline = time.monotonic() + timeout
    last_state = None
    while time.monotonic() < deadline:
        try:
            with db_connect(db) as connection:
                row = connection.execute(
                    "select state from dag_run where dag_id=? and run_id=?",
                    ("resource_benchmark", run_id),
                ).fetchone()
        except sqlite3.Error:
            row = None
        state = row["state"] if row else "ожидание регистрации"
        if state != last_state:
            print(f"Состояние прогона: {state}", flush=True)
            last_state = state
        if state in TERMINAL:
            return 0 if state == "success" else 2
        time.sleep(max(1.0, poll))
    print("Истекло время ожидания прогона", file=sys.stderr)
    return 3


def timestamp(value) -> float | None:
    if not value:
        return None
    text = str(value).replace("Z", "+00:00")
    try:
        parsed = datetime.fromisoformat(text)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.timestamp()
    except ValueError:
        return None


def percentile(values: list[float], fraction: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    position = max(0, min(len(ordered) - 1, round((len(ordered) - 1) * fraction)))
    return ordered[position]


def round_metrics(value: float) -> float:
    return round(float(value), 3)


def load_run(db: Path, run_id: str) -> tuple[dict, list[dict]]:
    with db_connect(db) as connection:
        dag_row = connection.execute(
            """select state, queued_at, start_date, end_date
               from dag_run where dag_id=? and run_id=?""",
            ("resource_benchmark", run_id),
        ).fetchone()
        task_rows = connection.execute(
            """select task_id, map_index, state, scheduled_dttm, queued_dttm,
                      start_date, end_date, duration, try_number
               from task_instance where dag_id=? and run_id=?
               order by task_id, map_index""",
            ("resource_benchmark", run_id),
        ).fetchall()
    if not dag_row:
        raise SystemExit(f"Прогон {run_id!r} не найден в {db}")
    return dict(dag_row), [dict(row) for row in task_rows]


def phase_metrics(tasks: list[dict], parallelism: int) -> tuple[dict, dict]:
    result = {}
    windows = {}
    for key, (title, label, purpose) in PHASES.items():
        selected = [row for row in tasks if row["task_id"] == key and row["map_index"] >= 0]
        starts = [timestamp(row["start_date"]) for row in selected]
        ends = [timestamp(row["end_date"]) for row in selected]
        starts = [value for value in starts if value is not None]
        ends = [value for value in ends if value is not None]
        durations = []
        queue_waits = []
        for row in selected:
            start = timestamp(row["start_date"])
            end_time = timestamp(row["end_date"])
            queued = timestamp(row["queued_dttm"] or row["scheduled_dttm"])
            if start is not None and queued is not None:
                queue_waits.append(max(0.0, start - queued))
            if start is not None and end_time is not None:
                durations.append(max(0.0, end_time - start))
        begin, end = (min(starts), max(ends)) if starts and ends else (0.0, 0.0)
        wall = max(0.0, end - begin)
        result[key] = {
            "key": key,
            "title": title,
            "label": label,
            "purpose": purpose,
            "tasks": len(selected),
            "success": sum(row["state"] == "success" for row in selected),
            "wall_seconds": round_metrics(wall),
            "duration_p50": round_metrics(statistics.median(durations) if durations else 0),
            "duration_p95": round_metrics(percentile(durations, 0.95)),
            "queue_p50": round_metrics(statistics.median(queue_waits) if queue_waits else 0),
            "queue_p95": round_metrics(percentile(queue_waits, 0.95)),
            "throughput": round_metrics(len(selected) / wall if wall else 0),
            "slot_use_percent": round_metrics(
                100 * sum(durations) / (wall * max(1, parallelism)) if wall else 0
            ),
            "components": [],
        }
        windows[key] = (begin, end)
    return result, windows


def phase_at(moment: float, windows: dict) -> str | None:
    for phase, (begin, end) in windows.items():
        if begin <= moment <= end:
            return phase
    return None


def process_metrics(samples_path: Path, windows: dict) -> tuple[dict, dict, int]:
    if not samples_path.exists():
        return {}, {}, 0
    by_process = defaultdict(list)
    rss_by_snapshot = defaultdict(float)
    worker_pools = defaultdict(int)
    with samples_path.open(encoding="utf-8") as source:
        for row in csv.DictReader(source):
            parsed = {
                "timestamp": float(row["timestamp"]),
                "component": row["component"],
                "cpu_ticks": int(row["cpu_ticks"]),
                "rss_bytes": int(row["rss_bytes"]),
                "read_bytes": int(row["read_bytes"]),
                "write_bytes": int(row["write_bytes"]),
            }
            identity = (int(row["pid"]), int(row["process_start_ticks"]))
            by_process[identity].append(parsed)
            if row.get("role") == "worker_pool":
                worker_pools[row["timestamp"]] += 1
            phase = phase_at(parsed["timestamp"], windows)
            if phase:
                rss_by_snapshot[(phase, parsed["component"], parsed["timestamp"])] += parsed["rss_bytes"]

    counters = defaultdict(lambda: {"cpu_seconds": 0.0, "read_bytes": 0, "write_bytes": 0})
    ticks = os.sysconf("SC_CLK_TCK")
    for rows in by_process.values():
        rows.sort(key=lambda item: item["timestamp"])
        for previous, current in zip(rows, rows[1:]):
            interval = current["timestamp"] - previous["timestamp"]
            if interval <= 0 or current["component"] != previous["component"]:
                continue
            cpu_delta = max(0, current["cpu_ticks"] - previous["cpu_ticks"]) / ticks
            read_delta = max(0, current["read_bytes"] - previous["read_bytes"])
            write_delta = max(0, current["write_bytes"] - previous["write_bytes"])
            for phase, (begin, end) in windows.items():
                overlap = max(0.0, min(current["timestamp"], end) - max(previous["timestamp"], begin))
                if not overlap:
                    continue
                share = overlap / interval
                metric = counters[(phase, current["component"])]
                metric["cpu_seconds"] += cpu_delta * share
                metric["read_bytes"] += read_delta * share
                metric["write_bytes"] += write_delta * share

    peaks = defaultdict(float)
    for (phase, component, _), rss in rss_by_snapshot.items():
        peaks[(phase, component)] = max(peaks[(phase, component)], rss)

    by_phase = defaultdict(list)
    totals = defaultdict(lambda: {"cpu_seconds": 0.0, "io_bytes": 0, "rss_peak_mib": 0.0})
    for (phase, component), values in counters.items():
        wall = max(0.001, windows[phase][1] - windows[phase][0])
        item = {
            "key": component,
            "name": COMPONENTS.get(component, component),
            "avg_cpu_cores": round_metrics(values["cpu_seconds"] / wall),
            "cpu_seconds": round_metrics(values["cpu_seconds"]),
            "rss_peak_mib": round_metrics(peaks[(phase, component)] / 1024**2),
            "storage_io_mib": round_metrics((values["read_bytes"] + values["write_bytes"]) / 1024**2),
        }
        by_phase[phase].append(item)
        totals[component]["cpu_seconds"] += values["cpu_seconds"]
        totals[component]["io_bytes"] += values["read_bytes"] + values["write_bytes"]
        totals[component]["rss_peak_mib"] = max(totals[component]["rss_peak_mib"], item["rss_peak_mib"])
    for values in by_phase.values():
        values.sort(key=lambda item: (item["key"] != "worker", -item["avg_cpu_cores"]))
    return dict(by_phase), dict(totals), max(worker_pools.values(), default=0)


def worker_metric(phase: dict, name: str) -> float:
    for item in phase.get("components", []):
        if item["key"] == "worker":
            return float(item.get(name, 0))
    return 0.0


def conclusions(summary: dict) -> list[dict]:
    phases = summary["phases"]
    parallelism = summary["effective_parallelism"]
    notes = []
    cpu = phases["cpu"]
    io = phases["io"]
    sleep = phases["sleep"]
    cpu_cores = worker_metric(cpu, "avg_cpu_cores")
    scheduler_core = max(
        (item["avg_cpu_cores"] for phase in phases.values() for item in phase["components"] if item["key"] == "scheduler"),
        default=0,
    )
    notes.append({
        "level": "info",
        "title": f"CPU: {cpu['work_rate_mhash_s']:.2f} млн хешей/с",
        "text": f"Workers использовали в среднем {cpu_cores:.2f} ядра при {parallelism} процессах. Решение об увеличении parallelism принимайте по изменению фиксированной работы/с, а не задач/с.",
    })
    if io["slot_use_percent"] > 70 and worker_metric(io, "avg_cpu_cores") < max(0.3, parallelism * 0.15):
        notes.append({"level": "info", "title": "I/O удерживает слоты, но почти не требует CPU", "text": "Для такой нагрузки больше слотов может повысить throughput, пока не насыщен внешний сервис."})
    if sleep["queue_p95"] > max(0.5, sleep["duration_p50"] * 0.25):
        notes.append({"level": "warn", "title": "Очередь заметна даже без полезной работы", "text": "Ограничение находится в слотах или оркестрации, а не в коде задач."})
    if scheduler_core > 0.7:
        notes.append({"level": "warn", "title": "Планировщик использует большую часть одного ядра", "text": "При росте числа задач сравните очередь и throughput: scheduler может стать ограничением."})
    baseline = summary.get("baseline", {})
    if baseline.get("run_id") and not baseline.get("compatible"):
        notes.append({"level": "warn", "title": "Прогоны нельзя сравнивать напрямую", "text": baseline.get("reason", "Параметры workload различаются.")})
    if baseline.get("compatible") and baseline.get("parallelism", parallelism) != parallelism:
        cpu_change = next((row["change"] for row in summary["comparison"] if row["metric"] == "Фиксированная работа, млн хешей/с"), 0)
        if cpu_change < 15:
            notes.append({"level": "warn", "title": "Дополнительные процессы почти не ускорили CPU", "text": f"Нормированная CPU-производительность изменилась на {cpu_change:+.1f}%. Рост parallelism здесь не окупается вычислительной работой."})
        else:
            notes.append({"level": "good", "title": "Больше процессов ускорило фиксированную CPU-работу", "text": f"Нормированная CPU-производительность выросла на {cpu_change:.1f}%. Проверьте, оправдан ли одновременный рост памяти."})
    if summary.get("config_mismatch"):
        notes.insert(0, {"level": "warn", "title": "Конфигурация запуска не совпала с scheduler", "text": "Проценты слотов рассчитаны по фактически найденным worker-процессам. Такой запуск нельзя считать чистым сравнением."})
    for warning in summary.get("quality_warnings", []):
        notes.append({"level": "warn", "title": "Ограничение измерения", "text": warning})
    return notes


def baseline_comparison(summary: dict, baseline_path: Path | None) -> tuple[list[dict], dict]:
    if not baseline_path:
        return [], {"compatible": False, "reason": "Базовый прогон не задан"}
    baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
    workload_keys = ("tasks_per_type", "cpu_iterations", "sleep_seconds", "io_chunks", "io_chunk_kib", "io_delay_ms")
    differences = [key for key in workload_keys if baseline.get("config", {}).get(key) != summary["config"].get(key)]
    info = {
        "run_id": baseline.get("run_id", baseline_path.parent.name),
        "parallelism": baseline.get("effective_parallelism", baseline.get("config", {}).get("parallelism")),
        "compatible": not differences,
        "reason": "" if not differences else "Различаются параметры workload: " + ", ".join(differences),
    }
    if differences:
        return [], info
    rows = []
    for phase in PHASES:
        current = summary["phases"][phase]
        old = baseline["phases"][phase]
        metrics = [
            ("throughput", "Задач/с", 1),
            ("queue_p95", "Очередь p95, с", -1),
            ("wall_seconds", "Длительность фазы, с", -1),
        ]
        if phase == "cpu":
            metrics.insert(0, ("work_rate_mhash_s", "Фиксированная работа, млн хешей/с", 1))
        for key, label, better in metrics:
            before, after = float(old[key]), float(current[key])
            change = ((after - before) / before * 100) if before else 0
            rows.append({
                "phase": PHASES[phase][1], "metric": label,
                "before": round_metrics(before), "after": round_metrics(after),
                "change": round_metrics(change), "good": change * better >= 0,
            })
    return rows, info


def make_summary(run_dir: Path, db: Path, baseline: Path | None) -> dict:
    config = json.loads((run_dir / "config.json").read_text(encoding="utf-8"))
    dag, tasks = load_run(db, config["run_id"])
    configured_parallelism = int(config["parallelism"])
    phases, windows = phase_metrics(tasks, configured_parallelism)
    components, totals, observed_workers = process_metrics(run_dir / "samples.csv", windows)
    effective_parallelism = observed_workers or configured_parallelism
    if effective_parallelism != configured_parallelism:
        phases, windows = phase_metrics(tasks, effective_parallelism)
    for key in phases:
        phases[key]["components"] = components.get(key, [])
    dag_start, dag_end = timestamp(dag["start_date"]), timestamp(dag["end_date"])
    total_wall = max(0.0, (dag_end or 0) - (dag_start or 0))
    active_wall = sum(phase["wall_seconds"] for phase in phases.values())
    observer_path = run_dir / "observer.json"
    observer = json.loads(observer_path.read_text(encoding="utf-8")) if observer_path.exists() else {}
    cpu_work = int(config.get("cpu_iterations", 0)) * phases["cpu"]["success"]
    phases["cpu"]["work_million_hashes"] = round_metrics(cpu_work / 1_000_000)
    phases["cpu"]["work_rate_mhash_s"] = round_metrics(cpu_work / 1_000_000 / phases["cpu"]["wall_seconds"] if phases["cpu"]["wall_seconds"] else 0)
    phases["cpu"]["primary_rate_label"] = "Фиксированная работа"
    phases["cpu"]["primary_rate_unit"] = "млн хешей/с"
    for key in ("io", "sleep"):
        phases[key]["primary_rate_label"] = "Пропускная способность"
        phases[key]["primary_rate_unit"] = "задач/с"
        phases[key]["work_rate_mhash_s"] = 0
    quality_warnings = []
    interval = float(observer.get("interval_seconds", 0))
    for phase in phases.values():
        if interval and phase["wall_seconds"] < interval * 3:
            quality_warnings.append(f"Фаза «{phase['label']}» короче трёх интервалов наблюдения; CPU процесса может быть занижен.")
    summary = {
        "schema_version": 2,
        "run_id": config["run_id"],
        "label": config.get("label", ""),
        "state": dag["state"],
        "created_at": config["created_at"],
        "config": config,
        "effective_parallelism": effective_parallelism,
        "config_mismatch": effective_parallelism != configured_parallelism,
        "total_wall_seconds": round_metrics(total_wall),
        "transition_seconds": round_metrics(max(0, total_wall - active_wall)),
        "phases": phases,
        "component_totals": totals,
        "observer": observer,
        "quality_warnings": quality_warnings,
    }
    summary["comparison"], summary["baseline"] = baseline_comparison(summary, baseline)
    summary["conclusions"] = conclusions(summary)
    return summary


def dashboard(summary: dict) -> str:
    payload = json.dumps(summary, ensure_ascii=False).replace("</", "<\\/")
    title = html.escape(summary["label"] or summary["run_id"])
    return f"""<!doctype html>
<html lang="ru"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Ресурсы Airflow — {title}</title>
<style>
:root{{--ink:#eaf3fb;--muted:#8fa5b8;--panel:#101d29;--panel2:#152636;--line:#294052;--cyan:#39c6d8;--amber:#ffb454;--green:#55d6a0;--red:#ff7b72;--bg:#07111a}}
*{{box-sizing:border-box}} body{{margin:0;background:var(--bg);color:var(--ink);font:16px/1.5 Inter,Segoe UI,Arial,sans-serif}} .wrap{{max-width:1240px;margin:auto;padding:28px}}
header{{display:flex;justify-content:space-between;gap:24px;align-items:end;border-bottom:1px solid var(--line);padding-bottom:20px}} h1{{font-size:clamp(1.7rem,4vw,3rem);line-height:1.05;margin:4px 0}} h2{{font-size:1.25rem;margin:0 0 14px}} .eyebrow,.meta{{color:var(--muted);font-size:.86rem;letter-spacing:.04em}} .state{{border:1px solid var(--green);color:var(--green);padding:7px 12px;border-radius:999px}}
.grid{{display:grid;grid-template-columns:repeat(4,1fr);gap:14px;margin:22px 0}} .card,.panel{{background:var(--panel);border:1px solid var(--line);border-radius:14px}} .card{{padding:17px}} .value{{font-size:1.8rem;font-weight:700;margin-top:5px}} .label{{color:var(--muted);font-size:.88rem}}
.notes{{display:grid;grid-template-columns:repeat(2,1fr);gap:12px;margin:22px 0}} .note{{padding:16px 18px;border-left:4px solid var(--cyan);background:var(--panel2);border-radius:6px 12px 12px 6px}} .note.warn{{border-color:var(--amber)}} .note.good{{border-color:var(--green)}} .note strong{{display:block;margin-bottom:3px}}
.panel{{padding:20px;margin:16px 0}} .tabs{{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:18px}} button{{background:#0b1721;color:var(--muted);border:1px solid var(--line);border-radius:999px;padding:9px 15px;font:inherit;cursor:pointer}} button.active{{color:#061017;background:var(--cyan);border-color:var(--cyan);font-weight:700}}
.phase-head{{display:flex;justify-content:space-between;gap:20px;align-items:start}} .purpose{{color:var(--muted);max-width:620px}} .phase-grid{{display:grid;grid-template-columns:repeat(5,1fr);gap:10px;margin:18px 0}} .mini{{padding:12px;background:#0a1721;border-radius:9px}} .mini b{{display:block;font-size:1.15rem}}
table{{border-collapse:collapse;width:100%;font-variant-numeric:tabular-nums}} th,td{{text-align:left;padding:11px 9px;border-bottom:1px solid var(--line)}} th{{color:var(--muted);font-size:.8rem;text-transform:uppercase;letter-spacing:.05em}} td.num{{text-align:right}} .bar{{height:7px;background:#213746;border-radius:5px;overflow:hidden;margin-top:5px}} .bar i{{display:block;height:100%;background:var(--cyan)}}
.compare .up{{color:var(--green)}} .compare .down{{color:var(--red)}} details{{margin:20px 0;color:var(--muted)}} details div{{padding:8px 0}} code{{color:#b8e4ec}} footer{{color:var(--muted);font-size:.86rem;margin:28px 0}}
@media(max-width:820px){{.grid,.phase-grid{{grid-template-columns:repeat(2,1fr)}}.notes{{grid-template-columns:1fr}}header{{align-items:start;flex-direction:column}}.table-scroll{{overflow-x:auto}}}}
@media(max-width:480px){{.wrap{{padding:18px}}.grid{{grid-template-columns:1fr 1fr}}.phase-grid{{grid-template-columns:1fr}}}}
</style></head><body><main class="wrap">
<header><div><div class="eyebrow">AIRFLOW · ПРОФИЛЬ РЕСУРСОВ</div><h1>{title}</h1><div class="meta" id="meta"></div></div><div class="state" id="state"></div></header>
<section class="grid" id="kpis"></section><section class="notes" id="notes"></section>
<section class="panel"><div class="tabs" id="tabs"></div><div id="phase"></div></section>
<section class="panel compare" id="compare" hidden><h2>Что изменилось относительно базового прогона</h2><div class="table-scroll"><table><thead><tr><th>Фаза</th><th>Показатель</th><th>Было</th><th>Стало</th><th>Изменение</th></tr></thead><tbody id="compare-body"></tbody></table></div></section>
<details><summary>Как читать показатели</summary>
<div><b>Фиксированная работа CPU</b> — общее число SHA-256 / длительность фазы. В отличие от «задач/с», этот показатель не растёт только потому, что задача получила меньше CPU за фиксированное время.</div>
<div><b>Средние ядра CPU</b> — фактическое процессорное время компонента / длительность фазы. 1,0 означает одно полностью занятое ядро; значение может быть больше 1.</div>
<div><b>Занятость слотов</b> — сумма длительностей задач / (длительность фазы × parallelism). Это полезная работа или ожидание внутри выделенной ёмкости Airflow.</div>
<div><b>Очередь p95</b> — 95% задач начали исполняться не позднее этого времени после постановки в очередь. Рост при том же workload указывает на нехватку слотов или пропускной способности оркестрации.</div>
<div><b>I/O процесса</b> — только физические чтения и записи из <code>/proc</code>. Сетевое ожидание намеренно видно как низкий CPU при высокой занятости слотов, а не как дисковые байты.</div>
</details><footer id="footer"></footer>
</main><script id="data" type="application/json">{payload}</script><script>
const d=JSON.parse(document.getElementById('data').textContent); const f=n=>new Intl.NumberFormat('ru-RU',{{maximumFractionDigits:2}}).format(n||0);
document.getElementById('state').textContent=d.state==='success'?'Прогон завершён':'Состояние: '+d.state;
document.getElementById('meta').textContent=`${{d.run_id}} · parallelism=${{d.effective_parallelism}} · ${{d.config.tasks_per_type}} задач каждого типа`;
const k=[['Общее время',f(d.total_wall_seconds)+' с'],['Переходы Airflow',f(d.transition_seconds)+' с'],['Интервал наблюдения',f(d.observer.interval_seconds)+' с'],['CPU наблюдателя',f(d.observer.cpu_seconds)+' с']];
document.getElementById('kpis').innerHTML=k.map(x=>`<div class="card"><div class="label">${{x[0]}}</div><div class="value">${{x[1]}}</div></div>`).join('');
document.getElementById('notes').innerHTML=d.conclusions.map(n=>`<div class="note ${{n.level}}"><strong>${{n.title}}</strong><span>${{n.text}}</span></div>`).join('');
let active='cpu'; const tabs=document.getElementById('tabs');
function render(){{const p=d.phases[active]; [...tabs.children].forEach(b=>b.classList.toggle('active',b.dataset.key===active));
 const rows=p.components.length?p.components.map(c=>`<tr><td>${{c.name}}</td><td class="num">${{f(c.avg_cpu_cores)}}<div class="bar"><i style="width:${{Math.min(100,c.avg_cpu_cores/Math.max(1,d.effective_parallelism)*100)}}%"></i></div></td><td class="num">${{f(c.rss_peak_mib)}}</td><td class="num">${{f(c.storage_io_mib)}}</td></tr>`).join(''):`<tr><td colspan="4">Нет выборок процессов для этой фазы</td></tr>`;
 const rate=active==='cpu'?p.work_rate_mhash_s:p.throughput;
 document.getElementById('phase').innerHTML=`<div class="phase-head"><div><h2>${{p.title}} · ${{p.label}}</h2><div class="purpose">${{p.purpose}}</div></div><div class="meta">${{p.success}} / ${{p.tasks}} успешно</div></div><div class="phase-grid"><div class="mini"><span class="label">Фаза</span><b>${{f(p.wall_seconds)}} с</b></div><div class="mini"><span class="label">${{p.primary_rate_label}}</span><b>${{f(rate)}} ${{p.primary_rate_unit}}</b></div><div class="mini"><span class="label">Задача p50</span><b>${{f(p.duration_p50)}} с</b></div><div class="mini"><span class="label">Очередь p95</span><b>${{f(p.queue_p95)}} с</b></div><div class="mini"><span class="label">Занятость слотов</span><b>${{f(p.slot_use_percent)}}%</b></div></div><div class="table-scroll"><table><thead><tr><th>Компонент</th><th>Средние ядра CPU</th><th>Пик памяти, МиБ</th><th>I/O процесса, МиБ</th></tr></thead><tbody>${{rows}}</tbody></table></div>`;}}
Object.values(d.phases).forEach(p=>{{const b=document.createElement('button');b.dataset.key=p.key;b.textContent=p.label;b.onclick=()=>{{active=p.key;render()}};tabs.appendChild(b)}});render();
if(d.comparison.length){{document.getElementById('compare').hidden=false;document.getElementById('compare-body').innerHTML=d.comparison.map(r=>`<tr><td>${{r.phase}}</td><td>${{r.metric}}</td><td class="num">${{f(r.before)}}</td><td class="num">${{f(r.after)}}</td><td class="num ${{r.good?'up':'down'}}">${{r.change>0?'+':''}}${{f(r.change)}}%</td></tr>`).join('')}}
const share=d.observer.wall_seconds?100*d.observer.cpu_seconds/d.observer.wall_seconds:0;document.getElementById('footer').textContent=`Наблюдатель читал только /proc и использовал ${{f(share)}}% одного ядра. Метрики задач взяты из metadata DB после исполнения.`;
</script></body></html>"""


def build(run_dir: Path, db: Path, baseline: Path | None) -> None:
    summary = make_summary(run_dir, db, baseline)
    (run_dir / "summary.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")
    (run_dir / "dashboard.html").write_text(dashboard(summary), encoding="utf-8")
    print(f"Дашборд: {run_dir / 'dashboard.html'}")


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    waiter = sub.add_parser("wait")
    waiter.add_argument("--db", required=True, type=Path)
    waiter.add_argument("--run-id", required=True)
    waiter.add_argument("--timeout", type=float, default=7200)
    waiter.add_argument("--poll", type=float, default=5)
    builder = sub.add_parser("build")
    builder.add_argument("--run-dir", required=True, type=Path)
    builder.add_argument("--db", required=True, type=Path)
    builder.add_argument("--baseline", type=Path)
    args = parser.parse_args()
    if args.command == "wait":
        raise SystemExit(wait_for_run(args.db, args.run_id, args.timeout, args.poll))
    build(args.run_dir, args.db, args.baseline)


if __name__ == "__main__":
    main()
