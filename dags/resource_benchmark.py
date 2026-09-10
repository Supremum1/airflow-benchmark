"""Three pure workloads. Metrics are intentionally collected outside this DAG."""

from __future__ import annotations

import hashlib
import socket
import threading
import time

from airflow.sdk import DAG, Param, task
from pendulum import datetime


def run_cpu(iterations: int) -> None:
    """Execute a fixed amount of useful work, independent of wall-clock time."""
    payload = b"airflow-resource-benchmark"
    for _ in range(iterations):
        payload = hashlib.sha256(payload).digest()


def run_waiting_io(chunks: int, chunk_kib: int, delay_ms: float) -> None:
    """A reproducible slow socket peer: real blocking I/O without disk-cache noise."""
    reader, writer = socket.socketpair()
    payload = b"x" * (chunk_kib * 1024)

    def producer() -> None:
        try:
            for _ in range(chunks):
                time.sleep(delay_ms / 1000)
                writer.sendall(payload)
        finally:
            writer.close()

    thread = threading.Thread(target=producer, daemon=True)
    thread.start()
    try:
        while reader.recv(len(payload)):
            pass
    finally:
        reader.close()
        thread.join()


with DAG(
    dag_id="resource_benchmark",
    start_date=datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    params={
        "tasks_per_type": Param(8, type="integer", minimum=1, maximum=256),
        "cpu_iterations": Param(5_000_000, type="integer", minimum=100_000, maximum=1_000_000_000),
        "sleep_seconds": Param(5.0, type="number", minimum=0.2, maximum=600),
        "io_chunks": Param(50, type="integer", minimum=1, maximum=10000),
        "io_chunk_kib": Param(64, type="integer", minimum=1, maximum=4096),
        "io_delay_ms": Param(100.0, type="number", minimum=0.1, maximum=60000),
    },
    tags=["benchmark", "cpu", "io", "sleep"],
    doc_md="""
    Чистый тест ресурсов: CPU, ожидание медленного сокета и sleep выполняются
    последовательно тремя фазами. Внутри задач нет профилировщика и метрик.
    """,
) as dag:

    @task
    def indexes(**context) -> list[int]:
        return list(range(int(context["params"]["tasks_per_type"])))

    @task(task_id="cpu")
    def cpu_task(index: int, **context) -> None:
        del index
        run_cpu(int(context["params"]["cpu_iterations"]))

    @task(task_id="io")
    def io_task(index: int, **context) -> None:
        del index
        params = context["params"]
        run_waiting_io(
            int(params["io_chunks"]),
            int(params["io_chunk_kib"]),
            float(params["io_delay_ms"]),
        )

    @task(task_id="sleep")
    def sleep_task(index: int, **context) -> None:
        del index
        time.sleep(float(context["params"]["sleep_seconds"]))

    task_indexes = indexes()
    cpu_phase = cpu_task.expand(index=task_indexes)
    io_phase = io_task.expand(index=task_indexes)
    sleep_phase = sleep_task.expand(index=task_indexes)
    cpu_phase >> io_phase >> sleep_phase
