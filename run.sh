#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
tasks=8
cpu_iterations=5000000
sleep_seconds=5
io_chunks=50
io_chunk_kib=64
io_delay_ms=100
interval=1
label=""
baseline=""

usage() {
  cat <<'EOF'
Запуск: bash run.sh [параметры]
  --tasks N          задач каждого типа (8)
  --cpu-iterations N фиксированный объём SHA-256 на задачу (5000000)
  --sleep-seconds N  длительность sleep (5)
  --io-chunks N      число порций I/O (50)
  --io-chunk-kib N   размер порции, Кб (64)
  --io-delay-ms N    задержка между порциями, мс (100)
  --interval SEC     интервал чтения /proc (1)
  --label TEXT       имя конфигурации
  --baseline FILE    summary.json базового прогона
EOF
}

while (($#)); do
  case "$1" in
    --tasks) tasks="$2"; shift 2 ;;
    --cpu-iterations) cpu_iterations="$2"; shift 2 ;;
    --sleep-seconds) sleep_seconds="$2"; shift 2 ;;
    --duration) echo "--duration удалён: задайте отдельно --cpu-iterations и --sleep-seconds" >&2; exit 2 ;;
    --io-chunks) io_chunks="$2"; shift 2 ;;
    --io-chunk-kib) io_chunk_kib="$2"; shift 2 ;;
    --io-delay-ms) io_delay_ms="$2"; shift 2 ;;
    --interval) interval="$2"; shift 2 ;;
    --label) label="$2"; shift 2 ;;
    --baseline) baseline="$2"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) echo "Неизвестный параметр: $1" >&2; usage >&2; exit 2 ;;
  esac
done

if ! command -v airflow >/dev/null; then
  echo "airflow не найден. Активируйте venv и выполните: source env.sh [parallelism]" >&2
  exit 2
fi
if [[ -z "${AIRFLOW_HOME:-}" || "$(airflow config get-value core dags_folder)" != "$ROOT/dags" ]]; then
  echo "Активна другая конфигурация. Выполните: source env.sh [parallelism]" >&2
  exit 2
fi

executor="$(airflow config get-value core executor)"
parallelism="$(airflow config get-value core parallelism)"
db_url="$(airflow config get-value database sql_alchemy_conn)"
if [[ "${executor,,}" != *localexecutor* || "$db_url" != sqlite:///* ]]; then
  echo "Минимальный локальный стенд ожидает LocalExecutor и SQLite; сейчас: $executor, $db_url" >&2
  exit 2
fi
db_path="${db_url#sqlite:///}"
if ! airflow dags list --local --output plain | awk '{print $1}' | grep -qx resource_benchmark; then
  echo "DAG resource_benchmark ещё не загружен. Перезапустите airflow standalone после source env.sh." >&2
  exit 2
fi
airflow dags unpause resource_benchmark >/dev/null
python "$ROOT/observe.py" --airflow-home "$AIRFLOW_HOME" --check-workers "$parallelism"

run_id="bench_$(date -u +%Y%m%dT%H%M%SZ)"
run_dir="$ROOT/results/$run_id"
mkdir -p "$run_dir"
created_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
python - "$run_dir/config.json" "$run_id" "$created_at" "$label" "$parallelism" "$tasks" "$cpu_iterations" "$sleep_seconds" "$io_chunks" "$io_chunk_kib" "$io_delay_ms" "$interval" <<'PY'
import json, sys
keys = ("run_id", "created_at", "label", "parallelism", "tasks_per_type", "cpu_iterations", "sleep_seconds", "io_chunks", "io_chunk_kib", "io_delay_ms", "observer_interval_seconds")
types = (str, str, str, int, int, int, float, int, int, float, float)
data = {key: cast(value) for key, cast, value in zip(keys, types, sys.argv[2:])}
if data["sleep_seconds"] < data["observer_interval_seconds"] * 3:
    raise SystemExit("--sleep-seconds должен быть не меньше трёх интервалов наблюдения")
io_seconds = data["io_chunks"] * data["io_delay_ms"] / 1000
if io_seconds < data["observer_interval_seconds"] * 3:
    raise SystemExit("I/O-фаза должна длиться не меньше трёх интервалов наблюдения")
open(sys.argv[1], "w", encoding="utf-8").write(json.dumps(data, ensure_ascii=False, indent=2))
PY

python "$ROOT/observe.py" --output "$run_dir/samples.csv" --airflow-home "$AIRFLOW_HOME" --interval "$interval" &
observer_pid=$!
cleanup() {
  if kill -0 "$observer_pid" 2>/dev/null; then
    kill -TERM "$observer_pid"
    wait "$observer_pid" || true
  fi
}
trap cleanup EXIT INT TERM

conf="{\"tasks_per_type\":$tasks,\"cpu_iterations\":$cpu_iterations,\"sleep_seconds\":$sleep_seconds,\"io_chunks\":$io_chunks,\"io_chunk_kib\":$io_chunk_kib,\"io_delay_ms\":$io_delay_ms}"
echo "Прогон $run_id: parallelism=$parallelism, tasks=$tasks"
airflow dags trigger resource_benchmark --run-id "$run_id" --conf "$conf" --output json >/dev/null
status=0
python "$ROOT/report.py" wait --db "$db_path" --run-id "$run_id" --poll 5 || status=$?
cleanup
trap - EXIT INT TERM

baseline_args=()
if [[ -n "$baseline" ]]; then
  baseline_args=(--baseline "$baseline")
fi
python "$ROOT/report.py" build --run-dir "$run_dir" --db "$db_path" "${baseline_args[@]}"
if ((status)); then
  echo "Прогон завершён с ошибкой; дашборд содержит доступные данные." >&2
  exit "$status"
fi
