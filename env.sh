#!/usr/bin/env bash
# Source this file in the terminal where `airflow standalone` is started.

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  echo "Запустите так: source env.sh [parallelism]" >&2
  exit 1
fi

BENCH_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export BENCH_DIR
export AIRFLOW_HOME="${BENCH_AIRFLOW_HOME:-$HOME/airflow-benchmark}"
export AIRFLOW__CORE__DAGS_FOLDER="$BENCH_DIR/dags"
export AIRFLOW__LOGGING__BASE_LOG_FOLDER="$AIRFLOW_HOME/logs"
export AIRFLOW__CORE__EXECUTOR="LocalExecutor"
export AIRFLOW__CORE__PARALLELISM="${1:-${BENCH_PARALLELISM:-8}}"
export AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG="$AIRFLOW__CORE__PARALLELISM"
export AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION="false"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="sqlite:///$AIRFLOW_HOME/airflow.db"
export AIRFLOW__CORE__LOAD_EXAMPLES="false"

mkdir -p "$AIRFLOW_HOME" "$BENCH_DIR/results"
echo "Стенд настроен: LocalExecutor, parallelism=$AIRFLOW__CORE__PARALLELISM"
echo "AIRFLOW_HOME=$AIRFLOW_HOME"
