# Стенд ресурсов Airflow

Стенд отвечает на практический вопрос: **что ограничивает Airflow при CPU,
I/O и ожидающих задачах и даст ли пользу изменение `parallelism`?**

## Что есть

```text
airflow-benchmark/
├── dags/resource_benchmark.py  # единственный DAG и три нагрузки
├── observe.py                  # наблюдатель /proc
├── report.py                   # расчёты и HTML
├── env.sh                      # конфигурация Airflow
└── run.sh                      # один запуск от trigger до dashboard.html
```

## Запуск в WSL

Остановите другой локальный `airflow standalone`, если он занимает порт 8080.
В первом терминале WSL:

```bash
cd /mnt/c/Users/Artem/Desktop/Programs/dag-io-cpu/airflow-benchmark
source ~/venvs/airflow-stress/bin/activate
source env.sh 8
airflow standalone
```

Число `8` — исследуемый `parallelism`. После первого старта дождитесь появления
`resource_benchmark` в интерфейсе Airflow.

Во втором терминале WSL:

```bash
cd /mnt/c/Users/Artem/Desktop/Programs/dag-io-cpu/airflow-benchmark
source ~/venvs/airflow-stress/bin/activate
source env.sh 8
bash run.sh --tasks 16 --cpu-iterations 5000000 --sleep-seconds 5 --label "parallelism 8"
```

Результат появится в `results/<run-id>/dashboard.html`. 

Параметры нагрузки:

```bash
bash run.sh --help
bash run.sh \
  --tasks 16 \
  --cpu-iterations 5000000 \
  --sleep-seconds 5 \
  --io-chunks 80 \
  --io-chunk-kib 64 \
  --io-delay-ms 100 \
  --interval 1 \
  --label "p8 / 16 задач"
```

- `--tasks` — задач каждого типа.
- `--cpu-iterations` — одинаковое число SHA-256 для каждой CPU-задачи. Это
  фиксированный объём работы, поэтому сравнивается показатель млн хешей/с.
- `--sleep-seconds` — длительность одной контрольной sleep-задачи.
- I/O длится примерно `io-chunks × io-delay-ms`; данные идут через локальный
  socket и моделируют медленный внешний сервис без влияния дискового cache.
- `--interval` — частота чтения `/proc`; 1 секунда — разумный компромисс.

Для CPU главный показатель — млн выполненных хешей/с; для I/O и sleep — задач/с. Очередь,
CPU компонентов и память объясняют, **почему** результат изменился:

- CPU: если рост `parallelism` почти не меняет млн хешей/с, предел находится в
  доступном CPU, даже если число worker-процессов больше числа занятых ядер.
- I/O: слоты заняты, CPU свободен — больше параллелизма может скрыть ожидание.
- Sleep: полезной работы нет — видна чистая цена оркестрации и лимита слотов.
- Рост очереди при неизменном workload означает, что конфигурация перестала
  успевать принимать работу.

