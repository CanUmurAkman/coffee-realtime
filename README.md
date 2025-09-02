Real-time Coffee Orders Analytics (Spark + Kafka + Airflow)

This is a small, end-to-end data engineering project that streams simulated “coffee order” events into Apache Kafka, processes them in near real-time with Apache Spark (PySpark), and produces daily Key Performance Indicators with a batch Spark job. You can orchestrate the batch in Apache Airflow (manual trigger only), and export a neat, queryable snapshot (DuckDB file and Comma-Separated Value files) to showcase on GitHub.

What you will learn

Apache Kafka basics: topic (a named stream), partition (a shard for parallelism), producer (sender), consumer (reader), offset (position within a partition).

Apache Spark Structured Streaming: reading Kafka events, parsing JavaScript Object Notation (JSON), converting to typed columns, windowed aggregations, watermarks (how long to wait for late data), and checkpoints (state for recovery).

Batch Spark: daily aggregates written as Parquet or Comma-Separated Value files.

Apache Airflow: a Directed Acyclic Graph (a workflow with no cycles) that triggers the daily batch manually.

Data lake layers:

bronze = raw validated events (Parquet)

silver = curated aggregates (per-minute revenue)

gold = final daily Key Performance Indicators (CSV)

Architecture (high-level)
[Order Simulator (Python Producer)]
          │  JSON events
          ▼
      [Kafka topic: orders]
          │
   [Spark Structured Streaming Job]
   ├─ writes raw →  data/bronze/orders/… (Parquet + checkpoints)
   └─ per-minute revenue → data/silver/minute_metrics/… (Parquet)
          │
[Manual Batch (Spark)]
   └─ daily KPIs → data/gold/daily_key_metrics_YYYY-MM-DD/ (CSV)

[ETL export]
   └─ artifacts/coffee.duckdb + CSVs for easy sharing

Repository layout
coffee-realtime/
├─ README.md
├─ .gitignore
├─ docker-compose.yml                    # Kafka (single broker)
├─ requirements.txt                      # Project venv (.venv)
├─ generator/
│  └─ generate_orders.py                 # Kafka producer (order simulator)
├─ spark/
│  ├─ stream_orders.py                   # Structured Streaming: Kafka → bronze + silver
│  ├─ batch_daily_kpis.py                # Batch daily KPIs (gold)
│  └─ batch_minute_metrics.py            # (optional) Batch per-minute metrics from bronze
├─ airflow/
│  └─ dags/
│     └─ daily_kpis.py                   # Airflow DAG (manual-only; uses .venv Python)
├─ etl/
│  └─ to_duckdb.py                       # Build artifacts/coffee.duckdb and CSVs
├─ data/                                 # Parquet outputs (git-ignored)
│  ├─ bronze/
│  ├─ silver/
│  └─ gold/
├─ checkpoints/                          # Spark checkpoints (git-ignored)
└─ artifacts/                            # Shareable outputs (DuckDB + CSVs)

Prerequisites
Docker Desktop (to run Kafka).
Java 17 for Spark.
    macOS example: brew install openjdk@17 and export JAVA_HOME=$(/usr/libexec/java_home -v 17)

Python
    Project venv (for producer, Spark, and export): Python 3.13 is fine.
    Airflow venv: Python 3.12 recommended (Airflow 2.9.x is smoothest there).

Installation
1) Project virtual environment (.venv) for producer, Spark, DuckDB
cd /Users/macbookpro/Desktop/coffee-realtime
python -m venv .venv
source .venv/bin/activate

# requirements used in this repo:
# - kafka-python (new enough for Python 3.13)
# - pyspark 3.5.1
# - dotenv (optional)
# - duckdb (new enough to ship wheels for 3.13)
# - numpy and pandas (compatible with 3.13)
cat > requirements.txt <<'REQ'
kafka-python>=2.1.2,<3
pyspark==3.5.1
python-dotenv==1.0.1
duckdb>=1.3.2
numpy>=2.2,<3
pandas>=2.2.3,<3
REQ

pip install --upgrade -r requirements.txt

# macOS only: ensure Java 17 is visible to Spark
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
java -version

2) Start Kafka (single broker in KRaft mode)
docker-compose.yml contains a minimal single-node setup with the required controller listener variables:
docker compose up -d
docker ps --filter name=kafka
docker logs kafka --tail=50

3) Airflow (manual-only runs)
Use a separate environment for Airflow:
brew install python@3.12
python3.12 -m venv venv_airflow
source venv_airflow/bin/activate
export AIRFLOW_HOME="$(pwd)/airflow"

AIRFLOW_VERSION=2.9.2
PYTHON_VERSION=$(python -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINT_URL}"

airflow db init
airflow users create \
  --username admin --firstname You --lastname User \
  --role Admin --email you@example.com --password admin

# DAG uses this variable to locate your repo
airflow variables set repo_root "$(pwd)"

Airflow parses many example Directed Acyclic Graphs by default. You can hide them by setting export AIRFLOW__CORE__LOAD_EXAMPLES=False before starting webserver and scheduler.

How to run (today-only dataset)

Let us generate and process data for 2025-09-02. You do not need your machine on at night.
Step A — Start streaming pipeline

Terminal 1 — Kafka
docker compose up -d

Terminal 2 — Spark stream (consumer)
source .venv/bin/activate
# stream_orders.py includes the Kafka connector via spark.jars.packages
python spark/stream_orders.py

Terminal 3 — Producer (order simulator)
source .venv/bin/activate
# Default rate ~1 event/second; press Ctrl+C when you have enough data
python generator/generate_orders.py

Let it run a few minutes. Then stop the producer with Ctrl+C.

Where is data stored?

    Kafka keeps messages inside the container (ephemeral).

    Your durable copy is written by the stream to data/bronze/orders/ (Parquet).

    Per-minute “silver” files are also written, but only after windows “close” (see the “Watermark note” below).

Step B — Compute daily KPI for today (gold)
source .venv/bin/activate
# Pass the date explicitly. We set the Spark session time zone to UTC in the script.
python spark/batch_daily_kpis.py 2025-09-02

This creates data/gold/daily_key_metrics_2025-09-02/part-*.csv containing a single row:
date,orders_count,revenue_usd
2025-09-02, ..., ...

Step C — Build a shareable snapshot (DuckDB + CSVs)
source .venv/bin/activate
python etl/to_duckdb.py

This writes:

    artifacts/coffee.duckdb — a compact database file with three tables:
        orders (raw), minute_metrics (per-minute for 2025-09-02), daily_kpis (gold)

    artifacts/daily_kpis.csv

    artifacts/minute_metrics_2025-09-02.csv

Watermark note: the streaming job uses a 5-minute watermark and “append” mode. If you stop everything immediately after the producer, the stream may not flush minute windows to disk yet. The export script therefore computes per-minute metrics from bronze for the selected day, so your minute_metrics_2025-09-02.csv is filled even if the stream has not flushed “silver.”
