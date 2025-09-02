from datetime import datetime
import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

IST = pendulum.timezone("Europe/Istanbul")
default_args = {"owner": "you", "retries": 0}

with DAG(
    dag_id="daily_kpis",
    description="Manual run: Spark batch to produce daily key metrics",
    start_date=pendulum.datetime(2025, 8, 1, tz=IST),
    schedule=None,  # manual only
    catchup=False,
    default_args=default_args,
) as dag:
    run_batch = BashOperator(
        task_id="spark_batch_daily_kpis",
        bash_command=r"""
set -euo pipefail
cd "{{ var.value.repo_root }}"
# Ensure Java 17 for PySpark
export JAVA_HOME="$(
  /usr/libexec/java_home -v 17 2>/dev/null || true
)"
echo "Using JAVA_HOME=${JAVA_HOME:-<unset>}"
# Run with your project venv's Python (not Airflow's venv)
"./.venv/bin/python" "spark/batch_daily_kpis.py" "{{ ds }}"
""",
        env={"DATA_ROOT": "{{ var.value.repo_root }}"},
    )
