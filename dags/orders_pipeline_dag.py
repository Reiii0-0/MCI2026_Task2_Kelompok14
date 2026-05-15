"""
Apache Airflow DAG: Orders Pipeline (Daily)
============================================
Pipeline untuk mengambil data orders dari REST API,
memproses dengan Apache Spark, dan meload ke ClickHouse.

Dataset: http://96.9.212.102:8000/orders
Schedule: Setiap hari sekali (@daily)

Mode data:
  - order_items & daily_summary : APPEND per tanggal (time-series)
  - top_products, department_summary, hourly_activity : TRUNCATE-INSERT
    (selalu menampilkan kondisi snapshot terkini)

Arsitektur:
  API Orders → [Fetch Python] → Data Lake (.parquet)
             → [Spark Processing] → ClickHouse (dengan ingested_date)
             → Metabase Dashboard (tren harian bisa dianalisis)

Terinspirasi dari: github.com/yogs14/wikipedia-realtime-pipeline
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "mci_team",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 5, 14),
}

with DAG(
    dag_id="orders_pipeline",
    default_args=default_args,
    description="Realtime ETL: API Orders → Data Lake → Spark → ClickHouse",
    schedule_interval="*/5 * * * *",   # Jalankan setiap 5 menit
    catchup=False,
    tags=["mci", "etl", "clickhouse", "orders", "realtime"],
) as dag:

    # Task 1: Fetch data dari API → simpan .parquet ke Data Lake
    fetch_orders = BashOperator(
        task_id="fetch_orders",
        bash_command="python /opt/airflow/dags/scripts/fetch_orders.py",
    )

    # Task 2: Spark agregasi + load ke ClickHouse (dengan tanggal ingest)
    process_with_spark = BashOperator(
        task_id="process_orders_spark",
        bash_command="python /opt/airflow/dags/scripts/process_orders_spark.py",
    )

    # Dependency: fetch dulu, baru proses
    fetch_orders >> process_with_spark
