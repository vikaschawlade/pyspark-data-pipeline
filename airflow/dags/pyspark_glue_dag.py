from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
import yaml
import os

# Environment selection
ENV = os.getenv("ENV", "dev")

CONFIG_FILE = f"/opt/airflow/config/{ENV}.yaml"

with open(CONFIG_FILE) as f:
    cfg = yaml.safe_load(f)

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="pyspark_csv_to_iceberg_glue",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=["glue", "pyspark", ENV],
) as dag:

    run_glue_job = GlueJobOperator(
        task_id="run_glue_job",
        job_name="csv_to_iceberg_job",
        script_args={
            "--csv_path": cfg["csv_path"],
            "--bad_record_path": cfg["bad_record_path"],
            "--iceberg_catalog": cfg["iceberg"]["catalog"],
            "--iceberg_db": cfg["iceberg"]["database"],
            "--iceberg_table": cfg["iceberg"]["table"],
            "--warehouse": cfg["iceberg"]["warehouse"],
        },
        region_name="us-east-1",
    )