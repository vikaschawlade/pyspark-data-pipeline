# PySpark Glue Data Pipeline

Environment-driven data ingestion framework using:
- AWS Glue (PySpark)
- Apache Iceberg
- Airflow orchestration

## Features
- Dynamic CSV schema
- Ragged row handling
- Header detection
- Idempotent Iceberg MERGE
- Environment-based configuration

## Environments
- dev
- qa
- prod

No DAG or Spark code changes required.