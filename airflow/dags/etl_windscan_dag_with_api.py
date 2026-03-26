from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator

from operators.s3_to_postgres import S3ToPostgresOperator

from tasks_with_api.extract_windscan import extract_dataset_batch_to_s3
from tasks_with_api.transform_predict_windscan import predict_with_model_turbine


default_args = {
    "owner": "Julien",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="etl_WINDSCAN_dag",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["demo", "etl", "windscan"],
) as dag:

    # =========================
    # 1) Extract
    # =========================
    extract_task = PythonOperator(
        task_id="extract_raw_turbine_batch",
        python_callable=extract_dataset_batch_to_s3,
        provide_context=True,
    )

    # =========================
    # 2) Transform / Predict via API
    # =========================
    transform = PythonOperator(
        task_id="predict_with_model_turbine",
        python_callable=predict_with_model_turbine,
        provide_context=True,
    )

    # extract then transform
    extract_task >> transform

    # =========================
    # 3) LOAD (Postgres)
    # =========================
    with TaskGroup(group_id="load_branch") as load_branch:

        create_predictions_table = PostgresOperator(
            task_id="create_predictions_table",
            sql="""
            CREATE TABLE IF NOT EXISTS wind_turbine_predictions (
                id SERIAL PRIMARY KEY,
                Hour_Index NUMERIC,
                Turbine_ID INTEGER,
                Rotor_Speed_RPM NUMERIC,
                Wind_Speed_mps NUMERIC,
                Power_Output_kW NUMERIC,
                Gearbox_Oil_Temp_C NUMERIC,
                Generator_Bearing_Temp_C NUMERIC,
                Vibration_Level_mmps NUMERIC,
                Ambient_Temp_C NUMERIC,
                Humidity_pct NUMERIC,
                Maintenance_Label INTEGER,
                prediction INTEGER,
                target_actual INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """,
            postgres_conn_id="postgres_default",
        )

        transfer_predictions_to_postgres = S3ToPostgresOperator(
            task_id="transfer_predictions_to_postgres",
            table="wind_turbine_predictions",
            bucket="{{ var.value.S3BucketName }}",
            key="{{ task_instance.xcom_pull(task_ids='predict_with_model_turbine', key='turbine_predictions_s3_key') }}",
            postgres_conn_id="postgres_default",
            aws_conn_id="aws_default",
        )

        create_predictions_table >> transfer_predictions_to_postgres

    transform >> load_branch
