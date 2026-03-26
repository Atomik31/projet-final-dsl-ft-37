from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.providers.postgres.operators.postgres import PostgresOperator


from operators.s3_to_postgres import S3ToPostgresOperator

from tasks_with_pkl.extract_ibm import extract_employees_batch_to_s3
from tasks_with_pkl.load_model_ibm import load_model_task
from tasks_with_pkl.transform_predict_ibm import predict_with_model_ibm


default_args = {
    "owner": "Julien",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="etl_attrition_dag",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["demo", "etl", "ibm", "attrition"],
) as dag:

    # =========================
    # 1) Extract
    # =========================
    extract_task = PythonOperator(
        task_id="extract_raw_employees_batch",
        python_callable=extract_employees_batch_to_s3,
        provide_context=True,
    )

    # =========================
    # 2) Load model (parallel)
    # =========================
    load_model = PythonOperator(
        task_id="load_model",
        python_callable=load_model_task,
        provide_context=True,
    )

    # =========================
    # 3) Transform / Predict (wait for extract + model)
    # =========================
    transform = PythonOperator(
        task_id="predict_with_model_ibm",
        python_callable=predict_with_model_ibm,
        provide_context=True,
    )

    # extract + load_model in parallel, then transform
    [extract_task, load_model] >> transform

    # =========================
    # 4) LOAD (Postgres) at the end (TaskGroup)
    # =========================
    with TaskGroup(group_id="load_branch") as load_branch:

        create_predictions_table = PostgresOperator(
            task_id="create_predictions_table",
            sql="""
            CREATE TABLE IF NOT EXISTS ibm_attrition_predictions (
                id SERIAL PRIMARY KEY,
                Age TEXT,
                BusinessTravel TEXT,
                DailyRate TEXT,
                Department TEXT,
                DistanceFromHome TEXT,
                Education TEXT,
                EducationField TEXT,
                EmployeeCount TEXT,
                EmployeeNumber TEXT,
                EnvironmentSatisfaction TEXT,
                Gender TEXT,
                HourlyRate TEXT,
                JobInvolvement TEXT,
                JobLevel TEXT,
                JobRole TEXT,
                JobSatisfaction TEXT,
                MaritalStatus TEXT,
                MonthlyIncome TEXT,
                MonthlyRate TEXT,
                NumCompaniesWorked TEXT,
                Over18 TEXT,
                OverTime TEXT,
                PercentSalaryHike TEXT,
                PerformanceRating TEXT,
                RelationshipSatisfaction TEXT,
                StandardHours TEXT,
                StockOptionLevel TEXT,
                TotalWorkingYears TEXT,
                TrainingTimesLastYear TEXT,
                WorkLifeBalance TEXT,
                YearsAtCompany TEXT,
                YearsInCurrentRole TEXT,
                YearsSinceLastPromotion TEXT,
                YearsWithCurrManager TEXT,
                prediction TEXT,
                proba_0 TEXT,
                proba_1 TEXT
            );
            """,
            postgres_conn_id="postgres_default",
        )

        transfer_predictions_to_postgres = S3ToPostgresOperator(
            task_id="transfer_predictions_to_postgres",
            table="ibm_attrition_predictions",
            bucket="{{ var.value.S3BucketName }}",
            key="{{ task_instance.xcom_pull(task_ids='predict_with_model_ibm', key='ibm_predictions_s3_key') }}",
            postgres_conn_id="postgres_default",
            aws_conn_id="aws_default",
        )

        create_predictions_table >> transfer_predictions_to_postgres

    # transform then load branch
    transform >> load_branch
