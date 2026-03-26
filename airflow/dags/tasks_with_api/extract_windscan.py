import json
import time
import logging
from datetime import datetime
import pandas as pd

import requests
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def extract_dataset_batch_to_s3(**context):
    """
    Call /current-employee N times (default 20), collect raw payloads,
    save to /tmp as JSON, upload to S3, push S3 key to XCom.
    """

    bucket = Variable.get("S3BucketName")
    s3_prefix = Variable.get("DATA_S3_PREFIX")

    s3_key = f"{s3_prefix}/dataset.csv"
    s3_hook = S3Hook(aws_conn_id="aws_default")
    local_path = s3_hook.download_file(
        key=s3_key,
        bucket_name=bucket,
        local_path="/tmp",
    )
    df = pd.read_csv(local_path)
    df_sample = df.sample(n=1, random_state=42)
    local_sample_path = "/tmp/sample.csv"
    df_sample.to_csv(local_sample_path, index=False)

    # Upload sample to S3
    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    s3_key_sample = f"{s3_prefix}/sample_{ts}.csv"
    s3_hook.load_file(
        filename=local_sample_path,
        key=s3_key_sample,
        bucket_name=bucket,
        replace=True,
    )

    ti = context["task_instance"]
    ti.xcom_push(key="data_raw_turbine_key", value=s3_key_sample)
    ti.xcom_push(key="data_recorded_collected", value=len(df_sample))
