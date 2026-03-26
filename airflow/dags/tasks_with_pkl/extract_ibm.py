import json
import time
import logging
from datetime import datetime

import requests
from airflow.models import Variable
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


def extract_employees_batch_to_s3(**context):
    """
    Call /current-employee N times (default 20), collect raw payloads,
    save to /tmp as JSON, upload to S3, push S3 key to XCom.
    """

    base_url = Variable.get("IBM_ATTRITION_BASE_URL")
    endpoint = Variable.get("IBM_ATTRITION_ENDPOINT")

    batch_size = int(Variable.get("IBM_ATTRITION_BATCH_SIZE", default_var="20"))
    sleep_seconds = float(
        Variable.get("IBM_ATTRITION_SLEEP_SECONDS", default_var="1.0")
    )

    bucket = Variable.get("S3BucketName")
    s3_prefix = Variable.get("IBM_ATTRITION_S3_PREFIX")

    url = f"{base_url}{endpoint}"

    logging.info(
        f"Starting extract batch: size={batch_size}, sleep={sleep_seconds}s, url={url}"
    )

    items = []
    errors = 0

    for i in range(batch_size):
        try:
            r = requests.get(url, timeout=30)
            r.raise_for_status()

            payload = json.loads(r.text)
            data = json.loads(payload) if isinstance(payload, str) else payload

            items.append(
                {
                    "pulled_at_utc": datetime.utcnow().isoformat(),
                    "source_url": url,
                    "data": data,
                }
            )
            logging.info(f"Pulled {i+1}/{batch_size}")

        except Exception as e:
            errors += 1
            logging.warning(f"Error on pull {i+1}/{batch_size}: {e}", exc_info=True)

        if i < batch_size - 1 and sleep_seconds > 0:
            time.sleep(sleep_seconds)

    filename = f"{datetime.now().strftime('%Y%m%d-%H%M%S')}_ibm_employees_batch.json"
    local_path = f"/tmp/{filename}"

    artifact = {
        "meta": {
            "batch_size_requested": batch_size,
            "records_collected": len(items),
            "errors": errors,
            "base_url": base_url,
            "endpoint": endpoint,
            "created_at_utc": datetime.utcnow().isoformat(),
        },
        "items": items,
    }

    with open(local_path, "w") as f:
        json.dump(artifact, f)

    s3_key = f"{s3_prefix}/{filename}"
    s3_hook = S3Hook(aws_conn_id="aws_default")
    s3_hook.load_file(
        filename=local_path,
        key=s3_key,
        bucket_name=bucket,
        replace=True,
    )

    ti = context["task_instance"]
    ti.xcom_push(key="ibm_raw_s3_key", value=s3_key)
    ti.xcom_push(key="ibm_records_collected", value=len(items))

    logging.info(
        f"Saved batch to s3://{bucket}/{s3_key} (records={len(items)}, errors={errors})"
    )
