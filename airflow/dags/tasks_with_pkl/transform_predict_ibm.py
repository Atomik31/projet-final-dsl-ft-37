import os
import json
import pickle
import pandas as pd
from datetime import datetime

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable


def predict_with_model_ibm(**context):
    ti = context["task_instance"]

    pickle_path = ti.xcom_pull(task_ids="load_model", key="model_pickle_path")
    if not pickle_path or not os.path.exists(pickle_path):
        raise FileNotFoundError(f"Pickle model path not found: {pickle_path}")

    print(f"[INFO] Using pickled model at {pickle_path}")
    with open(pickle_path, "rb") as f:
        model = pickle.load(f)

    raw_s3_key = ti.xcom_pull(
        task_ids="extract_raw_employees_batch", key="ibm_raw_s3_key"
    )
    if not raw_s3_key:
        raise ValueError("Missing XCom raw_s3_key (key='ibm_raw_s3_key').")

    bucket = Variable.get("S3BucketName")
    s3_hook = S3Hook(aws_conn_id="aws_default")

    local_raw_path = s3_hook.download_file(
        key=raw_s3_key,
        bucket_name=bucket,
        local_path="/tmp",
    )
    print(
        f"[INFO] Downloaded raw batch: s3://{bucket}/{raw_s3_key} -> {local_raw_path}"
    )

    with open(local_raw_path, "r") as f:
        raw_batch = json.load(f)

    items = raw_batch.get("items", [])
    if not items:
        raise ValueError("Raw batch contains no items.")

    rows = []
    for it in items:
        payload = it.get("data")
        if isinstance(payload, str):
            payload = json.loads(payload)

        cols = payload.get("columns")
        data = payload.get("data")
        if not cols or not data:
            continue

        values = data[0] if isinstance(data, list) and len(data) > 0 else []
        rows.append({c: v for c, v in zip(cols, values)})

    if not rows:
        raise ValueError("No usable rows rebuilt from raw batch.")

    features = pd.DataFrame(rows)
    print(f"[INFO] Features dataframe built: shape={features.shape}")

    preds = model.predict(features)

    probs = None
    try:
        probs = model.predict_proba(features)
    except Exception:
        probs = None

    result = features.copy()
    result["prediction"] = preds

    if probs is not None:
        try:
            result["proba_0"] = [p[0] for p in probs]
            result["proba_1"] = [p[1] for p in probs]
        except Exception:
            pass

    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    predictions_filename = f"{ts}_ibm_predictions.csv"
    local_results = f"/tmp/{predictions_filename}"
    result.to_csv(local_results, index=False)

    s3_prefix_predictions = Variable.get(
        "IBM_ATTRITION_S3_PRED_PREFIX",
    )
    s3_key_predictions = f"{s3_prefix_predictions}/{predictions_filename}"

    s3_hook.load_file(
        filename=local_results,
        key=s3_key_predictions,
        bucket_name=bucket,
        replace=True,
    )

    ti.xcom_push(key="ibm_predictions_s3_key", value=s3_key_predictions)
    ti.xcom_push(key="ibm_predictions_count", value=len(result))

    print(
        f"[INFO] Predictions saved: s3://{bucket}/{s3_key_predictions} (rows={len(result)})"
    )
