from typing import Sequence

import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from airflow.models.baseoperator import BaseOperator


class S3ToPostgresOperator(BaseOperator):
    template_fields: Sequence[str] = ("bucket", "key", "table", "postgres_conn_id", "aws_conn_id")

    def __init__(
        self,
        bucket,
        key,
        table,
        postgres_conn_id = "postgres_default",
        aws_conn_id = "aws_default",
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.bucket: str = bucket
        self.key: str = key
        self.table: str = table
        self.postgres_conn_id: str = postgres_conn_id
        self.aws_conn_id: str = aws_conn_id

    def execute(self, context):
        # Get the file from S3
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        returned_filename = s3_hook.download_file(self.key, bucket_name=self.bucket, local_path="/tmp")
        # Open the file
        df_file = pd.read_csv(returned_filename, header=0)
        # Create a new connection
        postgres_hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        # This is undocumented, but you can get a SQLAlchemy engine from the hook
        engine = postgres_hook.get_sqlalchemy_engine()
        # This engine can be used with Pandas `to_sql` to write to the database
        df_file.to_sql(self.table, engine, if_exists="replace", index=False)
