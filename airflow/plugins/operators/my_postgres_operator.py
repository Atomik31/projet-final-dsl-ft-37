from airflow.models.baseoperator import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook


class MyPostgresOperator(BaseOperator):
    def __init__(self, sql, postgres_conn_id="postgres_default", **kwargs) -> None:
        super().__init__(**kwargs)
        self.postgres_conn_id: str = postgres_conn_id
        self.sql: str = sql

    def execute(self, context):
        conn = PostgresHook(postgres_conn_id=self.postgres_conn_id).get_conn()
        cur = conn.cursor()
        cur.execute(self.sql)
        conn.commit()
        cur.close()
        conn.close()
