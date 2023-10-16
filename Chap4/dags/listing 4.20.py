"""
This piece is write to PostgreSQL from the SQL file we created.
"""

from airflow.providers.postgres.operators.postgres import PostgresOperator

dag = DAG(..., template_searchpath = "/tmp")

write_to_postgres = PostgresOperator(
    task_id             = "write_to_postgres",
    postgres_conn_id    = "my_postgres",
    sql                 = "postgres_query.sql",
    dag = dag,
)