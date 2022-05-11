import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator


with DAG(
    dag_id="dag_test_db_conn",
    start_date=datetime.datetime(2021, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    create_sales_table_source = MySqlOperator(
        task_id="dummy_datalake",
        mysql_conn_id='MYSQL_DATALAKE',
        sql="""
            CREATE TABLE IF NOT EXISTS test (
            id int PRIMARY KEY,
            sales_value int);
          """,
    )

    create_sales_table_target = PostgresOperator(
        task_id="dummy_warehouse",
        postgres_conn_id='POSTGRES_DATAWAREHOUSE',
        sql="""
            CREATE TABLE IF NOT EXISTS test (
            id INTEGER PRIMARY KEY,
            sales_value INTEGER NOT NULL,
            creation_date DATE NOT NULL);
          """,
    )
