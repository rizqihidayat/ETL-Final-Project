import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python_operator import PythonOperator

from script import main


with DAG(
    dag_id="dag_init_raw",
    start_date=datetime.datetime(2021, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    insert_to_lake = PythonOperator(
        task_id="insert_to_lake",
        python_callable=main.insert_raw_to_mysql
    )

    create_warehouse_table = PostgresOperator(
        task_id="create_warehouse_table",
        postgres_conn_id='POSTGRES_DATAWAREHOUSE',
        sql="./sql/create_table.sql",
    )

    # populate table in data warehouse
    insert_raw_to_warehouse = PythonOperator(
        task_id="insert_to_warehouse",
        python_callable=main.insert_raw_to_warehouse
    )

    insert_to_lake >> create_warehouse_table >> insert_raw_to_warehouse