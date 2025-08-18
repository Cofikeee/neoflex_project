from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
from airflow.hooks.base import BaseHook

from config import DWH_CONNECTION, AIRFLOW_DEFAULT_CONFIG, REPORT_DIR

OUTPUT_PATH = f'{REPORT_DIR}/dm_f101_round_f.csv'


# OUTPUT_PATH = f'dm_f101_round_f.csv'

def export_dm_f101_round_f():

    hook = PostgresHook(postgres_conn_id=DWH_CONNECTION)
    query = 'COPY dm.dm_f101_round_f TO STDOUT WITH CSV HEADER'
    with open(OUTPUT_PATH, 'w', encoding='utf-8') as f:
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.copy_expert(query, f)
        cursor.close()
        conn.close()


with DAG(
    dag_id='export_dm_f101_round_f',
    default_args=AIRFLOW_DEFAULT_CONFIG,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    tags=['export', 'f101', 'dm'],
) as dag:

    export_task = PythonOperator(
        task_id='export_dm_f101_round_f_csv',
        python_callable=export_dm_f101_round_f,
    )
