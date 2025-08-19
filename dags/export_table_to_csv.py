# AIRFLOW
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
# MISC
from datetime import datetime
# PROJECT
from config import DWH_CONNECTION, AIRFLOW_DEFAULT_CONFIG, REPORT_DIR
from utils import log_finished_task

EXPORTED_TABLE_NAME = 'dm_f101_round_f'
TARGET_SCHEMA = 'dm'


def export_dm_f101_round_f():

    hook = PostgresHook(postgres_conn_id=DWH_CONNECTION)
    query = f'COPY {TARGET_SCHEMA}.{EXPORTED_TABLE_NAME} TO STDOUT WITH CSV HEADER'
    with open(f'{REPORT_DIR}/{EXPORTED_TABLE_NAME}.csv', 'w', encoding='utf-8') as f:
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.copy_expert(query, f)
        cursor.close()
        conn.close()


with DAG(
    dag_id=f'export_{EXPORTED_TABLE_NAME}',
    default_args=AIRFLOW_DEFAULT_CONFIG,
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    tags=['export', TARGET_SCHEMA],
) as dag:

    start_timestamp = datetime.now()

    start = EmptyOperator(task_id='start')

    export_task = PythonOperator(
        task_id=f'export_csv_{EXPORTED_TABLE_NAME}',
        python_callable=export_dm_f101_round_f,
    )

    log = PythonOperator(
        task_id=f'log_{EXPORTED_TABLE_NAME}',
        python_callable=log_finished_task,
        op_kwargs={
            'target_schema': TARGET_SCHEMA,
            'target_table': EXPORTED_TABLE_NAME,
            'target_file': f'{EXPORTED_TABLE_NAME}.csv',
            'start_date': start_timestamp,
            'operation_type': 'EXPORT',
        },
    )

    end = EmptyOperator(task_id='end')

    start >> export_task >> log >> end
