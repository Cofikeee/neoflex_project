# AIRFLOW
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.empty import EmptyOperator
# MISC
from datetime import datetime
# PROJECT
from config import AIRFLOW_DEFAULT_CONFIG, DWH_CONNECTION
from utils import insert_data_from_csv, log_finished_task

ORIGINAL_TABLE_NAME = 'dm_f101_round_f'
IMPORTED_TABLE_NAME = 'dm_f101_round_f_v2'
TARGET_SCHEMA = 'dm'

query = f"""
        DROP TABLE IF EXISTS {TARGET_SCHEMA}.{IMPORTED_TABLE_NAME};
        CREATE TABLE {TARGET_SCHEMA}.{IMPORTED_TABLE_NAME} (LIKE {TARGET_SCHEMA}.{ORIGINAL_TABLE_NAME} INCLUDING ALL);
        """



with DAG(dag_id=f'import_{IMPORTED_TABLE_NAME}',
         default_args=AIRFLOW_DEFAULT_CONFIG,
         schedule_interval=None,
         tags=['import', TARGET_SCHEMA]
         ) as dag:

    start_timestamp = datetime.now()

    start = EmptyOperator(task_id='start')

    create_table_task = SQLExecuteQueryOperator(
        task_id=f'create_table_{IMPORTED_TABLE_NAME}',
        conn_id=DWH_CONNECTION,
        sql=query,
    )

    parse_task = PythonOperator(
        task_id=f'parse_csv_{IMPORTED_TABLE_NAME}',
        python_callable=insert_data_from_csv,
        op_kwargs={
            'target_schema': TARGET_SCHEMA,
            'target_table': IMPORTED_TABLE_NAME
        }
    )

    log = PythonOperator(
        task_id=f'log_{IMPORTED_TABLE_NAME}',
        python_callable=log_finished_task,
        op_kwargs={
            'target_schema': TARGET_SCHEMA,
            'target_table': IMPORTED_TABLE_NAME,
            'target_file': f'{IMPORTED_TABLE_NAME}.csv',
            'start_date': start_timestamp
        },
    )

    end = EmptyOperator(task_id='end')

    start >> create_table_task >> parse_task >> log >> end
