# AIRLFOW
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.task_group import TaskGroup
from airflow.configuration import conf

# MISC
import pandas as pd
import chardet
from datetime import datetime

# CONFIG
from config import SOURCE_DIR, QUERY_DIR, AIRFLOW_DEFAULT_CONFIG, DWH_CONNECTION
from utils import insert_data_from_csv
# SOURCES LIST
# dags/source/ft_balance_f.csv
# dags/source/ft_posting_f.csv
# dags/source/md_account_d.csv
# dags/source/md_currency_d.csv
# dags/source/md_exchange_rate_d.csv
# dags/source/md_ledger_account_s.csv

STG_DS_TABLE_LIST = [
    'ft_balance_f',
    'ft_posting_f',
    'md_account_d',
    'md_currency_d',
    'md_exchange_rate_d',
    'md_ledger_account_s'
]

conf.set('core', 'template_searchpath', QUERY_DIR)


def log_finished_task(target_table: str, source_name: str, start_date: datetime) -> None:
    """
    > Logs metadata about a finished task group run into `logs.ds_changelog`.
    > Added a small 5-second delay to `end_date` as required in task.
    """
    sql_hook = PostgresHook(DWH_CONNECTION)
    conn = sql_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(f"SELECT COUNT(*) FROM ds.{target_table}")
    rows_inserted = cursor.fetchone()[0]

    query = """
        INSERT INTO logs.ds_changelog
            (target_table, source, rows_inserted, start_date, end_date)
        VALUES (%s, %s, %s, %s, NOW() + INTERVAL '5 SECONDS')
    """
    cursor.execute(query, (target_table, source_name, rows_inserted, start_date))
    conn.commit()
    cursor.close()
    conn.close()


def make_table_group(group_id: str, target_table: str, sql_file: str) -> TaskGroup:
    """
    > Creates a TaskGroup for a given stg-to-ds pipeline.
    > Each group contains 3 tasks:
      - PARSE: loads raw CSV into 'stg' schema,
      - UPSERT: executes the SQL upsert into 'ds' schema,
      - LOG: records the results into `logs.ds_changelog`.
    > Returns the constructed TaskGroup.
    """
    start_timestamp = datetime.now()

    with TaskGroup(group_id) as tg:
        parse = PythonOperator(
            task_id=f'parse_{target_table}',
            python_callable=insert_data_from_csv,
            op_kwargs={'target_table': target_table,
                       'target_schema': 'stg'}
        )

        upsert = SQLExecuteQueryOperator(
            task_id=f'upsert_{target_table}',
            conn_id=DWH_CONNECTION,
            sql=f'{{% include "query/{sql_file}" %}}',
        )

        log = PythonOperator(
            task_id=f'log_{target_table}',
            python_callable=log_finished_task,
            op_kwargs={
                'target_table': target_table,
                'source_name': f'{target_table}.csv',
                'start_date': start_timestamp
            },
        )

        parse >> upsert >> log
    return tg


with DAG(dag_id='elt_pipeline',
         default_args=AIRFLOW_DEFAULT_CONFIG,
         schedule_interval=None,
         tags=['stg']
         ) as dag:

    start = EmptyOperator(task_id='start')

    parallel_tasks = []

    for table in STG_DS_TABLE_LIST:
        # Creating task group for each table
        task_group = make_table_group(
            group_id=f'{table}_group',
            target_table=table,
            sql_file=f'stg_ds__{table}.sql'
        )
        parallel_tasks.append(task_group)

    end = EmptyOperator(task_id='end')

    # DAG FLOW
    start >> parallel_tasks >> end
