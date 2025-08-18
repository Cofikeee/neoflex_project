# AIRFLOW
from airflow import DAG
from airflow.operators.python import PythonOperator
# PROJECT
from config import AIRFLOW_DEFAULT_CONFIG
from utils import insert_data_from_csv


with DAG(dag_id='import_dm_f101_round_f_v2',
         default_args=AIRFLOW_DEFAULT_CONFIG,
         schedule_interval=None,
         tags=['import', 'f101', 'dm']
         ) as dag:
    parse = PythonOperator(
        task_id='parse_csv',
        python_callable=insert_data_from_csv,
        op_kwargs={'target_schema': 'dm',
                   'target_table': 'dm_f101_round_f_v2',
                   'delimiter': ','},
    )
