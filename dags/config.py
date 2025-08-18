from datetime import datetime
from airflow.models import Variable

AIRFLOW_DEFAULT_CONFIG = {
    'owner': '@pbushmanov',
    'start_date': datetime(2025, 8, 16),
    'catchup': False
}

SOURCE_DIR = '/opt/airflow/dags/source'
QUERY_DIR = '/opt/airflow/dags/query'
REPORT_DIR = '/opt/airflow/reports'
DWH_CONNECTION = 'neoflex_conn'
DB_NAME = 'neoflex_db'
