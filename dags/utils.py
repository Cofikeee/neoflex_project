# AIRFLOW
from airflow.providers.postgres.hooks.postgres import PostgresHook
# MISC
import pandas as pd
import chardet
import csv
# PROJECT
from config import SOURCE_DIR, DWH_CONNECTION
from datetime import datetime


def insert_data_from_csv(target_schema: str, target_table: str) -> None:
    """
    Reads a CSV file for the given target table using Pandas, automatically detects
    encoding and delimiter, and writes the contents into the target schema using PostgresHook.
    """
    file_path = f'{SOURCE_DIR}/{target_table}.csv'

    # Detect encoding
    with open(file_path, 'rb') as f:
        raw_data = f.read(10000)
        result = chardet.detect(raw_data)
    encoding = result['encoding']

    # Detect delimiter
    with open(file_path, 'r', encoding=encoding) as f:
        first_line = f.readline().strip()  # читаем до первой новой строки
        sniffer = csv.Sniffer()
        dialect = sniffer.sniff(first_line)
        delimiter = dialect.delimiter

    # Read CSV
    df = pd.read_csv(file_path, delimiter=delimiter, encoding=encoding)

    # Insert into Postgres
    sql_hook = PostgresHook(DWH_CONNECTION)
    engine = sql_hook.get_sqlalchemy_engine()
    with engine.connect() as conn:
        df.to_sql(
            name=target_table,
            con=conn,
            schema=target_schema,
            if_exists='replace',
            index=False,
            method='multi'
        )


def log_finished_task(target_schema: str, target_table: str, target_file: str, start_date: datetime, operation_type: str = 'IMPORT') -> None:
    """
    > Logs metadata about a finished task group run into `logs.file_transaction_log`.
    > Added a small 5-second delay to `end_date` as required in task.
    """
    sql_hook = PostgresHook(DWH_CONNECTION)
    conn = sql_hook.get_conn()
    cursor = conn.cursor()

    cursor.execute(f"SELECT COUNT(*) FROM {target_schema}.{target_table}")
    rows_inserted = cursor.fetchone()[0]

    query = """
        INSERT INTO logs.file_transaction_log 
        (operation_type, table_name, file_name, rows_inserted, start_date, end_date)
        VALUES (%s, %s, %s, %s, %s, NOW() + INTERVAL '5 SECONDS')
    """
    cursor.execute(query, (f'{operation_type}', f'{target_schema}.{target_table}', target_file, rows_inserted, start_date))
    conn.commit()
    cursor.close()
    conn.close()
