from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import chardet
import csv
from config import SOURCE_DIR, DWH_CONNECTION


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
