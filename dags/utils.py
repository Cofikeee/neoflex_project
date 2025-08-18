from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import chardet
from config import SOURCE_DIR, DWH_CONNECTION


def insert_data_from_csv(target_schema: str, target_table: str, delimiter: str = ';') -> None:
    """
    > Reads a CSV file for the given target table using Pandas, detects encoding
    > Writes the contents into the `stg` schema using PostgresHook
    """
    file_path = f'{SOURCE_DIR}/{target_table}.csv'
    with open(file_path, 'rb') as f:
        raw_data = f.read(10000)
        result = chardet.detect(raw_data)
    df = pd.read_csv(file_path, delimiter=delimiter, encoding=result['encoding'])
    sql_hook = PostgresHook(DWH_CONNECTION)
    engine = sql_hook.get_sqlalchemy_engine()

    with engine.connect() as conn:
        df.to_sql(
            name=target_table,
            con=conn,  # ← real Connection, works with pandas
            schema=target_schema,
            if_exists='replace',
            index=False,
            method='multi'
        )

