CREATE SCHEMA logs;

CREATE TABLE logs.dm_changelog
(
    target_table  TEXT,
    source        TEXT,
    on_date       DATE,
    rows_inserted BIGINT,
    start_date    TIMESTAMP,
    end_date      TIMESTAMP
);

CREATE TABLE logs.file_transaction_log
(
    operation_type TEXT,
    table_name     TEXT,
    file_name      TEXT,
    rows_inserted  BIGINT,
    start_date     TIMESTAMP,
    end_date       TIMESTAMP
);
