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

CREATE TABLE logs.ds_changelog
(
    target_table  TEXT,
    source        TEXT,
    rows_inserted BIGINT,
    start_date    TIMESTAMP,
    end_date      TIMESTAMP
);
