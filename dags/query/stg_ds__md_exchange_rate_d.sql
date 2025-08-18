INSERT INTO ds.md_exchange_rate_d (
    data_actual_date,
    data_actual_end_date,
    currency_rk,
    reduced_cource,
    code_iso_num
)
SELECT DISTINCT
    "DATA_ACTUAL_DATE"::DATE,
    "DATA_ACTUAL_END_DATE"::DATE,
    "CURRENCY_RK",
    "REDUCED_COURCE",
    "CODE_ISO_NUM"
FROM stg.md_exchange_rate_d
WHERE "CURRENCY_RK" IS NOT NULL
ON CONFLICT (data_actual_date, currency_rk) DO UPDATE
SET
    data_actual_end_date = excluded.data_actual_end_date,
    reduced_cource = excluded.reduced_cource,
    code_iso_num = excluded.code_iso_num;
