INSERT INTO ds.md_currency_d (
    currency_rk,
    data_actual_date,
    data_actual_end_date,
    currency_code,
    code_iso_char
)
SELECT DISTINCT
    "CURRENCY_RK",
    "DATA_ACTUAL_DATE"::DATE,
    "DATA_ACTUAL_END_DATE"::DATE,
    "CURRENCY_CODE",
    "CODE_ISO_CHAR"
FROM stg.md_currency_d
WHERE "CURRENCY_RK" IS NOT NULL
ON CONFLICT (currency_rk, data_actual_date) DO UPDATE
SET
    data_actual_end_date = excluded.data_actual_end_date,
    currency_code = excluded.currency_code,
    code_iso_char = excluded.code_iso_char;
