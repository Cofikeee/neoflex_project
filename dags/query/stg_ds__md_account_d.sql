INSERT INTO ds.md_account_d (
    data_actual_date,
    data_actual_end_date,
    account_rk,
    account_number,
    char_type,
    currency_rk,
    currency_code
)
SELECT DISTINCT
    "DATA_ACTUAL_DATE"::DATE,
    "DATA_ACTUAL_END_DATE"::DATE,
    "ACCOUNT_RK",
    "ACCOUNT_NUMBER",
    "CHAR_TYPE",
    "CURRENCY_RK",
    "CURRENCY_CODE"
FROM stg.md_account_d
WHERE "ACCOUNT_RK" IS NOT NULL
ON CONFLICT (data_actual_date, account_rk) DO UPDATE
SET
    data_actual_end_date = excluded.data_actual_end_date,
    account_number = excluded.account_number,
    char_type = excluded.char_type,
    currency_rk = excluded.currency_rk,
    currency_code = excluded.currency_code;
