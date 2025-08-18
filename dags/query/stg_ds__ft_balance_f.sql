INSERT INTO ds.ft_balance_f (
    on_date,
    account_rk,
    currency_rk,
    balance_out
)
SELECT DISTINCT
TO_DATE("ON_DATE", 'DD.mm.YYYY'),
    "ACCOUNT_RK",
    "CURRENCY_RK",
    "BALANCE_OUT"
FROM stg.ft_balance_f
WHERE "ACCOUNT_RK" IS NOT NULL
ON CONFLICT (on_date, account_rk) DO UPDATE
SET
    currency_rk = excluded.currency_rk,
    balance_out = excluded.balance_out;

