TRUNCATE ds.ft_posting_f;

INSERT INTO ds.ft_posting_f (
    oper_date,
    credit_account_rk,
    debet_account_rk,
    credit_amount,
    debet_amount
)
SELECT
    TO_DATE("OPER_DATE", 'DD-mm-YYYY'),
    "CREDIT_ACCOUNT_RK",
    "DEBET_ACCOUNT_RK",
    "CREDIT_AMOUNT",
    "DEBET_AMOUNT"
FROM stg.ft_posting_f
WHERE "CREDIT_ACCOUNT_RK" IS NOT NULL;