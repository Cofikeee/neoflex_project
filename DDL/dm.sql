CREATE SCHEMA DM;
CREATE TABLE DM.DM_ACCOUNT_TURNOVER_F (
    on_date DATE,
    account_rk NUMERIC,
    credit_amount NUMERIC(23,8),
    credit_amount_rub NUMERIC(23,8),
    debet_amount NUMERIC(23,8),
    debet_amount_rub NUMERIC(23,8)
);

CREATE TABLE DM.DM_ACCOUNT_BALANCE_F (
    on_date DATE,
    account_rk NUMERIC,
    balance_out NUMERIC(23,8),
    balance_out_rub NUMERIC(23,8)
);

CREATE TABLE DM.DM_F101_ROUND_F (
    FROM_DATE DATE,
    TO_DATE DATE,
    CHAPTER CHAR(1),
    LEDGER_ACCOUNT CHAR(5),
    CHARACTERISTIC CHAR(1),
    BALANCE_IN_RUB NUMERIC(23,8),
    BALANCE_IN_VAL NUMERIC(23,8),
    BALANCE_IN_TOTAL NUMERIC(23,8),
    TURN_DEB_RUB NUMERIC(23,8),
    TURN_DEB_VAL NUMERIC(23,8),
    TURN_DEB_TOTAL NUMERIC(23,8),
    TURN_CRE_RUB NUMERIC(23,8),
    TURN_CRE_VAL NUMERIC(23,8),
    TURN_CRE_TOTAL NUMERIC(23,8),
    BALANCE_OUT_RUB NUMERIC(23,8),
    BALANCE_OUT_VAL NUMERIC(23,8),
    BALANCE_OUT_TOTAL NUMERIC(23,8)
);

INSERT INTO dm.dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
SELECT f.on_date
     , f.account_rk
     , f.balance_out
     , f.balance_out * COALESCE(er.reduced_cource, 1) AS balance_out_rub
FROM ds.ft_balance_f f
         LEFT JOIN ds.md_exchange_rate_d er
                   ON er.currency_rk = f.currency_rk
                       AND er.data_actual_date <= f.on_date
                       AND (er.data_actual_end_date IS NULL OR er.data_actual_end_date >= f.on_date)
WHERE f.on_date = '2017-12-31';