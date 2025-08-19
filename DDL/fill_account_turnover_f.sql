CREATE OR REPLACE PROCEDURE dm.fill_account_turnover_f(IN i_ondate date)
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time TIMESTAMP := clock_timestamp();
    v_rows BIGINT;
BEGIN
    DELETE FROM dm.dm_account_turnover_f WHERE on_date = i_OnDate;
    INSERT INTO dm.dm_account_turnover_f (
        on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub
    )
    SELECT
        i_OnDate AS on_date,
        acc.account_rk,
        COALESCE(SUM(CASE WHEN p.oper_date = i_OnDate AND p.credit_account_rk = acc.account_rk THEN p.credit_amount END), 0) AS credit_amount,
        COALESCE(SUM(CASE WHEN p.oper_date = i_OnDate AND p.credit_account_rk = acc.account_rk THEN p.credit_amount * COALESCE(er.reduced_cource, 1) END), 0) AS credit_amount_rub,
        COALESCE(SUM(CASE WHEN p.oper_date = i_OnDate AND p.debet_account_rk = acc.account_rk THEN p.debet_amount END), 0) AS debet_amount,
        COALESCE(SUM(CASE WHEN p.oper_date = i_OnDate AND p.debet_account_rk = acc.account_rk THEN p.debet_amount * COALESCE(er.reduced_cource, 1) END), 0) AS debet_amount_rub
    FROM ds.md_account_d acc
         JOIN ds.ft_posting_f p
         ON (p.credit_account_rk = acc.account_rk OR p.debet_account_rk = acc.account_rk)
             AND p.oper_date = i_OnDate
         LEFT JOIN ds.md_exchange_rate_d er
         ON er.currency_rk = acc.currency_rk
             AND er.data_actual_date <= i_OnDate
             AND (er.data_actual_end_date IS NULL OR er.data_actual_end_date >= i_OnDate)
    WHERE acc.data_actual_date <= i_OnDate
      AND (acc.data_actual_end_date IS NULL OR acc.data_actual_end_date >= i_OnDate)
    GROUP BY acc.account_rk, er.reduced_cource;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    INSERT INTO logs.dm_changelog(target_table, source, on_date, rows_inserted, start_date, end_date)
    VALUES ('dm.dm_account_turnover_f', 'procedure: dm.fill_account_turnover_f', i_OnDate, v_rows, v_start_time, clock_timestamp() + INTERVAL '5 seconds');
END;
$$;

CREATE OR REPLACE PROCEDURE dm.fill_account_turnover_f(IN i_from_date date, IN i_to_date date)
    LANGUAGE plpgsql
AS
$$
DECLARE
    d DATE;
BEGIN
    -- validate input
    IF i_from_date > i_to_date THEN
        RAISE EXCEPTION 'Invalid input: i_from_date (%) is greater than i_to_date (%)',
            i_from_date, i_to_date;
    END IF;

    FOR d IN
        SELECT generate_series(i_from_date, i_to_date, INTERVAL '1 day')::DATE
        LOOP
            CALL dm.fill_account_turnover_f(d);
        END LOOP;
END;
$$;
