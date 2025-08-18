CREATE OR REPLACE PROCEDURE dm.fill_account_balance_f(i_OnDate DATE)
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time TIMESTAMP := clock_timestamp();
    v_rows BIGINT;
BEGIN
    DELETE FROM dm.dm_account_balance_f WHERE on_date = i_OnDate;
    INSERT INTO dm.dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
    SELECT
        i_OnDate AS on_date,
        acc.account_rk,
        CASE
            WHEN acc.char_type = 'А'
                THEN COALESCE(prev.balance_out, 0)
                         + COALESCE(turn.debet_amount, 0)
                - COALESCE(turn.credit_amount, 0)
            ELSE COALESCE(prev.balance_out, 0)
                     - COALESCE(turn.debet_amount, 0)
                + COALESCE(turn.credit_amount, 0)
            END AS balance_out,
        CASE
            WHEN acc.char_type = 'А'
                THEN COALESCE(prev.balance_out_rub, 0)
                         + COALESCE(turn.debet_amount_rub, 0)
                - COALESCE(turn.credit_amount_rub, 0)
            ELSE COALESCE(prev.balance_out_rub, 0)
                     - COALESCE(turn.debet_amount_rub, 0)
                + COALESCE(turn.credit_amount_rub, 0)
            END AS balance_out_rub
    FROM ds.md_account_d acc
         LEFT JOIN dm.dm_account_balance_f prev
         ON prev.account_rk = acc.account_rk
             AND prev.on_date = i_OnDate - INTERVAL '1 day'
         LEFT JOIN dm.dm_account_turnover_f turn
         ON turn.account_rk = acc.account_rk
             AND turn.on_date = i_OnDate
    WHERE acc.data_actual_date <= i_OnDate
      AND (acc.data_actual_end_date IS NULL OR acc.data_actual_end_date >= i_OnDate);

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    INSERT INTO logs.dm_changelog(target_table, source, on_date, rows_inserted, start_date, end_date)
    VALUES ('dm.dm_account_balance_f', 'ds.fill_account_balance_f', i_OnDate , v_rows, v_start_time, clock_timestamp() + INTERVAL '5 seconds');
END;
$$;

CREATE PROCEDURE dm.fill_account_balance_f(IN i_from_date DATE, i_to_date DATE)
    LANGUAGE plpgsql
AS
$$
DECLARE
    d DATE;
BEGIN
    FOR d IN
        SELECT GENERATE_SERIES(i_from_date, i_to_date, INTERVAL '1 day')::DATE
        LOOP
            CALL dm.fill_account_balance_f(d);
        END LOOP;
END
$$;