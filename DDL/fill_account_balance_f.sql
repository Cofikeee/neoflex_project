CREATE OR REPLACE PROCEDURE dm.fill_account_balance_f(IN i_ondate date)
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_start_time TIMESTAMP := clock_timestamp();
    v_rows BIGINT;
    v_is_empty BOOLEAN;

BEGIN
    -- Вставка данных за день до нужной нам даты, если таблица пуста

    SELECT COUNT(*) = 0 FROM dm.dm_account_balance_f INTO v_is_empty;

    IF v_is_empty IS TRUE THEN
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
        WHERE f.on_date = i_ondate - INTERVAL '1 day';
    END IF;

    -- Основная процедура
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

    -- Логирование
    GET DIAGNOSTICS v_rows = ROW_COUNT;

    INSERT INTO logs.dm_changelog(target_table, source, on_date, rows_inserted, start_date, end_date)
    VALUES ('dm.dm_account_balance_f', 'procedure: dm.fill_account_balance_f', i_OnDate , v_rows, v_start_time, clock_timestamp() + INTERVAL '5 seconds');
END;
$$;

CREATE OR REPLACE PROCEDURE dm.fill_account_balance_f(IN i_from_date date, IN i_to_date date)
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
            CALL dm.fill_account_balance_f(d);
        END LOOP;
END;
$$;
