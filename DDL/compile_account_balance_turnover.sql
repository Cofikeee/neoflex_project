CREATE PROCEDURE compile_account_balance_turnover()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_rows bigint;
    v_start_time timestamp := clock_timestamp();
BEGIN
    DROP TABLE IF EXISTS dm.account_balance_turnover;
    CREATE TABLE IF NOT EXISTS dm.account_balance_turnover AS
    WITH ab AS (
        SELECT a.account_rk,
            COALESCE(dc.currency_name, '-1'::TEXT) AS currency_name,
            a.department_rk,
            ab.effective_date,
            ab.account_in_sum,
            ab.account_out_sum,
                    LAG(ab.account_out_sum) OVER (PARTITION BY a.account_rk ORDER BY ab.effective_date)  AS prev_out_sum,
                    LEAD(ab.account_in_sum) OVER (PARTITION BY a.account_rk ORDER BY ab.effective_date) AS next_in_sum
        FROM rd.account a
             LEFT JOIN rd.account_balance ab
             ON a.account_rk = ab.account_rk
             LEFT JOIN dm.dict_currency dc
             ON a.currency_cd = dc.currency_cd
    )
    SELECT account_rk,
        currency_name,
        department_rk,
        effective_date,
         -- корректируем account_in_sum
        CASE
            WHEN prev_out_sum IS NOT NULL AND account_in_sum IS DISTINCT FROM prev_out_sum
                THEN prev_out_sum
            ELSE account_in_sum
            END AS account_in_sum,
         -- корректируем account_out_sum
        CASE
            WHEN next_in_sum IS NOT NULL AND account_out_sum IS DISTINCT FROM next_in_sum
                THEN next_in_sum
            ELSE account_out_sum
            END AS account_out_sum
    FROM ab;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

-- Logging
    INSERT INTO logs.dm_changelog(target_table, source, on_date, rows_inserted, start_date, end_date)
    VALUES ('dm.account_balance_turnover', 'procedure: dm.compile_account_balance_turnover', NULL, v_rows, v_start_time, CLOCK_TIMESTAMP() + INTERVAL '5 seconds');

END;
$$;

ALTER PROCEDURE compile_account_balance_turnover() OWNER TO airflow;

