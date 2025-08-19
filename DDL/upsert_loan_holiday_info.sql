CREATE OR REPLACE PROCEDURE dm.upsert_loan_holiday_info()
    LANGUAGE plpgsql
AS
$$
DECLARE
    v_rows bigint;
    v_start_time timestamp := clock_timestamp();
BEGIN
    INSERT INTO dm.loan_holiday_info

    WITH
        deal AS (
            SELECT deal_rk,
                deal_num,
                deal_name,
                deal_sum,
                account_rk,
                client_rk,
                agreement_rk,
                deal_start_date,
                department_rk,
                product_rk,
                deal_type_cd,
                effective_from_date,
                effective_to_date
            FROM rd.deal_info
        ),
        loan_holiday AS (
            SELECT deal_rk,
                loan_holiday_type_cd,
                loan_holiday_start_date,
                loan_holiday_finish_date,
                loan_holiday_fact_finish_date,
                loan_holiday_finish_flg,
                loan_holiday_last_possible_date,
                effective_from_date,
                effective_to_date
            FROM rd.loan_holiday
        ),
        product AS (
            SELECT product_rk,
                product_name,
                effective_from_date,
                effective_to_date
            FROM rd.product
        ),
        holiday_info AS (
            SELECT d.deal_rk,
                lh.effective_from_date,
                lh.effective_to_date,
                d.deal_num AS deal_number,
                lh.loan_holiday_type_cd,
                lh.loan_holiday_start_date,
                lh.loan_holiday_finish_date,
                lh.loan_holiday_fact_finish_date,
                lh.loan_holiday_finish_flg,
                lh.loan_holiday_last_possible_date,
                d.deal_name,
                d.deal_sum,
                d.client_rk,
                d.account_rk,
                d.agreement_rk,
                d.deal_start_date,
                d.department_rk,
                d.product_rk,
                p.product_name,
                d.deal_type_cd
            FROM deal d
                 LEFT JOIN loan_holiday lh
                 ON d.deal_rk = lh.deal_rk
                     AND d.effective_from_date = lh.effective_from_date
                 LEFT JOIN product p
                 ON p.product_rk = d.product_rk
                     AND p.effective_from_date = d.effective_from_date
        )
    SELECT deal_rk,
        effective_from_date,
        effective_to_date,
        agreement_rk,
        account_rk,
        client_rk,
        department_rk,
        product_rk,
        product_name,
        deal_type_cd,
        deal_start_date,
        deal_name,
        deal_number,
        deal_sum,
        loan_holiday_type_cd,
        loan_holiday_start_date,
        loan_holiday_finish_date,
        loan_holiday_fact_finish_date,
        loan_holiday_finish_flg,
        loan_holiday_last_possible_date
    FROM holiday_info

    ON CONFLICT (deal_rk, effective_from_date)
        DO UPDATE SET
             effective_to_date = excluded.effective_to_date,
             agreement_rk = excluded.agreement_rk,
             account_rk = excluded.account_rk,
             client_rk = excluded.client_rk,
             department_rk = excluded.department_rk,
             product_rk = excluded.product_rk,
             product_name = excluded.product_name,
             deal_type_cd = excluded.deal_type_cd,
             deal_start_date = excluded.deal_start_date,
             deal_name = excluded.deal_name,
             deal_number = excluded.deal_number,
             deal_sum = excluded.deal_sum,
             loan_holiday_type_cd = excluded.loan_holiday_type_cd,
             loan_holiday_start_date = excluded.loan_holiday_start_date,
             loan_holiday_finish_date = excluded.loan_holiday_finish_date,
             loan_holiday_fact_finish_date = excluded.loan_holiday_fact_finish_date,
             loan_holiday_finish_flg = excluded.loan_holiday_finish_flg,
             loan_holiday_last_possible_date = excluded.loan_holiday_last_possible_date;

    GET DIAGNOSTICS v_rows = ROW_COUNT;

    -- Logging
    INSERT INTO logs.dm_changelog(target_table, source, on_date, rows_inserted, start_date, end_date)
    VALUES ('dm.loan_holiday_info', 'procedure: dm.compile_loan_holiday_info', NULL, v_rows, v_start_time, CLOCK_TIMESTAMP() + INTERVAL '5 seconds');
END;
$$;
