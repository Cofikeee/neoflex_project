CREATE PROCEDURE fill_f101_round_f(IN i_ondate date)
    LANGUAGE plpgsql
AS
$$
declare
    v_from_date date;
    v_to_date date;
    v_prev_date date;
    v_start_time timestamp := clock_timestamp();
    v_rows bigint;
begin
    -- Validation
    if i_OnDate <> date_trunc('month', i_OnDate) then
        raise exception 'Parameter i_OnDate (%) must be the first day of month', i_OnDate;
    end if;

    -- Periods
    v_from_date := (i_OnDate - interval '1 month')::date;
    v_to_date   := (i_OnDate - interval '1 day')::date;
    v_prev_date := (v_from_date - interval '1 day')::date;

    -- Clean up old data
    delete from dm.dm_f101_round_f
    where from_date = v_from_date and to_date = v_to_date;

    -- Insert new data
    insert into dm.dm_f101_round_f (
        from_date, to_date, chapter, ledger_account, characteristic,
        balance_in_rub, balance_in_val, balance_in_total,
        turn_deb_rub, turn_deb_val, turn_deb_total,
        turn_cre_rub, turn_cre_val, turn_cre_total,
        balance_out_rub, balance_out_val, balance_out_total
    )
    select
        v_from_date,
        v_to_date,
        l.chapter,
        substr(a.account_number, 1, 5) as ledger_account,
        a.char_type,
        sum(case when a.currency_code in ('810','643') then b_in.balance_out_rub else 0 end) as balance_in_rub,
        sum(case when a.currency_code not in ('810','643') then b_in.balance_out_rub else 0 end) as balance_in_val,
        sum(b_in.balance_out_rub) as balance_in_total,
        sum(case when a.currency_code in ('810','643') then t.debet_amount_rub else 0 end) as turn_deb_rub,
        sum(case when a.currency_code not in ('810','643') then t.debet_amount_rub else 0 end) as turn_deb_val,
        sum(t.debet_amount_rub) as turn_deb_total,
        sum(case when a.currency_code in ('810','643') then t.credit_amount_rub else 0 end) as turn_cre_rub,
        sum(case when a.currency_code not in ('810','643') then t.credit_amount_rub else 0 end) as turn_cre_val,
        sum(t.credit_amount_rub) as turn_cre_total,
        sum(case when a.currency_code in ('810','643') then b_out.balance_out_rub else 0 end) as balance_out_rub,
        sum(case when a.currency_code not in ('810','643') then b_out.balance_out_rub else 0 end) as balance_out_val,
        sum(b_out.balance_out_rub) as balance_out_total
    from ds.md_account_d a
         join ds.md_ledger_account_s l
         on l.ledger_account = substr(a.account_number, 1, 5)::INTEGER
             and v_from_date between l.start_date and coalesce(l.end_date, v_from_date)
         left join dm.dm_account_balance_f b_in
         on b_in.account_rk = a.account_rk and b_in.on_date = v_prev_date
         left join dm.dm_account_turnover_f t
         on t.account_rk = a.account_rk and t.on_date between v_from_date and v_to_date
         left join dm.dm_account_balance_f b_out
         on b_out.account_rk = a.account_rk and b_out.on_date = v_to_date
    group by l.chapter, substr(a.account_number, 1, 5), a.char_type;

    get diagnostics v_rows = row_count;

    -- 5. Logging
    insert into logs.dm_changelog(target_table, source, rows_inserted, start_date, end_date)
    values ('dm_f101_round_f', 'procedure: dm.fill_f101_round_f', v_rows, v_start_time, clock_timestamp() + INTERVAL '5 seconds');
end;
$$;

ALTER PROCEDURE fill_f101_round_f(DATE) OWNER TO airflow;

