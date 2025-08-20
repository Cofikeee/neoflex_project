-- Task 2.2

-- Констреинт для rd.account, rd.account_balance, тут к счастью без дублей
ALTER TABLE rd.account ADD CONSTRAINT account_pk PRIMARY KEY (account_rk, effective_from_date);
ALTER TABLE rd.account_balance ADD CONSTRAINT account_balance_pk PRIMARY KEY (account_rk, effective_date);
ALTER TABLE dm.account_balance_turnover ADD CONSTRAINT account_balance_turnover_pk PRIMARY KEY (account_rk, effective_date);

-- Завернул в процедуру upsert_account_balance_turnover
-- Процедуры реализованы через upsert, что избавляет от необходимости поиска строк, которые необходимо обновить,
-- Они будут обновлены по ON CONFLICT

CALL dm.upsert_account_balance_turnover()