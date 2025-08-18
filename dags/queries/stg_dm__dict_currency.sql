TRUNCATE dm.dict_currency;
INSERT INTO dm.dict_currency
SELECT currency_cd,
       currency_name,
       effective_from_date::DATE,
       effective_to_date::DATE
FROM stg.dict_currency;