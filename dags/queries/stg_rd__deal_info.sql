INSERT INTO rd.deal_info (
    deal_rk,
    deal_num,
    deal_name,
    deal_sum,
    client_rk,
    account_rk,
    agreement_rk,
    deal_start_date,
    department_rk,
    product_rk,
    deal_type_cd,
    effective_from_date,
    effective_to_date
)
WITH deals_rn AS
    (
        SELECT DISTINCT *, ROW_NUMBER() OVER (PARTITION BY deal_rk ORDER BY effective_from_date) AS rn
        FROM stg.deal_info
        WHERE deal_rk IS NOT NULL
    )
SELECT deal_rk
     , deal_num
     , deal_name
     , deal_sum
     , client_rk
     , account_rk
     , agreement_rk
     , deal_start_date::DATE
     , department_rk
     , product_rk
     , deal_type_cd
     , effective_from_date::DATE
     , effective_to_date::DATE
FROM deals_rn
WHERE rn = 1

ON CONFLICT (deal_rk) DO UPDATE
    SET deal_num            = EXCLUDED.deal_num,
        deal_name           = EXCLUDED.deal_name,
        deal_sum            = EXCLUDED.deal_sum,
        client_rk           = EXCLUDED.client_rk,
        account_rk          = EXCLUDED.account_rk,
        agreement_rk        = EXCLUDED.agreement_rk,
        deal_start_date     = EXCLUDED.deal_start_date,
        department_rk       = EXCLUDED.department_rk,
        product_rk          = EXCLUDED.product_rk,
        deal_type_cd        = EXCLUDED.deal_type_cd,
        effective_from_date = EXCLUDED.effective_from_date,
        effective_to_date   = EXCLUDED.effective_to_date;