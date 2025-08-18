INSERT INTO rd.product (
    product_rk,
    product_name,
    effective_from_date,
    effective_to_date
)
WITH product_inconsistant_dupes AS (
    SELECT product_rk, effective_from_date
    FROM stg.product
    GROUP BY product_rk, effective_from_date
    HAVING COUNT(*) > 1
)
SELECT DISTINCT product_rk,
       product_name,
       effective_from_date::DATE,
       effective_to_date::DATE
FROM stg.product
EXCEPT
SELECT DISTINCT product_rk,
       product_name,
       effective_from_date::DATE,
       effective_to_date::DATE
FROM stg.product
WHERE (product_rk, effective_from_date) IN
      (SELECT product_rk, effective_from_date FROM product_inconsistant_dupes)

ON CONFLICT (product_rk, effective_from_date) DO UPDATE
SET product_name        = EXCLUDED.product_name,
    effective_to_date   = EXCLUDED.effective_to_date;
