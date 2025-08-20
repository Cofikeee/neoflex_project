-- Task 2.2
-- Смотрим сколько дублей имеется в deal_info и product,
-- так как я планирую установить констеинт на уникальность

--------------------------------- deal_info
SELECT * FROM rd.deal_info
WHERE deal_rk IN (SELECT deal_rk
                  FROM rd.deal_info
                  GROUP BY deal_rk
                  HAVING count(*) > 1);

/*
 Видим, что таких случая всего два.
 По аналогии с решением в 2.1 удаляю запись с 449-999-323-979-378 и 759-157-722-146-287
 */

BEGIN;

DELETE FROM rd.deal_info WHERE deal_rk = 5055393 AND deal_num = '659-876-369-894-341';
DELETE FROM rd.deal_info WHERE deal_rk = 4531242 AND deal_num = '353-164-200-546-322';

-- Констреинт
ALTER TABLE rd.deal_info ADD CONSTRAINT deal_info_pk PRIMARY KEY (deal_rk);

COMMIT;


---------------------------------- product
DROP TABLE IF EXISTS product_dupes;
CREATE TEMP TABLE product_dupes AS
SELECT ctid AS postgres_ctid, *, ROW_NUMBER() OVER (PARTITION BY product_rk, effective_from_date, product_name) AS rn
FROM rd.product
WHERE (product_rk, effective_from_date) IN (
    SELECT product_rk, effective_from_date
    FROM rd.product
    GROUP BY product_rk, effective_from_date
    HAVING COUNT(*) > 1
);

SELECT * FROM product_dupes;
/*
 Тут 2 вида дублей, по rk и по DISTINCT *.
 Для кейсов DISTINCT *: я удаляю такие записи.
 После этого буду разбирать более сложные кейсы.
 */
BEGIN;

DELETE FROM rd.product p
       USING product_dupes r
       WHERE p.ctid = r.postgres_ctid
         AND r.rn > 1;
/*
Изучим оставшиеся дубли:
2 Дубля, которые не используются в rd.deal_info
1979096,Автокредит
1979096,Ипотека
1668282,Автокредит
1668282,Ипотека

Удаляю все 4 записи, так как не знаю, какое из значений является корректным.
Такой же подход будет использован в стейджинге.
Само собой в реальных условиях такие решения принимаются несколько иначе))

В случае, если совпадают rk, start_date, end_date, а названия разные, удаляю такие записи.
Из-за этого может быть упущенно несколько десятков записей в таблицах
Если бы был продуктовый проект и не было бы готового решения, я бы возможно тянул deal_info >> product
и валидировал бы product_name по связи product_rk и значению deal_info.deal_name
Поскольку проект учебный, ограничусь дропом невалидированных записей.

 */

DROP TABLE IF EXISTS product_dupes;
CREATE TEMP TABLE product_dupes AS
SELECT *
FROM rd.product
WHERE (product_rk, effective_from_date) IN (
    SELECT product_rk, effective_from_date
    FROM rd.product
    GROUP BY product_rk, effective_from_date
    HAVING COUNT(*) > 1
);

DELETE FROM rd.product WHERE (product_rk, effective_from_date) IN (SELECT product_rk, effective_from_date FROM product_dupes);

-- Констреинт:
ALTER TABLE rd.product ADD CONSTRAINT product_pk PRIMARY KEY (product_rk, effective_from_date);

COMMIT;


---------------------------------- loan_holiday

/*
В rd.loan_holiday тоже обнаружились дубликаты.
2 кейса, с отличием только в loan_holiday_fact_finish_date.
Оставляю те записи, где значение выше.
Опять-таки, не продуктовое решение)
 */

DROP TABLE IF EXISTS loan_holiday_dupes;
CREATE TEMP TABLE loan_holiday_dupes AS
SELECT ctid AS postgres_ctid, *, ROW_NUMBER() OVER (PARTITION BY deal_rk, effective_from_date ORDER BY effective_to_date, loan_holiday_fact_finish_date DESC) AS rn
FROM rd.loan_holiday
WHERE (deal_rk, effective_from_date)
          IN (
          SELECT deal_rk, effective_from_date
          FROM rd.loan_holiday
          GROUP BY deal_rk, effective_from_date
          HAVING COUNT(*) > 1
      );

SELECT * FROM loan_holiday_dupes;

BEGIN;
DELETE FROM rd.loan_holiday lh
    USING loan_holiday_dupes lhd
WHERE lh.ctid = lhd.postgres_ctid
  AND lhd.rn > 1;

DELETE FROM dm.loan_holiday_info lhi
    USING loan_holiday_dupes lhd
WHERE lhi.deal_rk = lhd.deal_rk
  AND lhd.rn > 1;

-- Констреинт:
ALTER TABLE rd.loan_holiday ADD CONSTRAINT loan_holiday_pk PRIMARY KEY (deal_rk, effective_from_date);
ALTER TABLE dm.loan_holiday_info ADD CONSTRAINT loan_holiday_info_pk PRIMARY KEY (deal_rk, effective_from_date);
COMMIT;



SELECT 'ВЫГРУЗКА rd.deal_info, rd.product из AIRFLOW';

-- Пересобираем витрину loan_holiday_dupes
CALL dm.upsert_loan_holiday_info();

SELECT COUNT(*) FROM  dm.loan_holiday_info;