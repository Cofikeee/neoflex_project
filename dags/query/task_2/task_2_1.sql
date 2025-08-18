-- Смотрим сколько неабсолютных дублей имеется, которые нельзя пофиксить простым DISTINCT *

SELECT *
FROM dm.client
WHERE (client_rk, effective_from_date) IN (
    SELECT client_rk, effective_from_date
    FROM (SELECT DISTINCT * FROM dm.client) t
    GROUP BY client_rk, effective_from_date
    HAVING COUNT(*) > 1
)
ORDER BY client_rk, effective_from_date;

 /*
 Видим, что такой случай всего один.
 Дубликат client_rk + effective_from_date с разными client_id вызывает вопросы.
 -- 3055149,2023-08-11,2023-09-21,ae63fc7df4fe867765ddc059b93aaa3e
 -- 3055149,2023-08-11,2999-12-31,4e53b848e0428699e50966328aac9d00
 Если я в компании недавно, я бы с таким вопросом обратился к лиду или коллегам, чтобы узнать через кого можно провалидировать подобные данные, чтобы внести необходимые изменения.
 Если знаю, к кому обратиться или на такие случаи уже есть процесс, тогда еще проще. Поэтому далее просто моделирую один из сценариев.
 */

SELECT DISTINCT client_rk, client_id
FROM dm.client
WHERE client_rk IN (
    SELECT client_rk
    FROM dm.client
    GROUP BY client_rk
    HAVING count(DISTINCT client_id) > 1
    )
ORDER BY client_rk, client_id;

 /*
 Судя по запросу выше, изменение client у client_rk не подразумевается. А значит менять поля effective_from_date / end_date не будет корректным решением.
 Поэтому представим сценарий, где мы согласовали измененение client_rk для этого одного из дублей.
 Менее рисковано менять идентификатор для уже отключенной записи (end_date - 2023-09-21), с client_id = ae63fc7df4fe867765ddc059b93aaa3e
 */

BEGIN;

ALTER TABLE dm.client RENAME TO depricated_table_client;
CREATE TABLE dm.client (LIKE dm.depricated_table_client INCLUDING ALL);

INSERT INTO dm.client
SELECT DISTINCT *
FROM dm.depricated_table_client;
-- Фиксим странный дубль 3055149, число для client_rk беру рандомное больше существующих.
UPDATE dm.client SET client_rk = 8999999 WHERE client_rk = 3055149 AND client_id ='ae63fc7df4fe867765ddc059b93aaa3e';

-- Создаем констреинт на уникальность по (client_rk, effective_from_date)
ALTER TABLE dm.client ADD CONSTRAINT client_pk PRIMARY KEY (client_rk, effective_from_date);

COMMIT;

