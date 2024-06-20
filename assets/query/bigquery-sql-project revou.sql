##INTERMEDIATE ASSIGNMENT

/*QUESTION 1*/
SELECT DATE_TRUNC(DATE(shipped_at), month) as date_of_sale,
       status,
       COUNT(DISTINCT user_id) as total_user,
       COUNT(order_id) as total_order,
       ROUND(SUM(sale_price), 1) as total_sale
FROM `bigquery-public-data.thelook_ecommerce.order_items`
WHERE shipped_at BETWEEN '2018-12-31' AND '2022-09-01'
GROUP BY date_of_sale,
         status
ORDER BY date_of_sale;


/*QUESTION 2*/
SELECT DATE_TRUNC(DATE(delivered_at), month) as date_of_completed,
       COUNT(DISTINCT user_id) as total_buyer,
       ROUND(COUNT(order_id)/COUNT(DISTINCT user_id), 3) as frequencies,
       ROUND(AVG(sale_price),2) as AOV
FROM `bigquery-public-data.thelook_ecommerce.order_items`
WHERE status = "Complete" AND delivered_at BETWEEN '2019-01-01' AND '2022-09-01'
GROUP BY date_of_completed,
         status
ORDER BY date_of_completed;


/*QUESTION 3*/
SELECT u.id,
       u.first_name,
       u.last_name,
       u.email,
       o.status
FROM `bigquery-public-data.thelook_ecommerce.users` as u
INNER JOIN `bigquery-public-data.thelook_ecommerce.orders` as o
ON u.id = o.user_id
WHERE o.returned_at BETWEEN '2022-08-01' AND '2022-09-01'
      AND o.status = "Returned"
ORDER BY o.returned_at;


/*QUESTION 4*/
WITH highest as (
SELECT "highest" AS datatype,
       p.id AS product_id,
       p.name AS product_name,
       ROUND(p.retail_price,2) AS ret_price,
       ROUND(p.cost,2) AS ret_cost,
       ROUND(SUM(o.sale_price - p.cost),2) as profit
FROM `bigquery-public-data.thelook_ecommerce.products` as p
INNER JOIN `bigquery-public-data.thelook_ecommerce.order_items` o
ON p.id = o.product_id
GROUP BY datatype,
         product_id,
         product_name,
         ret_price,
         ret_cost
ORDER BY profit DESC
limit 5
),
lowest as (
SELECT "lowest" AS datatype,
       p.id AS product_id,
       p.name AS product_name,
       ROUND(p.retail_price,2) AS ret_price,
       ROUND(p.cost,2) AS ret_cost,
       ROUND(SUM(o.sale_price - p.cost),2) as profit
FROM `bigquery-public-data.thelook_ecommerce.products` as p
INNER JOIN `bigquery-public-data.thelook_ecommerce.order_items` o
ON p.id = o.product_id
GROUP BY datatype,
         product_id,
         product_name,
         ret_price,
         ret_cost
ORDER BY profit
limit 5
)
SELECT *
FROM highest
UNION ALL
SELECT *
FROM lowest;


/*QUESTION 5*/
WITH base as (
SELECT DATE_TRUNC(DATE(o.delivered_at), day) as mtd_date,
       p.category as categories,
       ROUND(SUM(o.sale_price - p.cost),2) as profit
FROM `bigquery-public-data.thelook_ecommerce.products` as p
INNER JOIN `bigquery-public-data.thelook_ecommerce.order_items` o
ON p.id = o.product_id
WHERE DATE_TRUNC(DATE(o.delivered_at), day) IS NOT NULL AND
      DATE_TRUNC(DATE(o.delivered_at), day) BETWEEN '2022-05-31' AND '2022-08-15' AND
      EXTRACT (day from DATE_TRUNC(DATE(o.delivered_at), day)) BETWEEN 1 AND 15
GROUP BY mtd_date,
         categories
ORDER BY mtd_date
)
SELECT mtd_date,
       categories,
       profit,
       ROUND(SUM(profit)OVER(PARTITION BY EXTRACT(month from mtd_date), categories ORDER BY mtd_date),2) as cumulative_profit
FROM base;


##ADVANCED
/*QUESTION 6*/
WITH base as (
SELECT DATE_TRUNC(date(created_at), month) as monthly_growth,
       product_category as category,
       COUNT(product_id) as inventory,
FROM `bigquery-public-data.thelook_ecommerce.inventory_items` as i
WHERE created_at IS NOT NULL AND created_at BETWEEN '2018-12-31' AND '2022-04-30'
GROUP BY monthly_growth,
         category
ORDER BY monthly_growth DESC
),
base_2 as (
SELECT *,
       LEAD(inventory)OVER(PARTITION BY category ORDER BY monthly_growth DESC) as previous_inventory
FROM base
)
SELECT *,
       IFNULL(ROUND((inventory - previous_inventory)/previous_inventory*100,2),0) as percentage
FROM base_2;


/*QUESTION 7*/
/*
- cari first purchase (atau apapun itu) yg bakal dijadikan patokan cohort base nya
- cari purchase selanjutnya (atau next activites kl bakal balik beli lg) pd periode tertentu/sesuai yg diminta
- itung besar cohortnya (ukuran dari first_purchase atau aktivitas pertama yg dijadiin patokan)
- cari retention atau persentase dari keseluruhan pelanggan/user dr awal dia beli/beraktivitas sampe dia akhirnya mutusin utk beli lg/beraktivitas lg
- bikin querynya
*/
WITH first_purchase as (
SELECT o.user_id as user_id,
       MIN(DATE(DATE_TRUNC(shipped_at, month))) as first_purchase_month
FROM `bigquery-public-data.thelook_ecommerce.orders` as o
WHERE shipped_at BETWEEN '2022-01-01' AND '2022-12-31' AND shipped_at IS NOT NULL
GROUP BY o.user_id
),
next_purchase as (
SELECT DISTINCT o.user_id as user_id,
       DATE_DIFF(DATE(DATE_TRUNC(shipped_at, month)),first_purchase_month, month) as month_number,
FROM `bigquery-public-data.thelook_ecommerce.orders` as o
LEFT JOIN first_purchase as first
ON first.user_id = o.user_id
WHERE shipped_at BETWEEN '2022-01-01' AND '2022-12-31' AND shipped_at IS NOT NULL
),
cohort_size as (
SELECT first_purchase_month,
       COUNT(user_id) as num_users
FROM first_purchase
GROUP BY first_purchase_month
ORDER BY first_purchase_month
),
retention as(
SELECT first.first_purchase_month,
       month_number,
       COUNT(next.user_id)user_id
FROM next_purchase as next
LEFT JOIN first_purchase as first
ON next.user_id=first.user_id 
GROUP BY first_purchase_month,
         month_number
ORDER BY first_purchase_month,
         month_number
)
SELECT cohort.first_purchase_month,
       num_users,
       month_number,
       user_id,
       ROUND(user_id/num_users,2) as pct_retention
FROM cohort_size cohort
LEFT JOIN retention 
ON cohort.first_purchase_month=retention.first_purchase_month
ORDER BY cohort.first_purchase_month;