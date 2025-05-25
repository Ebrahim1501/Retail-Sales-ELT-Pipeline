{{ config(schema='DATA_MARTS') }}



SELECT EXTRACT(year FROM sales.TRANSACTION_TIME) AS "YEAR",
COUNT(*) AS TOTAL_SALES,
SUM(prod.PRICE) AS TOTAL_REVENUE
FROM {{ ref('sales_fact') }} AS sales JOIN {{ ref('products_dim') }} AS prod ON prod.PRODUCT_ID=sales.PRODUCT_ID
GROUP BY EXTRACT(year from sales.TRANSACTION_TIME)
ORDER BY TOTAL_REVENUE,TOTAL_SALES
