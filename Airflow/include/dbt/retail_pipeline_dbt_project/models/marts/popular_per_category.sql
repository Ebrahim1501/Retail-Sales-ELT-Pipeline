{{ config(schema='DATA_MARTS') }}





with total_revenue_per_product AS
(
    SELECT PRODUCT_ID,SUM(PRICE) AS TOTAL
    FROM {{ ref('sales_fact') }}
    GROUP BY PRODUCT_ID


),

 revenue_per_category AS
(
    SELECT p.PRODUCT_CATEGORY,p.PRODUCT,cte.TOTAL,ROW_NUMBER() OVER(PARTITION BY p.PRODUCT_CATEGORY ORDER BY TOTAL) as rownum
    FROM total_revenue_per_product as cte join {{ ref('products_dim') }} as p on p.PRODUCT_ID=cte.PRODUCT_ID




),

highest_revenue_per_category AS
(
    SELECT PRODUCT_CATEGORY,PRODUCT,TOTAL FROM revenue_per_category WHERE rownum=1



)
SELECT * FROM highest_revenue_per_category


