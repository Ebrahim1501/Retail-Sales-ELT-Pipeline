{{ config(schema='DATA_MARTS') }}

SELECT  
    CASE 
        WHEN prod.PRODUCT_CATEGORY IS NULL THEN 'Others' 
        ELSE prod.PRODUCT_CATEGORY 
    END AS PRODUCT_CATEGORY,
    
    COUNT(*) AS TOTAL_SALES,
    SUM(prod.PRICE) AS TOTAL_REVENUE

FROM {{ ref('sales_fact') }} AS sales
JOIN {{ ref('products_dim') }} AS prod 
    ON prod.PRODUCT_ID = sales.PRODUCT_ID

GROUP BY 
    CASE 
        WHEN prod.PRODUCT_CATEGORY IS NULL THEN 'Others' 
        ELSE prod.PRODUCT_CATEGORY 
    END

ORDER BY TOTAL_REVENUE DESC
