{{ config(
    materialized='incremental',
    schema='DATA_MODELS'
) }}

WITH dedup_customers AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY NAME, PHONE, EMAIL, AGE ORDER BY USER_ID) AS rn
        FROM {{ ref('customers_dim') }}
    )
    WHERE rn = 1
),

dedup_locations AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY COUNTRY, STATE, CITY ORDER BY LOCATION_ID) AS rn
        FROM {{ ref('locations_dim') }}
    )
    WHERE rn = 1
),

dedup_products AS (
    SELECT *
    FROM (
        SELECT *,
            ROW_NUMBER() OVER (PARTITION BY PRODUCT ORDER BY PRODUCT_ID) AS rn
        FROM {{ ref('products_dim') }}
    )
    WHERE rn = 1
)

SELECT DISTINCT
    t.ID AS SALES_ID,
    cust.USER_ID,
    loc.LOCATION_ID,
    t.TRANSACTION_TIME,
    prod.PRODUCT_ID,
    t.FEEDBACK,
    t.SHIPPING_METHOD,
    t.PAYMENT_METHOD,
    t.ORDER_STATUS,
    t.PRICE,
    t.RATING

FROM {{ ref('data_transformation') }} AS t

LEFT JOIN dedup_customers AS cust
    ON cust.NAME = t.NAME
    AND cust.PHONE = t.PHONE
    AND cust.EMAIL = t.EMAIL
    AND cust.AGE = t.AGE

LEFT JOIN dedup_locations AS loc
    ON loc.COUNTRY = t.COUNTRY
    AND loc.STATE = t.STATE
    AND loc.CITY = t.CITY

LEFT JOIN dedup_products AS prod
    ON prod.PRODUCT = t.PRODUCT



    

{% if is_incremental() %}
WHERE t.ID NOT IN (SELECT SALES_ID FROM {{ this }})
{% endif %}
