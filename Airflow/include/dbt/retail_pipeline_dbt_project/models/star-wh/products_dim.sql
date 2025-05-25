{{ config(schema='DATA_MODELS') }}


select row_number() over(order by PRODUCT) as PRODUCT_ID,
PRODUCT,
PRODUCT_CATEGORY,
PRODUCT_BRAND,
PRODUCT_TYPE,
PRICE

FROM(select DISTINCT PRODUCT,PRODUCT_CATEGORY,PRODUCT_BRAND,PRODUCT_TYPE,PRICE FROM {{ ref('data_transformation') }}) 



{% if is_incremental() %}
    WHERE (PRODUCT) NOT IN 
    (
    SELECT PRODUCT FROM {{ this }}
    )

{% endif %}
