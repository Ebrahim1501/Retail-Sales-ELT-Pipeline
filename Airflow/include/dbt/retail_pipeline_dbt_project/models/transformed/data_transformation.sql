--deduplicate
--remove nulls
--standarize text
--remove duplicates

{{ config(schema='TRANSFORMED_DATA') }}
 
 with get_dupl as
 (
select *,
row_number()
OVER(PARTITION BY TRANSACTION_ID,CUSTOMER_ID,"NAME",EMAIL,PHONE,"ADDRESS",CITY,"STATE",ZIPCODE,COUNTRY,AGE,GENDER,CUSTOMER_SEGMENT,TRAN_DATE,TRAN_TIME,PRICE,PRODUCT_CATEGORY,PRODUCT_BRAND,PRODUCT_TYPE,FEEDBACK,SHIPPING_METHOD,PAYMENT_METHOD,ORDER_STATUS,RATING,PRODUCT order by "NAME" ASC) as row_num
FROM {{source("raw_data_tables","raw_data")}}
)

,remove_dupl as
(
    SELECT 
    ID,
    TRANSACTION_ID,
    CUSTOMER_ID,
    "NAME",
    EMAIL,
    PHONE,
    "ADDRESS",
    CITY,
    "STATE",
    ZIPCODE,
    COUNTRY,
    AGE,
    GENDER,
    CUSTOMER_SEGMENT,
    TRAN_DATE,
    TRAN_TIME,
    PRICE,
    PRODUCT_CATEGORY,
    PRODUCT_BRAND,
    PRODUCT_TYPE,
    FEEDBACK,
    SHIPPING_METHOD,
    PAYMENT_METHOD,
    ORDER_STATUS,
    RATING,
    PRODUCT
    FROM get_dupl WHERE row_num =1



)

,
standarized_txt as
(

    SELECT      
    ID,
    TRANSACTION_ID,
    CUSTOMER_ID,
    INITCAP((TRIM("NAME")))as NAME, 
    LOWER(TRIM(EMAIL))as EMAIL,
    PHONE,
    REPLACE(TRIM("ADDRESS"),' ','-')as "ADDRESS",
    INITCAP(TRIM(CITY)) as city,
    INITCAP(TRIM("STATE")) as "STATE",
    ZIPCODE,
    INITCAP(TRIM(COUNTRY)) as country,
    AGE,
    CASE WHEN LOWER(TRIM(GENDER))  in ('female','f','fem') then 'F' else 'M' end as gender,
    INITCAP(TRIM(CUSTOMER_SEGMENT)) as CUSTOMER_SEGMENT,
    CONCAT(TRAN_DATE,' ',TRAN_TIME)::timestamp as TRANSACTION_TIME,
    PRICE,
    LOWER(TRIM(PRODUCT_CATEGORY)) as product_category,
    INITCAP(TRIM(PRODUCT_BRAND))as product_brand,
    INITCAP(TRIM(PRODUCT_TYPE))as product_type,
    LOWER(TRIM(FEEDBACK)) as feedback,
    INITCAP(TRIM(SHIPPING_METHOD))as shipping_method,
    INITCAP(TRIM(PAYMENT_METHOD))as payment_method,
    LOWER(TRIM(ORDER_STATUS))as order_status,
    case when RATING > 5 then 5.0 else ROUND (RATING,1) end as rating,
   INITCAP(TRIM(PRODUCT)) as product
   
    FROM remove_dupl
),


fillna AS
(
SELECT 
    ID,
    TRANSACTION_ID,
    CUSTOMER_ID,
    "NAME",
    EMAIL,
    PHONE,
    CITY,
    "STATE",
    ZIPCODE,
    COUNTRY,
    AGE,
    GENDER,
    COALESCE (CUSTOMER_SEGMENT,'Regular') as CUSTOMER_SEGMENT,
    TRANSACTION_TIME,
    PRICE,
    PRODUCT_CATEGORY,
    PRODUCT_BRAND,
    PRODUCT_TYPE,
    COALESCE(FEEDBACK,(select MODE(FEEDBACK) from standarized_txt))as FEEDBACK,
    COALESCE(SHIPPING_METHOD,(select MODE(SHIPPING_METHOD) from standarized_txt))AS SHIPPING_METHOD,
    PAYMENT_METHOD,
    ORDER_STATUS,
    RATING,
    PRODUCT
    FROM standarized_txt 
    WHERE TRANSACTION_ID IS NOT NULL AND CUSTOMER_ID IS NOT NULL AND "NAME" IS NOT NULL AND EMAIL IS NOT NULL AND PHONE IS NOT NULL AND PRODUCT IS NOT NULL AND COUNTRY IS NOT NULL AND CITY is NOT NULL AND "STATE" IS NOT NULL
)


SELECT * FROM fillna

{% if is_incremental() %}
    WHERE fillna.ID > (SELECT MAX(ID) FROM {{ this }})
{% endif %}
