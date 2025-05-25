{{ config(schema='DATA_MODELS') }}


select row_number() over(order by "NAME") as USER_ID,
NAME,
PHONE,
EMAIL,
AGE,
GENDER,
CUSTOMER_SEGMENT

FROM(select DISTINCT NAME,PHONE,EMAIL,AGE,GENDER,CUSTOMER_SEGMENT FROM {{ ref('data_transformation') }} WHERE "NAME" IS NOT NULL AND PHONE IS NOT NULL  ) 


{% if is_incremental() %}
    WHERE ("NAME", PHONE, EMAIL, AGE) NOT IN 
    (
    SELECT "NAME", PHONE, EMAIL, AGE FROM {{ this }}
    )

{% endif %}
