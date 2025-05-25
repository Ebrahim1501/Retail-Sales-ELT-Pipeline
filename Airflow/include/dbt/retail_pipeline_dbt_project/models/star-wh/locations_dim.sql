{{ config(schema='DATA_MODELS') }}


select row_number() over(order by country,city) as LOCATION_ID,
COUNTRY,
CITY,
"STATE"
FROM(select DISTINCT COUNTRY,CITY,"STATE" FROM {{ ref('data_transformation') }}) 


{% if is_incremental() %}
    WHERE (COUNTRY, CITY, "STATE") NOT IN 
    (
    SELECT COUNTRY, CITY, "STATE" FROM {{ this }}
    )

{% endif %}
