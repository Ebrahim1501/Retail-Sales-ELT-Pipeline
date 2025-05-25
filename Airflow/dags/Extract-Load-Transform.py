from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import sys
import os


project_path=os.path.join(os.path.dirname(__file__), '..','..','include','projects','wallmart_project')
dbt_path=os.path.join(project_path,'dbt','wallmart_retail_pipeline')
soda_path=os.path.join(project_path,'soda')
soda_config_name="snowflake_configuration.yaml"
soda_connection_name='snowflake'

 
ELT_Dag=DAG(dag_id="wallmart-ELT-Pipeline",start_date=datetime(2025,4,21,0,0),end_date=None,schedule="0 23 * * * ",catchup=False)

task1=SQLExecuteQueryOperator(task_id="Load_Data_Into_Raw_Table",
                        conn_id="snowflake_conn",
                        sql="""use database salesdb;
                        use schema raw;
                        COPY INTO raw_data(TRANSACTION_ID,CUSTOMER_ID,NAME,EMAIL,PHONE,ADDRESS,CITY,STATE,ZIPCODE,COUNTRY,AGE,GENDER,CUSTOMER_SEGMENT,TRAN_DATE,TRAN_TIME,PRICE,PRODUCT_CATEGORY,PRODUCT_BRAND,PRODUCT_TYPE,FEEDBACK,SHIPPING_METHOD,PAYMENT_METHOD,ORDER_STATUS,RATING,PRODUCT) FROM @s3_connection/data FILE_FORMAT=(type='csv' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
            ON_ERROR = 'CONTINUE';
                        """,
                        parameters={"database":"SALESDB",
                        "warehouse":"COMPUTE WH",
                        "schema":"RAW",
                        "role":"airflow_role"},
                        dag=ELT_Dag
    
)
task2=BashOperator(
    task_id='Transform_Data_In_Raw_Table',
    bash_command=f""" cd {dbt_path} && dbt run --select transformed """,dag=ELT_Dag                   
)

task3=BashOperator(
    task_id='Build_Dims_Tables',
    bash_command=f""" cd {dbt_path} && dbt run --select customers_dim locations_dim products_dim """,dag=ELT_Dag                   
)

task4=BashOperator(
    task_id='Quality_Checks_For_Dim_Tables',
    bash_command=f""" cd {soda_path} && soda scan -d {soda_connection_name} -c {soda_config_name} {os.path.join('checks','transformed_checks.yaml')} """,
    dag=ELT_Dag                   
)


task5=BashOperator(
    task_id='Build_Fact_Tables',
    bash_command=f""" cd {dbt_path} && dbt run --select sales_fact """ #run sales_fact.sql
    ,dag=ELT_Dag                   
)


task6=BashOperator(
    task_id='Quality_checks_For_Fact_Tables',
    bash_command=f""" cd {soda_path} && soda scan -d {soda_connection_name} -c {soda_config_name} {os.path.join('checks','fact_checks.yaml')} """,
    dag=ELT_Dag                   
)


task7=BashOperator(
    task_id='Build_Data_Marts',
    bash_command=f""" cd {dbt_path} && dbt run --select marts """   #run models inside the /marts directory
    ,dag=ELT_Dag                   
)


task1 >> task2 >> task3 >> task4 >> task5 >> task6 >> task7




