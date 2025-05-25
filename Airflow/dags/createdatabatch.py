from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
import sys
import os
import pandas as pd

project_path=os.path.join(os.path.dirname(__file__), '..','..','include','projects','wallmart_project')
sys.path.append(os.path.abspath(project_path))
sys.path.append('/opt/airflow/include/projects/wallmart_project')
#print(os.getcwd())
import datagenerator #a handmade python script that generates databatches in multiple formats (JSON,txt,CSV) 


df=pd.read_csv(os.path.join(project_path,"retail_data.csv"))


def create_batch():
    current_date = str(datetime.today().date())
    batch=datagenerator.generate_data_from(df)
    filename=os.path.join(project_path,"data",f"{current_date}.csv")
    if not os.path.exists(filename):
        batch.to_csv(filename, index=False)
        print(f"Batch file created: {filename}")
    else:
        print("batch file already exists")




    
generatordag=DAG(dag_id="wallmart_batches_generator",
 schedule="0 10 * * *",
tags=["generate","retail","wallmart","ELT"],
start_date=datetime(2025,1,1,0,0),
end_date=None,catchup=False)

task1=PythonOperator(
task_id="Generate_data_batch",
python_callable=create_batch,
dag=generatordag)

task2=LocalFilesystemToS3Operator(task_id="Upload_Batches_To_S3",
filename=os.path.join(project_path,"data",f"{datetime.today().date()}.csv"),
dest_bucket="wallmart-sales-ter3a",
dest_key=os.path.join("data",f"{datetime.today().date()}.csv"),
aws_conn_id='s3_conn',
dag=generatordag
    
)


task1>>task2
