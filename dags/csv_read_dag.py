import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

FILE_PATH = "/tmp/output.csv"

def create_csv_file():
    data = {
        'Name':['Faisal','Bob','Charlie'],
        'Age':[25,30,31]
    }
    df=pd.DataFrame(data)
    df.to_csv(FILE_PATH,index=False)
    print(f"CSV is written to {FILE_PATH}")

def read_csv_file():
    df= pd.read_csv(FILE_PATH);
    print(df)
    
with DAG(
    dag_id="csv_pipeline",
    start_date=datetime(2024,1,1),
    catchup=False
) as dag:
    
    task1 = PythonOperator(
        task_id='write_task',
        python_callable=create_csv_file
    )
    
    task2 = PythonOperator(
        task_id='read_task',
        python_callable=read_csv_file
    )
    
    task1>>task2