from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def say_hello():
    print("Pipeline is Working!!")
    
def ask_wellbeing():
    print("How are  u? ")


with DAG(
    dag_id="pipeline_working",
    start_date=datetime(2024,1,1),
    catchup=False
) as dag:
    
    
    task1 = PythonOperator(
        task_id="say_hello_task",
        python_callable=say_hello
    )
    
    task2 = PythonOperator(
        task_id="ask_how_are_you_task",
        python_callable=ask_wellbeing
    )
    
    task1>>task2