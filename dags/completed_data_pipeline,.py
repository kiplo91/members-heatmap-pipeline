import pandas as pd 
from airflow import DAG
from airflow.decorators import task
from airflow.providers.standard.sensors.filesystem import FileSensor
from datetime import datetime
import time
from pathlib import Path

INPUT_FILE_NAME ="member_address.csv" 
INPUT_FILE_PATH = "/opt/airflow/data/member_address.csv"
PROCCESSED_FILE_PATH = "/opt/airflow/data/original"
CLEANED_FILE_PATH = "/opt/airflow/data/cleaned"
OUTPUT_PATH = "/opt/airflow/data/read_output.csv"


TODAY= datetime.now()
TODAY_FORMATTED = TODAY.strftime(("%d_%m_%Y"))
TS_TIMESTAMP = int(time.time())

#FILE_PATH = "/opt/airflow/data/test.csv"

with DAG(
    
    dag_id="completed_data_pipeline",
    start_date=datetime(2024,1,1),
    #schedule="@daily",
    catchup=False
    
    ) as dag:
    
    
    fileCheck = FileSensor(task_id="wait_for_excel" ,
                           fs_conn_id="fs_default",
                           filepath=INPUT_FILE_NAME,
                           poke_interval=60,
                            timeout=3600,
                        mode="reschedule"
                           )
    
    
    @task
    def read_data():
        try:
           
            source = Path(INPUT_FILE_PATH)
            
            if not source.exists():
                raise FileNotFoundError(f"{INPUT_FILE_PATH} does not exist")
           
            df = pd.read_csv(INPUT_FILE_PATH,encoding="latin1")
            
            
            if df.empty:
                raise Exception(f"No data inside {INPUT_FILE_PATH}")  
            
                      
            df.to_csv(OUTPUT_PATH,index=False)
            
            
            folderName=Path(PROCCESSED_FILE_PATH) /TODAY_FORMATTED 
            folderName.mkdir(parents=True,exist_ok=True)
            print(f"Using folder {folderName}")
            
            
            
            destination = folderName/f"raw_{TS_TIMESTAMP}.csv"
            newDest = source.rename(destination)
            print(f"Successfull Read, Transfer to Location {newDest}")
            
        except FileNotFoundError as e:
            raise Exception(f"Wrong File or Path not Found : {e}")
        except Exception as e:
            raise Exception (f"Unexpected error occured:{e}")
        
    @task
    def clean_data():
        try:
            df = pd.read_csv(OUTPUT_PATH,encoding="latin1")
            if df.empty:
                raise Exception (f"No data inside {OUTPUT_PATH}")
            
            print("Before Cleaning:", len(df))  
            df['postcode_get']  = df['t_add2'].str.extract(r'(\d{5})')
            
            df['t_zip'] = df['t_zip'].fillna(df['postcode_get'])
            
            df = df[df['t_zip']!='']
            
            uniqDf = df.drop_duplicates(subset='i_mem_master_id',keep="first")
            
            folderName=Path(CLEANED_FILE_PATH)/TODAY_FORMATTED 
            folderName.mkdir(parents=True,exist_ok=True)
            print(f"Using folder {folderName}")
            
            
            cleanedFilePath = folderName/f"cleaned_{TS_TIMESTAMP}.csv"
            uniqDf.to_csv(cleanedFilePath,index=False)
            
            print("After Cleaning:", len(df))   
            print(f"Successfull Clean, New Location is {cleanedFilePath}")
            
        
        except FileNotFoundError as  e:
            raise Exception(f"Wrong File or path not found:{e}")     
        except Exception as e:
            raise Exception (f"Unexpected error occured:{e}")
        

        
        
        
    
    
    
    fileCheck>>read_data()>>clean_data()
    