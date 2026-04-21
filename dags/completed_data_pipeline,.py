import pandas as pd 
from airflow import DAG
from airflow.models.param import Param
from airflow.decorators import task
from airflow.providers.standard.sensors.filesystem import FileSensor
from datetime import datetime
import time
from pathlib import Path
import logging 
import shutil

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

#INPUT_FILE_NAME ="/opt/airflow/data/member_addressaa.csv" 





#FILE_PATH = "/opt/airflow/data/test.csv"

REQUIRED_HEADERS = ["t_name","t_full_desc_mline","i_id","i_mem_master_id","t_add_type","t_add1","t_add2","t_add3","t_add4","t_city_id","t_area_id","t_country_id","t_zip","i_active","created_at","created_by","created_center","updated_at","updated_by","updated_center","t_remark","i_address_edit_counter","last_address_updated_date_at"]

with DAG(
    
    dag_id="completed_data_pipeline",
    start_date=datetime(2024,1,1),
    #schedule="@daily",
    catchup=False,
    params={
        "input_file":Param("/opt/airflow/data/member_data.csv",type="string"),
        "output_file":Param("/opt/airflow/data/output_data.csv",type="string"),
        "failed_file":Param("/opt/airflow/data/logs/failed_log.csv",type="string"),
        "original_folder":Param("/opt/airflow/data/original",type="string")
    }
    
    ) as dag:
    
    
    #main funciton
    
    def completed_data_pipeline():
    
    #waiting for file to be there 
        t1 = FileSensor(task_id="wait_for_file",
                        filepath="{{ params.input_file }}",
                        poke_interval=30,
                        timeout=600,
                        mode="reschedule")

        @task
        def load_data(**context)->str:
              input_file = context['params']['input_file']
              path = Path(input_file)
              
              if not path.exists():
                  raise FileNotFoundError(f"File Not Found error:{path}")
              
              return str(path)

        @task
        def validate_data(input_file:str)->str:
            #read the csv
            df = pd.read_csv(input_file,encoding="latin1")
            
            #checking if dataframe is empty
            if df.empty:
                raise ValueError("Input DataFrame is empty")
            
            
            #check if the missing columns
            missing = [col for col in REQUIRED_HEADERS if col not in df.columns]
            if missing:
                raise KeyError(f"Missing Header:{missing}")
            
            #return back the inputn file
            return input_file
            
            

        @task
        def process_data(input_file:str,**context)-> str :
            
            output_file= context['params']['output_file']
            failed_file= context['params']['failed_file']
            
            #open file
            df = pd.read_csv(input_file,encoding="latin1").copy()
            failed_path = Path(failed_file);
            failed_path.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
            
            
            #this like extract zip code from t_add2 column
            df['postcode_get'] = df['t_add2'].fillna("").astype(str).str.extract(r"(\d{5})")
            
            
            #this will take the extracted post code and fill all empty
            df['t_zip'] = df['t_zip'].fillna(df['postcode_get'])
            
            
            #creating a mask
            valid_mask = df['t_zip']\
    .fillna("")\
    .astype(str)\
    .str.strip()\
    .str.fullmatch(r"\d{5}")
            
            df_valid_data = df[valid_mask]
            df_invalid_data = df[~valid_mask]
            
            
           
            
            logging.info(
                "valid_rows=%s invalid_rows=%s",
                len(df_valid_data),
                len(df_invalid_data)
            )
            
            '''this put all the invalid data toa csv file'''
            df_invalid_data.to_csv(failed_path,index=False)
            
            
           
            df_valid_data_unique = df_valid_data.drop_duplicates(subset=["i_mem_master_id"],keep='first')   
            df_valid_data_unique.to_csv(output_file,index=False)
            Path(output_file).parent.mkdir(parents=True, exist_ok=True)
            
            return str(output_file)
            
                
        
        @task
        def archive_data(input_file:str,**context):
            
            TODAY= datetime.now()
            TODAY_FORMATTED = TODAY.strftime(("%d_%m_%Y"))
            TS_TIMESTAMP = int(time.time())

            folder = context['params']['original_folder']
            archive_folder = Path(folder)/TODAY_FORMATTED
            
            archive_folder.mkdir(mode=0o755, parents=True, exist_ok=True)
            
            
            src =Path(input_file)
            
            if not src.exists():
                 raise FileNotFoundError(f"File missing before archive:{src}")
            
            dest = Path(archive_folder)/f"{TS_TIMESTAMP}.csv"
            
            moved_file = shutil.move(str(src), str(dest))
            logging.info(
    "event=archive_file src=%s dest=%s",
    src,
    moved_file
)
            
            
            return str(moved_file)
        
           
        
        existed_data = t1>>load_data()
        loaded_data=validate_data(existed_data)
        processed_data=process_data(loaded_data)
        archived_data = archive_data(loaded_data)
    
    
    completed_data_pipeline()
    