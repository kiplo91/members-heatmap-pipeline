import pandas as pd 
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

FILE_PATH = "/opt/airflow/data/member_address.csv"
CLEANED_FILE = "/opt/airflow/data/customer_cleaned.csv"
HEATMAP_FILE = "/opt/airflow/data/heatmap_output.csv"
LATLONG_FILE = "/opt/airflow/data/latlong.csv"
CLEANED_LATLONG_FILE = "/opt/airflow/data/cleaned_latlong.csv"
HEATMAP_FINAL_FILE = "/opt/airflow/data/heatmap_final.csv"


def read_csv():
    df = pd.read_csv(FILE_PATH,encoding="latin1")
    print(f"File is read from {FILE_PATH}")
    print(df)

def clean_csv():
    df = pd.read_csv(FILE_PATH,encoding="latin1")
    print("Before Cleaning:", len(df))  
    
    #extract postcode from t_add2
    df['postcode_get'] = df['t_add2'].str.extract(r'(\d{5})')
    
    #fill in the missing postcode
    df['t_zip']= df['t_zip'].fillna(df['postcode_get'])
    
    #remove invalid rows 
    df = df[df['t_zip']!=''] 
    
    # remove dedundancy to based on i_mem_master
    uniqueDf = df.drop_duplicates(subset=['i_mem_master_id'],keep='first')
    
    print("After Cleaning:",len(df)) 
    uniqueDf.to_csv(CLEANED_FILE,index=False)  
    print(f"Saved cleaned file to {CLEANED_FILE}")
    
def aggregate_csv():
    df = pd.read_csv(CLEANED_FILE,encoding="latin1")
    grouped = df.groupby("t_area_id").size().reset_index(name="member_count")
    grouped.head()
    grouped.to_csv(HEATMAP_FILE,index=False)
    print("Saved heatmap output")
    
def reformat_lat_lag_csv():
    state_map={
        "SARAWAK":"SWK",
        "JOHOR":"JOH",
        "N.SEMBILAN":"NES",
        "PAHANG":"PAH",
        "PERAK":"PRK",
        "SELANGOR":"SEL",
        "PENANG":"PEN",
        "PERLIS":"PEL",
        "MELAKA":"MEL",
        "TERENGGANU":"TER",
        "KEDAH":"KDH",
        "WP KUALA LUMPUR":"KUL",
        "WP LABUAN":"LBN",
        "WP PUTRAJAYA":"PUT",
        "WP LABUAN":"LBN",
        "SABAH":"SBH",
        "KELANTAN":"KEL",
        
    }
    df = pd.read_csv(LATLONG_FILE,encoding="latin1")
    
    df['States'] = df['States'].str.strip().str.upper() 
    df['t_area_id'] = df['States'].map(state_map)
    
    
    #changing the latLon to numeric
    df['Lat'] = pd.to_numeric(df['Lat'],errors="coerce")
    df['Lon'] = pd.to_numeric(df['Lon'],errors="coerce")
    
    df = df.dropna(subset=["Lat","Lon"])

    state_ref = (
        df.groupby("t_area_id")[["Lat","Lon"]]
        .mean()
        .reset_index()
    )    

    print(state_ref.head())
    
    state_ref.to_csv(CLEANED_LATLONG_FILE, index=False)

def merge_data():
    df_main = pd.read_csv(HEATMAP_FILE,encoding="latin1")
    df_ref = pd.read_csv(CLEANED_LATLONG_FILE,encoding="latin1")
    
    merged =  df_main.merge(df_ref,on="t_area_id",how="left")
    print(merged[merged["Lat"].isna()])
    print(merged.head()) 
    merged.to_csv(HEATMAP_FINAL_FILE, index=False)   
    
    print(f'added to {HEATMAP_FINAL_FILE}')

with DAG(
    dag_id='members_heatmap_pipeline',
    start_date=datetime(2024,1,1),
    catchup=False
    
) as dag:
    
    task1 = PythonOperator(
        task_id="extract_data_task",
        python_callable=read_csv
    )
    
    reformatTask = PythonOperator(
        task_id="reformat_lat_lag_task",
        python_callable=reformat_lat_lag_csv 
    )
    
    task2 = PythonOperator(
        task_id="clean_data_task",
        python_callable=clean_csv
    )
    
    task3 = PythonOperator(
        task_id="aggregate_data_task",
        python_callable=aggregate_csv
    )
    
    task4 = PythonOperator(
        task_id="merge_data_task",
        python_callable=merge_data
    )
    
    task1>>[task2,reformatTask]>>task3>>task4