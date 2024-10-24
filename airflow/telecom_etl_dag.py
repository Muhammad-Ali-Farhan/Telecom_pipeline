from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from extract.extract_call_data import extract_call_data
from transform.clean_call_data import clean_call_data
from load.load_to_data_warehouse import load_to_data_warehouse

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 10, 22),
    'retries': 1,
}

dag = DAG('telecom_etl_pipeline', default_args=default_args, schedule_interval='@daily')

def run_etl():
    call_data = extract_call_data()
    clean_data = clean_call_data(call_data)
    load_to_data_warehouse(clean_data, 'call_records_warehouse')

etl_task = PythonOperator(task_id='run_etl', python_callable=run_etl, dag=dag)
