from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import os
import dotenv
import sys
sys.path.append('../i_love_movies_etl')
from extract import extract
from datetime import datetime

dotenv.load_dotenv(dotenv.find_dotenv()) # Carregando variáveis de ambiente para memória
api_key = os.getenv('api_key')

with DAG(dag_id='extract', start_date=datetime(2023, 5, 11)) as dag:
    
    def process_data():
        df = extract(api_key)
        return df

    task1 = PythonOperator(
        task_id='extract',
        python_callable=process_data
    )
