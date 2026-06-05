from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
import requests


# Define connection IDs for Airflow connection
DATABRICKS_CONN_ID='databricksconnection'
BRONZE_JOB_ID=973330043141006
SILVER_JOB_ID=362829680347712
GOLD_JOB_ID=189445906639892

# default_args for the DAG
default_args = {
  'owner': 'airflow',
  'retries': 3,
  'retry_delay': timedelta(minutes=5),
  'depends_on_past': False,
  'start_date': datetime.now() - timedelta(days=1)
  
}

with DAG(dag_id='crypto_pipeline',
         schedule = "0 8 * * *",  # everyday at 8AM
         default_args = default_args,
         catchup = False
        ) as dag:
  
  # check if the API is reachable
  def check_api():
        response = requests.get(
            "https://api.coingecko.com/api/v3/ping",
            timeout=10
        )
        if response.status_code != 200:
            raise Exception(f"CoinGecko API unavailable: {response.status_code}")
        print("CoinGecko API is OK")

  task_api_check = PythonOperator(
        task_id="check_api",
        python_callable=check_api
    ) 

  task_bronze = DatabricksRunNowOperator(
    task_id = 'load_bronze',
    databricks_conn_id = DATABRICKS_CONN_ID,
    job_id = BRONZE_JOB_ID
  )


  task_silver = DatabricksRunNowOperator(
    task_id = 'load_silver',
    databricks_conn_id = DATABRICKS_CONN_ID,
    job_id = SILVER_JOB_ID
  )

  task_gold = DatabricksRunNowOperator(
    task_id = 'load_gold',
    databricks_conn_id = DATABRICKS_CONN_ID,
    job_id = GOLD_JOB_ID
  )

  task_api_check >> task_bronze >> task_silver >> task_gold

