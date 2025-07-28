from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

default_args = {
    'start_date': datetime(2024, 1, 1)
}

dag = DAG(
    dag_id='etl_dag',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
)

# 1. CSV'den PostgreSQL'e yükleme
load_postgres = PythonOperator(
    task_id='load_csv_to_postgres',
    python_callable=lambda: subprocess.run(["python3", "/opt/airflow/scripts/load_to_postgres.py"]),
    dag=dag
)

# 2. dbt çalıştırma
run_dbt = BashOperator(
    task_id='run_dbt_models',
    bash_command="cd /usr/app/film_dbt && dbt run",
    dag=dag
)

# 3. AI model eğitimi + tahmin
train_model = BashOperator(
    task_id='train_ai_model',
    bash_command="python3 /opt/airflow/scripts/train_recommender.py",
    dag=dag
)

load_postgres >> run_dbt >> train_model

