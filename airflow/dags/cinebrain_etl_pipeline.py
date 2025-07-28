from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime

def run_python_script(script_path: str):
    import subprocess
    subprocess.run(["python", script_path], check=True)

with DAG(
    dag_id='cinebrain_end_to_end_pipeline',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['cinebrain', 'elt'],
) as dag:

    # Görev 1: Ham veriyi PostgreSQL'e yükle
    load_raw_data = PythonOperator(
        task_id='load_csv_to_postgres',
        python_callable=run_python_script,
        op_kwargs={'script_path': '/opt/airflow/scripts/load_to_postgres.py'}
    )

    # Görev 2: dbt modellerini çalıştır (Doğru Yöntem)
    # Bu görev, geçici bir dbt container'ı başlatır ve işi bitince kapatır.
    run_dbt_models = DockerOperator(
        task_id='run_dbt_models',
        image='ghcr.io/dbt-labs/dbt-postgres:1.6.0',
        container_name='task___run_dbt_models',
        auto_remove=True,
        command="bash -c 'dbt run --project-dir /usr/app/cinebrain_dbt'",
        volumes=['./dbt/cinebrain_dbt:/usr/app/cinebrain_dbt'],
        network_mode='data-pipeline-project_default'
    )

    load_raw_data >> run_dbt_models