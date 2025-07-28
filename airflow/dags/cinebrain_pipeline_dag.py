# from airflow.providers.docker.operators.docker import DockerOperator
# from airflow.operators.python import PythonOperator
# from airflow import DAG
# from datetime import datetime

# # Bu fonksiyon, Python scriptlerini Airflow container'ı içinde çalıştırır
# def run_python_script(script_path: str):
#     import subprocess
#     # check=True, script hata verirse görevin de hata vermesini sağlar
#     subprocess.run(["python", script_path], check=True)

# with DAG(
#     dag_id='cinebrain_end_to_end_pipeline', # Benzersiz ve açıklayıcı bir ID
#     start_date=datetime(2024, 1, 1),
#     schedule_interval=None, # Manuel olarak tetiklenecek
#     catchup=False,
#     doc_md="CineBrain projesi için veri yükleme, dbt dönüşümü ve AI model eğitimini yöneten uçtan uca pipeline.",
#     tags=['cinebrain', 'dbt', 'elt', 'ai'],
# ) as dag:

#     # Görev 1: Veriyi PostgreSQL'e yükle
#     load_raw_data = PythonOperator(
#         task_id='load_csv_to_postgres',
#         python_callable=run_python_script,
#         op_kwargs={'script_path': '/opt/airflow/scripts/load_to_postgres.py'}
#     )

#     # Görev 2: dbt modellerini çalıştır (EN DOĞRU YÖNTEM)
#     # Bu operatör, dbt komutunu çalıştırmak için geçici bir dbt container'ı başlatır.
#     run_dbt_models = DockerOperator(
#         task_id='run_dbt_models',
#         image='ghcr.io/dbt-labs/dbt-postgres:1.6.0',
#         container_name='task___run_dbt_models', # Göreve özel geçici bir isim
#         auto_remove=True, # İş bittikten sonra container'ı otomatik sil
#         # Container içinde çalıştırılacak olan asıl komut
#         command="bash -c 'dbt run --project-dir /usr/app/cinebrain_dbt'",
#         # dbt container'ının proje dosyalarına erişmesi için volume bağlama
#         volumes=['./dbt/cinebrain_dbt:/usr/app/cinebrain_dbt'],
#         # Diğer container'larla aynı ağda çalışmasını sağla
#         network_mode='data-pipeline-project_default'
#     )

#     # Görev 3: AI modelini eğit ve önerileri kaydet
#     train_ai_model = PythonOperator(
#         task_id='train_ai_recommender',
#         python_callable=run_python_script,
#         op_kwargs={'script_path': '/opt/airflow/scripts/train_recommender.py'}
#     )

#     # Görevlerin akışını (bağımlılıklarını) belirle
#     load_raw_data >> run_dbt_models >> train_ai_model