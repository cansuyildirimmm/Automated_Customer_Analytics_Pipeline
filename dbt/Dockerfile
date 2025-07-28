FROM python:3.7-slim

# pip güncelle
RUN pip install --upgrade pip

# git ve diğer temel bağımlılıkları yükle
RUN apt-get update && apt-get install -y git && apt-get clean && rm -rf /var/lib/apt/lists/*

# dbt-postgres ve bağımlılıkları birlikte yükle (psycopg2-binary ile)
RUN pip install dbt-postgres==1.0.1 psycopg2-binary

# Sonra markupsafe ve jinja2 sürümünü sabitle (overwrite)
RUN pip install markupsafe==2.0.1 jinja2==3.0.3

# Çalışma klasörünü ayarla
WORKDIR /usr/app

# Komut satırında kal
CMD ["bash"]
