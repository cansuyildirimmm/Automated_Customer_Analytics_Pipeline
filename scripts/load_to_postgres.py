# scripts/load_to_postgres.py (EN SAĞLAM VERSİYON - Parametreli Sorgular)

import pandas as pd
import psycopg2

print("Bulut veritabanina veri yukleme islemi basliyor (En Saglam Yontem)...")

db_url = "postgresql://neondb_owner:npg_6FqdhW4KVavZ@ep-withered-mouse-a2oy4uj3-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require"

conn = None
cursor = None
try:
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()
    print("Bulut veritabanina basariyla baglanildi.")
except Exception as e:
    print(f"Veritabanina baglanirken bir hata olustu: {e}")
    exit()

data_path = "./data/"

tables = {
    "genres": "genres.csv",
    "movie_genres": "movie_genres.csv",
    "movies": "movies.csv",
    "user_ratings": "user_ratings.csv",
    "users": "users.csv"
}

for table_name, file_name in tables.items():
    try:
        df = pd.read_csv(f"{data_path}{file_name}")
        
        cursor.execute(f'DROP TABLE IF EXISTS public."{table_name}";')
        
        cols = ", ".join([f'"{col}" TEXT' for col in df.columns])
        create_table_sql = f'CREATE TABLE public."{table_name}" ({cols});'
        cursor.execute(create_table_sql)

        # --- YENİ VE DAHA SAĞLAM INSERT YÖNTEMİ ---
        # Sorguyu, değerler için yer tutucular (%s) kullanarak hazırlıyoruz.
        column_names = ", ".join([f'"{col}"' for col in df.columns])
        placeholders = ", ".join(['%s'] * len(df.columns))
        insert_sql = f'INSERT INTO public."{table_name}" ({column_names}) VALUES ({placeholders});'

        # Her bir satırı, psycopg2'nin güvenli bir şekilde işlemesi için bir demet (tuple) olarak gönderiyoruz.
        for index, row in df.iterrows():
            cursor.execute(insert_sql, tuple(row.astype(str)))

        print(f"-> '{table_name}' tablosu '{file_name}' dosyasindan basariyla yuklendi.")
    except Exception as e:
        print(f"HATA: '{table_name}' yuklenirken bir sorun olustu: {e}")
    finally:
        # Her döngüden sonra transaction'ı commit et
        conn.commit()

# Bağlantıyı güvenli bir şekilde kapat
if cursor:
    cursor.close()
if conn:
    conn.close()
print("\nVeri yukleme islemi tamamlandi. Baglanti kapatildi.")