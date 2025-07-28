# scripts/train_segmentation_model.py (EN SAĞLAM VERSİYON)
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

print("User segmentation model training started...")

db_url = "postgresql://neondb_owner:npg_6FqdhW4KVavZ@ep-withered-mouse-a2oy4uj3-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require"
conn = None

try:
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()
    print("Bulut veritabanina basariyla baglanildi.")
    
    query = """
    SELECT 
        user_id, total_movies_rated, average_rating_given, activity_duration_days 
    FROM public.mart_user_activity WHERE total_movies_rated > 0
    """
    cursor.execute(query)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    features_df = pd.DataFrame(data, columns=columns)

    if len(features_df) >= 4:
        engine = create_engine(db_url)

        # Veri tiplerini doğru ayarlayalım
        for col in ['user_id', 'total_movies_rated', 'average_rating_given', 'activity_duration_days']:
            features_df[col] = pd.to_numeric(features_df[col])

        user_ids = features_df['user_id']
        numerical_features = features_df[['total_movies_rated', 'average_rating_given', 'activity_duration_days']].fillna(0)
        scaler = StandardScaler()
        scaled_features = scaler.fit_transform(numerical_features)
        kmeans = KMeans(n_clusters=4, random_state=42, n_init='auto')
        clusters = kmeans.fit_predict(scaled_features)
        segment_map = {0: 'Casual Watchers', 1: 'Power Users', 2: 'Newbies', 3: 'Critics'}
        segments = [segment_map.get(c, 'Unknown') for c in clusters]
        results_df = pd.DataFrame({'user_id': user_ids, 'segment_name': segments})
        results_df.to_sql("ai_user_segments", engine, if_exists="replace", index=False)
        print(f"Successfully segmented {len(results_df)} users.")
    else:
        print("Not enough data to perform segmentation.")

except Exception as e:
    print(f"An error occurred during segmentation model training: {e}")
finally:
    if conn:
        conn.close()
        print("Database connection closed for segmentation.")