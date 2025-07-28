# scripts/train_churn_model.py (EN SAĞLAM VERSİYON)
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from sklearn.linear_model import LogisticRegression
import numpy as np

print("Churn prediction model training started...")

db_url = "postgresql://neondb_owner:npg_6FqdhW4KVavZ@ep-withered-mouse-a2oy4uj3-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require"
conn = None

try:
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()
    print("Bulut veritabanina basariyla baglanildi.")

    query = f"""
    WITH latest_date AS (
        SELECT MAX(CAST(rated_at AS TIMESTAMP)) as max_date FROM public.fct_ratings
    )
    SELECT
        user_id, total_movies_rated, average_rating_given, activity_duration_days,
        DATE_PART('day', (SELECT max_date FROM latest_date) - last_rating_date) AS days_since_last_rating
    FROM public.mart_user_activity
    WHERE last_rating_date IS NOT NULL AND total_movies_rated > 0
    """
    cursor.execute(query)
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    features_df = pd.DataFrame(data, columns=columns)

    if not features_df.empty:
        engine = create_engine(db_url)
        
        # Veri tiplerini doğru ayarlayalım
        for col in ['total_movies_rated', 'average_rating_given', 'days_since_last_rating']:
            features_df[col] = pd.to_numeric(features_df[col])

        features_df['is_churn'] = (features_df['days_since_last_rating'] > 90).astype(int)
        X = features_df[['total_movies_rated', 'average_rating_given', 'days_since_last_rating']].fillna(0)
        y = features_df['is_churn']
        if len(np.unique(y)) > 1:
            model = LogisticRegression(class_weight='balanced', max_iter=1000)
            model.fit(X, y)
            churn_probabilities = model.predict_proba(X)[:, 1]
        else:
            churn_probabilities = y.values 
        results_df = pd.DataFrame({'user_id': features_df['user_id'], 'churn_probability': churn_probabilities})
        results_df.to_sql("ai_churn_predictions", engine, if_exists="replace", index=False)
        print(f"Successfully predicted churn risk for {len(results_df)} users.")
    else:
        print("Not enough data to train churn model.")

except Exception as e:
    print(f"An error occurred during churn model training: {e}")
finally:
    if conn:
        conn.close()
        print("Database connection closed for churn model.")