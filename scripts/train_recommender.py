# scripts/train_recommender.py (EN SAĞLAM VERSİYON)
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from sklearn.metrics.pairwise import cosine_similarity

print("Recommender model training started...")

db_url = "postgresql://neondb_owner:npg_6FqdhW4KVavZ@ep-withered-mouse-a2oy4uj3-pooler.eu-central-1.aws.neon.tech/neondb?sslmode=require"
conn = None

try:
    conn = psycopg2.connect(db_url)
    cursor = conn.cursor()
    print("Bulut veritabanina basariyla baglanildi.")
    
    query = "SELECT user_id, movie_id, rating FROM public.fct_ratings"
    cursor.execute(query)
    
    # --- YENİ VE GARANTİLİ KISIM ---
    # Veriyi ham olarak çekip, manuel olarak DataFrame'e dönüştürüyoruz.
    data = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    ratings = pd.DataFrame(data, columns=columns)
    # --- BİTTİ ---
    
    if ratings.empty:
        print("Not enough rating data to train recommender model.")
    else:
        # Veritabanına yazma işlemi için SQLAlchemy motorunu burada oluşturuyoruz
        engine = create_engine(db_url)

        # Veri tiplerini doğru ayarlayalım
        ratings['user_id'] = pd.to_numeric(ratings['user_id'])
        ratings['movie_id'] = pd.to_numeric(ratings['movie_id'])
        ratings['rating'] = pd.to_numeric(ratings['rating'])

        user_movie_matrix = ratings.pivot_table(index='user_id', columns='movie_id', values='rating').fillna(0)
        user_similarity = cosine_similarity(user_movie_matrix)
        user_similarity_df = pd.DataFrame(user_similarity, index=user_movie_matrix.index, columns=user_movie_matrix.index)

        recommendations = []
        for user_id in user_movie_matrix.index:
            similar_users = user_similarity_df[user_id].drop(user_id).sort_values(ascending=False)
            if similar_users.empty: continue
            most_similar_user_id = similar_users.index[0]
            similar_user_ratings = user_movie_matrix.loc[most_similar_user_id]
            user_ratings = user_movie_matrix.loc[user_id]
            recommended_movies = similar_user_ratings[(similar_user_ratings >= 4) & (user_ratings == 0)]
            for movie_id, rating in recommended_movies.head(5).items():
                recommendations.append({'user_id': user_id, 'recommended_movie_id': int(movie_id)})

        if recommendations:
            recs_df = pd.DataFrame(recommendations)
            recs_df.to_sql("ai_recommendations", engine, if_exists="replace", index=False)
            print(f"Successfully generated and saved {len(recs_df)} recommendations.")
        else:
            print("No new recommendations could be generated.")
            
except Exception as e:
    print(f"An error occurred during recommender model training: {e}")
finally:
    if conn:
        conn.close()
        print("Database connection closed for recommender.")