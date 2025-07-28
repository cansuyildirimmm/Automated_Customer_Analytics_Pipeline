-- models/marts/recommendations/user_recommendations.sql
-- Bu model, yapay zeka tarafından üretilen ham öneri verilerini alır
-- ve bunları kullanıcı ve film detaylarıyla birleştirerek son kullanıcı için
-- zenginleştirilmiş bir tablo oluşturur.

WITH raw_recommendations AS (
    -- AI script'inin oluşturduğu tablodan veriyi kaynak olarak alıyoruz
    SELECT * FROM {{ source('public', 'ai_recommendations') }}
),

dim_movies AS (
    SELECT * FROM {{ ref('dim_movies') }}
),

stg_users AS (
    -- Hata buradaydı! Var olmayan "user_name" yerine,
    -- kullanıcının diğer demografik bilgilerini seçiyoruz.
    SELECT 
        user_id,
        signup_date,
        country,
        age_range,
        gender
    FROM {{ ref('stg_users') }}
)

SELECT
    r.user_id,
    u.signup_date,
    u.country,
    u.age_range,
    u.gender,
    r.recommended_movie_id,
    m.title AS recommended_movie_title,
    m.genres AS recommended_movie_genres
FROM raw_recommendations r
LEFT JOIN stg_users u ON r.user_id = u.user_id
LEFT JOIN dim_movies m ON r.recommended_movie_id = m.movie_id
ORDER BY
    r.user_id