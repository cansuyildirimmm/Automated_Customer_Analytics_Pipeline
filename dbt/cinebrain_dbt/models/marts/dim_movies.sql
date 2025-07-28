-- models/marts/dim_movies.sql
-- Bu model, her film için bir ana kayıt oluşturur. Analizleri hızlandırmak için
-- filmlerle ilgili tüm temel bilgileri ve türlerini tek bir yerde birleştirir.

WITH movies AS (
    SELECT * FROM {{ ref('stg_movies') }}
),
movie_genres AS (
    SELECT * FROM {{ ref('stg_movie_genres') }}
),
genres AS (
    SELECT * FROM {{ ref('stg_genres') }}
)
SELECT
    m.movie_id,
    m.title,
    m.release_year,
    m.type,
    m.director,
    m.duration,
    m.rating AS age_rating, -- "rating" kelimesi karışıklık yaratmasın diye yeniden adlandıralım
    -- PostgreSQL'e özel ARRAY_AGG, bir filme ait tüm türleri bir dizi (array) olarak toplar.
    COALESCE(ARRAY_AGG(g.genre_name ORDER BY g.genre_name), '{}') AS genres
FROM movies m
LEFT JOIN movie_genres mg ON m.movie_id = mg.movie_id
LEFT JOIN genres g ON mg.genre_id = g.genre_id
GROUP BY m.movie_id, m.title, m.release_year, m.type, m.director, m.duration, m.rating