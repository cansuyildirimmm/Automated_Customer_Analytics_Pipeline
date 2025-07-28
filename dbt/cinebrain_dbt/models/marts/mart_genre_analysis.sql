-- models/marts/mart_genre_analysis.sql
WITH movie_performance AS (
    -- Bir önceki martımızı yeniden kullanıyoruz! Bu dbt'nin gücüdür.
    SELECT * FROM {{ ref('mart_movie_performance') }}
),

-- Filmler ve türlerini ayıran CTE (her film-tür ikilisi için bir satır)
movies_with_genres_unnested AS (
    SELECT
        movie_id,
        title,
        -- PostgreSQL'in UNNEST fonksiyonu, {'Action', 'Sci-Fi'} gibi bir diziyi
        -- iki ayrı satıra dönüştürür: (movie_id, 'Action'), (movie_id, 'Sci-Fi')
        UNNEST(genres) AS genre
    FROM movie_performance
    WHERE ARRAY_LENGTH(genres, 1) > 0 -- Türü olmayan filmleri dahil etme
)
-- Her tür için metrikleri hesaplıyoruz
SELECT
    g.genre,
    COUNT(DISTINCT g.movie_id) AS number_of_movies,
    SUM(p.total_ratings) AS total_ratings_for_genre,
    ROUND(AVG(p.average_rating), 2) AS average_rating_for_genre,
    SUM(p.count_positive_ratings) AS total_positive_ratings_for_genre,
    SUM(p.count_negative_ratings) AS total_negative_ratings_for_genre
FROM movies_with_genres_unnested g
LEFT JOIN movie_performance p ON g.movie_id = p.movie_id
GROUP BY g.genre
ORDER BY total_ratings_for_genre DESC