-- models/marts/mart_movie_performance.sql
-- Bu mart, her bir film için performans metriklerini önceden hesaplar.

WITH ratings_summary AS (
    SELECT
        movie_id,
        COUNT(rating) AS total_ratings,
        AVG(rating) AS average_rating,
        COUNT(CASE WHEN rating >= 4 THEN 1 END) AS count_positive_ratings,
        COUNT(CASE WHEN rating <= 2 THEN 1 END) AS count_negative_ratings
    FROM {{ ref('fct_ratings') }}
    GROUP BY movie_id
)
SELECT
    d.movie_id,
    d.title,
    d.release_year,
    d.genres,
    COALESCE(s.total_ratings, 0) AS total_ratings,
    ROUND(COALESCE(s.average_rating, 0), 2) AS average_rating,
    COALESCE(s.count_positive_ratings, 0) AS count_positive_ratings,
    COALESCE(s.count_negative_ratings, 0) AS count_negative_ratings,
    CASE
        WHEN COALESCE(s.total_ratings, 0) > 0
        THEN ROUND((COALESCE(s.count_positive_ratings, 0) * 1.0 / s.total_ratings) * 100, 2)
        ELSE 0
    END AS approval_percentage
FROM {{ ref('dim_movies') }} d
LEFT JOIN ratings_summary s ON d.movie_id = d.movie_id