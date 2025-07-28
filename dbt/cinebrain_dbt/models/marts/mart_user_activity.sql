-- models/marts/mart_user_activity.sql
-- Bu mart, her bir kullanıcının platformdaki aktivitesini özetler.

WITH user_ratings AS (
    SELECT
        user_id,
        rating,
        CAST(rated_at AS TIMESTAMP) AS rated_at_ts
    FROM {{ ref('fct_ratings') }}
),
user_activity_calculations AS (
    SELECT
        user_id,
        COUNT(rating) AS total_movies_rated,
        AVG(rating) AS average_rating_given,
        MIN(rated_at_ts) AS first_rating_date,
        MAX(rated_at_ts) AS last_rating_date,
        DATE_PART('day', MAX(rated_at_ts) - MIN(rated_at_ts)) AS activity_duration_days
    FROM user_ratings
    GROUP BY user_id
)
SELECT
    u.user_id,
    u.signup_date,
    u.country,
    u.age_range,
    u.gender,
    COALESCE(a.total_movies_rated, 0) AS total_movies_rated,
    ROUND(COALESCE(a.average_rating_given, 0), 2) AS average_rating_given,
    a.first_rating_date,
    a.last_rating_date,
    COALESCE(a.activity_duration_days, 0) AS activity_duration_days
FROM {{ ref('stg_users') }} u
LEFT JOIN user_activity_calculations a ON u.user_id = a.user_id