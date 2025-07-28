-- models/marts/fct_ratings.sql
-- Bu model, stg_user_ratings'den veri alarak merkezi olay tablosunu oluşturur.

SELECT
    -- Her bir puanlama olayına eşsiz bir anahtar atıyoruz.
    {{ dbt_utils.generate_surrogate_key(['user_id', 'movie_id']) }} AS rating_id,
    user_id,
    movie_id,
    rating,
    -- Verimizdeki "rating_date"i, veri ambarımızda standart olan "rated_at" ismine dönüştürüyoruz.
    rating_date AS rated_at
FROM {{ ref('stg_user_ratings') }}