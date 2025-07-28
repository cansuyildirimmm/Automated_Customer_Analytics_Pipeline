-- models/staging/stg_user_ratings.sql

with source as (

    select * from {{ source('public', 'user_ratings') }}

)

select
    "user_id",
    "movie_id",
    -- Hata buradaydı! "rating" sütununu metinden tam sayıya (integer) çeviriyoruz.
    CAST("rating" AS INTEGER) AS rating,
    "rating_date"

from source