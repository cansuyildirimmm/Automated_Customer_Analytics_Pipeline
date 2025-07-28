with source as (
    select * from {{ source('public', 'movie_genres') }}
)

select
    movie_id,
    genre_id
from source
