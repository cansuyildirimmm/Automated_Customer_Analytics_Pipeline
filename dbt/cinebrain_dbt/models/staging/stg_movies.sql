with source as (
    select * from {{ source('public', 'movies') }}
)

select
    movie_id,
    title,
    type,
    director,
    release_year,
    duration,
    rating,
    description
from source
