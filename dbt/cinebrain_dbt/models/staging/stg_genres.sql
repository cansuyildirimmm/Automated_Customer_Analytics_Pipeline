with source as (
    select * from {{ source('public', 'genres') }}
)

select
    genre_id,
    genre_name
from source