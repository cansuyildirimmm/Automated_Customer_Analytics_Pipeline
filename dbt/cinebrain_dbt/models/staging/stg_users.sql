-- models/staging/stg_users.sql

with source as (

    select * from {{ source('public', 'users') }}

)

select
    -- Ham tablodan sadece var olan sütunları seçiyoruz.
    "user_id",
    "signup_date",
    "country",
    "age_range",
    "gender"

from source
