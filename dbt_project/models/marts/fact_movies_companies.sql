with movies as (
    select * from {{ source('movies_sources', 'dim_movies') }}
),
companies as (
    select * from {{ source('movies_sources', 'dim_companies') }}
),
movies_companies as (
    select * from {{ source('movies_sources', 'bridge_movie_companies') }}
)

select
    m.movie_id,
    m.title,
    m.release_date,
    fm.company_id,
    c.company_name
from
    movies m
    left join movies_companies fm on m.movie_id = fm.movie_id
    left join companies c on fm.company_id = c.company_id