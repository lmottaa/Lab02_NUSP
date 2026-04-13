with movies as (
    select * from {{ source('movies_sources', 'dim_movies') }}
),
genre as (
    select * from {{ source('movies_sources', 'dim_genres') }}
),
movies_genre as (
    select * from {{ source('movies_sources', 'bridge_movie_genres') }}
)

select
    m.movie_id,
    m.title,
    m.release_date,
    fm.genre_id,
    g.genre_name,
    -- Usando a Macro para duração planejada
    {{ get_profit('m.revenue', 'm.budget') }} as profit
from
    movies m
    left join movies_genre fm on m.movie_id = fm.movie_id
    left join genre g on fm.genre_id = g.genre_id