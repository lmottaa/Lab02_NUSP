-- Esquema da Camada Gold (Business/Warehouse)
-- Modelagem: Snowflake Schema com Tabelas Bridge para relações N:N

-- 1. Dimensões Básicas
CREATE TABLE IF NOT EXISTS dim_movies (
    movie_id INT PRIMARY KEY,
    title VARCHAR(500) NOT NULL,
    release_date DATE,
    release_year INT,
    runtime INT,
    adult BOOLEAN,
    original_language VARCHAR(10),
    status VARCHAR(50)
);

CREATE TABLE IF NOT EXISTS dim_genres (
    genre_id SERIAL PRIMARY KEY,
    genre_name VARCHAR(100) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_companies (
    company_id SERIAL PRIMARY KEY,
    company_name VARCHAR(255) UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_countries (
    country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(100) UNIQUE NOT NULL
);

-- 2. Tabela Fato (Métricas de Performance)
CREATE TABLE IF NOT EXISTS fact_movie_performance (
    movie_id INT PRIMARY KEY REFERENCES dim_movies(movie_id),
    budget NUMERIC(15, 2),
    revenue NUMERIC(15, 2),
    popularity NUMERIC(12, 4),
    vote_average NUMERIC(4, 2),
    vote_count INT,
    profit NUMERIC(15, 2) GENERATED ALWAYS AS (revenue - budget) STORED,
    roi NUMERIC(15, 4) GENERATED ALWAYS AS (
        CASE WHEN budget > 0 THEN (revenue - budget) / budget ELSE NULL END
    ) STORED
);

-- 3. Tabelas Bridge (Relacionamentos N:N)
CREATE TABLE IF NOT EXISTS bridge_movie_genres (
    movie_id INT REFERENCES dim_movies(movie_id),
    genre_id INT REFERENCES dim_genres(genre_id),
    PRIMARY KEY (movie_id, genre_id)
);

CREATE TABLE IF NOT EXISTS bridge_movie_companies (
    movie_id INT REFERENCES dim_movies(movie_id),
    company_id INT REFERENCES dim_companies(company_id),
    PRIMARY KEY (movie_id, company_id)
);

CREATE TABLE IF NOT EXISTS bridge_movie_countries (
    movie_id INT REFERENCES dim_movies(movie_id),
    country_id INT REFERENCES dim_countries(country_id),
    PRIMARY KEY (movie_id, country_id)
);

-- Índices para otimização de queries analíticas
CREATE INDEX IF NOT EXISTS idx_movies_year ON dim_movies(release_year);
CREATE INDEX IF NOT EXISTS idx_fact_roi ON fact_movie_performance(roi);
CREATE INDEX IF NOT EXISTS idx_fact_popularity ON fact_movie_performance(popularity);