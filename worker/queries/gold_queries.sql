-- 1. Qual é o Retorno sobre o Investimento (ROI) dos filmes?
-- Ranking dos 10 filmes mais rentáveis (ROI)
SELECT 
    m.title, 
    f.roi, 
    f.revenue, 
    f.budget 
FROM fact_movie_performance f
JOIN dim_movies m ON f.movie_id = m.movie_id
WHERE f.budget > 0
ORDER BY f.roi DESC
LIMIT 10;

-- 2. Quais gêneros cinematográficos geram o maior lucro absoluto médio?
-- Média de (revenue - budget) agrupada por gênero
SELECT 
    g.genre_name, 
    AVG(f.profit) AS lucro_medio_absoluto
FROM fact_movie_performance f
JOIN bridge_movie_genres b ON f.movie_id = b.movie_id
JOIN dim_genres g ON b.genre_id = g.genre_id
GROUP BY g.genre_name
ORDER BY lucro_medio_absoluto DESC;

-- 3. Produtoras que investem mais (maior budget) têm, necessariamente, maior receita?
-- Ticket médio de receita por faixas de orçamento (Buckets)
WITH budget_buckets AS (
    SELECT 
        CASE 
            WHEN budget < 1000000 THEN '1. Low (< 1M)'
            WHEN budget BETWEEN 1000000 AND 50000000 THEN '2. Medium (1M - 50M)'
            WHEN budget BETWEEN 50000001 AND 150000000 THEN '3. High (50M - 150M)'
            ELSE '4. Blockbuster (> 150M)'
        END AS budget_range,
        revenue
    FROM fact_movie_performance
    WHERE budget > 0
)
SELECT 
    budget_range, 
    AVG(revenue) AS receita_media,
    COUNT(*) AS total_filmes
FROM budget_buckets
GROUP BY budget_range
ORDER BY budget_range;

-- 4. Quais países estão aumentando sua participação na produção de filmes globais?
-- Contagem de lançamentos agrupada por país e ano de lançamento
SELECT 
    c.country_name, 
    m.release_year, 
    COUNT(m.movie_id) AS total_producoes
FROM dim_movies m
JOIN bridge_movie_countries b ON m.movie_id = b.movie_id
JOIN dim_countries c ON b.country_id = c.country_id
WHERE m.release_year IS NOT NULL
GROUP BY c.country_name, m.release_year
ORDER BY c.country_name, m.release_year DESC;

-- 5. A produção de filmes classificados para adultos (adult = True) está crescendo ou diminuindo, e qual seu impacto financeiro?
-- Contagem e receita média agrupadas pela flag adult ao longo do tempo
SELECT 
    m.adult, 
    m.release_year, 
    COUNT(m.movie_id) AS total_filmes,
    AVG(f.revenue) AS receita_media
FROM dim_movies m
JOIN fact_movie_performance f ON m.movie_id = f.movie_id
WHERE m.release_year IS NOT NULL
GROUP BY m.adult, m.release_year
ORDER BY m.release_year DESC, m.adult;
