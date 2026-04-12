# Relatório Analítico - Camada Silver (Filmes)
Data de Processamento: 2026-03-24 12:36:19

## 1. Caracterização de Dados (Data Profiling)
### Tipos de Dados Finais
|                      | Tipo           |
|:---------------------|:---------------|
| id                   | int64          |
| title                | str            |
| vote_average         | float64        |
| vote_count           | int64          |
| status               | str            |
| release_date         | datetime64[us] |
| revenue              | int64          |
| runtime              | int64          |
| adult                | bool           |
| budget               | int64          |
| imdb_id              | str            |
| original_language    | str            |
| original_title       | str            |
| overview             | str            |
| popularity           | float64        |
| tagline              | str            |
| genres               | str            |
| production_companies | str            |
| production_countries | str            |
| spoken_languages     | str            |
| keywords             | str            |

### Contagem de Valores Nulos
|                      |   Nulos |
|:---------------------|--------:|
| id                   |       0 |
| title                |       0 |
| vote_average         |       0 |
| vote_count           |       0 |
| status               |       0 |
| release_date         |  183292 |
| revenue              |       0 |
| runtime              |       0 |
| adult                |       0 |
| budget               |       0 |
| imdb_id              |  487461 |
| original_language    |       0 |
| original_title       |      13 |
| overview             |  215550 |
| popularity           |       0 |
| tagline              |  895146 |
| genres               |       0 |
| production_companies |       0 |
| production_countries |       0 |
| spoken_languages     |  440071 |
| keywords             |  755305 |

## 2. Insights de Negócio (Tabelas Analíticas)
### Top 10 Filmes por Receita
| title                        |    revenue | release_date        |
|:-----------------------------|-----------:|:--------------------|
| babben: the movie            | 4999999999 | NaT                 |
| TikTok Rizz Party            | 3000000000 | 2024-04-01 00:00:00 |
| Adventures in Bora Bora      | 3000000000 | 2023-08-23 00:00:00 |
| Bee Movie                    | 2930000000 | NaT                 |
| Avatar                       | 2923706026 | 2009-12-15 00:00:00 |
| Avengers: Endgame            | 2800000000 | 2019-04-24 00:00:00 |
| Avatar: The Way of Water     | 2320250281 | 2022-12-14 00:00:00 |
| Titanic                      | 2264162353 | 1997-11-18 00:00:00 |
| Star Wars: The Force Awakens | 2068223624 | 2015-12-15 00:00:00 |
| Avengers: Infinity War       | 2052415039 | 2018-04-25 00:00:00 |

### Distribuição dos 10 Idiomas mais Frequentes
| original_language   |   Contagem |
|:--------------------|-----------:|
| en                  |     565735 |
| fr                  |      62041 |
| es                  |      52864 |
| de                  |      49697 |
| ja                  |      45566 |
| zh                  |      36556 |
| pt                  |      30199 |
| it                  |      22202 |
| ru                  |      21388 |
| ko                  |      12240 |

### Resumo por Status
| status | total_filmes | receita_media |
| --- | --- | --- |
| Canceled | 262 | 0.76 |
| In Production | 10598 | 302091.65 |
| Planned | 6320 | 11.67 |
| Post Production | 8227 | 372554.12 |
| Released | 1022029 | 774030.16 |
| Rumored | 345 | 470.39 |

---
*Relatório gerado automaticamente pelo SilverLayerProcessor (Pandas 3.0.1).*
