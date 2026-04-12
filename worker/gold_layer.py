import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime
import logging
import warnings

# Configuração de Logs e Warnings
warnings.filterwarnings('ignore')
logging.getLogger('sqlalchemy').setLevel(logging.ERROR)
logging.getLogger('pyarrow').setLevel(logging.ERROR)


class GoldLayerProcessor:
    """
    Engenheiro de Dados Sênior: Camada Silver -> Gold.
    Responsável pela carga otimizada no PostgreSQL (Snowflake Schema).
    """

    def __init__(self):
        DB_NAME = os.getenv("POSTGRES_DB", "postgres")
        DB_USER = os.getenv("POSTGRES_USER", "postgres")
        DB_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")
        DB_HOST = os.getenv("DB_HOST", "localhost")
        DB_PORT = os.getenv("DB_PORT", "5432")
        DB_URL = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

        self.engine = create_engine(DB_URL)
        
        self.silver_path = os.getenv("SILVER_OUTPUT_PATH", "C:/source/pos-graduacao/Lab01_PART2_NUSP/worker")
        self.input_file = f"{self.silver_path}/data/silver/dataset_silver.parquet"

    def _load_parquet(self) -> pd.DataFrame:
        """Lê os dados da camada Silver (Parquet)."""
        print(f"Lendo dados Silver: {self.input_file}")

        # Garantir existência dos diretórios de destino
        os.makedirs(os.path.dirname(self.input_file), exist_ok=True)
        return pd.read_parquet(self.input_file)

    def _upsert_dimension(self, df: pd.DataFrame, table_name: str, db_name_col: str, input_column: str):
        """Insere valores únicos em tabelas de dimensão (Idempotente)."""
        # Assume que os dados já foram limpos na camada Silver
        unique_names = df[input_column].unique()
        
        # Remove 'Unknown' da dimensão se desejar (opcional, mas recomendado para limpeza)
        unique_names = [name for name in unique_names if name and name.lower() != 'unknown']
        
        dim_df = pd.DataFrame({db_name_col: unique_names})
        
        if not dim_df.empty:
            print(f"Carregando {len(dim_df)} registros em {table_name}...")
            dim_df.to_sql(table_name, self.engine, if_exists='append', index=False, method='multi', chunksize=1000)
            print(f"Dimensão {table_name} carregada.")
        else:
            print(f"Nenhum dado novo para a dimensão {table_name}.")

    def _populate_bridge(self, df: pd.DataFrame, bridge_table: str, dim_table: str, 
                         source_col: str, dim_id_col: str, dim_name_col: str):
        """Popula tabelas bridge para relações N:N."""
        # Explode a coluna de lista (já limpa na Silver)
        df_exploded = df[['id', source_col]].copy()
        df_exploded[source_col] = df_exploded[source_col].str.split(',')
        df_exploded = df_exploded.explode(source_col)
        
        # Busca IDs da dimensão
        dim_df = pd.read_sql(f"SELECT {dim_id_col}, {dim_name_col} FROM {dim_table}", self.engine)
        bridge_df = df_exploded.merge(dim_df, left_on=source_col, right_on=dim_name_col)
        
        # Limpa e salva
        bridge_df = bridge_df[['id', dim_id_col]].rename(columns={'id': 'movie_id'})
        bridge_df.to_sql(bridge_table, self.engine, if_exists='append', index=False, method='multi', chunksize=1000)
        print(f"Tabela Bridge {bridge_table} carregada.")

    def _get_table_columns(self, table_name: str) -> list:
        """Busca os nomes das colunas reais da tabela no PostgreSQL."""
        query = text(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
        with self.engine.connect() as conn:
            result = conn.execute(query)
            return [row[0] for row in result]

    def run(self):
        """Orquestração da carga Gold."""
        df = self._load_parquet()
        
        # 1. Carregar Dimensões Simples
        # dim_movies
        available_cols = ['id', 'title', 'release_date', 'runtime', 'adult', 'original_language', 'status']
        # Só seleciona o que existe no DataFrame
        cols_to_select = [c for c in available_cols if c in df.columns]
        movies_df = df[cols_to_select].copy()
        
        # Tratamento de release_year (evita float NaN em coluna INT)
        movies_df['release_year'] = pd.to_datetime(movies_df['release_date']).dt.year
        # Converte para Int64 (nullable integer do Pandas) para evitar float64
        movies_df['release_year'] = movies_df['release_year'].astype('Int64')
        
        movies_df = movies_df.rename(columns={'id': 'movie_id'})
        
        # REMOÇÃO DE DUPLICATAS (Fix para UniqueViolation)
        movies_df = movies_df.drop_duplicates(subset=['movie_id'])

        # Validação final: só envia o que existe na tabela
        db_cols = self._get_table_columns('dim_movies')
        final_cols = [c for c in movies_df.columns if c in db_cols]
        
        print(f"Carregando dim_movies com colunas: {final_cols}")
        movies_df[final_cols].to_sql('dim_movies', self.engine, if_exists='append', index=False, method='multi')

        # 2. Carregar Dimensões N:N
        # Explodir e carregar nomes únicos com os nomes de colunas corretos do DB
        self._upsert_dimension(df.assign(g=df['genres'].str.split(',')).explode('g'), 'dim_genres', 'genre_name', 'g')
        self._upsert_dimension(df.assign(c=df['production_companies'].str.split(',')).explode('c'), 'dim_companies', 'company_name', 'c')
        self._upsert_dimension(df.assign(co=df['production_countries'].str.split(',')).explode('co'), 'dim_countries', 'country_name', 'co')

        # 3. Popular Tabelas Bridge
        self._populate_bridge(df, 'bridge_movie_genres', 'dim_genres', 'genres', 'genre_id', 'genre_name')
        self._populate_bridge(df, 'bridge_movie_companies', 'dim_companies', 'production_companies', 'company_id', 'company_name')
        self._populate_bridge(df, 'bridge_movie_countries', 'dim_countries', 'production_countries', 'country_id', 'country_name')

        # 4. Carregar Tabela Fato
        fact_df = df[['id', 'budget', 'revenue', 'popularity', 'vote_average', 'vote_count']].copy()
        fact_df = fact_df.rename(columns={'id': 'movie_id'})

        # REMOÇÃO DE DUPLICATAS (Fix para UniqueViolation)
        fact_df = fact_df.drop_duplicates(subset=['movie_id'])

        fact_df.to_sql('fact_movie_performance', self.engine, if_exists='append', index=False, method='multi')

        print("Carga da Camada Gold finalizada com sucesso!")