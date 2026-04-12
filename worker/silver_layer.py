import os
import pandas as pd
from datetime import datetime

from log_utils import calculate_latency

class SilverLayerProcessor:
    """
    Processador de Dados Sênior: Camada Bronze -> Silver.
    Focado em limpeza, transformação e profiling estatístico.
    Bibliotecas permitidas: pandas, sqlalchemy, psycopg2-binary, pyarrow.
    """

    def __init__(self):
        # 1. Configuração dinâmica de diretórios via variáveis de ambiente
        self.raw_path = os.getenv("RAW_OUTPUT_PATH", "C:/source/pos-graduacao/Lab01_PART2_NUSP/worker")
        self.silver_path = os.getenv("SILVER_OUTPUT_PATH", "C:/source/pos-graduacao/Lab01_PART2_NUSP/worker")
        
        # Caminhos completos conforme requisito
        self.input_file = f"{self.raw_path}/data/raw/dataset.csv"
        self.output_dir = f"{self.silver_path}/data/silver/"
        self.report_dir = f"{self.silver_path}/data/silver/reports/"
        
        # Garantir existência dos diretórios de destino
        os.makedirs(self.output_dir, exist_ok=True)
        os.makedirs(self.report_dir, exist_ok=True)

    def ingest_bronze(self) -> pd.DataFrame:
        """Lê os dados da camada Bronze (CSV)."""
        print(f"Ingerindo dados de: {self.input_file}")
        # Pandas 3.0.1 suporta leitura eficiente
        return pd.read_csv(self.input_file)

    def transform_to_silver(self, df: pd.DataFrame) -> pd.DataFrame:
        """Executa limpeza e transformações para a camada Silver."""
        print("Iniciando transformações da camada Silver...")
        
        # A. Padronização para snake_case
        df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]

        # B. Remoção de duplicatas baseada no ID
        df = df.drop_duplicates(subset=['id'], keep='first')

        # C. Conversão de tipos de dados
        df['id'] = pd.to_numeric(df['id'], errors='coerce')
        df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
        df['budget'] = pd.to_numeric(df['budget'], errors='coerce').fillna(0)
        df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce').fillna(0)
        df['vote_average'] = pd.to_numeric(df['vote_average'], errors='coerce').fillna(0)
        df['vote_count'] = pd.to_numeric(df['vote_count'], errors='coerce').fillna(0)
        
        # Novas colunas para Gold
        if 'runtime' in df.columns:
            df['runtime'] = pd.to_numeric(df['runtime'], errors='coerce').fillna(0).astype(int)
        if 'adult' in df.columns:
            df['adult'] = df['adult'].map({'True': True, 'False': False, True: True, False: False}).fillna(False)

        # D. Tratamento de valores ausentes e saneamento de strings
        df = df.dropna(subset=['id'])
        df['id'] = df['id'].astype(int)
        
        # Saneamento de colunas de texto e listas
        text_cols = ['title', 'status', 'original_language', 'genres', 'production_companies', 'production_countries']
        list_cols = ['genres', 'production_companies', 'production_countries']
        
        for col in text_cols:
            if col in df.columns:
                # Preenche nulos, converte para string e remove espaços
                df[col] = df[col].fillna('Unknown').astype(str).str.strip()
                # Remove strings que representam nulos ou listas vazias
                df.loc[df[col].isin(['nan', '[]', 'None', '']), col] = 'Unknown'
                
                # Deduplicação interna para colunas de lista (ex: "Ação, Drama, Ação" -> "Ação, Drama")
                if col in list_cols and df[col].iloc[0] != 'Unknown':
                    df[col] = df[col].apply(
                        lambda x: ",".join(sorted(list(set([i.strip() for i in str(x).split(",") if i.strip()])))) 
                        if x != 'Unknown' else x
                    )

        return df

    def generate_analytical_report(self, df: pd.DataFrame):
        """Gera um relatório Markdown automatizado com profiling estatístico detalhado."""
        print("Gerando relatório analítico Markdown...")
        
        # 1. Contagem de Nulos por Coluna
        null_counts = df.isnull().sum().to_frame(name='Nulos').to_markdown()

        # 2. Tipos Finais de Dados
        data_types = df.dtypes.to_frame(name='Tipo').to_markdown()

        # 4. Top 10 Filmes por Receita (Insight de Dados)
        top_revenue = df.nlargest(10, 'revenue')[['title', 'revenue', 'release_date']].to_markdown(index=False)

        # 5. Distribuição por Idioma (Insight de Dados)
        lang_dist = df['original_language'].value_counts().head(10).to_frame(name='Contagem').to_markdown()

        # 6. Resumo por Status
        status_summary = df.groupby('status').agg(
            total_filmes=('id', 'count'),
            receita_media=('revenue', 'mean')
        ).reset_index()
        
        # Formata receita para 2 casas decimais
        status_summary['receita_media'] = status_summary['receita_media'].round(2)
        status_view = self._to_markdown_simple(status_summary)

        # Gerar Markdown Automatizado
        md_content = f"""# Relatório Analítico - Camada Silver (Filmes)
Data de Processamento: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## 1. Caracterização de Dados (Data Profiling)
### Tipos de Dados Finais
{data_types}

### Contagem de Valores Nulos
{null_counts}

## 2. Insights de Negócio (Tabelas Analíticas)
### Top 10 Filmes por Receita
{top_revenue}

### Distribuição dos 10 Idiomas mais Frequentes
{lang_dist}

### Resumo por Status
{status_view}

---
*Relatório gerado automaticamente pelo SilverLayerProcessor (Pandas 3.0.1).*
"""
        with open(f"{self.report_dir}analytic_data_report.md", "w", encoding='utf-8') as f:
            f.write(md_content)

    def _to_markdown_simple(self, df: pd.DataFrame) -> str:
        """Converte um DataFrame em uma tabela Markdown sem dependências externas."""
        if df.empty:
            return "Nenhum dado disponível."
        
        headers = "| " + " | ".join(df.columns) + " |"
        separator = "| " + " | ".join(["---"] * len(df.columns)) + " |"
        rows = []
        for _, row in df.iterrows():
            rows.append("| " + " | ".join(str(val) for val in row.values) + " |")
        
        return "\n".join([headers, separator] + rows)
    
    def persist_silver(self, df: pd.DataFrame):
        """Salva os dados tratados em formato Parquet utilizando pyarrow."""
        output_file = f"{self.output_dir}dataset_silver.parquet"
        print(f"Persistindo dados na camada Silver: {output_file}")
        # Pandas 3.0.1 utiliza pyarrow como engine padrão para Parquet
        df.to_parquet(output_file, index=False, engine='pyarrow')

    def run(self):
        """Orquestração principal do pipeline Silver."""
        try:
            startDateTime = datetime.now()
            # Ingestão
            df_bronze = self.ingest_bronze()
            
            # Transformação
            df_silver = self.transform_to_silver(df_bronze)
            
            # Relatório Analítico 
            self.generate_analytical_report(df_silver)
            
            # Persistência
            self.persist_silver(df_silver)
            
            endDateTime = datetime.now()
            latency = calculate_latency(startDateTime, endDateTime)
            print(f"Pipeline Silver concluído com sucesso! Latência: {latency}")
            
        except Exception as e:
            print(f"Erro crítico no processamento Silver: {str(e)}")
