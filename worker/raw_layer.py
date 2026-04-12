import os
import pandas as pd
import pyarrow
from datetime import datetime, timedelta
import time
from log_utils import calculate_latency

class RawLayerProcessor:
    def run(self):
        """
        Executa a ingestão da camada raw (bronze) de um pipeline de dados.
        
        Ingestão: Lê um CSV de um diretório parametrizado via variável de ambiente 
        que deve terminar em \archive\.
        
        Processamento: Dados brutos sem alterações.
        
        Armazenamento: Salva os dados em um diretório local parametrizado via 
        variável de ambiente que deve terminar em data/raw/.
        """
        startDateTime = datetime.now()
        print("Iniciando a camada Raw...")

        # 1. Ingestão (Input)
        input_base = os.getenv("RAW_INPUT_PATH", "C:/source/pos-graduacao/Lab01_PART2_NUSP/worker")
        output_base = os.getenv("RAW_OUTPUT_PATH", "C:/source/pos-graduacao/Lab01_PART2_NUSP/worker")
        print(f"Input base: {input_base}")
        print(f"Output base: {output_base}")

        if not input_base.endswith("/archive/"):
            if input_base.endswith("/"):
                input_path = input_base + "archive/"
            else:
                input_path = input_base + "/archive/"
        else:
            input_path = input_base
            
        full_input_path = input_path + "dataset.csv"

        os.makedirs(os.path.dirname(full_input_path), exist_ok=True)
        print(f"Full input path: {full_input_path}")
        # 2. Processamento (Leitura sem alteração)
        df = pd.read_csv(full_input_path, engine='pyarrow')

        # 3. Armazenamento (Output)
        if not output_base.endswith("data/raw/"):
            if output_base.endswith("/"):
                output_path = output_base + "data/raw/"
            else:
                output_path = output_base + "/data/raw/"
        else:
            output_path = output_base
            
        full_output_path = output_path + "dataset.csv"
        os.makedirs(os.path.dirname(full_output_path), exist_ok=True)
        print(f"Full output path: {full_output_path}")

        # Salvar dados brutos localmente (sem alterações)
        df.to_csv(full_output_path, index=False)

        endDateTime = datetime.now()
        latency = calculate_latency(startDateTime, endDateTime)
        print(f"Camada Raw finalizada. Latência: {latency}. Arquivo salvo em: {full_output_path}")
        return f"Camada Raw finalizada. Arquivo salvo em: {full_output_path}"