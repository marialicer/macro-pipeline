import pendulum
from airflow import DAG, Dataset
from airflow.sdk import task
import sys
from pathlib import Path
# Adiciona a raiz do projeto ao PYTHONPATH
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.load.postgres import (carregar_indicador, registrar_execucao_pipeline)

# =============================================================================
# Dataset publicado pela DAG de transformação.
# Esta DAG só executará quando a transformação terminar.
# =============================================================================

bcb_transformacao = Dataset("bcb_transformacao")

# =============================================================================
# Lista de indicadores que serão carregados para o PostgreSQL
# =============================================================================

INDICADORES = [
    "ipca",
    "selic",
    "cambio",
    "desemprego",
]

# =============================================================================
# Metadados de cada indicador.
# Serão enviados para a função carregar_indicador().
# =============================================================================

METADADOS = {
    "ipca": {
        "unidade": "%",
        "periodicidade": "mensal"
    },
    "selic": {
        "unidade": "%",
        "periodicidade": "mensal"
    },
    "cambio": {
        "unidade": "R$",
        "periodicidade": "diaria"
    },
    "desemprego": {
        "unidade": "%",
        "periodicidade": "mensal"
    }
}

with DAG(
    dag_id="relatorio_macropipeline",
    description="Carga dos dados transformados para o PostgreSQL",
    schedule=[bcb_transformacao],
    start_date=pendulum.datetime(2025,1,1,tz="America/Sao_Paulo"),
    catchup=False,
    tags=["load", "postgres"]
) as dag:

  
    
    # =========================================================================
    # Task responsável por carregar um indicador no PostgreSQL
    # =========================================================================

    @task
    def carregar(nome_indicador: str):

        metadado = METADADOS[nome_indicador]

        carregar_indicador(
            nome_indicador=nome_indicador,
            unidade=metadado["unidade"],
            periodicidade=metadado["periodicidade"]
        )

        return f"{nome_indicador} carregado"

    # =========================================================================
    # Task de auditoria.
    # Posteriormente substituir o print por um INSERT real na tabela
    # execucoes_pipeline.
    # =========================================================================

    @task
    def registrar_execucao(resultados: list[str]):

        registrar_execucao_pipeline(
            dag_id="relatorio_macropipeline",
            indicador="todos",
            status="SUCESSO",
            registros_inseridos=len(resultados),
            mensagem="Carga concluída"
        )

    # =========================================================================
    # Dynamic Task Mapping
    # Cria uma task de carga para cada indicador.
    # =========================================================================

    cargas = carregar.expand(
        nome_indicador=INDICADORES
    )

    registro = registrar_execucao(cargas)

    # =========================================================================
    # Fluxo da DAG
    # =========================================================================

    cargas >> registro