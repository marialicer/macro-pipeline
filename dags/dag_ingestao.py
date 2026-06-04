# ==============================================================================
# DAG de Ingestão do Macropipeline
# ------------------------------------------------------------------------------
# Extrai séries históricas de indicadores macroeconômicos da API do BCB
# (Banco Central do Brasil) e sinaliza a conclusão via Airflow Dataset,
# disparando automaticamente a DAG de transformação downstream.
# ==============================================================================

import pendulum
from airflow import DAG
from airflow.sdk import task
import sys
from pathlib import Path
from airflow.datasets import Dataset

# Adiciona o diretório raiz ao sys.path para permitir imports de módulos locais
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.extract.bcb import extrair_serie_bcb


# ------------------------------------------------------------------------------
# Indicadores macroeconômicos a extrair
# ------------------------------------------------------------------------------
# Cada entrada define:
#   codigo_serie → código da série no SGS (Sistema Gerenciador de Séries do BCB)
#   nome         → identificador amigável, usado em nomes de arquivos/tabelas
#   data_inicio  → início do período histórico desejado
#   data_fim     → fim do período histórico desejado
# ------------------------------------------------------------------------------
INDICADORES = [
    {"codigo_serie": 433,   "nome": "ipca",       "data_inicio": "01/01/2020", "data_fim": "31/12/2024"},  # Inflação oficial (IPCA)
    {"codigo_serie": 432,   "nome": "selic",      "data_inicio": "01/01/2020", "data_fim": "31/12/2024"},  # Taxa básica de juros
    {"codigo_serie": 1,     "nome": "cambio",     "data_inicio": "01/01/2020", "data_fim": "31/12/2024"},  # Taxa de câmbio USD/BRL
    {"codigo_serie": 24369, "nome": "desemprego", "data_inicio": "01/01/2020", "data_fim": "31/12/2024"},  # Taxa de desemprego (PNAD Contínua)
]


# ------------------------------------------------------------------------------
# Dataset de saída (contrato entre DAGs)
# ------------------------------------------------------------------------------
# Quando a task 'publicar_dataset' for concluída, o Airflow marca este Dataset
# como atualizado. A DAG 'transformacao_macropipeline', que tem este Dataset
# como schedule, será disparada automaticamente em seguida.
# ------------------------------------------------------------------------------
dataset_ingestao = Dataset("bcb_ingestao")


# ------------------------------------------------------------------------------
# Definição da DAG
# ------------------------------------------------------------------------------
with DAG(
    dag_id="ingestao_macropipeline",
    description="ingestao",

    # Executa uma vez por mês; como os dados são históricos (2020–2024),
    # na prática serve para re-ingestão ou atualização incremental futura
    schedule="@monthly",

    start_date=pendulum.datetime(2024, 1, 1, tz="America/Sao_Paulo"),

    # Não executa retroativamente os meses anteriores ao ativar a DAG
    catchup=False,

    tags=["ingestao", "bcb"],
) as dag:

    # --------------------------------------------------------------------------
    # Task 1: extrair
    # --------------------------------------------------------------------------
    # Chama a função de extração do BCB para um único indicador.
    # Será expandida dinamicamente via .expand(), gerando uma instância
    # desta task para cada item da lista INDICADORES (paralelismo automático).
    #
    # Retorna o nome do indicador para ser consumido pela task downstream.
    # --------------------------------------------------------------------------
    @task
    def extrair(indicador: dict) -> str:
        """
        Chama a função real de extração do BCB
        e retorna o nome do indicador processado.
        """
        extrair_serie_bcb(
            codigo_serie=indicador["codigo_serie"],
            nome_indicador=indicador["nome"],
            data_inicio=indicador["data_inicio"],
            data_fim=indicador["data_fim"]
        )

        return indicador["nome"]  # Propagado como lista para 'publicar_dataset'


    # --------------------------------------------------------------------------
    # Task 2: publicar_dataset
    # --------------------------------------------------------------------------
    # Aguarda a conclusão de TODAS as instâncias de 'extrair' (fan-in automático
    # do Dynamic Task Mapping) e então publica o Dataset 'bcb_ingestao'.
    #
    # O parâmetro outlets=[dataset_ingestao] é o que aciona o Dataset:
    # ao concluir com sucesso, o Airflow registra o Dataset como atualizado
    # e dispara as DAGs dependentes.
    # --------------------------------------------------------------------------
    @task(outlets=[dataset_ingestao])
    def publicar_dataset(resultados: list[str]) -> str:
        print("Ingestão concluída para:", resultados)
        return "dataset publicado"


    # --------------------------------------------------------------------------
    # Orquestração: Dynamic Task Mapping + fan-in
    # --------------------------------------------------------------------------
    # .expand() cria N instâncias paralelas de 'extrair', uma por indicador.
    # O retorno é uma XComArg que o Airflow resolve automaticamente como lista
    # ao passar para 'publicar_dataset', criando o fan-in (N→1).
    #
    # Fluxo resultante:
    #   extrair[ipca] ──┐
    #   extrair[selic]──┤
    #   extrair[cambio]─┼──► publicar_dataset ──► (Dataset atualizado)
    #   extrair[desem.]─┘
    # --------------------------------------------------------------------------
    resultados_extracao = extrair.expand(indicador=INDICADORES)

    publicar_dataset(resultados_extracao)