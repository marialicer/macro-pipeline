import pendulum
from airflow import DAG
from airflow.sdk import task
import sys
from pathlib import Path
from airflow.datasets import Dataset
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.extract.bcb import extrair_serie_bcb

INDICADORES = [
    {"codigo_serie": 433,  "nome": "ipca",     "data_inicio": "01/01/2020", "data_fim": "31/12/2024"},
    {"codigo_serie": 432,  "nome": "selic",    "data_inicio": "01/01/2020", "data_fim": "31/12/2024"},
    {"codigo_serie": 1,    "nome": "cambio",   "data_inicio": "01/01/2020", "data_fim": "31/12/2024"},
    {"codigo_serie": 24369,"nome": "desemprego","data_inicio": "01/01/2020", "data_fim": "31/12/2024"},
]

# Dataset (Airflow vai usar isso como "sinal de atualização")
dataset_ingestao = Dataset("bcb_ingestao")

with DAG(
    dag_id="ingestao_macropipeline",
    description="ingestao",
    schedule="@monthly",
    start_date=pendulum.datetime(2024,1,1,tz="America/Sao_Paulo"),
    catchup=False,
    tags=["ingestao","bcb"]
) as dag:
    
    # 1) EXTRACT
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

        return indicador["nome"]


    # 2) PUBLICAÇÃO DO DATASET (marcador de pipeline concluído)
    @task(outlets=[dataset_ingestao])
    def publicar_dataset(resultados: list[str]) -> str:
        print("Ingestão concluída para:", resultados)
        return "dataset publicado"


    # EXECUÇÃO COM MAPPING
    resultados_extracao = extrair.expand(indicador=INDICADORES)

    publicar_dataset(resultados_extracao)