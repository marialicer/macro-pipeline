from airflow.decorators import task
from airflow import DAG, Dataset
import pendulum
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.transform.bcb import transformar_indicador

bcb_ingestao = Dataset("bcb_ingestao")

INDICADORES = [
    {"nome": "ipca", "fonte": "BCB"},
    {"nome": "selic", "fonte": "BCB"},
    {"nome": "cambio", "fonte": "BCB"},
    {"nome": "desemprego", "fonte": "BCB"},
]


with DAG(
    dag_id="transformacao_macropipeline",
    schedule=[bcb_ingestao],
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Sao_Paulo"),
    catchup=False,
) as dag:

    @task.branch
    def decidir_branch(indicador: dict):
        nome = indicador["nome"]
        if indicador["fonte"] == "BCB":
            return f"transformar_bcb_{nome}"
        else:
            return f"transformar_ibge_{nome}"


    @task
    def transformar_bcb(indicador: dict):
        transformar_indicador(indicador["nome"])


    @task
    def transformar_ibge(indicador: dict):
        print(f"IBGE não implementado ainda: {indicador['nome']}")


    # cria uma execução por indicador
    for ind in INDICADORES:
        branch = decidir_branch.override(task_id=f"branch_{ind['nome']}")(ind)
        branch >> transformar_bcb.override(task_id=f"transformar_bcb_{ind['nome']}")(ind)
        branch >> transformar_ibge.override(task_id=f"transformar_ibge_{ind['nome']}")(ind)
