# ==============================================================================
# DAG de Transformação do Macropipeline
# ------------------------------------------------------------------------------
# Responsável por transformar os dados macroeconômicos ingeridos pelo pipeline
# do Banco Central do Brasil (BCB). É acionada automaticamente sempre que o
# Dataset 'bcb_ingestao' é atualizado pela DAG de ingestão.
# ==============================================================================

from airflow.decorators import task
from airflow import DAG, Dataset
import pendulum
import sys
from pathlib import Path

# Garante que o diretório raiz do projeto esteja no sys.path,
# permitindo importar módulos locais como 'scripts.transform.bcb'
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.transform.bcb import transformar_indicador


# ------------------------------------------------------------------------------
# Dataset Sensor
# ------------------------------------------------------------------------------
# O Airflow monitora este Dataset. Quando a DAG de ingestão o atualiza
# (via outlet), esta DAG de transformação é disparada automaticamente.
# ------------------------------------------------------------------------------
bcb_ingestao = Dataset("bcb_ingestao")

# Dataset publicado ao final da transformação
dataset_transformacao = Dataset("bcb_transformacao")

# ------------------------------------------------------------------------------
# Indicadores a serem processados
# ------------------------------------------------------------------------------
# Cada dicionário representa um indicador macroeconômico.
# 'nome'  → identificador usado para nomear tasks e chamar funções
# 'fonte' → define qual branch de transformação será executada
# ------------------------------------------------------------------------------
INDICADORES = [
    {"nome": "ipca",       "fonte": "BCB"},   # Índice de inflação oficial
    {"nome": "selic",      "fonte": "BCB"},   # Taxa básica de juros
    {"nome": "cambio",     "fonte": "BCB"},   # Taxa de câmbio USD/BRL
    {"nome": "desemprego", "fonte": "BCB"},   # Taxa de desemprego (PNAD via BCB)
]


# ------------------------------------------------------------------------------
# Definição da DAG
# ------------------------------------------------------------------------------
with DAG(
    dag_id="transformacao_macropipeline",

    # Acionamento orientado a dados: executa quando 'bcb_ingestao' é atualizado
    schedule=[bcb_ingestao],

    # Data de início; como catchup=False, não executa retroativamente
    start_date=pendulum.datetime(2025, 1, 1, tz="America/Sao_Paulo"),

    # Não preenche execuções passadas ao ativar a DAG
    catchup=False,
) as dag:

    # --------------------------------------------------------------------------
    # Task: decidir_branch
    # --------------------------------------------------------------------------
    # BranchPythonOperator: decide qual task de transformação executar
    # com base na fonte do indicador.
    # Retorna o task_id da próxima task a ser executada; as demais são ignoradas.
    # --------------------------------------------------------------------------
    @task.branch
    def decidir_branch(indicador: dict):
        nome = indicador["nome"]

        if indicador["fonte"] == "BCB":
            return f"transformar_bcb_{nome}"   # Segue para transformação BCB
        else:
            return f"transformar_ibge_{nome}"  # Segue para transformação IBGE


    # --------------------------------------------------------------------------
    # Task: transformar_bcb
    # --------------------------------------------------------------------------
    # Executa a lógica de transformação para indicadores oriundos do BCB.
    # Chama a função importada do módulo local 'scripts/transform/bcb.py'.
    # --------------------------------------------------------------------------
    @task
    def transformar_bcb(indicador: dict):
        transformar_indicador(indicador["nome"])


    # --------------------------------------------------------------------------
    # Task: transformar_ibge
    # --------------------------------------------------------------------------
    # Placeholder para futura integração com fontes do IBGE.
    # Por enquanto apenas loga que a fonte ainda não foi implementada.
    # --------------------------------------------------------------------------
    @task
    def transformar_ibge(indicador: dict):
        print(f"IBGE não implementado ainda: {indicador['nome']}")

    # Publica o Dataset para disparar a DAG de carga
    @task(outlets=[dataset_transformacao])
    def publicar_dataset():
        print("Transformação concluída")

    tarefas_finais = []


    # --------------------------------------------------------------------------
    # Geração dinâmica de tasks por indicador
    # --------------------------------------------------------------------------
    # Para cada indicador da lista, cria um trio de tasks independentes:
    #
    #   branch_{nome}  →  transformar_bcb_{nome}
    #                  →  transformar_ibge_{nome}
    #
    # O uso de .override(task_id=...) garante task_ids únicos no Airflow,
    # evitando colisão entre as instâncias da mesma função reutilizada.
    # --------------------------------------------------------------------------
    for ind in INDICADORES:
        branch = decidir_branch.override(
            task_id=f"branch_{ind['nome']}"
        )(ind)

        bcb = transformar_bcb.override(
            task_id=f"transformar_bcb_{ind['nome']}"
        )(ind)

        ibge = transformar_ibge.override(
            task_id=f"transformar_ibge_{ind['nome']}"
        )(ind)

        # Define as dependências
        branch >> bcb
        branch >> ibge

        tarefas_finais.extend([bcb, ibge])

    publicacao = publicar_dataset()

    for tarefa in tarefas_finais:
        tarefa >> publicacao
    