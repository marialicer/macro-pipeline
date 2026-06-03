from pathlib import Path
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os

BASE_DIR = Path(__file__).resolve().parent.parent.parent


def carregar_indicador(nome_indicador: str, unidade: str, periodicidade: str):

    # caminho do parquet processado
    dados_entrada = BASE_DIR / "data" / "processed" / f"{nome_indicador}.parquet"

    # lê os dados
    df = pd.read_parquet(dados_entrada)

    # adiciona metadados obrigatórios da sua tabela
    df["indicador"] = nome_indicador
    df["fonte"] = "BCB"
    df["unidade"] = "unidade"  # você pode ajustar depois
    df["periodicidade"] = "periodicidade"  # IPCA é mensal

    # renomeia colunas para bater com o banco
    df = df.rename(columns={
        "data": "data_referencia",
        "valor": "valor"
    })

    # conecta no banco
    conexao = psycopg2.connect(
        dbname=os.getenv("POSTGRES_PROJETO_DB"),
        user=os.getenv("POSTGRES_PROJETO_USER"),
        password=os.getenv("POSTGRES_PROJETO_PASSWORD"),
        host="postgres_projeto",
        port="5432"
    )

    cursor = conexao.cursor()

    try:
        # transforma dataframe em lista de tuplas
        valores = list(df[[
            "indicador",
            "fonte",
            "data_referencia",
            "valor",
            "unidade",
            "periodicidade"
        ]].itertuples(index=False, name=None))

        query = """
            INSERT INTO indicadores_macroeconomicos (
                indicador,
                fonte,
                data_referencia,
                valor,
                unidade,
                periodicidade
            )
            VALUES %s
            ON CONFLICT (indicador, data_referencia) DO NOTHING;
        """

        execute_values(cursor, query, valores)

        conexao.commit()
        print(f"{len(valores)} registros inseridos com sucesso")

    except Exception as e:
        conexao.rollback()
        print(f"Erro ao inserir dados: {e}")

    finally:
        cursor.close()
        conexao.close()