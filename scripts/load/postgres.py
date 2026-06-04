# ==============================================================================
# Carregamento de Indicadores — Camada Load do Pipeline (ETL)
# ------------------------------------------------------------------------------
# Lê os dados já transformados (Parquet) e os insere na tabela
# 'indicadores_macroeconomicos' do PostgreSQL, enriquecendo cada registro
# com metadados (fonte, unidade, periodicidade) antes da carga.
# ==============================================================================

from pathlib import Path
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values  # Inserção em lote otimizada
import os

# Navega da localização deste arquivo até a raiz do projeto:
# scripts/load/bcb.py → scripts/load → scripts → raiz
BASE_DIR = Path(__file__).resolve().parent.parent.parent


def carregar_indicador(nome_indicador: str, unidade: str, periodicidade: str):
    """
    Lê o Parquet processado de um indicador e insere os registros
    no banco de dados PostgreSQL.

    Args:
        nome_indicador: Nome do indicador (ex: "ipca", "selic").
                        Usado para localizar o arquivo e preencher a coluna 'indicador'.
        unidade:        Unidade de medida do indicador (ex: "%", "R$").
        periodicidade:  Frequência dos dados (ex: "mensal", "diária").
    """

    # --------------------------------------------------------------------------
    # 1. Leitura dos dados processados
    # --------------------------------------------------------------------------
    # Aponta para o arquivo gerado pela etapa de transformação.
    # O formato Parquet preserva tipos de dados (datas, decimais) com mais
    # fidelidade do que CSV, evitando conversões manuais antes da carga.
    # --------------------------------------------------------------------------
    dados_entrada = BASE_DIR / "data" / "processed" / f"{nome_indicador}.parquet"

    df = pd.read_parquet(dados_entrada)

    # --------------------------------------------------------------------------
    # 2. Enriquecimento com metadados
    # --------------------------------------------------------------------------
    # Adiciona colunas que não existem no arquivo transformado mas são
    # obrigatórias na tabela do banco (NOT NULL ou parte da chave).
    # Os valores vêm dos parâmetros da função, tornando o módulo reutilizável
    # para qualquer indicador independente da sua unidade ou periodicidade.
    # --------------------------------------------------------------------------
    df["indicador"]    = nome_indicador  # Ex: "ipca", "selic"
    df["fonte"]        = "BCB"           # Origem fixa para este módulo
    df["unidade"]      = unidade         # Ex: "%", "R$"
    df["periodicidade"] = periodicidade  # Ex: "mensal", "diária"

    # --------------------------------------------------------------------------
    # 3. Renomeação de colunas
    # --------------------------------------------------------------------------
    # Alinha os nomes do DataFrame com os nomes das colunas no banco.
    # O Parquet usa "data" e "valor"; a tabela espera "data_referencia" e "valor".
    # --------------------------------------------------------------------------
    df = df.rename(columns={
        "data": "data_referencia",
        "valor": "valor"          # Sem mudança de nome; explícito para clareza
    })

    # --------------------------------------------------------------------------
    # 4. Conexão com o PostgreSQL
    # --------------------------------------------------------------------------
    # As credenciais são lidas de variáveis de ambiente para não expor
    # senhas no código-fonte (boa prática de segurança).
    # O host "postgres_projeto" é o nome do serviço definido no docker-compose.
    # --------------------------------------------------------------------------
    conexao = psycopg2.connect(
        dbname=os.getenv("POSTGRES_PROJETO_DB"),
        user=os.getenv("POSTGRES_PROJETO_USER"),
        password=os.getenv("POSTGRES_PROJETO_PASSWORD"),
        host="postgres_projeto",  # Nome do container/serviço no Docker
        port="5432"
    )

    cursor = conexao.cursor()

    try:
        # ----------------------------------------------------------------------
        # 5. Preparação dos dados para inserção em lote
        # ----------------------------------------------------------------------
        # execute_values exige uma lista de tuplas; itertuples() converte cada
        # linha do DataFrame em uma tupla eficientemente, sem overhead do índice.
        # A ordem das colunas aqui deve ser idêntica à da query INSERT abaixo.
        # ----------------------------------------------------------------------
        valores = list(df[[
            "indicador",
            "fonte",
            "data_referencia",
            "valor",
            "unidade",
            "periodicidade"
        ]].itertuples(index=False, name=None))

        # ----------------------------------------------------------------------
        # 6. Query de inserção com tratamento de duplicatas
        # ----------------------------------------------------------------------
        # ON CONFLICT DO NOTHING: se já existir um registro com o mesmo
        # (indicador, data_referencia), a linha é ignorada silenciosamente.
        # Isso torna a operação idempotente — segura para re-execuções
        # e retries automáticos do Airflow sem gerar dados duplicados.
        # ----------------------------------------------------------------------
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

        # execute_values envia todos os registros em um único comando SQL,
        # muito mais eficiente do que um INSERT por linha em loop.
        execute_values(cursor, query, valores)

        # Confirma a transação; sem commit() os dados não são persistidos.
        conexao.commit()
        print(f"{len(valores)} registros inseridos com sucesso")

    except Exception as e:
        # Desfaz todas as operações da transação em caso de erro,
        # garantindo que o banco não fique em estado inconsistente.
        conexao.rollback()
        print(f"Erro ao inserir dados: {e}")
        raise
        # 'raise' para que retries e alertas funcionem corretamente.

    finally:
        # Executado sempre, com ou sem erro — garante que a conexão
        # seja liberada e não fique pendurada no pool do PostgreSQL.
        cursor.close()
        conexao.close()