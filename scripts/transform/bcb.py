from pathlib import Path
import pandas as pd

# caminho para rodar no container

BASE_DIR = Path(__file__).resolve().parent.parent.parent

def transformar_indicador(nome_indicador: str):

    # Define o caminho do arquivo JSON de entrada
    arquivo_entrada = (
        BASE_DIR / "data" / "raw" / f"{nome_indicador}.json"
    )

    # Lê o JSON e converte para DataFrame
    df = pd.read_json(arquivo_entrada)

    # Converte a coluna "data" para o tipo datetime
    df["data"] = pd.to_datetime(df["data"])

    # Converte a coluna "valor" para float
    # Troca vírgula por ponto antes da conversão
    df["valor"] = (df["valor"].astype(float).round(2))

    # Padroniza os nomes das colunas
    df.columns = (
        df.columns
        .str.normalize("NFKD")
        .str.encode("ascii", errors="ignore")
        .str.decode("utf-8")
        .str.lower()
        .str.replace(r"[\s\(\)]", "_", regex=True)
        .str.replace("__", "_", regex=True)
        .str.strip("_")
    )

    # Define a pasta de saída
    pasta_saida = BASE_DIR / "data" / "processed"

    # Cria a pasta caso ela não exista
    pasta_saida.mkdir(parents=True, exist_ok=True)

    # Define o nome do arquivo parquet de saída
    arquivo_saida = (
        pasta_saida / f"{nome_indicador}.parquet"
    )

    # Salva o DataFrame em formato parquet
    df.to_parquet(
        arquivo_saida,
        index=False
    )

    # Exibe mensagem de sucesso
    print(f"Arquivo salvo em: {arquivo_saida}")

    # Retorna o DataFrame transformado
    return df

# Executa apenas quando o arquivo for rodado diretamente
if __name__ == "__main__":

    # Exemplo: transforma os dados do IPCA
    transformar_indicador("ipca")