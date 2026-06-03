from pathlib import Path
import json
import requests

BASE_DIR = Path(__file__).resolve().parent.parent.parent

def extrair_serie_bcb(
    codigo_serie: int,
    nome_indicador: str,
    data_inicio: str,
    data_fim: str
) -> list[dict]:
    """
    Extrai uma série temporal da API SGS do Banco Central
    e salva os dados em data/raw/{nome_indicador}.json.

    Args:
        codigo_serie: Código da série no SGS.
        nome_indicador: Nome do indicador para o arquivo de saída.
        data_inicio: Data inicial no formato dd/MM/yyyy.
        data_fim: Data final no formato dd/MM/yyyy.

    Returns:
        Lista de dicionários contendo os dados retornados pela API.
    """

    # Monta a URL da API com os parâmetros recebidos
    url = (
        f"https://api.bcb.gov.br/dados/serie/"
        f"bcdata.sgs.{codigo_serie}/dados"
        f"?formato=json"
        f"&dataInicial={data_inicio}"
        f"&dataFinal={data_fim}"
    )

    # Faz a requisição HTTP GET para a API
    response = requests.get(url, timeout=30)

    # Lança uma exceção caso a resposta tenha erro (404, 500, etc.)
    response.raise_for_status()

    # Converte o JSON retornado pela API para uma lista de dicionários Python
    dados = response.json()

    # Verifica se a API retornou algum dado
    if not dados:
        raise ValueError(
            f"Nenhum dado encontrado para a série {codigo_serie}"
        )

    # Define o caminho onde o arquivo será salvo
    pasta_saida = BASE_DIR / "data" / "raw"

    # Cria a pasta caso ela não exista
    pasta_saida.mkdir(parents=True, exist_ok=True)

    # Define o nome final do arquivo JSON
    arquivo_saida = pasta_saida / f"{nome_indicador}.json"

    # Abre o arquivo em modo escrita ("w") w-> write
    # "r" == ler arquivo
    # "a" == adicionar conteúdo ao final
    # "x" == criar arquivo novo (erro se já existir)
    # utiliza o with para o python fechar o arquivo sozinho após o 'open'
    with open(arquivo_saida, "w", encoding="utf-8") as arquivo:

        # Salva os dados em formato JSON
        # ensure_ascii=False mantém caracteres acentuados
        # indent=4 deixa o arquivo formatado e legível
        json.dump(
            dados,
            arquivo,
            ensure_ascii=False,
            indent=4
        )

    # Exibe mensagem indicando onde o arquivo foi salvo
    print(f"Arquivo salvo em: {arquivo_saida}")

    # Retorna os dados para uso em outras etapas do pipeline
    return dados


# Executa apenas quando o arquivo é rodado diretamente
if __name__ == "__main__":

    # Exemplo: extrai a série do IPCA (código 433)
    extrair_serie_bcb(
        codigo_serie=433,
        nome_indicador="ipca",
        data_inicio="01/01/2020",
        data_fim="31/12/2024"
    )