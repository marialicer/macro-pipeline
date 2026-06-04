# ==============================================================================
# Extração de Séries Temporais — API SGS do Banco Central do Brasil
# ------------------------------------------------------------------------------
# Este módulo é responsável pela camada de extração (Extract) do pipeline.
# Consome a API pública do BCB, valida a resposta e persiste os dados brutos
# em data/raw/{nome_indicador}.json para processamento posterior.
# ==============================================================================

from pathlib import Path
import json
import requests

# Resolve o diretório raiz do projeto navegando 3 níveis acima deste arquivo:
# scripts/extract/bcb.py → scripts/extract → scripts → raiz do projeto
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
        codigo_serie:   Código da série no SGS (ex: 433 = IPCA).
        nome_indicador: Nome do indicador usado no arquivo de saída (ex: "ipca").
        data_inicio:    Data inicial no formato dd/MM/yyyy.
        data_fim:       Data final no formato dd/MM/yyyy.

    Returns:
        Lista de dicionários com os dados retornados pela API.
        Exemplo de item: {"data": "01/01/2020", "valor": "0.21"}
    """

    # --------------------------------------------------------------------------
    # 1. Construção da URL
    # --------------------------------------------------------------------------
    # Endpoint público do SGS (Sistema Gerenciador de Séries Temporais do BCB).
    # Os parâmetros de data filtram o período desejado diretamente na requisição.
    # --------------------------------------------------------------------------
    url = (
        f"https://api.bcb.gov.br/dados/serie/"
        f"bcdata.sgs.{codigo_serie}/dados"
        f"?formato=json"
        f"&dataInicial={data_inicio}"
        f"&dataFinal={data_fim}"
    )

    # --------------------------------------------------------------------------
    # 2. Requisição HTTP
    # --------------------------------------------------------------------------
    # timeout=30 evita que a task do Airflow fique presa indefinidamente
    # caso a API do BCB esteja lenta ou indisponível.
    # --------------------------------------------------------------------------
    response = requests.get(url, timeout=30)

    # Lança HTTPError automaticamente para status 4xx (cliente) e 5xx (servidor)
    response.raise_for_status()

    # --------------------------------------------------------------------------
    # 3. Parse e validação da resposta
    # --------------------------------------------------------------------------
    # Deserializa o JSON da resposta para uma lista de dicionários Python.
    dados = response.json()

    # Garante que a API retornou registros; séries com código errado ou fora do
    # período retornam lista vazia sem lançar erro HTTP, então validamos aqui.
    if not dados:
        raise ValueError(
            f"Nenhum dado encontrado para a série {codigo_serie}"
        )

    # --------------------------------------------------------------------------
    # 4. Persistência em disco (camada Raw)
    # --------------------------------------------------------------------------
    # Os dados brutos são salvos sem transformação, preservando o formato
    # original da API para auditoria e reprocessamento futuro se necessário.
    # --------------------------------------------------------------------------

    # Monta o caminho de saída: <raiz>/data/raw/
    pasta_saida = BASE_DIR / "data" / "raw"

    # Cria a pasta e todos os diretórios intermediários caso não existam.
    # exist_ok=True evita erro se a pasta já existir.
    pasta_saida.mkdir(parents=True, exist_ok=True)

    arquivo_saida = pasta_saida / f"{nome_indicador}.json"

    # Modos de abertura de arquivo mais comuns:
    #   "r" → leitura (padrão; erro se não existir)
    #   "w" → escrita, sobrescreve o arquivo se já existir
    #   "a" → append, adiciona conteúdo ao final sem apagar o existente
    #   "x" → criação exclusiva (lança erro se o arquivo já existir)
    #
    # O bloco 'with' garante que o arquivo seja fechado automaticamente
    # ao sair do bloco, mesmo que ocorra uma exceção durante a escrita.
    with open(arquivo_saida, "w", encoding="utf-8") as arquivo:
        json.dump(
            dados,
            arquivo,
            ensure_ascii=False,  # Preserva caracteres acentuados (ã, ç, etc.)
            indent=4             # Indentação de 4 espaços para leitura humana
        )

    print(f"Arquivo salvo em: {arquivo_saida}")

    # Retorna os dados para que a task do Airflow possa propagá-los via XCom
    # para tasks downstream, sem necessidade de reler o arquivo do disco.
    return dados


# ==============================================================================
# Ponto de entrada para execução direta (fora do Airflow)
# ------------------------------------------------------------------------------
# Útil para testar a extração localmente sem precisar subir o ambiente Airflow.
# Este bloco NÃO é executado quando o módulo é importado por outro script.
# ==============================================================================
if __name__ == "__main__":

    # Exemplo: extrai a série do IPCA (código 433) para o período 2020–2024
    extrair_serie_bcb(
        codigo_serie=433,
        nome_indicador="ipca",
        data_inicio="01/01/2020",
        data_fim="31/12/2024"
    )