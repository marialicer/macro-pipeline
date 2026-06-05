# 📊 Macro Pipeline — Monitoramento de Indicadores Macroeconômicos

> Pipeline de dados automatizado para monitoramento de indicadores macroeconômicos brasileiros, desenvolvido como projeto de portfólio com Apache Airflow, PostgreSQL e Power BI.

---

## 🏢 Contexto de Negócio

A **Vesta Capital**, gestora de investimentos, precisa acompanhar continuamente a evolução dos principais indicadores macroeconômicos brasileiros para embasar decisões de alocação, hedge e timing de mercado.

**Problema:** os dados estão dispersos em portais do governo e exigem coleta manual e recorrente.

**Solução:** pipeline automatizado que coleta, processa e disponibiliza séries históricas de indicadores-chave, com visualização de tendências e variações period over period em dashboard no Power BI.

**Metodologia de design:** CRISP-DM

---

## 🏗️ Arquitetura

```
APIs Públicas (BCB / IBGE)
        │
        ▼
 DAG 1 — ingestao_macropipeline     → data/raw/*.json
 (Dynamic Task Mapping)
        │ Dataset: bcb_ingestao
        ▼
 DAG 2 — transformacao_macropipeline → data/processed/*.parquet
 (Branching + Datasets)
        │ Dataset: bcb_transformacao
        ▼
 DAG 3 — relatorio_macropipeline    → PostgreSQL
 (Dynamic Task Mapping + Auditoria)
        │
        ▼
   Power BI Desktop
```

### Serviços Docker

| Serviço | Descrição |
|---|---|
| `postgres` | Banco de metadados interno do Airflow |
| `postgres_projeto` | Banco de dados do projeto (porta 5433) |
| `redis` | Broker de mensagens do CeleryExecutor |
| `airflow-apiserver` | Interface web do Airflow (porta 8080) |
| `airflow-scheduler` | Agendador de DAGs |
| `airflow-worker` | Executor de tasks |

---

## 📡 Fontes de Dados

Todas as fontes são públicas e gratuitas, sem necessidade de autenticação.

| Indicador | Fonte | Código SGS | Frequência |
|---|---|---|---|
| IPCA | API SGS/BCB | 433 | Mensal |
| Taxa Selic | API SGS/BCB | 432 | Mensal |
| Câmbio USD/BRL | API SGS/BCB | 1 | Diária → agregada mensalmente |
| Taxa de Desemprego (PNAD) | API SGS/BCB | 24369 | Mensal |

Período histórico carregado: **janeiro/2020 a dezembro/2024**

---

## 🔄 DAGs

### DAG 1 — `ingestao_macropipeline`
- **Schedule:** `@monthly`
- **Conceitos praticados:** Dynamic Task Mapping, Datasets, XComs
- **O que faz:** usa `.expand()` para criar automaticamente uma task de extração por indicador (paralelismo automático). Cada task chama a API SGS do BCB e salva o resultado em `data/raw/{indicador}.json`. Ao final, a task `publicar_dataset` atualiza o Dataset `bcb_ingestao`, disparando a DAG 2.

```
extrair[ipca]      ──┐
extrair[selic]     ──┤
extrair[cambio]    ──┼──► publicar_dataset ──► (Dataset: bcb_ingestao)
extrair[desemprego]──┘
```

### DAG 2 — `transformacao_macropipeline`
- **Schedule:** trigada por Dataset `bcb_ingestao`
- **Conceitos praticados:** Branching, inter-DAG triggering via Dataset
- **O que faz:** para cada indicador, um `@task.branch` decide qual transformação executar com base na fonte (`BCB` ou `IBGE`). Converte tipos, corrige datas e salva em `data/processed/{indicador}.parquet`. Publica o Dataset `bcb_transformacao` ao final.

```
branch_ipca  →  transformar_bcb_ipca  (transformar_ibge_ipca skipped)
branch_selic →  transformar_bcb_selic (transformar_ibge_selic skipped)
...
└──► publicar_dataset ──► (Dataset: bcb_transformacao)
```

### DAG 3 — `relatorio_macropipeline`
- **Schedule:** trigada por Dataset `bcb_transformacao`
- **Conceitos praticados:** Dynamic Task Mapping, auditoria de pipeline
- **O que faz:** carrega os parquets no PostgreSQL via upsert para cada indicador (Dynamic Task Mapping). Ao final, registra a execução na tabela `execucoes_pipeline` para auditoria.

```
carregar[ipca]      ──┐
carregar[selic]     ──┤
carregar[cambio]    ──┼──► registrar_execucao
carregar[desemprego]──┘
```

> **Nota:** o `FileSensor` foi removido da versão final pois o Dataset já garante que a transformação foi concluída antes de disparar esta DAG.

---

## 🗄️ Schema do Banco de Dados

### `indicadores_macroeconomicos`
| Coluna | Tipo | Descrição |
|---|---|---|
| `id` | SERIAL PK | Identificador único |
| `indicador` | VARCHAR(50) | Nome do indicador (ex: "ipca") |
| `fonte` | VARCHAR(10) | Origem do dado (BCB, IBGE) |
| `data_referencia` | DATE | Primeiro dia do período |
| `valor` | NUMERIC(12,4) | Valor do indicador |
| `unidade` | VARCHAR(20) | Unidade de medida (%, R$) |
| `periodicidade` | VARCHAR(15) | mensal, trimestral, diaria |
| `criado_em` | TIMESTAMP | Data de inserção (DEFAULT NOW()) |

Restrição de unicidade: `UNIQUE (indicador, data_referencia)`

### `execucoes_pipeline`
| Coluna | Tipo | Descrição |
|---|---|---|
| `id` | SERIAL PK | Identificador único |
| `dag_id` | VARCHAR(50) | Nome da DAG executada |
| `indicador` | VARCHAR(50) | Indicador processado |
| `status` | VARCHAR(15) | SUCESSO, ERRO, etc. |
| `registros_inseridos` | INT | Quantidade de registros carregados |
| `data_execucao` | TIMESTAMP | Data do registro (DEFAULT NOW()) |
| `mensagem` | TEXT | Mensagem complementar (opcional) |

---

## 🚀 Como Executar

### Pré-requisitos
- Docker Desktop instalado e em execução
- Mínimo 4GB de RAM alocados para o Docker
- Power BI Desktop (para o dashboard)

### 1. Clone o repositório
```bash
git clone https://github.com/marialicer/macro-pipeline.git
cd macro-pipeline
```

### 2. Configure as variáveis de ambiente
Crie um arquivo `.env` na raiz do projeto:
```env
AIRFLOW_IMAGE_NAME=apache/airflow:3.1.0
AIRFLOW_UID=50000
POSTGRES_PROJETO_USER=seu_usuario
POSTGRES_PROJETO_PASSWORD=sua_senha
POSTGRES_PROJETO_DB=nome_do_banco
```

### 3. Inicialize o Airflow
```bash
docker-compose up airflow-init
```
Aguarde a mensagem `airflow-init exited with code 0`.

### 4. Suba os serviços
```bash
docker-compose up -d
```

### 5. Crie as tabelas no banco do projeto
```powershell
# PowerShell (Windows)
Get-Content sql/create_tables.sql | docker exec -i postgres_projeto psql -U seu_usuario -d nome_do_banco
```

### 6. Configure a Connection no Airflow
Acesse `http://localhost:8080` (usuário: `airflow` / senha: `airflow`) → Admin → Connections → e crie:

| Campo | Valor |
|---|---|
| Connection Id | `postgres_projeto` |
| Connection Type | `Postgres` |
| Host | `postgres_projeto` |
| Port | `5432` |
| Database | valor do `POSTGRES_PROJETO_DB` |
| Login | valor do `POSTGRES_PROJETO_USER` |
| Password | valor do `POSTGRES_PROJETO_PASSWORD` |

### 7. Execute o pipeline
Ative e dispare a DAG `ingestao_macropipeline` na interface do Airflow. As DAGs 2 e 3 serão trigadas automaticamente via Datasets ao final de cada etapa.

### 8. Conecte o Power BI
Abra o Power BI Desktop → Obter Dados → PostgreSQL:
- **Servidor:** `localhost:5433`
- **Banco de dados:** valor do `POSTGRES_PROJETO_DB`
- Importe a tabela `indicadores_macroeconomicos`

> A atualização do dashboard é feita manualmente após cada execução — abra o `.pbix` e clique em **Atualizar**.

---

## 📁 Estrutura do Projeto

```
macro-pipeline/
├── dags/
│   ├── dag_ingestao.py           # DAG 1 — Extração (Dynamic Task Mapping)
│   ├── dag_transformacao.py      # DAG 2 — Transformação (Branching)
│   └── dag_relatorio.py          # DAG 3 — Carga (Dataset trigger + auditoria)
├── scripts/
│   ├── extract/
│   │   └── bcb.py                # Extração da API SGS/BCB
│   ├── transform/
│   │   └── bcb.py                # Transformação dos dados BCB
│   └── load/
│       └── postgres.py           # Carga no PostgreSQL + auditoria
├── sql/
│   └── create_tables.sql         # Schema do banco de dados
├── data/
│   ├── raw/                      # JSONs brutos da API (gerado em execução)
│   └── processed/                # Parquets transformados (gerado em execução)
├── docker-compose.yaml
├── .env                          # Não versionado
└── README.md
```

---

## 🛠️ Stack Tecnológica

| Tecnologia | Versão | Uso |
|---|---|---|
| Apache Airflow | 3.1.0 | Orquestração do pipeline |
| PostgreSQL | 16 | Armazenamento dos dados |
| Python | 3.12 | Scripts de ETL |
| pandas | — | Transformação dos dados |
| psycopg2-binary | 2.9 | Conexão com PostgreSQL |
| pyarrow | — | Leitura/escrita de parquet |
| Docker + Docker Compose | — | Containerização |
| Power BI Desktop | — | Dashboard |

---

## 📈 Dashboard (Power BI)

O dashboard da Vesta Capital apresenta:

- Evolução mensal média dos indicadores
- Últimos valores dos indicadores
- Filtro por fonte e Data (mês e ano)

---

## 📝 Decisões de Design

**Por que upsert em vez de insert simples?**
Indicadores como desemprego têm frequência trimestral mas o pipeline roda mensalmente. O `ON CONFLICT DO NOTHING` por `(indicador, data_referencia)` torna a operação idempotente — segura para re-execuções e retries automáticos do Airflow.

**Por que parquet como formato intermediário?**
Parquet é colunar, comprimido e preserva tipos de dados nativamente — ideal para passar dados entre etapas de um pipeline sem perda de informação de tipo, especialmente datas e decimais.

**Por que separar scripts das DAGs?**
Mantém as DAGs limpas (somente orquestração) e os scripts testáveis de forma independente — cada módulo em `scripts/` pode ser executado diretamente via `python -m scripts.extract.bcb` para testes locais sem o Airflow.

**Por que dois bancos PostgreSQL no docker-compose?**
O banco `postgres` é interno ao Airflow (metadados de DAGs, execuções, logs). O `postgres_projeto` é exclusivo para os dados do projeto, isolando responsabilidades e facilitando conexão direta do Power BI sem interferir no funcionamento do Airflow.

---

## 👩‍💻 Autora

**Maria Alice Rocha** — Data Analyst  
[LinkedIn](#) · [GitHub](https://github.com/marialicer)
