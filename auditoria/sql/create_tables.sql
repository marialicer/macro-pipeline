CREATE TABLE IF NOT EXISTS indicadores_macroeconomicos (
    id SERIAL PRIMARY KEY,
    indicador VARCHAR(50) NOT NULL,
    fonte VARCHAR(10) NOT NULL,
    data_referencia DATE NOT NULL,
    valor NUMERIC(12,4) NOT NULL,
    unidade VARCHAR(20) NOT NULL,
    periodicidade VARCHAR(15) NOT NULL,
    criado_em TIMESTAMP DEFAULT NOW(),
    UNIQUE (indicador, data_referencia)
);

CREATE TABLE IF NOT EXISTS execucoes_pipeline(
    id SERIAL PRIMARY KEY,
    dag_id VARCHAR(50) NOT NULL,
    indicador VARCHAR(50) NOT NULL,
    status VARCHAR(15) NOT NULL,
    registros_inseridos INT,
    data_execucao TIMESTAMP DEFAULT NOW() NOT NULL,
    mensagem TEXT
);