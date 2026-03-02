use Planejamento

CREATE TABLE sintetico_digital_estacio (
    DATA DATE NOT NULL,
    CANAL VARCHAR(50) NOT NULL,
    GRUPO_SEGMENTADO VARCHAR(100),
    PRODUTO_SEGMENTADO VARCHAR(100),
    qte_cpf_unique INT,
    qte_envio INT,
    custo DECIMAL(18,2)
);


SELECT TOP 100 * FROM sintetico_digital_estacio

-- =====================================================
-- TABELA UNIFICADA DE ANALÍTICOS - CANAIS DIGITAIS
-- =====================================================

CREATE TABLE analytical_digital_estacio (

    -- Chave surrogate
    ID              BIGINT          IDENTITY(1,1)   NOT NULL,

    -- Controle de carga
    CANAL           VARCHAR(20)     NOT NULL,   -- 'EMAIL', 'SMS', 'RCS', 'WHATSAPP'
    DT_CARGA        DATETIME2       NOT NULL    DEFAULT GETDATE(),

    -- Campos da fonte original
    DATA_DISPARO    DATETIME2       NULL,
    CPF             VARCHAR(14)     NULL,
    CUSTO           DECIMAL(18,4)   NULL,
    CONTATO         VARCHAR(100)    NULL,
    CONTRATO        VARCHAR(50)     NULL,
    ATRASO          INT             NULL,
	CORRESPONDENCIA  BIT  NOT NULL  DEFAULT 0,  -- 0 = sem correspondência, 1 = com correspondência
    COD_CLI         BIGINT          NULL,
    ID_CLIENTE      VARCHAR(50)     NULL,
    REGIONAL        VARCHAR(100)    NULL,
    GRUPO           VARCHAR(100)    NULL,
    GRUPO_SEGMENTADO        VARCHAR(100)    NULL,
    PRODUTO_SEGMENTADO      VARCHAR(100)    NULL,
    SPD             VARCHAR(50)     NULL,
    BU              VARCHAR(100)    NULL,
    MODALIDE        VARCHAR(100)    NULL,
    STDEBITO        VARCHAR(50)     NULL,
    STALUNO         VARCHAR(50)     NULL,
    CURSO           VARCHAR(200)    NULL,
    APROACAD        VARCHAR(100)    NULL,
    PRODUTO         VARCHAR(200)    NULL,
    ULTRENOV        DATETIME2       NULL,
    COD_PRODUT      BIGINT          NULL,
    FX_ATRASO       VARCHAR(50)     NULL,

    CONSTRAINT PK_analytical_digital_estacio PRIMARY KEY (ID)
);

-- =====================================================
-- ÍNDICES
-- =====================================================

CREATE INDEX IX_analytical_canal_disparo
    ON analytical_digital_estacio (CANAL, DATA_DISPARO);

CREATE INDEX IX_analytical_cpf
    ON analytical_digital_estacio (CPF);

CREATE INDEX IX_analytical_contrato
    ON analytical_digital_estacio (CONTRATO);

CREATE INDEX IX_analytical_cod_cli
    ON analytical_digital_estacio (COD_CLI);

CREATE INDEX IX_analytical_contato
    ON analytical_digital_estacio (CONTATO);

CREATE INDEX IX_analytical_dt_carga
    ON analytical_digital_estacio (DT_CARGA);

CREATE INDEX IX_analytical_correspondencia
	ON analytical_digital_estacio (CANAL, CORRESPONDENCIA);

ALTER TABLE analytical_digital_estacio
ADD VALOR DECIMAL(18,4) NULL;

use Planejamento
--drop table analytical_digital_estacio
SELECT 
	top 100 * 
	--count(*)
	--DISTINCT GRUPO_SEGMENTADO
FROM analytical_digital_estacio
WHERE VALOR IS NULL
CANAL = 'sms' AND CAST(DATA_DISPARO AS DATE) = '20250902'

