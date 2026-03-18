CREATE TABLE sintetico_cresol (
    id            BIGINT          IDENTITY(1,1)   NOT NULL,
	dt_carga      DATETIME2       NOT NULL    DEFAULT GETDATE(),
	data          DATE          NOT NULL,
    indicador     VARCHAR(100)  NOT NULL,
    qte           INT           NULL,
    PF_PJ         VARCHAR(10)   NULL,
    PA            VARCHAR(50)   NULL,
    mes_abreviado  CHAR(3)       NULL,
    nr_dia_util   TINYINT       NULL,
    quartil       VARCHAR(20)   NULL,
    dt_mes        TINYINT       NULL,
    valor         FLOAT         NULL,
    tipo          VARCHAR(50)   NULL
);

CREATE TABLE analytical_tempos_cresol (
    id            BIGINT          IDENTITY(1,1)   NOT NULL,
	dt_carga      DATETIME2       NOT NULL    DEFAULT GETDATE(),
	data          DATE          NOT NULL,
    indicador     VARCHAR(100)  NOT NULL,
    qte           INT           NULL,
    PF_PJ         VARCHAR(10)   NULL,
    PA            VARCHAR(50)   NULL,
    mes_abreviado  CHAR(3)       NULL,
    nr_dia_util   TINYINT       NULL,
    quartil       VARCHAR(20)   NULL,
    dt_mes        TINYINT       NULL,
    valor         FLOAT         NULL,
    tipo          VARCHAR(50)   NULL
);

CREATE TABLE analytical_mailing_cresol (
    id					BIGINT          IDENTITY(1,1)   NOT NULL,
	dt_carga			DATETIME2       NOT NULL    DEFAULT GETDATE(),
	data				DATE          NOT NULL,
    indicador			VARCHAR(100)  NOT NULL,
    qte					INT           NULL,
    PF_PJ				VARCHAR(10)   NULL,
    PA					VARCHAR(50)   NULL,
    mes_abreviado		CHAR(3)       NULL,
    nr_dia_util			TINYINT       NULL,
    quartil				VARCHAR(20)   NULL,
    dt_mes				TINYINT       NULL,
    valor				FLOAT         NULL,
    tipo				VARCHAR(50)   NULL
);

CREATE TABLE analytical_discagens_expert_cresol (
    ID                  BIGINT      IDENTITY(1,1)   NOT NULL,
    dt_carga            DATETIME2   NOT NULL DEFAULT GETDATE(),
    data                DATE        NOT NULL,
    id_discagem         VARCHAR(50) NULL,
    contrato            VARCHAR(50) NULL,
    agente              VARCHAR(100) NULL,
    ddd                 CHAR(2)     NULL,
    telefone            VARCHAR(20) NULL,
    data_encerramento   DATE        NULL,
    campanha            VARCHAR(100) NULL,
    cod_sip             VARCHAR(20) NULL,
    class_retorno       VARCHAR(50) NULL,
    desc_motivo_encerr  VARCHAR(100) NULL,
    cod_motivo_encerr   VARCHAR(20) NULL,
    operacao            VARCHAR(50) NULL,
    estado              CHAR(2)     NULL,
    id_car              VARCHAR(20) NULL,
    atraso              INT         NULL,
    cod_cli             VARCHAR(20) NULL,
    valor               FLOAT       NULL,
    pf_pj               VARCHAR(10) NULL,
    pa                  VARCHAR(50) NULL,
    cpf                 VARCHAR(14) NULL,
    nr_dia_util         TINYINT     NULL,
    quartil             VARCHAR(20) NULL,
    dt_mes              TINYINT     NULL,
    mes_abreviado       CHAR(3)     NULL,
);

CREATE TABLE analytical_acionamentos_cresol (
    ID                          BIGINT          IDENTITY(1,1)   NOT NULL,
    dt_carga                    DATETIME2       NOT NULL DEFAULT GETDATE(),
    data_aciona                 DATE            NOT NULL,
    hora                        TIME            NULL,
    contrato_fin                VARCHAR(50)     NULL,
    cpf_dev                     VARCHAR(14)     NULL,
    cod_aciona                  VARCHAR(20)     NULL,
    desc_acionamento            VARCHAR(100)    NULL,
    cod_recup                   VARCHAR(20)     NULL,
    nome_recup                  VARCHAR(100)    NULL,
    login_recup                 VARCHAR(50)     NULL,
    ultgrupo_recup              VARCHAR(50)     NULL,
    valorprin_fin               FLOAT           NULL,
    statcont_fin                VARCHAR(20)     NULL,
    dtdevol_fin                 DATE            NULL,
    dtentrada_fin               DATE            NULL,
    classificacao_acionamento   VARCHAR(50)     NULL,
    acionamentos                INT             NULL,
    cpc                         TINYINT         NULL,
    cpca                        TINYINT         NULL,
    promessa                    TINYINT         NULL,
    descr                       VARCHAR(200)    NULL,
    cpf                         VARCHAR(14)     NULL,
    id_car                      VARCHAR(20)     NULL,
    atraso                      INT             NULL,
    valor                       FLOAT           NULL,
    pf_pj                       VARCHAR(10)     NULL,
    pa                          VARCHAR(50)     NULL,
    cod_cli                     VARCHAR(20)     NULL,
    nr_dia_util                 TINYINT         NULL,
    quartil                     VARCHAR(20)     NULL,
    dt_mes                      TINYINT         NULL,
    mes_abreviado               CHAR(3)         NULL,
);


CREATE TABLE analytical_massivos_cresol (
    ID            BIGINT        IDENTITY(1,1) NOT NULL,
    dt_carga      DATETIME2     NOT NULL DEFAULT GETDATE(),
    cpf           VARCHAR(14)   NULL,
    data          DATE          NOT NULL,
    canal         VARCHAR(20)   NULL,
    contrato      VARCHAR(50)   NULL,
    id_car        VARCHAR(20)   NULL,
    atraso        INT           NULL,
    cod_cli       VARCHAR(20)   NULL,
    valor         FLOAT         NULL,
    pf_pj         VARCHAR(10)   NULL,
    pa            VARCHAR(50)   NULL,
    nr_dia_util   TINYINT       NULL,
    quartil       VARCHAR(20)   NULL,
    dt_mes        TINYINT       NULL,
    mes_abreviado CHAR(3)       NULL,
);

select 
	data,
	sum(valor) 
from sintetico_cresol
where indicador = 'Carteira (CPFs)' and tipo = 'daily' and pf_pj = 'unique'
group by data
order by data asc

select max(data) from sintetico_cresol

select 
	*
from sintetico_cresol where process is null
where tipo = 'daily' and pf_pj = 'unique'

TRUNCATE TABLE sintetico_cresol;

drop table sintetico_cresol

SELECT TOP 100 * FROM MAILING_HIST WHERE DATA = '20260117' AND COD_CLI = 247