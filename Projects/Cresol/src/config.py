"""
Configuração do Projeto - Novo_Projeto
Define faixas de atraso, parâmetros de filtro e mapeamentos para o projeto.
"""
import os
 
# =============================================================================
# LOGS — caminhos para os arquivos de log da carteira Cresol
# =============================================================================
LOGS_DIR = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(LOGS_DIR, exist_ok=True)
 
LOG_ACIONAMENTOS = os.path.join(LOGS_DIR, 'acionamentos.txt')
LOG_DISCAGENS    = os.path.join(LOGS_DIR, 'discagens.txt')
LOG_MAILING      = os.path.join(LOGS_DIR, 'mailing.txt')
LOG_PAGAMENTOS   = os.path.join(LOGS_DIR, 'pagamentos.txt')
LOG_LOADING      = os.path.join(LOGS_DIR, 'loading.txt')
LOG_CHANNELS     = os.path.join(LOGS_DIR, 'channels.txt')
 
LOGS = {
    'acionamentos': LOG_ACIONAMENTOS,
    'discagens':    LOG_DISCAGENS,
    'mailing':      LOG_MAILING,
    'pagamentos':   LOG_PAGAMENTOS,
    'loading':      LOG_LOADING,
    'channels':     LOG_CHANNELS,
}
 

segmentacoes_extras = ['PF_PJ', 'PA']

# ==================== PARÂMETROS DE FILTRO ====================
COD_CLI_LISTA = [181, 230, 231, 254, 220]
ID_CAMPANHA = 40

# ==================== FAIXAS DE ATRASO ====================
# Bins e labels para categorização de atraso (em dias)
FAIXAS_ATRASO_BINS = [
    -1,      # Menor que 0
    0,       # 0
    30,      # 0-30
    60,      # 31-60
    90,      # 61-90
    120,     # 91-120
    150,     # 121-150
    180,     # 151-180
    360,     # 181-360
    720,     # 361-720
    1440,    # 721-1440
    float('inf')  # Maior que 1440
]

FAIXAS_ATRASO_LABELS = [
    'Preventivo',
    '0 - 0-30',
    '1 - 31-60',
    '2 - 61-90',
    '3 - 91-120',
    '4 - 121-150',
    '5 - 151-180',
    '5 - 181-360',
    '5 - 361-720',
    '5 - 721-1440',
    '6 - Maior que 1440'
]

# Datasets a carregar (apenas os necessários)
DATASETS_TO_LOAD = {
    'discagens_expert': {'active': True,  'query': None},
    'mailing_hist':     {'active': True,  'query': None},
    'tab_acionamentos': {'active': True,  'query': None},
    'tabulacao_aciona': {'active': True,  'query': None},
    'dw_calendario':    {'active': True,  'query': None},
    'pagamentos':       {'active': True,  'query': None},
    'acordos':          {'active': True,  'query': None},
    'sms':              {'active': True,  'query': None},
    'rcs':              {'active': True,  'query': None},
    'email':            {'active': True,  'query': None},
    'whats':            {'active': True,  'query': None},
}

# ============================================
# FILTROS SQL (WHERE CLAUSES)
# ============================================

WHERE_CLAUSES = {
    'campanhas': "A.GrupoPrincipal IN (SELECT G.id_grupo FROM grupo G WHERE G.ID_CAMPANHA = 40)",
    'clientes_mailing': "WHERE COD_CLI IN (181, 230, 231, 254, 220)",
    'acionamentos': """
        WHERE C.COD_CLI IN (181, 230, 231, 254, 220)
        AND A.COD_RECUP NOT IN (4084) 
        AND B.CLASSIFICACAO_ACIONAMENTO = 1
    """,
    'tabulacao': "WHERE IDCRM = 1",
    'clientes_pagamentos': "WHERE B.COD_CLI IN (181, 230, 231, 254, 220)",
    'clientes_acordos': "WHERE B.COD_CLI IN (181, 230, 231, 254, 220)",
    'massivos': "WHERE ID_CAR IN (89, 90, 91)"
}


# ==================== CONFIGURAÇÕES DE TRANSFORMAÇÃO ====================
# Mapeamento de grupos principais (será preenchido conforme necessário)
GRUPO_PRINCIPAL_MAP = {
    (4304, 4150): "ATIVO",
    4310: "MANUAL"
}

# Dimensões para análise de pagamentos
DIMENSOES_PAGAMENTOS = [
    'DATA',
    'CPF',
    'CONTRATO',
    'FAIXA'
]

# Transformações para discagens
TRANSFORMACOES_DISCAGENS = {
    'aplicar_faixa_atraso': True,
    'grupos_principais': True,
    'resultados_classificacao': True
}

# Caminhos base do projeto
# CAMINHO_BASE_CRESOL = r"c:\Users\claudiano.alves\Documents\Claudiano\repository\Data-Analysis-Portfolio\Projects\Cresol"

# CRESOL_PATHS = {
#     'base': CAMINHO_BASE_CRESOL,
#     'data': f"{CAMINHO_BASE_CRESOL}\data",
#     'logs': f"{CAMINHO_BASE_CRESOL}\logs",
#     'src': f"{CAMINHO_BASE_CRESOL}\src",
#     'utils': f"{CAMINHO_BASE_CRESOL}\utils",
# }

COLUMNS = {
    "discagens_expert": """
        Date(a.instante) AS DATA,
        a.id AS ID_DISCAGEM,
        a.chave1 AS CONTRATO,
        a.Chave3 AS CPF,
        a.Agente AS AGENTE,
        a.ddd AS DDD,
        a.fone AS TELEFONE,
        a.Instante200OKPub AS DATA_ENCERRAMENTO,
        a.GrupoPrincipal AS CAMPANHA,
        a.UltCodSigRecPublica AS COD_SIP,
        a.ResultadoClassificacao AS CLASS_RETORNO,
        a.MotivoEncerramentoBilhete AS DESC_MOTIVO_ENCERR
    """,
    "mailing_hist": """
        DATA,
        UPPER(LTRIM(RTRIM(CONTRATO))) AS CONTRATO,
        LTRIM(RTRIM(CPF)) AS CPF,
        ID_CAR,
        ATRASO,
        COD_CLI,
        VALOR
    """,
    "tab_acionamentos": """
        CAST(A.DATA_ACIONA AS DATE) AS DATA_ACIONA,
        CAST(A.DATA_ACIONA AS TIME) AS HORA,
        UPPER(LTRIM(RTRIM(A.CONTRATO_FIN))) AS CONTRATO_FIN,
        LTRIM(RTRIM(C.CPF_DEV)) AS CPF_DEV,
        LTRIM(RTRIM(A.COD_ACIONAMENTO)) AS COD_ACIONA,
        LTRIM(RTRIM(B.DESC_ACIONAMENTO)) AS DESC_ACIONAMENTO,
        LTRIM(RTRIM(REC.COD_RECUP)) AS COD_RECUP,
        LTRIM(RTRIM(REC.NOME_RECUP)) AS NOME_RECUP,
        LTRIM(RTRIM(REC.LOGIN_RECUP)) AS LOGIN_RECUP,
        LTRIM(RTRIM(REC.ULTGRUPO_RECUP)) AS ULTGRUPO_RECUP,
        LTRIM(RTRIM(C.COD_CLI)) AS COD_CLI,
        C.VALORPRIN_FIN AS VALORPRIN_FIN,
        LTRIM(RTRIM(C.STATCONT_FIN)) AS STATCONT_FIN,
        C.DTDEVOL_FIN AS DTDEVOL_FIN,
        C.DTENTRADA_FIN AS DTENTRADA_FIN,
        LTRIM(RTRIM(B.CLASSIFICACAO_ACIONAMENTO)) AS CLASSIFICACAO_ACIONAMENTO
    """,
    "acordos": """
        B.CONTRATO_FIN,
        B.CPF_DEV,
        CAST(A.DTACORDOHORA_ACO AS DATE) DATA_ACORDO,
        CAST(P.VENC_ACOP AS DATE) VENC_PARCELA,
        CAST(A.DTCANCELAMENTOACO_ACO AS DATE) CANC_ACORDO,
        A.NACORDO_ACO,
        A.VLRACORDO_ACO,
        P.VALOR_ACOP,
        A.RECUP_ACO,
        P.PARCELA_ACOP,
        A.COD_STAC,
        Q.NOME_RECUP AS RECUPERADOR,
        B.COD_CLI,
        B.COD_CAR,
        B.ATRASO_FIN
    """,
    'sms': """
        CAST(DATA AS DATE) AS DATA_DISPARO,
        CPF,
        CASE 
            WHEN LEFT(RIGHT(LTRIM(RTRIM(CPF)),6),3) = '000' THEN 'PJ' 
            ELSE 'PF'
        END PF_PJ
    """,
    'rcs': """
        CAST(DATA AS DATE) AS DATA_DISPARO,
        CPF,
        CASE 
            WHEN LEFT(RIGHT(LTRIM(RTRIM(CPF)),6),3) = '000' THEN 'PJ' 
            ELSE 'PF'
        END PF_PJ
    """,
    'whats': """
        CAST(DATA AS DATE) AS DATA_DISPARO,
        CPF,
        CASE 
            WHEN LEFT(RIGHT(LTRIM(RTRIM(CPF)),6),3) = '000' THEN 'PJ' 
            ELSE 'PF'
        END PF_PJ
    """,
    'email': """
        DATA,
        CPF,
        CASE 
            WHEN LEFT(RIGHT(LTRIM(RTRIM(CPF)),6),3) = '000' THEN 'PJ' 
            ELSE 'PF'
        END PF_PJ
    """
}

ORIGEM_MAP = None
ORIGEM_DEFAULT = None

TRANSFORMACOES_DISCAGENS = {
    'grupo_map': GRUPO_PRINCIPAL_MAP,
    'adicionar_estado': True,
    'enriquecer_mailing_calendario': True,
}


# ==================== CONFIGURAÇÕES ADICIONAIS ====================
# Adicione aqui outras configurações conforme necessário
DEBUG = False
LOG_LEVEL = 'INFO'
