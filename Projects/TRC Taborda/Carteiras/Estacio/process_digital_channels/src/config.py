# config.py

# WHERE clauses para Estácio
from Estacio.process_digital_channels.utils.queries import get_query_indicadores


WHERE_CLAUSES = {
    'campanhas': "A.GrupoPrincipal IN (SELECT G.id_grupo FROM grupo G WHERE G.ID_CAMPANHA = 141)",
    'clientes_mailing': "WHERE COD_CLI = 252",
    'acionamentos': """
        WHERE C.COD_CLI = 252
        AND C.COD_CAR = 95
        AND A.COD_RECUP NOT IN (15721)
        AND B.CLASSIFICACAO_ACIONAMENTO = 1
    """,
    'tabulacao': "WHERE COD_CLI = 252",
    'clientes_pagamentos': "WHERE B.COD_CLI = 252",
    'clientes_acordos': "WHERE B.COD_CLI = 252 AND B.COD_CAR = 95",
    'massivos': "WHERE ID_CAR = 95",
    'telefones': "WHERE COD_CLI = 252"
}

# Datasets a serem carregados para Estácio
DATASETS_TO_LOAD = {
    'discagens_expert': False,    
    'mailing_hist': True,        
    'tab_acionamentos': False,    
    'tabulacao_aciona': False,    
    'dw_calendario': False,       
    'pagamentos': False,          
    'acordos': False,
    'sms': True,               
    'rcs': True,               
    'email': True,
    'whats': True,             
    'telefone': False,          
    'blacklist_expert': False,  
    'discagens_trestto': False,
    'base_auxiliar': True       # ← exclusivo Estácio, não entra na função utilitária
}

DATASETS_EXCLUSIVOS_ESTACIO = {
    'base_auxiliar': get_query_indicadores,
}

# Colunas para cada tipo de massivo
COLUMNS_MASSIVOS = {
    'sms': """
        CAST(DATA AS DATE) AS DATA_DISPARO,
        TELEFONE AS TELEFONE,
        CPF,
        CUSTO,
        RECEPTIVO,
        PGTO,
        PGTO_VLR,
        RECEITA,
        CAST(PROMESSA_VCTO AS DATE) AS PROMESSA_VCTO
    """,
    'rcs': """
        CAST(DATA AS DATE) AS DATA_DISPARO,
        TELEFONE AS TELEFONE,
        CPF,
        CUSTO,
        RECEPTIVO,
        PGTO,
        PGTO_VLR,
        RECEITA,
        CAST(PROMESSA_VCTO AS DATE) AS PROMESSA_VCTO
    """,
    'whats': """
        CAST(DATA AS DATE) AS DATA_DISPARO,
        TELEFONE AS TELEFONE,
        CPF,
        CUSTO,
        RECEPTIVO,
        PGTO,
        PGTO_VLR,
        RECEITA,
        CAST(PROMESSA_VCTO AS DATE) AS PROMESSA_VCTO
    """,
    'email': """
        DATA,
        CUSTO,
        EMAIL,
        CPF,
        ENV_DEEPCENTER,
        NOME
    """
}


# ==============================================================================
# REGRAS DE NEGÓCIO - MAILING
# ==============================================================================

# Definição de Faixas de Atraso (Bins para pd.cut)
# Estrutura baseada na segmentação de 30 dias solicitada.
# Ajustado para evitar sobreposições (ex: 31-60 vs 31-65 presentes na solicitação).
FAIXAS_ATRASO_BINS = [
    float('-inf'), 
    1, 
    7, 
    15, 
    30, 
    60, 
    90, 
    180, 
    270, 
    360, 
    540, 
    720, 
    1080, 
    1440, 
    1800, 
    float('inf')
]

# Labels correspondentes aos Bins
FAIXAS_ATRASO_LABELS = [
    'Preventivo',
    'A - 01 - 07',    
    'B - 08 - 15',     
    'C - 16 - 30',     
    'D - 31 - 60',      
    'E - 61 - 90', 
    'F - 91 - 180', 
    'G - 181 - 270', 
    'H - 271 - 360', 
    'I - 361 - 540', 
    'J - 541 - 720', 
    'K - 721 - 1080', 
    'L - 1081 - 1440',
    'M - 1441 - 1800',
    'N - Maior 1800'
]
