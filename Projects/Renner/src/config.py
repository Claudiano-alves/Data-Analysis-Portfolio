"""
Arquivo de configuração global do projeto Ouze.
Contém constantes, caminhos e regras de negócio específicas.
"""
import os
from Renner.utils.queries import get_query_base_aux_renner, get_query_discagens_olos

# ==============================================================================
# CAMINHOS E LOGS
# ==============================================================================
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
LOG_PATH = os.path.join(PROJECT_ROOT, 'logs')
os.makedirs(LOG_PATH, exist_ok=True)
LOG_OUZE = os.path.join(LOG_PATH, 'pipeline_ouze.log')

# ==============================================================================
# REGRAS DE NEGÓCIO - MAILING
# ==============================================================================

# Definição de Faixas de Atraso (Bins para pd.cut)
# Estrutura baseada na segmentação de 30 dias solicitada.
# Ajustado para evitar sobreposições (ex: 31-60 vs 31-65 presentes na solicitação).
FAIXAS_ATRASO_BINS = [
    float('-inf'), 
    0, 
    30, 
    60, 
    90, 
    120, 
    150, 
    180, 
    360, 
    720, 
    1440, 
    float('inf')
]

# Labels correspondentes aos Bins
FAIXAS_ATRASO_LABELS = [
    'Preventivo', 
    '0 - 0-30',     
    '1 - 31-60',    
    '2 - 61-90',    
    '3 - 91-120',   
    '4 - 121-150',  
    '5 - 151-180',  
    '6 - 181-360',  
    '7 - 361-720',  
    '8 - 721-1440', 
    '9 - Maior 1440'
]

DATASETS_EXCLUSIVOS_RENNER = {
    'base_auxiliar_renner': get_query_base_aux_renner,
    'discagens_olos': get_query_discagens_olos
}

# Datasets a carregar (apenas os necessários)
DATASETS_TO_LOAD = {
    'discagens_expert': True,    
    'mailing_hist': True,              
    'tab_acionamentos': True,    
    'tabulacao_aciona': True,    
    'dw_calendario': True,       
    'pagamentos': True,          
    'acordos': True,
    'sms': True,               
    'rcs': True,               
    'email': True,             
    'whats': True,
    'base_auxiliar_renner': True,
    'discagens_olos': True
}

# ============================================
# FILTROS SQL (WHERE CLAUSES)
# ============================================

WHERE_CLAUSES = {
    'campanhas': "A.GrupoPrincipal IN (SELECT G.id_grupo FROM grupo G WHERE G.ID_CAMPANHA = 130)",
    'clientes_mailing': "WHERE COD_CLI = 247",
    'acionamentos': """
        WHERE C.COD_CLI = 247
        AND A.COD_RECUP NOT IN (15721, 4084) 
        AND B.CLASSIFICACAO_ACIONAMENTO = 1
    """,
    'tabulacao': "WHERE IDCRM = 1",
    'clientes_pagamentos': "WHERE B.COD_CLI = 247",
    'clientes_acordos': "WHERE B.COD_CLI = 247",
    'massivos': "WHERE ID_CAR = 79",
    'telefones': "WHERE COD_CLI = 247"
}


# Colunas para cada tipo de massivo
COLUMNS_MASSIVOS = {
    'sms': """
        CAST(DATA AS DATE) AS DATA_DISPARO,
        CPF
    """,
    'rcs': """
        CAST(DATA AS DATE) AS DATA_DISPARO,
        CPF
    """,
    'whats': """
        CAST(DATA AS DATE) AS DATA_DISPARO,
        CPF
    """,
    'email': """
        DATA,
        CPF
    """
}

# Diretório base de logs (criado automaticamente se não existir)
LOGS_DIR = os.path.join(os.path.dirname(__file__), 'logs')

# Caminhos específicos por módulo
LOG_ACIONAMENTOS = os.path.join(LOGS_DIR, 'acionamentos.txt')
LOG_DISCAGENS = os.path.join(LOGS_DIR, 'discagens.txt')
LOG_MAILING = os.path.join(LOGS_DIR, 'mailing.txt')
LOG_PAGAMENTOS = os.path.join(LOGS_DIR, 'pagamentos.txt')
LOG_LOADING = os.path.join(LOGS_DIR, 'loading.txt')

# Dicionário de logs (útil para acesso dinâmico)
LOGS = {
    'acionamentos': LOG_ACIONAMENTOS,
    'discagens': LOG_DISCAGENS,
    'mailing': LOG_MAILING,
    'pagamentos': LOG_PAGAMENTOS,
    'loading': LOG_LOADING,
}

# ============================================
# MAPEAMENTO DE GRUPOS PRINCIPAIS
# ============================================

GRUPO_PRINCIPAL_MAP = {
    ('4638', '4543', '4525', '4522', '4709', '4710', '4792'): "ATIVO",
    4529: "MANUAL",
    4528: "RECEPTIVO",
    (4549, 4527, 4791): "URA"
}

# Dimensões de agrupamento para acumulados
DIMENSOES_ACUMULADO = ['FX_ATRASO']

# Mapeamento de origem (para adicionar_origem)
ORIGEM_MAP = None
ORIGEM_DEFAULT = None

# Ouze/src/config.py

# Configuração de transformações
# Ouze/src/config.py

# Ouze/src/config.py

TRANSFORMACOES_DISCAGENS = {
    'grupo_map': GRUPO_PRINCIPAL_MAP,
    'origem_default': ORIGEM_DEFAULT,
    'adicionar_estado': True,
    'enriquecer_mailing_calendario': True,
    'colunas_obrigatorias': {
        'TRABALHADO': 1,      # ← Sempre 1 (CPF foi trabalhado)
        'ACIONAMENTOS': 0,    # ← 0 porque não tem tabulação robô
        'CPC': 0,
        'CPCA': 0,
        'PROMESSA': 0
    }
}

# OutraCarteira/src/config.py

# Sem segmentação
DIMENSOES_PAGAMENTOS = []  # Ou None

from pathlib import Path

# ============================================================================
# CONFIGURAÇÃO DOS CAMINHOS
# ============================================================================

CAMINHO_BASE_OUZE = Path(r"C:\Users\claudiano.alves\Documents\Claudiano\repository\Data-Analysis-Portfolio\Projects\Ouze\Data\data_analytcs")

OUZE_PATHS = {
    # Canais principais
    "acionamentos": CAMINHO_BASE_OUZE / "anaytical_acionamentos",
    "acordos": CAMINHO_BASE_OUZE / "analytical_acordos",
    "discagens": CAMINHO_BASE_OUZE / "analytical_discagens",
    "pagamentos": CAMINHO_BASE_OUZE / "analytical_pagamentos",
    
    # Digital - Pasta base
    "digital": CAMINHO_BASE_OUZE / "analytical_digital",
    
    # Digital - Canais específicos (se precisar usar diretamente)
    "digital_email": CAMINHO_BASE_OUZE / "analytical_digital" / "data_email",
    "digital_sms": CAMINHO_BASE_OUZE / "analytical_digital" / "data_sms",
    "digital_rcs": CAMINHO_BASE_OUZE / "analytical_digital" / "data_rcs",
}