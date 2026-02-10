"""
Arquivo de configuração global do projeto Ouze.
Contém constantes, caminhos e regras de negócio específicas.
"""
import os

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
    210, 
    240, 
    270, 
    300, 
    330, 
    360, 
    float('inf')
]

# Labels correspondentes aos Bins
FAIXAS_ATRASO_LABELS = [
    'Preventivo',   # < 0
    '000-030',      # 0-30
    '031-060',      # 30-60
    '061-090',      # 60-90
    '091-120',      # 90-120
    '121-150',      # 120-150
    '151-180',      # 150-180
    '181-210',      # 180-210
    '211-240',      # 210-240
    '241-270',      # 240-270
    '271-300',      # 270-300
    '301-330',      # 300-330
    '331-360',      # 330-360
    '360 acima'     # > 360
]

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
    'telefone': True,          
    'blacklist_expert': True,  
    'discagens_trestto': False  
}

# ============================================
# FILTROS SQL (WHERE CLAUSES)
# ============================================

WHERE_CLAUSES = {
    'campanhas': "A.GrupoPrincipal IN (SELECT G.id_grupo FROM grupo G WHERE G.ID_CAMPANHA = 141)",
    'clientes_mailing': "WHERE COD_CLI = 253",
    'acionamentos': """
        WHERE C.COD_CLI = 253
        AND C.COD_CAR = 1
        AND A.COD_RECUP NOT IN (15721)
        AND B.CLASSIFICACAO_ACIONAMENTO = 1
    """,
    'tabulacao': "WHERE COD_CLI = 253",
    'clientes_pagamentos': "WHERE B.COD_CLI = 253",
    'clientes_acordos': "WHERE B.COD_CLI = 253 AND B.COD_CAR = 1",
    'massivos': "WHERE ID_CAR = 100",
    'telefones': "WHERE COD_CLI = 253"
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
    (4712, 4713): "ATIVO",
    4717: "MANUAL",
    (4702, 4714, 4716): "RECEPTIVO",
    4715: "AGV"
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
    'origem_map': ORIGEM_MAP,
    'origem_default': ORIGEM_DEFAULT,
    'adicionar_estado': True,
    'enriquecer_tabulacoes_robo': False,
    'enriquecer_mailing_calendario': True,
    'colunas_obrigatorias': {
        'TRABALHADO': 1,      # ← Sempre 1 (CPF foi trabalhado)
        'ACIONAMENTOS': 0,    # ← 0 porque não tem tabulação robô
        'CPC': 0,
        'CPCA': 0,
        'PROMESSA': 0
    }
}