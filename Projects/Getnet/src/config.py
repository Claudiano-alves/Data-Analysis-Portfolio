"""
Configuração específica da carteira Getnet
Define constantes, caminhos e configurações usadas em toda a carteira.
"""

import os

# ============================================
# CAMINHOS DE LOGS - ESPECÍFICOS DA CARTEIRA
# ============================================
"""
Todos os logs da carteira Getnet são salvos em:
Projects/Getnet/src/logs/
"""

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
# OUTRAS CONFIGURAÇÕES (Adicionar conforme necessário)
# ============================================

# config.py
from pathlib import Path

# Caminho base para salvar os arquivos analíticos
caminho_base = Path(r"\\trc-dc-ad\Planejamento\MIS\CARTEIRAS\GetNet\Analíticos")

# Dicionário com os caminhos específicos
PROCESS_PATHS = {
    "acionamentos": caminho_base / "acionamentos",
    "pagamentos": caminho_base / "pagamentos",
    "acordos": caminho_base / "acordos",
    "discagens": caminho_base / "discagens"
}



# ============================================
# OUTRAS CONFIGURAÇÕES (Adicionar conforme necessário)
# ============================================

# Exemplo: Configurações de banco de dados
DB_CONFIG = {
    'timeout': 30,  # segundos
    'max_workers': 6,  # para carregamento paralelo
}

# Exemplo: Configurações de processamento
PROCESSING_CONFIG = {
    'ano_minimo_data': 2020,
    'remover_duplicatas': True,
    'fill_na_with_zero': True,
}

# Filtros base reutilizáveis
WHERE_CLIENTES = "WHERE COD_CLI IN(196,198,228)"

# Dicionário de filtros SQL para o pipeline
FILTROS_SQL = {
    'where_campanhas': "A.GrupoPrincipal IN (SELECT G.id_grupo FROM grupo G WHERE G.ID_CAMPANHA IN (19, 30))",
    'where_clientes_mailing': WHERE_CLIENTES,
    'where_acionamentos': "WHERE ((C.COD_CLI = 198 AND C.COD_CAR IN (1, 2, 3)) OR (C.COD_CLI = 196 AND C.COD_CAR IN (1, 3, 4)) OR (C.COD_CLI = 228 AND C.COD_CAR = 2))",
    'where_tabulacao': WHERE_CLIENTES,
    'where_clientes_pagamentos': WHERE_CLIENTES,
    'where_clientes_acordos': WHERE_CLIENTES
    #'where_massivos': "",
    #'where_telefones': ""  # ou f"CPF_DEV IN (SELECT CPF_DEV FROM CAD_DEVF {WHERE_CLIENTES})" se precisar
}

# Datasets (se necessário)
# Datasets a carregar (apenas os necessários)
DATASETS_TO_LOAD = {
    'discagens_expert': True,    
    'mailing_hist': True,        
    'tab_acionamentos': True,    
    'tabulacao_aciona': True,    
    'dw_calendario': True,       
    'pagamentos': True,          
    'acordos': True,
    'sms': False,               
    'rcs': False,               
    'email': False,             
    'telefone': False,          
    'blacklist_expert': False,  
    'discagens_trestto': False  
}