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

# Dicionário de logs (útil para acesso dinâmico)
LOGS = {
    'acionamentos': LOG_ACIONAMENTOS,
    'discagens': LOG_DISCAGENS,
    'mailing': LOG_MAILING,
    'pagamentos': LOG_PAGAMENTOS,
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
