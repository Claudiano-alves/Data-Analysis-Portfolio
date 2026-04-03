# Cresol/src/database.py
# Cresol/src/config.py

TABELAS = {
    'sintetico_cresol': {
        'tabela':   'sintetico_cresol',
        'col_data': 'data',
        'tipos':    {'qte': 'Int64', 'nr_dia_util': 'Int64', 'dt_mes': 'Int64', 'VALOR': 'float64'},
        'colunas':  {
            'DATA':         'data',
            'Indicador':    'indicador',
            'qte':          'qte',
            'PF_PJ':        'pf_pj',
            'PA':           'pa',
            'MesAbreviado': 'mes_abreviado',
            'nr_dia_util':  'nr_dia_util',
            'quartil':      'quartil',
            'dt_mes':       'dt_mes',
            'VALOR':        'valor',
            'TIPO':         'tipo',
        },
    },
    'analytical_discagens_expert_cresol': {
        'tabela':   'analitico_discagens_cresol',
        'col_data': 'data',
        'tipos':    {'atraso': 'Int64', 'nr_dia_util': 'Int64', 'dt_mes': 'Int64', 'valor': 'float64'},
        'colunas':  {
            'DATA':               'data',
            'ID_DISCAGEM':        'id_discagem',
            'CONTRATO':           'contrato',
            'AGENTE':             'agente',
            'DDD':                'ddd',
            'TELEFONE':           'telefone',
            'DATA_ENCERRAMENTO':  'data_encerramento',
            'CAMPANHA':           'campanha',
            'COD_SIP':            'cod_sip',
            'CLASS_RETORNO':      'class_retorno',
            'DESC_MOTIVO_ENCERR': 'desc_motivo_encerr',
            'COD_MOTIVO_ENCERR':  'cod_motivo_encerr',
            'OPERACAO':           'operacao',
            'ESTADO':             'estado',
            'ID_CAR':             'id_car',
            'ATRASO':             'atraso',
            'COD_CLI':            'cod_cli',
            'VALOR':              'valor',
            'PF_PJ':              'pf_pj',
            'PA':                 'pa',
            'CPF':                'cpf',
            'nr_dia_util':        'nr_dia_util',
            'quartil':            'quartil',
            'dt_mes':             'dt_mes',
            'mes_abreviado':      'mes_abreviado',
        },
    },
    'analytical_mailing_cresol': {
        'tabela':   'analitico_mailing_cresol',
        'col_data': 'data',
        'tipos':    {'atraso': 'Int64', 'valor': 'float64'},
        'colunas':  {
            'DATA':     'data',
            'CONTRATO': 'contrato',
            'CPF':      'cpf',
            'ID_CAR':   'id_car',
            'ATRASO':   'atraso',
            'COD_CLI':  'cod_cli',
            'VALOR':    'valor',
            'PF_PJ':    'pf_pj',
            'PA':       'pa',
        },
    },
    'analytical_acionamentos_cresol': {
        'tabela':   'analitico_acionamentos_cresol',
        'col_data': 'data_aciona',
        'tipos':    {'atraso': 'Int64', 'nr_dia_util': 'Int64', 'dt_mes': 'Int64', 'valorprin_fin': 'float64', 'valor': 'float64'},
        'colunas':  {
            'DATA_ACIONA':               'data_aciona',
            'HORA':                      'hora',
            'CONTRATO_FIN':              'contrato_fin',
            'CPF_DEV':                   'cpf_dev',
            'COD_ACIONA':                'cod_aciona',
            'DESC_ACIONAMENTO':          'desc_acionamento',
            'COD_RECUP':                 'cod_recup',
            'NOME_RECUP':                'nome_recup',
            'LOGIN_RECUP':               'login_recup',
            'ULTGRUPO_RECUP':            'ultgrupo_recup',
            'VALORPRIN_FIN':             'valorprin_fin',
            'STATCONT_FIN':              'statcont_fin',
            'DTDEVOL_FIN':               'dtdevol_fin',
            'DTENTRADA_FIN':             'dtentrada_fin',
            'CLASSIFICACAO_ACIONAMENTO': 'classificacao_acionamento',
            'ACIONAMENTOS':              'acionamentos',
            'CPC':                       'cpc',
            'CPCA':                      'cpca',
            'PROMESSA':                  'promessa',
            'DESCR':                     'descr',
            'CPF':                       'cpf',
            'ID_CAR':                    'id_car',
            'ATRASO':                    'atraso',
            'VALOR':                     'valor',
            'PF_PJ':                     'pf_pj',
            'PA':                        'pa',
            'COD_CLI':                   'cod_cli',
            'nr_dia_util':               'nr_dia_util',
            'quartil':                   'quartil',
            'dt_mes':                    'dt_mes',
            'mes_abreviado':             'mes_abreviado',
        },
    },
    'analytical_massivos_cresol': {
        'tabela':   'analitico_massivos_cresol',
        'col_data': 'data',
        'tipos': {
            'atraso':       'Int64',
            'nr_dia_util':  'Int64',
            'dt_mes':       'Int64',
            'VALOR':        'float64',
        },
        'colunas': {
            'CPF':          'cpf',
            'DATA':         'data',
            'CANAL':        'canal',
            'CONTRATO':     'contrato',
            'ID_CAR':       'id_car',
            'ATRASO':       'atraso',
            'COD_CLI':      'cod_cli',
            'VALOR':        'valor',
            'PF_PJ':        'pf_pj',
            'PA':           'pa',
            'nr_dia_util':  'nr_dia_util',
            'quartil':      'quartil',
            'dt_mes':       'dt_mes',
            'mes_abreviado': 'mes_abreviado',
        },
    },
}

COLUNAS_DISCAGENS = {
    'DATA':                 'data',
    'ID_DISCAGEM':          'id_discagem',
    'CONTRATO':             'contrato',
    'AGENTE':               'agente',
    'DDD':                  'ddd',
    'TELEFONE':             'telefone',
    'DATA_ENCERRAMENTO':    'data_encerramento',
    'CAMPANHA':             'campanha',
    'COD_SIP':              'cod_sip',
    'CLASS_RETORNO':        'class_retorno',
    'DESC_MOTIVO_ENCERR':   'desc_motivo_encerr',
    'COD_MOTIVO_ENCERR':    'cod_motivo_encerr',
    'OPERACAO':             'operacao',
    'ESTADO':               'estado',
    'ID_CAR':               'id_car',
    'ATRASO':               'atraso',
    'COD_CLI':              'cod_cli',
    'VALOR':                'valor',
    'PF_PJ':                'pf_pj',
    'PA':                   'pa',
    'CPF':                  'cpf',
    'nr_dia_util':          'nr_dia_util',
    'quartil':              'quartil',
    'dt_mes':               'dt_mes',
    'mes_abreviado':        'mes_abreviado',
}

COLUNAS_MAILING = {
    'DATA':      'data',
    'CONTRATO':  'contrato',
    'CPF':       'cpf',
    'ID_CAR':    'id_car',
    'ATRASO':    'atraso',
    'COD_CLI':   'cod_cli',
    'VALOR':     'valor',
    'PF_PJ':     'pf_pj',
    'PA':        'pa',
}

COLUNAS_ACIONAMENTOS = {
    'DATA_ACIONA':                  'data_aciona',
    'HORA':                         'hora',
    'CONTRATO_FIN':                 'contrato_fin',
    'CPF_DEV':                      'cpf_dev',
    'COD_ACIONA':                   'cod_aciona',
    'DESC_ACIONAMENTO':             'desc_acionamento',
    'COD_RECUP':                    'cod_recup',
    'NOME_RECUP':                   'nome_recup',
    'LOGIN_RECUP':                  'login_recup',
    'ULTGRUPO_RECUP':               'ultgrupo_recup',
    'VALORPRIN_FIN':                'valorprin_fin',
    'STATCONT_FIN':                 'statcont_fin',
    'DTDEVOL_FIN':                  'dtdevol_fin',
    'DTENTRADA_FIN':                'dtentrada_fin',
    'CLASSIFICACAO_ACIONAMENTO':    'classificacao_acionamento',
    'ACIONAMENTOS':                 'acionamentos',
    'CPC':                          'cpc',
    'CPCA':                         'cpca',
    'PROMESSA':                     'promessa',
    'DESCR':                        'descr',
    'CPF':                          'cpf',
    'ID_CAR':                       'id_car',
    'ATRASO':                       'atraso',
    'VALOR':                        'valor',
    'PF_PJ':                        'pf_pj',
    'PA':                           'pa',
    'COD_CLI':                      'cod_cli',
    'nr_dia_util':                  'nr_dia_util',
    'quartil':                      'quartil',
    'dt_mes':                       'dt_mes',
    'mes_abreviado':                'mes_abreviado',
}

# Cresol/src/config.py

# ── WHERE clauses — filtros específicos da carteira ───────────────────────────
# Consumido por load_data_cresol → data_loader → get_query_*
WHERE_CLAUSES = {
    'campanhas':           "A.GrupoPrincipal IN (SELECT G.id_grupo FROM grupo G WHERE G.ID_CAMPANHA = 40)",
    'clientes_mailing':    "WHERE COD_CLI IN (181, 230, 231, 254, 220)",
    'acionamentos':        """
        WHERE C.COD_CLI IN (181, 230, 231, 254, 220)
        AND A.COD_RECUP NOT IN (4084)
        AND B.CLASSIFICACAO_ACIONAMENTO = 1
    """,
    'tabulacao':           "WHERE IDCRM = 1",
    'clientes_pagamentos': "WHERE B.COD_CLI IN (181, 230, 231, 254, 220)",
    'clientes_acordos':    "WHERE B.COD_CLI IN (181, 230, 231, 254, 220)",
    'massivos':            "WHERE ID_CAR IN (89, 90, 91)",
}

# ── Colunas SELECT por dataset — três camadas de resolução ────────────────────
# 1. COLUMNS[dataset]         → colunas específicas da Cresol (definidas aqui)
# 2. DEFAULT_COLUMNS[dataset] → colunas padrão (utils/config.py)
# 3. None                     → SELECT * na query
COLUMNS = {
    # 'discagens_expert': 'DATA, CPF, CONTRATO, ...',  # descomente para restringir colunas
    # 'mailing_hist':     'DATA, CPF, CONTRATO, ...',
}

# ── Datasets a carregar ───────────────────────────────────────────────────────
# active: False → ignorado completamente pelo load_data_cresol
# query:  None  → usa fluxo padrão via data_loader
#         fn    → carregamento exclusivo com query própria
DATASETS_TO_LOAD = {
    'discagens_expert': {'active': True,  'query': None},
    'mailing_hist':     {'active': True,  'query': None},
    'tab_acionamentos': {'active': True,  'query': None},
    'tabulacao_aciona': {'active': True,  'query': None},
    'dw_calendario':    {'active': True,  'query': None},
    'pagamentos':       {'active': False, 'query': None},  # sem pipeline desenvolvida
    'acordos':          {'active': False, 'query': None},  # sem pipeline desenvolvida
    'sms':              {'active': True,  'query': None},
    'rcs':              {'active': True,  'query': None},
    'email':            {'active': True,  'query': None},
    'whats':            {'active': True,  'query': None},
}

# ── Mapeamento indicador → datasets necessários ───────────────────────────────
# Usado pelo pipeline para carga seletiva por indicador
DATASETS_POR_INDICADOR = {
    'MAILING':      ['mailing_hist', 'dw_calendario'],
    'DISCAGENS':    ['discagens_expert', 'dw_calendario'],
    'ACIONAMENTOS': ['tab_acionamentos', 'tabulacao_aciona', 'dw_calendario'],
    'MASSIVOS':     ['sms', 'rcs', 'email', 'whats', 'dw_calendario'],
}

#O fluxo completo de resolução quando o pipeline chama `load_data_cresol`:
'''
load_data_cresol(datasets_to_load=['discagens_expert', 'dw_calendario'])
    │
    ├── filtra DATASETS_TO_LOAD pelos datasets solicitados e ativos
    │
    ├── resolve colunas:
    │       COLUMNS['discagens_expert']         → None (não definido)
    │       DEFAULT_COLUMNS['discagens_expert'] → colunas padrão do utils
    │       fallback                            → SELECT *
    │
    └── data_loader(
            where_campanhas = WHERE_CLAUSES['campanhas'],   ← filtro da carteira
            where_massivos  = WHERE_CLAUSES['massivos'],    ← filtro da carteira
            datasets_to_load = ['discagens_expert', ...],
            columns = resolved_columns,
        )
'''