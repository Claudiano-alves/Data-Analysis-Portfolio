# config.py

# WHERE clauses para Estácio
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
    'mailing_hist': False,        
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
    'discagens_trestto': False  
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