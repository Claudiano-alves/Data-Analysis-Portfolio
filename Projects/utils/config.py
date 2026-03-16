"""
Arquivo de configuração global do projeto Ouze.
Contém constantes, caminhos e regras de negócio específicas.
"""
import os
from typing import Dict, Optional, Any

# Diretório base de logs (criado automaticamente se não existir)
LOGS_DIR = os.path.join(os.path.dirname(__file__), 'logs')

# Caminhos específicos por módulo
LOG_ACIONAMENTOS = os.path.join(LOGS_DIR, 'acionamentos.txt')
LOG_DISCAGENS = os.path.join(LOGS_DIR, 'discagens.txt')
LOG_MAILING = os.path.join(LOGS_DIR, 'mailing.txt')
LOG_PAGAMENTOS = os.path.join(LOGS_DIR, 'pagamentos.txt')
LOG_LOADING = os.path.join(LOGS_DIR, 'loading.txt')
LOG_CHANNELS = os.path.join(LOGS_DIR, 'channels.txt')

# Dicionário de logs (útil para acesso dinâmico)
LOGS = {
    'acionamentos': LOG_ACIONAMENTOS,
    'discagens': LOG_DISCAGENS,
    'mailing': LOG_MAILING,
    'pagamentos': LOG_PAGAMENTOS,
    'loading': LOG_LOADING,
    'channels': LOG_CHANNELS,
}

# ============================================
# CONSTANTES
# ============================================

DDD_ESTADO = {
    '11': 'SP', '12': 'SP', '13': 'SP', '14': 'SP', '15': 'SP', '16': 'SP', '17': 'SP', '18': 'SP', '19': 'SP',
    '21': 'RJ', '22': 'RJ', '24': 'RJ',
    '27': 'ES', '28': 'ES',
    '31': 'MG', '32': 'MG', '33': 'MG', '34': 'MG', '35': 'MG', '37': 'MG', '38': 'MG',
    '41': 'PR', '42': 'PR', '43': 'PR', '44': 'PR', '45': 'PR', '46': 'PR',
    '47': 'SC', '48': 'SC', '49': 'SC',
    '51': 'RS', '53': 'RS', '54': 'RS', '55': 'RS',
    '61': 'DF', '62': 'GO', '63': 'TO', '64': 'GO', '65': 'MT', '66': 'MT', '67': 'MS',
    '68': 'AC', '69': 'RO',
    '71': 'BA', '73': 'BA', '74': 'BA', '75': 'BA', '77': 'BA',
    '79': 'SE',
    '81': 'PE', '82': 'AL', '83': 'PB', '84': 'RN', '85': 'CE', '86': 'PI', '87': 'PE', '88': 'CE', '89': 'PI',
    '91': 'PA', '92': 'AM', '93': 'PA', '94': 'PA', '95': 'RR', '96': 'AP', '97': 'AM', '98': 'MA', '99': 'MA'
}

DEFAULT_COLUMNS = {
    "discagens_expert": """
        DATE(A.instante) AS DATA,
        A.id AS ID_DISCAGEM,
        A.chave1 AS CONTRATO,
        A.Chave3 AS CPF,
        A.Agente AS AGENTE,
        A.ddd AS DDD,
        A.fone AS TELEFONE,
        A.Instante200OKPub AS DATA_ENCERRAMENTO,
        A.GrupoPrincipal AS CAMPANHA,
        A.UltCodSigRecPublica AS COD_SIP,
        A.ResultadoClassificacao AS CLASS_RETORNO,
        A.MotivoEncerramentoBilhete AS DESC_MOTIVO_ENCERR,
        A.tempoconversacao_ms AS TEMPO_CONVERSAÇÃO
    """,
     "mailing_hist": """
        DATA,
        UPPER(LTRIM(RTRIM(CONTRATO))) AS CONTRATO,
        LTRIM(RTRIM(CPF)) AS CPF,
        ATRASO,
        COD_CLI,
        COD_CAR,
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
     "pagamentos": """
        CAST(R.DATA_PAGTO AS DATE) AS DATA_PAGTO,
        R.VALOR_PARC,
        R.NACORDO_ACO,
        R.CONTRATO_FIN,
        B.CPF_DEV,
        B.COD_CLI,
        B.VALORPRIN_FIN,
        B.VALOR_FIN,
        B.STATCONT_FIN,
        B.DTDEVOL_FIN
    """,
     "acordos": """
        B.CONTRATO_FIN,
        B.CPF_DEV,
        CAST(A.DTACORDOHORA_ACO AS DATE) AS DATA_ACORDO,
        CAST(P.VENC_ACOP AS DATE) AS VENC_PARCELA,
        CAST(A.DTCANCELAMENTOACO_ACO AS DATE) AS CANC_ACORDO,
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
     "sms": """
        id_sms,
        data,
        id_car,
        telefone,
        cpf,
        custo,
        entregue,
        receptivo
    """,
     "email": """
        data,
        id_car,
        custo,
        email,
        cpf,
        id_email,
        env_deepcenter,
        nome
    """,
     "rcs": """
        id_rcs,
        data,
        id_car,
        telefone,
        cpf,
        custo,
        entregue,
        receptivo
    """,
     "whats": """
        id_w,
        data,
        id_car,
        telefone,
        cpf,
        custo,
        entregue,
        receptivo
    """,
}

def get_columns(df_name: str, custom_columns: str = None) -> Optional[str]:
    """
    Resolve as colunas para o SELECT seguindo três camadas de prioridade:
 
    1. custom_columns (passado via parâmetro na carteira)  → usa essas colunas
    2. DEFAULT_COLUMNS (definido neste config)             → usa o default
    3. Nenhum dos dois                                     → retorna None
       (subentende-se que as colunas já estão na get_query)
 
    Args:
        df_name: Nome do dataset (ex: 'sms', 'discagens', 'mailing')
        custom_columns: Colunas customizadas passadas pela carteira (opcional)
 
    Returns:
        String de colunas SQL ou None (quando a query já define suas próprias colunas)
    """
    if custom_columns:
        return custom_columns
    return DEFAULT_COLUMNS.get(df_name, None)