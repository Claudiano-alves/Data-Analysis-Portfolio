from datetime import datetime
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

def get_query_discagens(dt_ini, dt_fim):
    """
    Retorna a query SQL para buscar discagens com período parametrizado
    
    Args:
        dt_ini (str): Data inicial no formato 'YYYY-MM-DD'
        dt_fim (str): Data final no formato 'YYYY-MM-DD'
    
    Returns:
        str: Query SQL formatada com as datas e tabela dinâmica
    """
    # Converter a data inicial para extrair ano e mês
    data_obj = datetime.strptime(dt_ini, '%Y-%m-%d')
    ano = data_obj.year
    mes = data_obj.strftime('%m')  # Formato com zero à esquerda (01, 02, etc.)
    
    # Gerar nome da tabela dinamicamente
    tabela = f"totalinfo_{ano}_{mes}"
    
    query = f"""
    SELECT 
        * 
    FROM OPENQUERY (EXPERT,'
    SELECT
        DATE(A.instante) DATA,
        A.id,
        A.chave1 AS CONTRATO,
        A.Chave3 AS CPF,
        A.ddd,
        A.fone,
        A.GrupoPrincipal,
        A.UltCodSigRecPublica,
        A.ResultadoClassificacao,
        A.MotivoEncerramentoBilhete,
        A.Instante200OKPub,
        A.Agente,
        A.tempoconversacao_ms,
        c.codtabulacao
    FROM {tabela} A
    LEFT JOIN tabulacaooper			c ON a.CallID = c.callid AND a.GrupoPrincipal = c.codgrupo
    WHERE A.GrupoPrincipal IN (SELECT G.id_grupo FROM grupo G WHERE G.ID_CAMPANHA IN (19, 30))
    ')
    """
    return query

def get_query_mailing_hist(dt_ini, dt_fim):
    """
    Retorna a query SQL para buscar mailing_hist com período parametrizado
    
    Args:
        dt_ini (str): Data inicial no formato 'YYYY-MM-DD'
        dt_fim (str): Data final no formato 'YYYY-MM-DD'
    
    Returns:
        str: Query SQL formatada com as datas
    """
    query = f"""
    SELECT 
        DATA,
        UPPER(LTRIM(RTRIM(CONTRATO))) AS CONTRATO,
        LTRIM(RTRIM(CPF)) AS CPF,
        ATRASO,
        COD_CLI,
        COD_CAR
    FROM MAILING_HIST 
    WHERE DATA BETWEEN '{dt_ini}' AND '{dt_fim}'
    AND COD_CLI IN(196,198,228)
    """
    return query

def get_query_cad_devf():
    """
    Retorna a query SQL para buscar dados do cadastro de devolução financeira
    
    Esta query não necessita de parâmetros de data pois busca o cadastro atual
    
    Returns:
        str: Query SQL
    """
    query = """
    SELECT 
        LTRIM(RTRIM(D.CPF_DEV)) AS CPF_DEV,
        UPPER(LTRIM(RTRIM(D.CONTRATO_FIN))) AS CONTRATO_FIN,
        D.VALORPRIN_FIN,
        D.VALOR_FIN,
        D.DTDEVOL_FIN,
        D.ATRASO_FIN,
        D.COD_CLI,
        D.COD_CAR,
        D.STATCONT_FIN,
        C.DESC_CAR
    FROM CAD_DEVF D
    INNER JOIN CAD_CAR C WITH (NOLOCK) ON D.COD_CLI = C.COD_CLI AND D.COD_CAR = C.COD_CAR
    WHERE (D.COD_CLI = 198 AND D.COD_CAR IN (1,2,3)) 
       OR (D.COD_CLI = 196 AND D.COD_CAR IN (1,3,4)) 
       OR (D.COD_CLI = 228 AND D.COD_CAR = 2)
    """
    return query

def get_query_discagens_trestto(dt_ini, dt_fim):
    """
    Retorna a query SQL para buscar discagens do Trestto (Robô)
    
    Args:
        dt_ini (str): Data inicial no formato 'YYYY-MM-DD'
        dt_fim (str): Data final no formato 'YYYY-MM-DD'
    
    Returns:
        str: Query SQL formatada com as datas
    """
    query = f"""
    SELECT  
        DATA, 
        CPF, 
        SUBSTATUSURA, 
        'ROBÔ' TIPO, 
        DISCAGEM, 
        ALO, 
        CPC, 
        CPCA, 
        PROMESSA  
    FROM DISCAGENS_TRESTTO 
    WHERE DATA BETWEEN '{dt_ini}' AND '{dt_fim}'
    """
    return query

def get_query_tabulacao_aciona():
    query = f"""
        SELECT 
            COD_ACIONA,
            DESC_ACIONA,
            CPC,
            CPCA,
            PROMESSA
        FROM ACIONAMENTO_CARTEIRA
        WHERE COD_CLI = 196
    """
    return query

def get_query_base_acionamentos(dt_ini, dt_fim):
    query = f"""
        SELECT 
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
        FROM ACIONA A 
        LEFT JOIN CAD_ACIONAMENTO B ON A.COD_ACIONAMENTO = B.COD_ACIONAMENTO
        LEFT JOIN CAD_RECUP REC ON REC.COD_RECUP = A.COD_RECUP
        INNER JOIN CAD_DEVF       C ON A.CONTRATO_FIN = C.CONTRATO_FIN
        WHERE ((C.COD_CLI = 198 AND C.COD_CAR IN (1, 2, 3)) 
            OR (C.COD_CLI = 196 AND C.COD_CAR IN (1, 3, 4)) 
            OR (C.COD_CLI = 228 AND C.COD_CAR = 2))
        AND CAST(A.DATA_ACIONA AS DATE) BETWEEN '{dt_ini}' AND '{dt_fim}'
    """
    return query

def get_query_dw_calendario():
    query = """
        SELECT 
            *
        FROM DW_CALENDARIO
    """
    return query