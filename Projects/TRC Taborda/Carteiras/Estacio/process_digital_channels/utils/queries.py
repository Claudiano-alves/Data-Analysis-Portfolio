from datetime import datetime
import warnings
warnings.filterwarnings("ignore", category=FutureWarning)

def get_query_mailing_hist(dt_ini, dt_fim, where_clause=""):
    """
    Retorna a query SQL para buscar mailing_hist com período parametrizado e filtros customizados
    
    Args:
        dt_ini (str): Data inicial no formato 'YYYY-MM-DD'
        dt_fim (str): Data final no formato 'YYYY-MM-DD'
        where_clause (str): Cláusula WHERE adicional customizada (opcional)
                           Ex: "COD_CLI IN(196,198,228)"
                           Nota: O filtro de DATA BETWEEN é sempre aplicado
    
    Returns:
        str: Query SQL formatada com as datas e filtros
    """
    # Filtro de data é obrigatório
    where_data = f"DATA BETWEEN '{dt_ini}' AND '{dt_fim}'"
    
    # Adicionar filtro customizado se houver
    if where_clause:
        where_completo = f"{where_clause} AND {where_data}"
    else:
        where_completo = where_data
    
    query = f"""
    SELECT
        M.DATA,
        M.CONTRATO,
        M.CPF,
        M.ATRASO,
        M.COD_CLI,
        Y.REGIONAL,
        Y.GRUPO,
        CASE 
            WHEN TRIM(Y.GRUPO) IN ('ESTACIO','ATHENAS') AND STALUNO = 'ATIVO' THEN 'ESTACIO ATIVO'
            WHEN TRIM(Y.GRUPO) IN ('ESTACIO','ATHENAS') AND STALUNO = 'INATIVO' THEN 'ESTACIO INATIVO'
            WHEN TRIM(Y.GRUPO) IN ('WYDEN','UNITOLEDO') AND STALUNO = 'ATIVO' THEN 'WYDEN ATIVO'
            WHEN TRIM(Y.GRUPO) IN ('WYDEN','UNITOLEDO') AND STALUNO = 'INATIVO' THEN 'WYDEN INATIVO'
            ELSE 'OUTROS'
        END AS GRUPO_SEGMENTADO,
        CASE 
            WHEN Y.COD_PRODUT = 2 THEN 'RENOVACAO'
            WHEN Y.COD_PRODUT = 1 THEN 'BASE LIQUIDA'
            ELSE 'OUTROS'
        END AS PRODUTO_SEGMENTADO,
        Y.SPD,
        Y.BU,
        Y.MODALIDE,
        Y.STDEBITO,
        Y.STALUNO,
        Y.CURSO,
        Y.APROACAD,
        Y.PRODUTO,
        Y.ULTRENOV,
        Y.COD_PRODUT,
        CASE
            WHEN ATRASO BETWEEN 1 AND 7             THEN 'A - 01 - 07'
            WHEN ATRASO BETWEEN 8 AND 15            THEN 'B - 08 - 15'
            WHEN ATRASO BETWEEN 16 AND 30           THEN 'C - 16 - 30'
            WHEN ATRASO BETWEEN 31 AND 60           THEN 'D - 31 - 60'
            WHEN ATRASO BETWEEN 61 AND 90           THEN 'E - 61 - 90'
            WHEN ATRASO BETWEEN 91 AND 180          THEN 'F - 91 - 180'
            WHEN ATRASO BETWEEN 181 AND 270         THEN 'G - 181 - 270'
            WHEN ATRASO BETWEEN 271 AND 360         THEN 'H - 271 - 360'
            WHEN ATRASO BETWEEN 361 AND 540         THEN 'I - 361 - 540'
            WHEN ATRASO BETWEEN 541 AND 720         THEN 'J - 541 - 720'
            WHEN ATRASO BETWEEN 721 AND 1080        THEN 'K - 721 - 1080'
            WHEN ATRASO BETWEEN 1081 AND 1440       THEN 'L - 1081 - 1440'
            WHEN ATRASO BETWEEN 1441 AND 1800       THEN 'M - 1441 - 1800'
            WHEN ATRASO > 1800                      THEN 'N - Maior 1800'
            ELSE '0 - Menor 8'
        END FX_ATRASO
    FROM [TRC-DC-BD2].PLANEJAMENTO.DBO.MAILING_HIST M
    LEFT JOIN (
        SELECT
            A.CONTRATO_FIN,
            A.COD_PRODUT,
            MAX(CASE WHEN A.COD_INDICADOR = 'REGIONAL' THEN A.VL_INDICADOR ELSE NULL END) AS REGIONAL,
            MAX(CASE WHEN A.COD_INDICADOR = 'GRUPO' THEN A.VL_INDICADOR ELSE NULL END) AS GRUPO,
            MAX(CASE WHEN A.COD_INDICADOR = 'SPD' THEN A.VL_INDICADOR ELSE NULL END) AS SPD,
            MAX(CASE WHEN A.COD_INDICADOR = 'BU' THEN A.VL_INDICADOR ELSE NULL END) AS BU,
            MAX(CASE WHEN A.COD_INDICADOR = 'MODALIDE' THEN A.VL_INDICADOR ELSE NULL END) AS MODALIDE,
            MAX(CASE WHEN A.COD_INDICADOR = 'STDEBITO' THEN A.VL_INDICADOR ELSE NULL END) AS STDEBITO,
            MAX(CASE WHEN A.COD_INDICADOR = 'STALUNO' THEN STA.SITUACAO_FINAL ELSE NULL END) AS STALUNO,
            MAX(CASE WHEN A.COD_INDICADOR = 'APROACAD' THEN A.VL_INDICADOR ELSE NULL END) AS APROACAD,
            MAX(CASE WHEN A.COD_INDICADOR = 'CURSO' THEN A.DESC_INDICADOR ELSE NULL END) AS CURSO,
            MAX(CASE WHEN B.CONTRATO_FIN IS NOT NULL THEN 'RENOVACAO' ELSE 'MENSALIDADE' END) AS PRODUTO,
            MAX(CASE WHEN A.COD_INDICADOR = 'ULTRENOV' THEN A.VL_INDICADOR ELSE NULL END) AS ULTRENOV,
            MAX(CASE WHEN A.COD_INDICADOR = 'LTCOMER' THEN A.VL_INDICADOR ELSE NULL END) AS LTCOMER
        FROM AUX_SYSOPENINDICADOR_YDUQS A
        LEFT JOIN AUX_SYSOPENSTATUSALUNO_YDUQS STA 
            ON A.COD_INDICADOR = 'STALUNO' AND TRY_CAST(A.VL_INDICADOR AS INT) = STA.COD_SITUACAO
        LEFT JOIN AUX_SYSOPENINDICADOR_YDUQS B 
            ON A.ID_CLIENTE = B.ID_CLIENTE AND A.COD_PRODUT = 1 AND B.COD_PRODUT = 2
        GROUP BY
            A.CONTRATO_FIN,
            A.COD_PRODUT
    ) Y ON M.CONTRATO = Y.CONTRATO_FIN 
    {where_completo}
    """
    return query

def get_query_indicadores():
    """
    Retorna a query SQL para buscar os indicadores pivotados por contrato/produto/cliente.

    Returns:
        str: Query SQL formatada.
    """
    query = """
        SELECT
            A.CONTRATO_FIN,
            A.COD_PRODUT,
            A.ID_CLIENTE,
            MAX(CASE WHEN A.COD_INDICADOR = 'REGIONAL' THEN A.VL_INDICADOR       ELSE NULL END) AS REGIONAL,
            MAX(CASE WHEN A.COD_INDICADOR = 'GRUPO'    THEN A.VL_INDICADOR       ELSE NULL END) AS GRUPO,
            MAX(CASE WHEN A.COD_INDICADOR = 'SPD'      THEN A.VL_INDICADOR       ELSE NULL END) AS SPD,
            MAX(CASE WHEN A.COD_INDICADOR = 'BU'       THEN A.VL_INDICADOR       ELSE NULL END) AS BU,
            MAX(CASE WHEN A.COD_INDICADOR = 'MODALIDE' THEN A.VL_INDICADOR       ELSE NULL END) AS MODALIDE,
            MAX(CASE WHEN A.COD_INDICADOR = 'STDEBITO' THEN A.VL_INDICADOR       ELSE NULL END) AS STDEBITO,
            MAX(CASE WHEN A.COD_INDICADOR = 'STALUNO'  THEN STA.SITUACAO_FINAL   ELSE NULL END) AS STALUNO,
            MAX(CASE WHEN A.COD_INDICADOR = 'APROACAD' THEN A.VL_INDICADOR       ELSE NULL END) AS APROACAD,
            MAX(CASE WHEN A.COD_INDICADOR = 'CURSO'    THEN A.DESC_INDICADOR     ELSE NULL END) AS CURSO,
            MAX(CASE WHEN B.CONTRATO_FIN IS NOT NULL THEN 'RENOVACAO' ELSE 'MENSALIDADE' END)   AS PRODUTO,
            MAX(CASE WHEN A.COD_INDICADOR = 'ULTRENOV' THEN A.VL_INDICADOR       ELSE NULL END) AS ULTRENOV,
            MAX(CASE WHEN A.COD_INDICADOR = 'LTCOMER'  THEN A.VL_INDICADOR       ELSE NULL END) AS LTCOMER
        FROM AUX_SYSOPENINDICADOR_YDUQS A
        LEFT JOIN AUX_SYSOPENSTATUSALUNO_YDUQS STA
            ON A.COD_INDICADOR = 'STALUNO'
           AND TRY_CAST(A.VL_INDICADOR AS INT) = STA.COD_SITUACAO
        LEFT JOIN AUX_SYSOPENINDICADOR_YDUQS B
            ON A.ID_CLIENTE = B.ID_CLIENTE
           AND A.COD_PRODUT = 1
           AND B.COD_PRODUT = 2
        GROUP BY
            A.CONTRATO_FIN,
            A.COD_PRODUT,
            A.ID_CLIENTE
    """
    return query