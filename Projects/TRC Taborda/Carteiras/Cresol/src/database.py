# Cresol/src/database.py

from datetime import date
from utils._database.operations import inserir_analitico
from utils._database.query import consultar_dataframe
from utils._database.check import verificar_ultima_data, esta_atualizado, datas_faltantes
from utils.utils import salvar_log
from Cresol.src.config import LOG_PIPELINE, TABELAS


def inserir(nome: str, df, conn, data_fim: date = None, arquivo_log=LOG_PIPELINE):
    """
    Insert genérico — usa configuração de TABELAS do config.
    
    Parameters:
    -----------
    nome      : Chave em TABELAS (ex: 'discagens')
    df        : DataFrame com os dados
    conn      : Conexão com o banco
    data_fim  : Data limite do ciclo. Se None, usa D-1
    arquivo_log: Caminho do arquivo de log
    """
    config = TABELAS[nome]
    return inserir_analitico(
        df=df,
        conn=conn,
        tabela=config['tabela'],
        colunas=config['colunas'],
        col_data=config['col_data'],
        tipos=config.get('tipos'),
        arquivo_log=arquivo_log,
        data_fim=data_fim,
    )


def processar_indicador(
    nome_indicador: str,
    conn,
    fn_acumulado,
    df_analitico=None,
    data_fim: date = None,
    arquivo_log=LOG_PIPELINE,
):
    """
    Orquestra o fluxo completo para um indicador de forma isolada:
        1. Verifica se o acumulado está atualizado
        2. Verifica se o analítico está atualizado — usa banco ou df em memória
        3. Calcula o acumulado
        4. Insere o acumulado no banco

    Parameters:
    -----------
    nome_indicador : Chave em TABELAS (ex: 'discagens')
    conn           : Conexão com o banco
    fn_acumulado   : Função que calcula o acumulado — recebe df_analitico
    df_analitico   : DataFrame em memória (opcional — se None, busca do banco)
    data_fim       : Data limite do ciclo. Se None, usa D-1
    arquivo_log    : Caminho do arquivo de log
    """
    def log(msg):
        print(msg)
        salvar_log(msg, arquivo_log)

    config_analitico = TABELAS[nome_indicador]
    config_sintetico = TABELAS['sintetico']

    log(f"{'─' * 50}")
    log(f"INDICADOR » {nome_indicador.upper()}")

    # ── 1. Verifica acumulado ─────────────────────────────────────────────────
    ultima_data_sintetico = verificar_ultima_data(
        tabela=config_sintetico['tabela'],
        conn=conn,
        col_data=config_sintetico['col_data'],
        filtros={'Indicador': nome_indicador},
    )

    if esta_atualizado(ultima_data_sintetico, data_fim=data_fim):
        log(f"SKIP  » {nome_indicador} — acumulado já atualizado até {ultima_data_sintetico}")
        return True

    log(f"INFO  » {nome_indicador} — última data no acumulado: {ultima_data_sintetico}")

    # ── 2. Verifica analítico ─────────────────────────────────────────────────
    ultima_data_analitico = verificar_ultima_data(
        tabela=config_analitico['tabela'],
        conn=conn,
        col_data=config_analitico['col_data'],
    )

    analitico_atualizado = esta_atualizado(ultima_data_analitico, data_fim=data_fim)

    if not analitico_atualizado:
        if df_analitico is not None:
            log(f"INFO  » {nome_indicador} — analítico desatualizado, usando df em memória")
            inserir(nome_indicador, df_analitico, conn, data_fim=data_fim, arquivo_log=arquivo_log)
        else:
            log(f"FALHA » {nome_indicador} — analítico desatualizado e sem df em memória")
            return False

    # ── 3. Consulta analítico do banco ────────────────────────────────────────
    data_inicio = str(ultima_data_sintetico) if ultima_data_sintetico else None

    log(f"INFO  » {nome_indicador} — consultando analítico no banco a partir de {data_inicio}")

    df_banco = consultar_dataframe(
        tabela=config_analitico['tabela'],
        conn=conn,
        col_data=config_analitico['col_data'],
        data_inicio=data_inicio,
    )

    if df_banco.empty:
        log(f"FALHA » {nome_indicador} — nenhum dado no banco para calcular acumulado")
        return False

    # ── 4. Calcula acumulado ──────────────────────────────────────────────────
    log(f"INFO  » {nome_indicador} — calculando acumulado")
    try:
        df_acumulado = fn_acumulado(df_banco)
    except Exception as e:
        log(f"FALHA » {nome_indicador} — erro no cálculo do acumulado: {e}")
        return False

    # ── 5. Insere acumulado ───────────────────────────────────────────────────
    return inserir('sintetico', df_acumulado, conn, data_fim=data_fim, arquivo_log=arquivo_log)