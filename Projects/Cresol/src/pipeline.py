"""
Ponto de entrada da pipeline Cresol.
Execute: python -m Cresol.src.pipeline
"""
import traceback
import pandas as pd
from datetime import date, timedelta
from calendar import monthrange

from Cresol.src.mailing import mailing_pipeline
from Cresol.src.discagens import discagens_pipeline
from Cresol.src.acionamentos import acionamentos_pipeline
from Cresol.src.digital_channels import massivos_pipeline
from Cresol.src.data_loader import load_data_cresol
from Cresol.src.database import inserir
from Cresol.src.config import LOG_PIPELINE, TABELAS, DATASETS_POR_INDICADOR, DATA_INICIO_CRESOL
from utils._database.check import verificar_ultima_data, esta_atualizado
from utils._database.query import consultar_dataframe
from utils.utils import salvar_log, registrar_tempo
from utils.db_connection import get_db_connections


# ── Helpers ───────────────────────────────────────────────────────────────────

def _executar_etapa(nome, fn, *args, **kwargs):
    salvar_log(f"{'─' * 50}", LOG_PIPELINE)
    salvar_log(f"INÍCIO » {nome}", LOG_PIPELINE)
    try:
        resultado = fn(*args, **kwargs)
        salvar_log(f"OK    » {nome}", LOG_PIPELINE)
        return resultado
    except Exception as e:
        salvar_log(f"FALHA » {nome}: {str(e)}", LOG_PIPELINE)
        salvar_log(traceback.format_exc(), LOG_PIPELINE)
        raise

def _inserir_isolado(nome, df, conn):
    salvar_log(f"{'─' * 50}", LOG_PIPELINE)
    salvar_log(f"INSERT » {nome}", LOG_PIPELINE)
    try:
        sucesso = inserir(nome, df, conn=conn)
        if sucesso:
            salvar_log(f"OK    » {nome}", LOG_PIPELINE)
        else:
            salvar_log(f"SKIP  » {nome} — dados já existentes", LOG_PIPELINE)
    except Exception as e:
        salvar_log(f"FALHA » {nome}: {str(e)}", LOG_PIPELINE)
        salvar_log(traceback.format_exc(), LOG_PIPELINE)

def _menor_data_pendente(conn) -> date:
    """
    Retorna a menor data desatualizada entre todos os analíticos.
    Se nenhum analítico tiver dados no banco, usa DATA_INICIO_CRESOL como fallback.
    """
    ontem = date.today() - timedelta(days=1)
    menor_data = None

    for config_key in ['mailing', 'discagens', 'acionamentos', 'massivos']:
        config = TABELAS[config_key]
        ultima = verificar_ultima_data(
            tabela=config['tabela'],
            conn=conn,
            col_data=config['col_data'],
        )
        if ultima is None:
            candidata = DATA_INICIO_CRESOL
        elif ultima < ontem:
            candidata = ultima + timedelta(days=1)
        else:
            continue

        if menor_data is None or candidata < menor_data:
            menor_data = candidata

    return menor_data

def _ciclos_pendentes(menor_data: date) -> list:
    """Gera ciclos mensais entre menor_data e D-1."""
    ontem = date.today() - timedelta(days=1)
    ciclos = []
    atual = menor_data

    while atual <= ontem:
        ano, mes = atual.year, atual.month
        ultimo_dia_mes = date(ano, mes, monthrange(ano, mes)[1])
        fim_ciclo = min(ultimo_dia_mes, ontem)

        ciclos.append({
            'data_inicio': atual,
            'data_fim':    fim_ciclo,
            'mes':         f"{ano}-{mes:02d}",
        })

        if ultimo_dia_mes >= ontem:
            break
        atual = date(ano + (mes // 12), (mes % 12) + 1, 1)

    return ciclos

def _obter_mailing(ciclo, conn_trc, conn_bd2, conn_src) -> pd.DataFrame | None:
    config = TABELAS['mailing']
    ultima = verificar_ultima_data(
        tabela=config['tabela'],
        conn=conn_bd2,
        col_data=config['col_data'],
    )

    if esta_atualizado(ultima):
        salvar_log(f"INFO  » mailing atualizado — consultando banco a partir de {ciclo['data_inicio']}", LOG_PIPELINE)
        return consultar_dataframe(
            tabela=config['tabela'],
            conn=conn_bd2,
            col_data=config['col_data'],
            data_inicio=str(ciclo['data_inicio']),
        )

    dados = _executar_etapa(
        f"Carga mailing — {ciclo['mes']}",
        load_data_cresol,
        data_inicio=str(ciclo['data_inicio']),
        data_fim=str(ciclo['data_fim']),
        datasets_to_load=['mailing_hist', 'dw_calendario'],
        conn_trc=conn_trc, conn_bd2=conn_bd2, conn_src=conn_src,  # ← passa conexões
    )

    resultado = _executar_etapa(
        f"Mailing — {ciclo['mes']}",
        mailing_pipeline.executar_analitico,
        dados.get('mailing_hist', pd.DataFrame()),
        dados.get('dw_calendario', pd.DataFrame()),
    )

    _inserir_isolado('mailing', resultado['analitico'], conn_bd2)
    return resultado['analitico']


# ── Pipeline de analíticos ────────────────────────────────────────────────────

@registrar_tempo("Pipeline Cresol — Analíticos", arquivo_log=LOG_PIPELINE)
def executar_analiticos():
    """
    Etapa 1: Carrega, processa e persiste os dados analíticos.
    Independente do cálculo de acumulados.
    """
    salvar_log("=" * 50, LOG_PIPELINE)
    salvar_log("ANALÍTICOS — INICIANDO", LOG_PIPELINE)
    salvar_log("=" * 50, LOG_PIPELINE)

    with get_db_connections() as (conn_trc, conn_bd2, conn_src):

        # ── 1. Verifica se há algo a processar ────────────────────────────────
        menor_data = _menor_data_pendente(conn_bd2)

        if menor_data is None:
            salvar_log("✅ Todos os analíticos atualizados — pipeline encerrada", LOG_PIPELINE)
            return

        ciclos = _ciclos_pendentes(menor_data)
        salvar_log(f"📅 Ciclos pendentes: {[c['mes'] for c in ciclos]}", LOG_PIPELINE)

        for ciclo in ciclos:
            salvar_log("=" * 50, LOG_PIPELINE)
            salvar_log(f"CICLO » {ciclo['mes']} ({ciclo['data_inicio']} até {ciclo['data_fim']})", LOG_PIPELINE)
            salvar_log("=" * 50, LOG_PIPELINE)

            # ── 2. Mailing — base obrigatória, cobre período de todos ─────────
            try:
                df_mailing_analitico = _obter_mailing(ciclo, conn_trc, conn_bd2, conn_src)
            except Exception:
                salvar_log("FALHA » Mailing indisponível — ciclo interrompido", LOG_PIPELINE)
                continue

            # ── 3. Cada indicador: carrega, processa e insere isoladamente ────
            etapas = {
                'DISCAGENS': {
                    'nome':     'discagens',
                    'fn':       discagens_pipeline.executar_analitico,
                    'datasets': ['discagens_expert', 'dw_calendario'],
                    'kwargs':   lambda d: {
                        'df_discagens_expert':  d.get('discagens_expert', pd.DataFrame()),
                        'df_mailing_analitico': df_mailing_analitico,
                        'df_dw_calendario':     d.get('dw_calendario', pd.DataFrame()),
                    },
                },
                'ACIONAMENTOS': {
                    'nome':     'acionamentos',
                    'fn':       acionamentos_pipeline.executar_analitico,
                    'datasets': ['tab_acionamentos', 'tabulacao_aciona', 'dw_calendario'],
                    'kwargs':   lambda d: {
                        'df_tab_acionamentos':  d.get('tab_acionamentos', pd.DataFrame()),
                        'df_tabulacao_aciona':  d.get('tabulacao_aciona', pd.DataFrame()),
                        'df_mailing_analitico': df_mailing_analitico,
                        'df_dw_calendario':     d.get('dw_calendario', pd.DataFrame()),
                    },
                },
                'MASSIVOS': {
                    'nome':     'massivos',
                    'fn':       massivos_pipeline.executar_analitico,
                    'datasets': ['sms', 'rcs', 'email', 'whats', 'dw_calendario'],
                    'kwargs':   lambda d: {
                        'df_sms':               d.get('sms', pd.DataFrame()),
                        'df_rcs':               d.get('rcs', pd.DataFrame()),
                        'df_email':             d.get('email', pd.DataFrame()),
                        'df_whats':             d.get('whats', pd.DataFrame()),
                        'df_mailing_analitico': df_mailing_analitico,
                        'df_dw_calendario':     d.get('dw_calendario', pd.DataFrame()),
                    },
                },
            }

            for indicador, cfg in etapas.items():
                nome = cfg['nome']
                config_tabela = TABELAS[nome]

                ultima = verificar_ultima_data(
                    tabela=config_tabela['tabela'],
                    conn=conn_bd2,
                    col_data=config_tabela['col_data'],
                )
                if esta_atualizado(ultima):
                    salvar_log(f"SKIP » {nome} — analítico já atualizado até {ultima}", LOG_PIPELINE)
                    continue

                try:
                    dados = _executar_etapa(
                        f"Carga {nome} — {ciclo['mes']}",
                        load_data_cresol,
                        data_inicio=str(ciclo['data_inicio']),
                        data_fim=str(ciclo['data_fim']),
                        datasets_to_load=cfg['datasets'],
                        conn_trc=conn_trc, conn_bd2=conn_bd2, conn_src=conn_src,
)

                    resultado = _executar_etapa(
                        f"{nome} — {ciclo['mes']}",
                        cfg['fn'],
                        **cfg['kwargs'](dados),
                    )

                    _inserir_isolado(nome, resultado['analitico'], conn_bd2)

                except Exception as e:
                    salvar_log(f"FALHA » {nome}: {str(e)}", LOG_PIPELINE)
                    salvar_log(traceback.format_exc(), LOG_PIPELINE)

    salvar_log("=" * 50, LOG_PIPELINE)
    salvar_log("ANALÍTICOS — CONCLUÍDO", LOG_PIPELINE)
    salvar_log("=" * 50, LOG_PIPELINE)


if __name__ == "__main__":
    executar_analiticos()