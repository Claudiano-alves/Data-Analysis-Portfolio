"""
Ponto de entrada da pipeline Cresol.
Execute: python -m Cresol.src.pipeline
"""
import traceback
from datetime import date, timedelta

from Cresol.src.mailing import mailing_pipeline
from Cresol.src.discagens import discagens_pipeline
from Cresol.src.acionamentos import acionamentos_pipeline
from Cresol.src.digital_channels import massivos_pipeline
from Cresol.src.data_loader import load_data_cresol
from Cresol.src.database import processar_indicador, inserir
from Cresol.src.config import LOG_PIPELINE, TABELAS, DATASETS_POR_INDICADOR
from utils._database.check import verificar_ultima_data, esta_atualizado
from utils.utils import salvar_log, registrar_tempo, unir_dataframes
from utils.db_connection import get_db_connections

# ── Helpers ───────────────────────────────────────────────────────────────────

def _ciclos_pendentes(conn) -> list:
    """
    Retorna lista de ciclos (ano_mes) pendentes em ordem cronológica.
    Cada ciclo representa um mês que precisa ser processado.
    
    Exemplo:
        Janeiro completo, fevereiro parcial, março iniciando →
        [{'data_inicio': '2026-02-01', 'data_fim': '2026-02-28'},
         {'data_inicio': '2026-03-01', 'data_fim': '2026-03-17'}]  ← D-1
    """
    from calendar import monthrange

    ontem = date.today() - timedelta(days=1)

    # Busca a menor data desatualizada entre todos os analíticos
    menor_data = ontem
    for config_key in ['mailing', 'discagens', 'acionamentos', 'massivos']:
        config = TABELAS[config_key]
        ultima = verificar_ultima_data(
            tabela=config['tabela'],
            conn=conn,
            col_data=config['col_data'],
        )
        if ultima is None:
            # tabela vazia — começa do primeiro dia do mês atual
            primeira_data = date(ontem.year, ontem.month, 1)
            if primeira_data < menor_data:
                menor_data = primeira_data
        elif ultima < menor_data:
            menor_data = ultima + timedelta(days=1)

    # Gera ciclos mensais entre menor_data e ontem
    ciclos = []
    atual = menor_data

    while atual <= ontem:
        ano, mes = atual.year, atual.month
        ultimo_dia_mes = date(ano, mes, monthrange(ano, mes)[1])

        # fim do ciclo: último dia do mês ou D-1, o que vier primeiro
        fim_ciclo = min(ultimo_dia_mes, ontem)

        ciclos.append({
            'data_inicio': str(atual),
            'data_fim':    str(fim_ciclo),
            'mes':         f"{ano}-{mes:02d}",
        })

        # próximo ciclo: primeiro dia do mês seguinte
        if ultimo_dia_mes >= ontem:
            break
        atual = date(ano + (mes // 12), (mes % 12) + 1, 1)

    return ciclos

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

def _indicadores_pendentes(conn) -> list:
    """Verifica quais indicadores precisam ser atualizados no acumulado."""
    config = TABELAS['sintetico']
    pendentes = []

    for indicador in DATASETS_POR_INDICADOR.keys():
        ultima_data = verificar_ultima_data(
            tabela=config['tabela'],
            conn=conn,
            col_data=config['col_data'],
            filtros={'Indicador': indicador},
        )
        if not esta_atualizado(ultima_data):
            salvar_log(f"PENDENTE » {indicador} — última data: {ultima_data}", LOG_PIPELINE)
            pendentes.append(indicador)
        else:
            salvar_log(f"SKIP     » {indicador} — acumulado atualizado até {ultima_data}", LOG_PIPELINE)

    return pendentes

def _datasets_necessarios(indicadores_pendentes: list) -> list:
    """Resolve quais datasets precisam ser carregados para os indicadores pendentes."""
    datasets = set()
    for indicador in indicadores_pendentes:
        datasets.update(DATASETS_POR_INDICADOR[indicador])
    return list(datasets)

def _datas_carga(conn) -> tuple:
    """Determina o período de carga com base na menor data desatualizada."""
    ontem = date.today() - timedelta(days=1)
    menor_data = ontem

    for indicador, config_key in [
        ('MAILING', 'mailing'),
        ('DISCAGENS', 'discagens'),
        ('ACIONAMENTOS', 'acionamentos'),
        ('MASSIVOS', 'massivos'),
    ]:
        config = TABELAS[config_key]
        ultima = verificar_ultima_data(
            tabela=config['tabela'],
            conn=conn,
            col_data=config['col_data'],
        )
        if ultima and ultima < menor_data:
            menor_data = ultima + timedelta(days=1)

    return str(menor_data), str(ontem)


# ── Pipeline ──────────────────────────────────────────────────────────────────

@registrar_tempo("Pipeline Cresol", arquivo_log=LOG_PIPELINE)
def executar_pipeline():
    salvar_log("=" * 50, LOG_PIPELINE)
    salvar_log("PIPELINE CRESOL — INICIANDO", LOG_PIPELINE)
    salvar_log("=" * 50, LOG_PIPELINE)

    with get_db_connections() as (conn_trc, conn_bd2, conn_src):

        # ── 1. Verifica ciclos pendentes ──────────────────────────────────────
        ciclos = _ciclos_pendentes(conn_bd2)

        if not ciclos:
            salvar_log("✅ Todos os dados estão atualizados — pipeline encerrada", LOG_PIPELINE)
            return {}

        salvar_log(f"📅 Ciclos pendentes: {[c['mes'] for c in ciclos]}", LOG_PIPELINE)

        # ── 2. Processa um ciclo por vez ──────────────────────────────────────
        resultados = {}

        for ciclo in ciclos:
            salvar_log("=" * 50, LOG_PIPELINE)
            salvar_log(f"CICLO » {ciclo['mes']} ({ciclo['data_inicio']} até {ciclo['data_fim']})", LOG_PIPELINE)
            salvar_log("=" * 50, LOG_PIPELINE)

            # verifica indicadores pendentes para esse ciclo
            indicadores = _indicadores_pendentes(conn_bd2)

            if not indicadores:
                salvar_log(f"SKIP » {ciclo['mes']} — todos os indicadores atualizados", LOG_PIPELINE)
                continue

            datasets_necessarios = _datasets_necessarios(indicadores)

            # ── 3. Carga seletiva do ciclo ────────────────────────────────────
            dados = _executar_etapa(
                f"Carga — {ciclo['mes']}",
                load_data_cresol,
                data_inicio=ciclo['data_inicio'],
                data_fim=ciclo['data_fim'],
                datasets_to_load=datasets_necessarios,
            )

            import pandas as pd
            _df = lambda key: dados.get(key, pd.DataFrame())

            # ── 4. Mailing ────────────────────────────────────────────────────
            df_mailing_analitico, df_mailing_acumulado = _executar_etapa(
                f"Mailing — {ciclo['mes']}",
                mailing_pipeline.executar,
                _df('mailing_hist'),
                _df('dw_calendario'),
            )
            _inserir_isolado('mailing', df_mailing_analitico, conn_bd2)

            resultados_ciclo = {'mailing': (df_mailing_analitico, df_mailing_acumulado)}

            # ── 5. Dependentes do mailing ─────────────────────────────────────
            etapas = {
                'DISCAGENS': (
                    'discagens',
                    discagens_pipeline.executar,
                    {'df_discagens_expert':  _df('discagens_expert'),
                     'df_mailing_analitico': df_mailing_analitico,
                     'df_dw_calendario':     _df('dw_calendario')},
                ),
                'ACIONAMENTOS': (
                    'acionamentos',
                    acionamentos_pipeline.executar,
                    {'df_tab_acionamentos':  _df('tab_acionamentos'),
                     'df_tabulacao_aciona':  _df('tabulacao_aciona'),
                     'df_mailing_analitico': df_mailing_analitico,
                     'df_dw_calendario':     _df('dw_calendario')},
                ),
                'MASSIVOS': (
                    'massivos',
                    massivos_pipeline.executar,
                    {'df_sms':               _df('sms'),
                     'df_rcs':               _df('rcs'),
                     'df_email':             _df('email'),
                     'df_whats':             _df('whats'),
                     'df_mailing_analitico': df_mailing_analitico,
                     'df_dw_calendario':     _df('dw_calendario')},
                ),
            }

            for indicador, (nome, fn_pipeline, kwargs) in etapas.items():
                if indicador not in indicadores:
                    continue
                try:
                    df_analitico, df_acumulado = _executar_etapa(
                        f"{nome} — {ciclo['mes']}", fn_pipeline, **kwargs
                    )
                    _inserir_isolado(nome, df_analitico, conn_bd2)
                    resultados_ciclo[nome] = (df_analitico, df_acumulado)
                except Exception as e:
                    salvar_log(f"FALHA » {nome}: {str(e)}", LOG_PIPELINE)
                    salvar_log(traceback.format_exc(), LOG_PIPELINE)

            # ── 6. Acumulados do ciclo ────────────────────────────────────────
            acumulados = [v[1] for v in resultados_ciclo.values() if len(v) > 1 and v[1] is not None]

            if acumulados:
                df_consolidado = _executar_etapa(
                    f"Consolidação — {ciclo['mes']}",
                    unir_dataframes,
                    *acumulados,
                )
                for indicador in df_consolidado['Indicador'].unique():
                    df_indicador = df_consolidado[df_consolidado['Indicador'] == indicador]
                    _inserir_isolado('sintetico', df_indicador, conn_bd2)

            resultados[ciclo['mes']] = resultados_ciclo

        salvar_log("=" * 50, LOG_PIPELINE)
        salvar_log("PIPELINE CRESOL — CONCLUÍDA", LOG_PIPELINE)
        salvar_log("=" * 50, LOG_PIPELINE)

        return resultados


if __name__ == "__main__":
    executar_pipeline()