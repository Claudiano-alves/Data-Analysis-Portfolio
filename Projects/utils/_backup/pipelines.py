"""
Ponto de entrada da pipeline Cresol.
Execute: python -m Cresol.src.pipeline
"""
import traceback
import pandas as pd

from Cresol.src.mailing import mailing_pipeline
from Cresol.src.discagens import discagens_pipeline
from Cresol.src.acionamentos import acionamentos_pipeline
from Cresol.src.digital_channels import massivos_pipeline
from Cresol.src.data_loader import load_data_cresol
from Cresol.src.config import LOGS, LOG_PIPELINE
from utils.utils import salvar_log, registrar_tempo, unir_dataframes
from Cresol.src.database import inserir
from utils.db_connection import get_db_connections


# ── Helpers ───────────────────────────────────────────────────────────────────

def _executar_etapa(nome: str, fn, *args, **kwargs):
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


def _inserir_isolado(nome: str, df, conn):
    """
    Executa um insert de forma isolada — falha ou dado já existente
    não interrompem os demais inserts.
    """
    salvar_log(f"{'─' * 50}", LOG_PIPELINE)
    salvar_log(f"INSERT » {nome}", LOG_PIPELINE)
    try:
        sucesso = inserir(nome, df, conn=conn)
        if sucesso:
            salvar_log(f"OK    » {nome}", LOG_PIPELINE)
        else:
            salvar_log(f"SKIP  » {nome} — dados já existentes ou nenhum registro a inserir", LOG_PIPELINE)
    except Exception as e:
        salvar_log(f"FALHA » {nome}: {str(e)}", LOG_PIPELINE)
        salvar_log(traceback.format_exc(), LOG_PIPELINE)


# ── Pipeline ──────────────────────────────────────────────────────────────────

@registrar_tempo("Pipeline Cresol", arquivo_log=LOG_PIPELINE)
def executar_pipeline():
    salvar_log("=" * 50, LOG_PIPELINE)
    salvar_log("PIPELINE CRESOL — INICIANDO", LOG_PIPELINE)
    salvar_log("=" * 50, LOG_PIPELINE)

    # ── 1. Carga ──────────────────────────────────────────────────────────────
    (
        df_discagens_expert,
        df_mailing_hist,
        df_tab_acionamentos,
        df_tabulacao_aciona,
        df_dw_calendario,
        df_pagamentos,
        df_acordos,
        df_sms,
        df_rcs,
        df_email,
        df_whats,
    ) = _executar_etapa("Carga de dados", load_data_cresol).values()

    # ── 2. Mailing ────────────────────────────────────────────────────────────
    df_mailing_analitico, df_mailing_acumulado = _executar_etapa(
        "Mailing",
        mailing_pipeline.executar,
        df_mailing_hist,
        df_dw_calendario,
    )

    # ── 3. Discagens ──────────────────────────────────────────────────────────
    df_discagens_analitico, df_discagens_acumulado, df_ringing_acumulados, _ = _executar_etapa(
        "Discagens",
        discagens_pipeline.executar,
        df_discagens_expert,
        df_mailing_analitico,
        df_dw_calendario,
    )

    # ── 4. Acionamentos ───────────────────────────────────────────────────────
    df_acionamentos_analitico, df_acionamentos_acumulados, _, _ = _executar_etapa(
        "Acionamentos",
        acionamentos_pipeline.executar,
        df_tab_acionamentos,
        df_tabulacao_aciona,
        df_mailing_analitico,
        df_dw_calendario,
    )

    # ── 5. Massivos ───────────────────────────────────────────────────────────
    df_massivos_analitico, df_massivos_acumulado, _ = _executar_etapa(
        "Massivos",
        massivos_pipeline.executar,
        df_sms, df_rcs, df_email, df_whats,
        df_mailing_analitico,
        df_dw_calendario,
    )

    # ── 6. Consolidação dos acumulados ────────────────────────────────────────
    df_acumulados_consolidado = _executar_etapa(
        "Consolidação dos acumulados",
        unir_dataframes,
        df_mailing_acumulado,
        df_discagens_acumulado,
        df_ringing_acumulados,
        df_acionamentos_acumulados,
        df_massivos_acumulado,
    )

    # ── 7. Inserts isolados ───────────────────────────────────────────────────
    salvar_log("=" * 50, LOG_PIPELINE)
    salvar_log("INSERTS — INICIANDO", LOG_PIPELINE)
    salvar_log("=" * 50, LOG_PIPELINE)

    with get_db_connections() as (_, conn_bd2, __):

        # analíticos
        _inserir_isolado('mailing',      df_mailing_analitico,      conn_bd2)
        _inserir_isolado('discagens',    df_discagens_analitico,    conn_bd2)
        _inserir_isolado('acionamentos', df_acionamentos_analitico, conn_bd2)
        _inserir_isolado('massivos',     df_massivos_analitico,     conn_bd2)

        # sintético — por indicador
        for indicador in df_acumulados_consolidado['Indicador'].unique():
            df_indicador = df_acumulados_consolidado[
                df_acumulados_consolidado['Indicador'] == indicador
            ]
            _inserir_isolado('sintetico', df_indicador, conn_bd2)

    salvar_log("=" * 50, LOG_PIPELINE)
    salvar_log("PIPELINE CRESOL — CONCLUÍDA", LOG_PIPELINE)
    salvar_log("=" * 50, LOG_PIPELINE)

    return {
        "mailing":      (df_mailing_analitico, df_mailing_acumulado),
        "discagens":    (df_discagens_analitico, df_discagens_acumulado, df_ringing_acumulados),
        "acionamentos": (df_acionamentos_analitico, df_acionamentos_acumulados),
        "massivos":     (df_massivos_analitico, df_massivos_acumulado),
        "acumulados":   df_acumulados_consolidado,
    }


if __name__ == "__main__":
    executar_pipeline()