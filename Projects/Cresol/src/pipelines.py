"""
Ponto de entrada da pipeline Cresol.
Execute: python -m Cresol.src.pipeline
"""
import logging
import traceback
from datetime import datetime

from Projects.Cresol.src.mailing import mailing_pipeline
from Cresol.src.discagens import discagens_pipeline
from Cresol.src.acionamentos import acionamentos_pipeline
from Cresol.src.digital_channels import massivos_pipeline
from Cresol.src.data_loader import load_data_cresol
from Cresol.src.config import LOGS, LOGS_DIR

import os
import pandas as pd

log = logging.getLogger(__name__)

# ── Helpers de log ────────────────────────────────────────────────────────────

def _configurar_logging():
    """Configura logging no terminal e em arquivo resumido."""
    log_pipeline = os.path.join(LOGS_DIR, 'pipeline.txt')
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(levelname)-8s | %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler(),                          # terminal
            logging.FileHandler(log_pipeline, encoding='utf-8'),  # arquivo
        ]
    )

def _executar_etapa(nome: str, fn, *args, **kwargs):
    """
    Executa uma etapa da pipeline com log padronizado.
    Lança a exceção original em caso de falha para interromper o fluxo.
    """
    log.info(f"{'─' * 50}")
    log.info(f"INÍCIO » {nome}")
    inicio = datetime.now()
    try:
        resultado = fn(*args, **kwargs)
        duracao = (datetime.now() - inicio).seconds
        log.info(f"OK    » {nome} — concluído em {duracao}s")
        return resultado
    except Exception:
        duracao = (datetime.now() - inicio).seconds
        log.error(f"FALHA » {nome} — erro após {duracao}s")
        log.error(traceback.format_exc())
        raise  # interrompe a pipeline e preserva o traceback original


# ── Pipeline ──────────────────────────────────────────────────────────────────

def executar_pipeline():
    _configurar_logging()
    log.info("=" * 50)
    log.info("PIPELINE CRESOL — INICIANDO")
    log.info("=" * 50)
    inicio_total = datetime.now()

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
        _consolidar_acumulados,
        df_mailing_acumulado,
        df_discagens_acumulado,
        df_ringing_acumulados,
        df_acionamentos_acumulados,
        df_massivos_acumulado,
    )

    duracao_total = (datetime.now() - inicio_total).seconds
    log.info("=" * 50)
    log.info(f"PIPELINE CRESOL — CONCLUÍDA em {duracao_total}s")
    log.info("=" * 50)

    return {
        "mailing":      (df_mailing_analitico, df_mailing_acumulado),
        "discagens":    (df_discagens_analitico, df_discagens_acumulado, df_ringing_acumulados),
        "acionamentos": (df_acionamentos_analitico, df_acionamentos_acumulados),
        "massivos":     (df_massivos_analitico, df_massivos_acumulado),
        "acumulados":   df_acumulados_consolidado,
    }


def _consolidar_acumulados(*dfs: pd.DataFrame) -> pd.DataFrame:
    """
    Concatena todos os DataFrames acumulados em um único consolidado.
    Assume que todos compartilham as mesmas colunas — ajuste o parâmetro
    join='inner' ou 'outer' conforme a necessidade do seu caso.
    """
    return pd.concat(dfs, ignore_index=True)


if __name__ == "__main__":
    executar_pipeline()