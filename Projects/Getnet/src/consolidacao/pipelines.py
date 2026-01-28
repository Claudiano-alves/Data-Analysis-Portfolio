"""
Módulo de Consolidação de Funil
Orquestra o pipeline completo integrando todos os módulos.
"""

import time
from utils.utils import unir_dataframes, salvar_log, registrar_tempo
from ..mailing.pipelines import processar_mailing_completo
from ..pagamentos.pipelines import processar_pagamentos_completo
from ..acionamentos.pipelines import acionamentos_humano
from ..discagens.expert import processar_discagens_expert_completo
from ..discagens.trestto import processar_discagens_trestto_completo
from ..config import LOG_LOADING

@registrar_tempo("Pipeline completo de funil", arquivo_log=LOG_LOADING)
def executar_pipeline_funil_completo(
    df_tab_acionamentos,
    df_tabulacao_aciona,
    df_mailing_hist,
    df_dw_calendario,
    df_pagamentos,
    df_acordos,
    df_discagens_expert=None,
    df_discagens_trestto=None
):
    """
    Executa o pipeline completo respeitando as dependências entre módulos.
    
    Fluxo:
    1. MAILING (independente) → saída usada por acionamentos e pagamentos
    2. ACIONAMENTOS (depende de mailing)
    3. PAGAMENTOS (depende de mailing)
    4. DISCAGENS (independentes)
    5. CONSOLIDAÇÃO (usa tudo)
    
    Args:
        df_tab_acionamentos (pd.DataFrame): Acionamentos brutos
        df_tabulacao_aciona (pd.DataFrame): Tabulações
        df_mailing_hist (pd.DataFrame): Mailing bruto
        df_dw_calendario (pd.DataFrame): Calendário
        df_pagamentos (pd.DataFrame): Pagamentos brutos
        df_acordos (pd.DataFrame): Acordos
        df_discagens_expert (pd.DataFrame, optional): Discagens expert
        df_discagens_trestto (pd.DataFrame, optional): Discagens trestto
    
    Returns:
        dict: Dicionário com todos os resultados do pipeline
    """
    salvar_log("=" * 80, arquivo_log=LOG_LOADING)
    salvar_log("INICIANDO PIPELINE COMPLETO DE FUNIL", arquivo_log=LOG_LOADING)
    salvar_log("=" * 80, arquivo_log=LOG_LOADING)
    
    tempo_inicio = time.time()
    
    resultados = {}
    
    try:
        # ============================================
        # ETAPA 1: MAILING (independente)
        # ============================================
        salvar_log("\n📧 ETAPA 1: Processando Mailing...", arquivo_log=LOG_LOADING)
        tempo_etapa = time.time()
        
        df_mailing_hist, df_mailing_final = processar_mailing_completo(
            df_mailing_hist,
            df_dw_calendario
        )
        
        tempo_mailing = time.time() - tempo_etapa
        salvar_log(f"✓ Mailing processado em {tempo_mailing:.1f}s", arquivo_log=LOG_LOADING)
        resultados['mailing'] = df_mailing_final
        
        # ============================================
        # ETAPA 2: ACIONAMENTOS (depende de mailing)
        # ============================================
        salvar_log("\n📞 ETAPA 2: Processando Acionamentos...", arquivo_log=LOG_LOADING)
        tempo_etapa = time.time()
        
        (
            df_acionamentos_final,
            df_acionamentos_analitico,
            df_acion_semFaixa,
            df_acion_semDescricao,
            df_acion_semOrigem
        ) = acionamentos_humano(
            df_tab_acionamentos,
            df_tabulacao_aciona,
            df_dw_calendario,
            df_mailing_hist  # ⬅️ Usa resultado de mailing
        )
        
        tempo_acionamentos = time.time() - tempo_etapa
        salvar_log(f"✓ Acionamentos processados em {tempo_acionamentos:.1f}s", arquivo_log=LOG_LOADING)
        resultados['acionamentos'] = df_acionamentos_final
        resultados['acionamentos_analitico'] = df_acionamentos_analitico
        
        # ============================================
        # ETAPA 3: PAGAMENTOS (depende de mailing)
        # ============================================
        salvar_log("\n💰 ETAPA 3: Processando Pagamentos...", arquivo_log=LOG_LOADING)
        tempo_etapa = time.time()
        
        (
            df_pagamentos_final,
            df_pagtos_semFaixa,
            df_pagamentos_analitico
        ) = processar_pagamentos_completo(
            df_pagamentos,
            df_acordos,
            df_mailing_hist,  # ⬅️ Usa resultado de mailing
            df_dw_calendario
        )
        
        tempo_pagamentos = time.time() - tempo_etapa
        salvar_log(f"✓ Pagamentos processados em {tempo_pagamentos:.1f}s", arquivo_log=LOG_LOADING)
        resultados['pagamentos'] = df_pagamentos_final
        resultados['pagamentos_analitico'] = df_pagamentos_analitico
        
        # ============================================
        # ETAPA 4: DISCAGENS (independentes)
        # ============================================
        salvar_log("\n📞 ETAPA 4: Processando Discagens...", arquivo_log=LOG_LOADING)
        
        df_discagens_expert_final = None
        df_discagens_trestto_final = None
        
        if df_discagens_expert is not None and not df_discagens_expert.empty:
            tempo_etapa = time.time()
            salvar_log("\n   🔹 Processando Discagens EXPERT...", arquivo_log=LOG_LOADING)
            
            (
                df_discagens_expert_final,
                df_expert_analitico,
                df_expert_semFaixa,
                df_expert_humano,
                df_expert_outros
            ) = processar_discagens_expert_completo(
                df_discagens_expert,
                df_mailing_hist,
                df_dw_calendario
            )
            
            tempo_expert = time.time() - tempo_etapa
            salvar_log(f"   ✓ Discagens EXPERT: {len(df_discagens_expert_final):,} registros ({tempo_expert:.1f}s)", arquivo_log=LOG_LOADING)
        
        if df_discagens_trestto is not None and not df_discagens_trestto.empty:
            tempo_etapa = time.time()
            salvar_log("\n   🔹 Processando Discagens TRESTTO...", arquivo_log=LOG_LOADING)
            
            (
                df_discagens_trestto_final,
                df_trestto_analitico,
                df_trestto_semFaixa
            ) = processar_discagens_trestto_completo(
                df_discagens_trestto,
                df_mailing_hist,
                df_dw_calendario
            )
            
            tempo_trestto = time.time() - tempo_etapa
            salvar_log(f"   ✓ Discagens TRESTTO: {len(df_discagens_trestto_final):,} registros ({tempo_trestto:.1f}s)", arquivo_log=LOG_LOADING)
        
        # ============================================
        # ETAPA 5: CONSOLIDAÇÃO
        # ============================================
        salvar_log("\n🔗 ETAPA 5: Consolidando resultados...", arquivo_log=LOG_LOADING)
        tempo_etapa = time.time()
        
        df_consolidado = consolidar_dataframes_funil(
            df_mailing_final,
            df_acionamentos_final,
            df_pagamentos_final,
            df_discagens_expert_final,
            df_discagens_trestto_final
        )
        
        tempo_consolidacao = time.time() - tempo_etapa
        salvar_log(f"✓ Consolidação realizada em {tempo_consolidacao:.1f}s", arquivo_log=LOG_LOADING)
        resultados['consolidado'] = df_consolidado
        
        # ============================================
        # RESUMO FINAL
        # ============================================
        tempo_total = time.time() - tempo_inicio
        
        salvar_log("\n" + "=" * 80, arquivo_log=LOG_LOADING)
        salvar_log("RESUMO DO PIPELINE", arquivo_log=LOG_LOADING)
        salvar_log("=" * 80, arquivo_log=LOG_LOADING)
        salvar_log(f"✓ Mailing: {len(df_mailing_final):,} registros ({tempo_mailing:.1f}s)", arquivo_log=LOG_LOADING)
        salvar_log(f"✓ Acionamentos: {len(df_acionamentos_final):,} registros ({tempo_acionamentos:.1f}s)", arquivo_log=LOG_LOADING)
        salvar_log(f"✓ Pagamentos: {len(df_pagamentos_final):,} registros ({tempo_pagamentos:.1f}s)", arquivo_log=LOG_LOADING)
        salvar_log(f"✓ Consolidado: {len(df_consolidado):,} registros ({tempo_consolidacao:.1f}s)", arquivo_log=LOG_LOADING)
        salvar_log(f"\n⏱️ TEMPO TOTAL: {tempo_total:.1f}s", arquivo_log=LOG_LOADING)
        salvar_log("=" * 80, arquivo_log=LOG_LOADING)
        
        return resultados
    
    except Exception as e:
        salvar_log(f"\n✗ ERRO NO PIPELINE: {str(e)}", arquivo_log=LOG_LOADING)
        salvar_log("=" * 80, arquivo_log=LOG_LOADING)
        raise


def consolidar_dataframes_funil(
    df_mailing,
    df_acionamentos,
    df_pagamentos,
    df_expert=None,
    df_trestto=None
):
    """
    Consolida todos os DataFrames em um único DataFrame de funil.
    
    Args:
        df_mailing (pd.DataFrame): Resultados de mailing
        df_acionamentos (pd.DataFrame): Resultados de acionamentos
        df_pagamentos (pd.DataFrame): Resultados de pagamentos
        df_expert (pd.DataFrame, optional): Discagens expert
        df_trestto (pd.DataFrame, optional): Discagens trestto
    
    Returns:
        pd.DataFrame: DataFrame consolidado
    """
    salvar_log("Consolidando DataFrames...", arquivo_log=LOG_LOADING)
    
    dfs_para_unir = [df_mailing, df_acionamentos, df_pagamentos]
    
    if df_expert is not None and not df_expert.empty:
        dfs_para_unir.append(df_expert)
    
    if df_trestto is not None and not df_trestto.empty:
        dfs_para_unir.append(df_trestto)
    
    df_consolidado = unir_dataframes(*dfs_para_unir)
    
    salvar_log(f"✓ Consolidação concluída: {len(df_consolidado):,} registros", arquivo_log=LOG_LOADING)
    
    return df_consolidado
