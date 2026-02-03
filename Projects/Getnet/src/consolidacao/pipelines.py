"""
Módulo de Consolidação de Funil
Orquestra o pipeline completo integrando todos os módulos.
"""
import pandas as pd
import time
from utils.utils import unir_dataframes, salvar_log, registrar_tempo, transformar_funil_formato_long
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
        # PRÉ-PROCESSAMENTO: Adicionar coluna ORIGEM aos acordos
        # ============================================
        salvar_log("\n🔧 PRÉ-PROCESSAMENTO: Enriquecendo acordos com ORIGEM...", arquivo_log=LOG_LOADING)
        
        # Códigos que identificam acordos do tipo ROBÔ
        codigos_robo = [1626, 1, 11003]
        
        # Criar coluna ORIGEM baseada em RECUP_ACO
        df_acordos['ORIGEM'] = df_acordos['RECUP_ACO'].apply(
            lambda x: 'ROBÔ' if x in codigos_robo else 'HUMANO'
        )
        
        salvar_log(f"✓ Coluna ORIGEM adicionada aos acordos", arquivo_log=LOG_LOADING)
        salvar_log(f"  - Acordos ROBÔ: {(df_acordos['ORIGEM'] == 'ROBÔ').sum():,}", arquivo_log=LOG_LOADING)
        salvar_log(f"  - Acordos HUMANO: {(df_acordos['ORIGEM'] == 'HUMANO').sum():,}", arquivo_log=LOG_LOADING)
        
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
        # ETAPA 5: UNIÃO ACIONAMENTO/DISCAGENS E TRANSFORMAÇÃO EM FORMATO LONG
        # ============================================
        df_acionamentos_discagens = unir_dataframes(df_acionamentos_final, df_discagens_expert_final, df_discagens_trestto_final)
        df_acionamentos_discagens = transformar_funil_formato_long(df_acionamentos_discagens)

        # ============================================
        # PRÉ-CONSOLIDAÇÃO: Padronizar nomes de colunas
        # ============================================
        salvar_log("\n🔄 Padronizando nomes de colunas para consolidação...", arquivo_log=LOG_LOADING)
        
        # Renomear VALOR → VALORPRIN_FIN no mailing
        # if df_mailing_final is not None and not df_mailing_final.empty:
        #     if 'VALOR' in df_mailing_final.columns:
        #         df_mailing_final = df_mailing_final.copy()
        #         df_mailing_final.rename(columns={'VALOR': 'VALORPRIN_FIN'}, inplace=True)
        #         salvar_log("✓ Coluna VALOR renomeada para VALORPRIN_FIN no mailing", arquivo_log=LOG_LOADING)
        # if 'VALOR' in df_mailing_final.columns:
        #     df_mailing_final.rename(columns={'VALOR': 'VALORPRIN_FIN'}, inplace=True)
        # elif 'VALORPRIN_FIN' not in df_mailing_final.columns:
        #     salvar_log("⚠️ Atenção: coluna de valor não encontrada no mailing", arquivo_log=LOG_LOADING)
        # ============================================
        # ETAPA 6: CONSOLIDAÇÃO
        # ============================================
        salvar_log("\n🔗 ETAPA 5: Consolidando resultados...", arquivo_log=LOG_LOADING)
        tempo_etapa = time.time()

        df_consolidado = consolidar_dataframes_funil(
            df_mailing_final,
            df_acionamentos_discagens,
            df_pagamentos_final
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
    df_pagamentos
):
    """
    Consolida todos os DataFrames em um único DataFrame de funil.
    """
    salvar_log("Consolidando DataFrames...", arquivo_log=LOG_LOADING)
    
    try:
        # Verifica se os DataFrames não estão vazios
        dfs_info = {
            'df_mailing': df_mailing,
            'df_acionamentos': df_acionamentos,
            'df_pagamentos': df_pagamentos
        }
        
        for nome_df, df in dfs_info.items():
            if df is None or df.empty:
                salvar_log(f"⚠ AVISO: {nome_df} está vazio ou é None", arquivo_log=LOG_LOADING)
        
        # Log das colunas de cada DataFrame para debug
        salvar_log("\n--- Estrutura dos DataFrames ---", arquivo_log=LOG_LOADING)
        for nome_df, df in dfs_info.items():
            if df is not None and not df.empty:
                colunas = list(df.columns)
                salvar_log(f"{nome_df}: {len(df)} registros, {len(colunas)} colunas", arquivo_log=LOG_LOADING)
                salvar_log(f"  Colunas: {colunas}", arquivo_log=LOG_LOADING)
            else:
                salvar_log(f"{nome_df}: Vazio ou None", arquivo_log=LOG_LOADING)
        salvar_log("--- Fim da estrutura ---\n", arquivo_log=LOG_LOADING)
        
        # Preparar df_mailing
        df_mailing_prep = df_mailing.copy()
        df_mailing_prep['TIPO_ORIGEM'] = ''
        # ✅ CORREÇÃO: df_mailing já tem VALORPRIN_FIN, renomear para VALOR
        if 'VALORPRIN_FIN' in df_mailing_prep.columns:
            df_mailing_prep = df_mailing_prep.rename(columns={'VALORPRIN_FIN': 'VALOR'})
        elif 'VALOR' not in df_mailing_prep.columns:
            salvar_log("⚠️ AVISO: Nenhuma coluna de valor encontrada no mailing", arquivo_log=LOG_LOADING)
            df_mailing_prep['VALOR'] = 0  # Cria coluna vazia para evitar erro
        
        # Preparar df_pagamentos
        df_pagamentos_prep = df_pagamentos.copy()
        df_pagamentos_prep = df_pagamentos_prep.rename(columns={
            'DATA_PAGTO': 'DATA',
            'ORIGEM': 'TIPO_ORIGEM',
            'VALOR_PARC': 'VALOR'
        })
        
        # Preparar df_acionamentos
        df_acionamentos_prep = df_acionamentos.copy()
        df_acionamentos_prep = df_acionamentos_prep.rename(columns={
            'ORIGEM': 'TIPO_ORIGEM',
            'VALORPRIN_FIN': 'VALOR'
        })
        
        # Garantir que todos tenham as mesmas colunas na mesma ordem
        colunas_padrao = ['DATA', 'Indicador', 'qte', 'FX_ATRASO', 'TIPO_ORIGEM', 
                          'MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
        
        # ✅ CORREÇÃO: Adicionar verificação e preenchimento de colunas faltantes
        for nome, df_prep in [('mailing', df_mailing_prep), 
                               ('pagamentos', df_pagamentos_prep), 
                               ('acionamentos', df_acionamentos_prep)]:
            colunas_faltantes = set(colunas_padrao) - set(df_prep.columns)
            if colunas_faltantes:
                salvar_log(f"⚠️ Colunas faltantes em {nome}: {colunas_faltantes}", arquivo_log=LOG_LOADING)
                for col in colunas_faltantes:
                    df_prep[col] = None if col != 'VALOR' else 0
        
        # Selecionar apenas as colunas padrão
        df_mailing_prep = df_mailing_prep[colunas_padrao]
        df_pagamentos_prep = df_pagamentos_prep[colunas_padrao]
        df_acionamentos_prep = df_acionamentos_prep[colunas_padrao]
        
        # Log antes da concatenação
        salvar_log(f"\n📊 Preparação para concatenação:", arquivo_log=LOG_LOADING)
        salvar_log(f"  - Mailing: {len(df_mailing_prep):,} registros", arquivo_log=LOG_LOADING)
        salvar_log(f"  - Pagamentos: {len(df_pagamentos_prep):,} registros", arquivo_log=LOG_LOADING)
        salvar_log(f"  - Acionamentos: {len(df_acionamentos_prep):,} registros", arquivo_log=LOG_LOADING)
        
        # Concatenar os três DataFrames
        df_consolidado = pd.concat(
            [df_mailing_prep, df_pagamentos_prep, df_acionamentos_prep], 
            ignore_index=True
        )
        
        # Padronizar coluna DATA
        df_consolidado['DATA'] = pd.to_datetime(df_consolidado['DATA']).dt.date
        
        salvar_log(f"\n✓ Consolidação concluída: {len(df_consolidado):,} registros", arquivo_log=LOG_LOADING)
        salvar_log(f"  - Do mailing: {len(df_mailing_prep):,}", arquivo_log=LOG_LOADING)
        salvar_log(f"  - De pagamentos: {len(df_pagamentos_prep):,}", arquivo_log=LOG_LOADING)
        salvar_log(f"  - De acionamentos: {len(df_acionamentos_prep):,}", arquivo_log=LOG_LOADING)
        
        return df_consolidado
        
    except Exception as e:
        salvar_log(f"\n❌ ERRO na consolidação dos DataFrames: {str(e)}", arquivo_log=LOG_LOADING)
        salvar_log(f"Tipo do erro: {type(e).__name__}", arquivo_log=LOG_LOADING)
        
        # Log detalhado das colunas em caso de erro
        salvar_log("\n--- DIAGNÓSTICO DE ERRO ---", arquivo_log=LOG_LOADING)
        for nome_df, df in dfs_info.items():
            if df is not None and not df.empty:
                salvar_log(f"\n{nome_df}:", arquivo_log=LOG_LOADING)
                salvar_log(f"  Shape: {df.shape}", arquivo_log=LOG_LOADING)
                salvar_log(f"  Colunas ({len(df.columns)}): {list(df.columns)}", arquivo_log=LOG_LOADING)
                salvar_log(f"  Dtypes: {df.dtypes.to_dict()}", arquivo_log=LOG_LOADING)
        salvar_log("--- FIM DO DIAGNÓSTICO ---\n", arquivo_log=LOG_LOADING)
        
        raise

def consolidar_dataframes_funil_(
    df_mailing,
    df_acionamentos,
    df_pagamentos
):
    """
    Consolida todos os DataFrames em um único DataFrame de funil.
    
    Args:
        df_mailing (pd.DataFrame): Resultados de mailing
        df_acionamentos (pd.DataFrame): Resultados de acionamentos
        df_pagamentos (pd.DataFrame): Resultados de pagamentos
    
    Returns:
        pd.DataFrame: DataFrame consolidado
    """
    salvar_log("Consolidando DataFrames...", arquivo_log=LOG_LOADING)
    
    try:
        # Verifica se os DataFrames não estão vazios
        dfs_info = {
            'df_mailing': df_mailing,
            'df_acionamentos': df_acionamentos,
            'df_pagamentos': df_pagamentos
        }
        
        for nome_df, df in dfs_info.items():
            if df is None or df.empty:
                salvar_log(f"⚠ AVISO: {nome_df} está vazio ou é None", arquivo_log=LOG_LOADING)
        
        # Log das colunas de cada DataFrame para debug
        salvar_log("\n--- Estrutura dos DataFrames ---", arquivo_log=LOG_LOADING)
        for nome_df, df in dfs_info.items():
            if df is not None and not df.empty:
                colunas = list(df.columns)
                salvar_log(f"{nome_df}: {len(df)} registros, {len(colunas)} colunas", arquivo_log=LOG_LOADING)
                salvar_log(f"  Colunas: {colunas}", arquivo_log=LOG_LOADING)
            else:
                salvar_log(f"{nome_df}: Vazio ou None", arquivo_log=LOG_LOADING)
        salvar_log("--- Fim da estrutura ---\n", arquivo_log=LOG_LOADING)
        
        # Preparar df_mailing
        df_mailing_prep = df_mailing.copy()
        df_mailing_prep['TIPO_ORIGEM'] = ''
        # df_mailing já tem a coluna VALOR, não precisa renomear
        
        # Preparar df_pagamentos
        df_pagamentos_prep = df_pagamentos.copy()
        df_pagamentos_prep = df_pagamentos_prep.rename(columns={
            'DATA_PAGTO': 'DATA',
            'ORIGEM': 'TIPO_ORIGEM',
            'VALOR_PARC': 'VALOR'
        })
        
        # Preparar df_acionamentos
        df_acionamentos_prep = df_acionamentos.copy()
        df_acionamentos_prep = df_acionamentos_prep.rename(columns={
            'ORIGEM': 'TIPO_ORIGEM',
            'VALORPRIN_FIN': 'VALOR'
        })
        
        # Garantir que todos tenham as mesmas colunas na mesma ordem
        colunas_padrao = ['DATA', 'Indicador', 'qte', 'FX_ATRASO', 'TIPO_ORIGEM', 
                          'MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
        
        df_mailing_prep = df_mailing_prep[colunas_padrao]
        df_pagamentos_prep = df_pagamentos_prep[colunas_padrao]
        df_acionamentos_prep = df_acionamentos_prep[colunas_padrao]
        
        # Concatenar os três DataFrames
        df_consolidado = pd.concat([df_mailing_prep, df_pagamentos_prep, df_acionamentos_prep], ignore_index=True)
        
        # Padronizar coluna DATA
        df_consolidado['DATA'] = pd.to_datetime(df_consolidado['DATA']).dt.date
        
        salvar_log(f"✓ Consolidação concluída: {len(df_consolidado):,} registros", arquivo_log=LOG_LOADING)
        
        return df_consolidado
        
    except Exception as e:
        salvar_log(f"\n❌ ERRO na consolidação dos DataFrames: {str(e)}", arquivo_log=LOG_LOADING)
        salvar_log(f"Tipo do erro: {type(e).__name__}", arquivo_log=LOG_LOADING)
        
        # Log detalhado das colunas em caso de erro
        salvar_log("\n--- DIAGNÓSTICO DE ERRO ---", arquivo_log=LOG_LOADING)
        for nome_df, df in dfs_info.items():
            if df is not None and not df.empty:
                salvar_log(f"\n{nome_df}:", arquivo_log=LOG_LOADING)
                salvar_log(f"  Shape: {df.shape}", arquivo_log=LOG_LOADING)
                salvar_log(f"  Colunas ({len(df.columns)}): {list(df.columns)}", arquivo_log=LOG_LOADING)
                salvar_log(f"  Dtypes: {df.dtypes.to_dict()}", arquivo_log=LOG_LOADING)
        salvar_log("--- FIM DO DIAGNÓSTICO ---\n", arquivo_log=LOG_LOADING)
        
        raise