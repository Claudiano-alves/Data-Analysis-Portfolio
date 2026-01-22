"""
Módulo de Carregamento e Pipeline Integrado
Combina o carregamento de dados com a execução do pipeline modular.
"""

import time
from ..utils import salvar_log, registrar_tempo
from Projects.utils.data_loader import load_all_data
from .pipelines import executar_pipeline_funil_completo


@registrar_tempo("Carregamento de dados")
def carregar_dados_paralelo(
    csv_path=None,
    max_workers=6,
    where_campanhas="",
    where_clientes_mailing="",
    where_acionamentos="",
    where_tabulacao="",
    where_clientes_pagamentos="",
    where_clientes_acordos="",
    where_massivos="",
    where_telefones=""
):
    """
    Carrega todos os dados do banco de forma paralela usando load_all_data.
    
    Args:
        csv_path (str, optional): Caminho do CSV para determinar período
        max_workers (int): Número de threads paralelas (padrão: 6)
        where_campanhas (str): Filtro SQL customizado para campanhas
        where_clientes_mailing (str): Filtro SQL customizado para mailing
        where_acionamentos (str): Filtro SQL customizado para acionamentos
        where_tabulacao (str): Filtro SQL customizado para tabulação
        where_clientes_pagamentos (str): Filtro SQL customizado para pagamentos
        where_clientes_acordos (str): Filtro SQL customizado para acordos
        where_massivos (str): Filtro SQL customizado para discagens massivos
        where_telefones (str): Filtro SQL customizado para telefones
    
    Returns:
        dict: Dicionário com todos os DataFrames carregados
            {
                'tab_acionamentos': pd.DataFrame,
                'tabulacao_aciona': pd.DataFrame,
                'mailing_hist': pd.DataFrame,
                'dw_calendario': pd.DataFrame,
                'pagamentos': pd.DataFrame,
                'acordos': pd.DataFrame,
                'discagens_expert': pd.DataFrame,
                'discagens_trestto': pd.DataFrame
            }
    """
    salvar_log("=" * 80)
    salvar_log("INICIANDO CARREGAMENTO PARALELO DE DADOS")
    salvar_log("=" * 80)
    
    tempo_inicio = time.time()
    
    try:
        # Usar load_all_data que já implementa carregamento paralelo
        dados = load_all_data(
            csv_path=csv_path,
            where_campanhas=where_campanhas,
            where_clientes_mailing=where_clientes_mailing,
            where_acionamentos=where_acionamentos,
            where_tabulacao=where_tabulacao,
            where_clientes_pagamentos=where_clientes_pagamentos,
            where_clientes_acordos=where_clientes_acordos,
            where_massivos=where_massivos,
            where_telefones=where_telefones
        )
        
        tempo_total = time.time() - tempo_inicio
        
        salvar_log(f"\n✅ Todos os dados carregados em {tempo_total:.1f}s")
        salvar_log("=" * 80)
        
        return dados
    
    except Exception as e:
        salvar_log(f"\n✗ ERRO ao carregar dados: {str(e)}")
        salvar_log("=" * 80)
        raise


def executar_pipeline_completo_com_carregamento(
    csv_path=None,
    max_workers=6,
    where_campanhas="",
    where_clientes_mailing="",
    where_acionamentos="",
    where_tabulacao="",
    where_clientes_pagamentos="",
    where_clientes_acordos="",
    where_massivos="",
    where_telefones=""
):
    """
    Executa o pipeline COMPLETO: carregamento + processamento modular.
    
    Este é o ponto de entrada principal para rodar TODO o pipeline.
    
    Args:
        csv_path (str, optional): Caminho do CSV para determinar período
        max_workers (int): Número de threads paralelas (padrão: 6)
        where_*: Filtros SQL customizados para cada dataset
    
    Returns:
        dict: Resultados completos do pipeline com chaves:
            - 'mailing': Dados de mailing processados
            - 'acionamentos': Dados de acionamentos processados
            - 'acionamentos_analitico': Análise de acionamentos
            - 'pagamentos': Dados de pagamentos processados
            - 'pagamentos_analitico': Análise de pagamentos
            - 'consolidado': DataFrame final consolidado
    
    Exemplo de uso:
        # 1. Pipeline completo com todos os dados
        resultados = executar_pipeline_completo_com_carregamento()
        df_funil = resultados['consolidado']
        
        # 2. Com filtros específicos
        resultados = executar_pipeline_completo_com_carregamento(
            where_campanhas="AND A.GrupoPrincipal IN (SELECT G.id_grupo FROM grupo G WHERE G.ID_CAMPANHA = 141)",
            where_clientes_mailing="COD_CLI = 253"
        )
        
        # 3. Com período customizado via CSV
        resultados = executar_pipeline_completo_com_carregamento(
            csv_path=r"\\path\\to\\base.csv"
        )
    """
    salvar_log("\n" + "=" * 80)
    salvar_log("INICIANDO PIPELINE COMPLETO (CARREGAMENTO + PROCESSAMENTO)")
    salvar_log("=" * 80)
    
    tempo_total_inicio = time.time()
    
    try:
        # ============================================
        # ETAPA 1: CARREGAR DADOS
        # ============================================
        salvar_log("\n📥 ETAPA 1: Carregando dados do banco...")
        tempo_carga = time.time()
        
        dados = carregar_dados_paralelo(
            csv_path=csv_path,
            max_workers=max_workers,
            where_campanhas=where_campanhas,
            where_clientes_mailing=where_clientes_mailing,
            where_acionamentos=where_acionamentos,
            where_tabulacao=where_tabulacao,
            where_clientes_pagamentos=where_clientes_pagamentos,
            where_clientes_acordos=where_clientes_acordos,
            where_massivos=where_massivos,
            where_telefones=where_telefones
        )
        
        tempo_carga_total = time.time() - tempo_carga
        
        # ============================================
        # ETAPA 2: PROCESSAR DADOS
        # ============================================
        salvar_log("\n⚙️ ETAPA 2: Processando dados com pipeline modular...")
        tempo_proc = time.time()
        
        resultados = executar_pipeline_funil_completo(
            df_tab_acionamentos=dados.get('tab_acionamentos'),
            df_tabulacao_aciona=dados.get('tabulacao_aciona'),
            df_mailing_hist=dados.get('mailing_hist'),
            df_dw_calendario=dados.get('dw_calendario'),
            df_pagamentos=dados.get('pagamentos'),
            df_acordos=dados.get('acordos'),
            df_discagens_expert=dados.get('discagens_expert'),
            df_discagens_trestto=dados.get('discagens_trestto')
        )
        
        tempo_proc_total = time.time() - tempo_proc
        
        # ============================================
        # RESUMO FINAL
        # ============================================
        tempo_total = time.time() - tempo_total_inicio
        
        salvar_log("\n" + "=" * 80)
        salvar_log("RESUMO DO PIPELINE COMPLETO")
        salvar_log("=" * 80)
        salvar_log(f"📥 Carregamento: {tempo_carga_total:.1f}s")
        salvar_log(f"⚙️ Processamento: {tempo_proc_total:.1f}s")
        salvar_log(f"📊 TEMPO TOTAL: {tempo_total:.1f}s")
        salvar_log(f"📈 Registros consolidados: {len(resultados['consolidado']):,}")
        salvar_log("=" * 80 + "\n")
        
        return resultados
    
    except Exception as e:
        salvar_log(f"\n✗ ERRO NO PIPELINE COMPLETO: {str(e)}")
        salvar_log("=" * 80)
        raise
