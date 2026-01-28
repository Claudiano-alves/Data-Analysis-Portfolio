"""
Módulo de Carregamento e Pipeline Integrado
Combina o carregamento de dados com a execução do pipeline modular.
"""

import time
from utils.utils import salvar_log, registrar_tempo
from utils.data_loader import load_all_data
from .pipelines import executar_pipeline_funil_completo
from ..config import LOG_LOADING, DATASETS_TO_LOAD

@registrar_tempo("Carregamento de dados", arquivo_log=LOG_LOADING)
def carregar_dados_paralelo(
    datasets_to_load=None,
    csv_path=None,
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
    Carrega todos os dados do banco de forma SEQUENCIAL usando load_all_data.
    
    NOTA: Nome mantido como 'paralelo' por compatibilidade, mas agora executa
    de forma SEQUENCIAL para evitar erros de "Conexão ocupada".
    
    Args:
        csv_path (str, optional): Caminho do CSV para determinar período
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
                'discagens_expert': pd.DataFrame,
                'mailing_hist': pd.DataFrame,
                'tab_acionamentos': pd.DataFrame,
                'tabulacao_aciona': pd.DataFrame,
                'dw_calendario': pd.DataFrame,
                'pagamentos': pd.DataFrame,
                'acordos': pd.DataFrame,
                'sms': pd.DataFrame,
                'rcs': pd.DataFrame,
                'email': pd.DataFrame,
                'telefone': pd.DataFrame,
                'blacklist_expert': pd.DataFrame,
                'discagens_trestto': pd.DataFrame
            }
    """
    salvar_log("=" * 80, arquivo_log=LOG_LOADING)
    salvar_log("INICIANDO CARREGAMENTO DE DADOS (SEQUENCIAL)", arquivo_log=LOG_LOADING)
    salvar_log("=" * 80, arquivo_log=LOG_LOADING)
    
    tempo_inicio = time.time()
    
    try:
        # Usar load_all_data (agora executando de forma sequencial)
        resultado_tupla = load_all_data(
            datasets_to_load=datasets_to_load,
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
        
        # Converter tupla para dicionário (mantém compatibilidade com código existente)
        dados = {
            'discagens_expert': resultado_tupla[0],
            'mailing_hist': resultado_tupla[1],
            'tab_acionamentos': resultado_tupla[2],
            'tabulacao_aciona': resultado_tupla[3],
            'dw_calendario': resultado_tupla[4],
            'pagamentos': resultado_tupla[5],
            'acordos': resultado_tupla[6],
            'sms': resultado_tupla[7],
            'rcs': resultado_tupla[8],
            'email': resultado_tupla[9],
            'telefone': resultado_tupla[10],
            'blacklist_expert': resultado_tupla[11],
        }
        
        # Se tiver discagens_trestto na tupla (índice 12), adicionar
        if len(resultado_tupla) > 12:
            dados['discagens_trestto'] = resultado_tupla[12]
        
        tempo_total = time.time() - tempo_inicio
        
        salvar_log(f"\n✅ Todos os dados carregados em {tempo_total:.1f}s", arquivo_log=LOG_LOADING)
        salvar_log("=" * 80, arquivo_log=LOG_LOADING)
        
        return dados
    
    except Exception as e:
        salvar_log(f"\n✗ ERRO ao carregar dados: {str(e)}", arquivo_log=LOG_LOADING)
        salvar_log("=" * 80, arquivo_log=LOG_LOADING)
        raise


def executar_pipeline_completo_com_carregamento(
    csv_path=None,
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
    Agora executa o carregamento de forma SEQUENCIAL para evitar erros de conexão.
    
    Args:
        csv_path (str, optional): Caminho do CSV para determinar período
        where_*: Filtros SQL customizados para cada dataset
    
    Returns:
        dict: Resultados completos do pipeline com chaves:
            - 'mailing': Dados de mailing processados
            - 'acionamentos': Dados de acionamentos processados
            - 'acionamentos_analitico': Análise de acionamentos
            - 'pagamentos': Dados de pagamentos processados
            - 'pagamentos_analitico': Análise de pagamentos
            - 'consolidado': DataFrame final consolidado
    
    Exemplos de uso:
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
    salvar_log("\n" + "=" * 80, arquivo_log=LOG_LOADING)
    salvar_log("INICIANDO PIPELINE COMPLETO (CARREGAMENTO + PROCESSAMENTO)", arquivo_log=LOG_LOADING)
    salvar_log("=" * 80, arquivo_log=LOG_LOADING)
    
    tempo_total_inicio = time.time()
    
    try:
        # ============================================
        # ETAPA 1: CARREGAR DADOS
        # ============================================
        salvar_log("\n📥 ETAPA 1: Carregando dados do banco...", arquivo_log=LOG_LOADING)
        tempo_carga = time.time()
        
        dados = carregar_dados_paralelo(
            datasets_to_load=DATASETS_TO_LOAD,
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
        
        tempo_carga_total = time.time() - tempo_carga
        
        # ============================================
        # ETAPA 2: PROCESSAR DADOS
        # ============================================
        salvar_log("\n⚙️ ETAPA 2: Processando dados com pipeline modular...", arquivo_log=LOG_LOADING)
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
        
        salvar_log("\n" + "=" * 80, arquivo_log=LOG_LOADING)
        salvar_log("RESUMO DO PIPELINE COMPLETO", arquivo_log=LOG_LOADING)
        salvar_log("=" * 80, arquivo_log=LOG_LOADING)
        salvar_log(f"📥 Carregamento: {tempo_carga_total:.1f}s", arquivo_log=LOG_LOADING)
        salvar_log(f"⚙️ Processamento: {tempo_proc_total:.1f}s", arquivo_log=LOG_LOADING)
        salvar_log(f"📊 TEMPO TOTAL: {tempo_total:.1f}s", arquivo_log=LOG_LOADING)
        salvar_log(f"📈 Registros consolidados: {len(resultados['consolidado']):,}")
        salvar_log("=" * 80 + "\n", arquivo_log=LOG_LOADING)
        
        return resultados
    
    except Exception as e:
        salvar_log(f"\n✗ ERRO NO PIPELINE COMPLETO: {str(e)}", arquivo_log=LOG_LOADING)
        salvar_log("=" * 80, arquivo_log=LOG_LOADING)
        raise
