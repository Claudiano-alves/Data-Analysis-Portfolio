"""
Módulo de Carregamento e Pipeline Integrado (Ouze)
Combina o carregamento de dados com a execução do pipeline modular.
"""

import time
from utils.utils import salvar_log, registrar_tempo
from utils.data_loader import load_all_data
from .config import LOG_LOADING, DATASETS_TO_LOAD

# TODO: Implementar módulo de pipelines quando a arquitetura estiver pronta
# from .pipelines import executar_pipeline_funil_completo

@registrar_tempo("Carregamento de dados (Ouze)", arquivo_log=LOG_LOADING)
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
    Adaptado para o contexto Ouze.
    
    Args:
        datasets_to_load (dict, optional): Dicionário indicando quais datasets carregar
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
    """
    if datasets_to_load is None:
        datasets_to_load = DATASETS_TO_LOAD
    
    salvar_log("=" * 80, arquivo_log=LOG_LOADING)
    salvar_log("INICIANDO CARREGAMENTO DE DADOS OUZE (SEQUENCIAL)", arquivo_log=LOG_LOADING)
    salvar_log("=" * 80, arquivo_log=LOG_LOADING)
    
    tempo_inicio = time.time()
    
    try:
        # Usar load_all_data (executando de forma sequencial)
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
        
        # Converter tupla para dicionário baseado na ordem do load_all_data
        # A ordem deve corresponder ao retorno de utils.data_loader.load_all_data
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
    Executa o pipeline COMPLETO do Ouze: carregamento + processamento (Futuro).
    
    Args:
        datasets_to_load (dict, optional): Dicionário indicando quais datasets carregar
        csv_path (str, optional): Caminho do CSV para determinar período
        where_*: Filtros SQL customizados para cada dataset
    
    Returns:
        dict: Resultados do pipeline
    """
    if datasets_to_load is None:
        datasets_to_load = DATASETS_TO_LOAD

    salvar_log("\n" + "=" * 80, arquivo_log=LOG_LOADING)
    salvar_log("INICIANDO PIPELINE OUZE (CARREGAMENTO)", arquivo_log=LOG_LOADING)
    salvar_log("=" * 80, arquivo_log=LOG_LOADING)
    
    try:
        # ============================================
        # ETAPA 1: CARREGAR DADOS
        # ============================================
        salvar_log("\n📥 ETAPA 1: Carregando dados do banco...", arquivo_log=LOG_LOADING)
        
        dados = carregar_dados_paralelo(
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
        
        # ============================================
        # ETAPA 2: PROCESSAR DADOS (TODO)
        # ============================================
        salvar_log("\n⚙️ ETAPA 2: Processamento (Aguardando implementação do pipeline modular)...", arquivo_log=LOG_LOADING)
        
        # Placeholder para quando tivermos o pipelines.py
        # resultados = executar_pipeline_funil_completo(...)
        
        return {
            'dados_carregados': dados,
            'status': 'Carregamento concluído, processamento pendente'
        }

    except Exception as e:
        salvar_log(f"\n❌ Erro fatal no pipeline: {str(e)}", arquivo_log=LOG_LOADING)
        raise