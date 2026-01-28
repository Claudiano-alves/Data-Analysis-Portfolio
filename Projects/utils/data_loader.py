import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
import time
from typing import Tuple, Optional, List, Dict

from .db_connection import get_db_connections
from utils.utils import get_date_range_from_csv, salvar_log
from .queries import (
    get_query_mailing_hist,
    get_query_dw_calendario
)

def data_loader_(
    conn_trc, 
    conn_bd2, 
    conn_src, 
    data_inicio: str, 
    data_fim: str,
    where_campanhas: str = "",
    where_clientes_mailing: str = "",
    where_acionamentos: str = "",
    where_tabulacao: str = "",
    where_clientes_pagamentos: str = "",
    where_clientes_acordos: str = "",
    where_massivos: str = "",
    where_telefones: str = "",
    datasets_to_load: Optional[List[str]] = None
) -> Dict[str, pd.DataFrame]:
    """
    Carrega dados necessários dos bancos de dados de forma configurável.
    
    Args:
        conn_trc: Conexão com banco TRC
        conn_bd2: Conexão com banco BD2
        conn_src: Conexão com banco SRC
        data_inicio: Data inicial no formato 'YYYY-MM-DD'
        data_fim: Data final no formato 'YYYY-MM-DD'
        where_campanhas: Cláusula WHERE para discagens
        where_clientes_mailing: Cláusula WHERE para mailing_hist
        where_acionamentos: Cláusula WHERE para acionamentos
        where_tabulacao: Cláusula WHERE para tabulação
        where_clientes_pagamentos: Cláusula WHERE para pagamentos
        where_clientes_acordos: Cláusula WHERE para acordos
        where_massivos: Cláusula WHERE para SMS, RCS e Email
        where_telefones: Cláusula WHERE para telefones
        datasets_to_load: Lista de datasets a serem carregados. Se None, carrega todos.
                         Opções: 'discagens_expert', 'mailing_hist', 'tab_acionamentos',
                                'tabulacao_aciona', 'dw_calendario', 'pagamentos', 'acordos',
                                'sms', 'rcs', 'email', 'telefone', 'blacklist_expert',
                                'discagens_trestto'
        
    Returns:
        Dicionário com DataFrames carregados
    """
    import warnings
    warnings.filterwarnings('ignore', category=UserWarning)
    
    print(f"\n📊 Carregando dados de {data_inicio} até {data_fim}...\n")
    salvar_log("="*80)
    salvar_log(f"📊 INÍCIO DO CARREGAMENTO DE DADOS")
    salvar_log(f"   Período: {data_inicio} até {data_fim}")
    salvar_log("="*80)
    
    # Se não especificado, carrega todos os disponíveis
    if datasets_to_load is None:
        datasets_to_load = [
            'discagens_expert', 'mailing_hist', 'tab_acionamentos',
            'tabulacao_aciona', 'dw_calendario', 'pagamentos', 'acordos',
            'sms', 'rcs', 'email', 'telefone', 'blacklist_expert',
            'discagens_trestto'
        ]
    
    # ===============================
    # REGISTRO BASE (sempre disponível)
    # ===============================
    all_queries_config = {
        "mailing_hist": (
            "Mailing Histórico",
            get_query_mailing_hist,
            conn_bd2,
            (data_inicio, data_fim, where_clientes_mailing),
        ),
        "dw_calendario": (
            "Calendário",
            get_query_dw_calendario,
            conn_bd2,
            (data_inicio, data_fim),
        ),
    }
    # ===============================
    # REGISTRO DINÂMICO (sob demanda)
    # ===============================
    
    if "discagens_expert" in datasets_to_load:
        from utils.queries import get_query_discagens
        all_queries_config["discagens_expert"] = (
            "Discagens Expert",
            get_query_discagens,
            conn_src,
            (data_inicio, data_fim, where_campanhas),
        )
    
    if "tab_acionamentos" in datasets_to_load:
        from utils.queries import get_query_base_acionamentos
        all_queries_config["tab_acionamentos"] = (
            "Tabulação de Acionamentos",
            get_query_base_acionamentos,
            conn_src,
            (data_inicio, data_fim, where_acionamentos),
        )
    
    if "tabulacao_aciona" in datasets_to_load:
        from utils.queries import get_query_tabulacao_aciona
        all_queries_config["tabulacao_aciona"] = (
            "Tabulação Acionamentos",
            get_query_tabulacao_aciona,
            conn_bd2,
            (where_tabulacao,),
        )
    
    if "pagamentos" in datasets_to_load:
        from utils.queries import get_query_pagamentos
        all_queries_config["pagamentos"] = (
            "Pagamentos",
            get_query_pagamentos,
            conn_src,
            (data_inicio, data_fim, where_clientes_pagamentos),
        )
    
    if "acordos" in datasets_to_load:
        from utils.queries import get_query_acordos
        all_queries_config["acordos"] = (
            "Acordos",
            get_query_acordos,
            conn_src,
            (data_inicio, data_fim, where_clientes_acordos),
        )
    
    if "sms" in datasets_to_load:
        from utils.queries import get_query_sms
        all_queries_config["sms"] = (
            "SMS",
            get_query_sms,
            conn_bd2,
            (data_inicio, data_fim, where_massivos),
        )
    
    if "rcs" in datasets_to_load:
        from utils.queries import get_query_rcs
        all_queries_config["rcs"] = (
            "RCS",
            get_query_rcs,
            conn_bd2,
            (data_inicio, data_fim, where_massivos),
        )
    
    if "email" in datasets_to_load:
        from utils.queries import get_query_email
        all_queries_config["email"] = (
            "Email",
            get_query_email,
            conn_bd2,
            (data_inicio, data_fim, where_massivos),
        )
    
    if "telefone" in datasets_to_load:
        from utils.queries import get_query_telefone
        all_queries_config["telefone"] = (
            "Telefones",
            get_query_telefone,
            conn_src,
            (where_telefones,),
        )
    
    if "blacklist_expert" in datasets_to_load:
        from utils.queries import get_query_blacklist_expert
        all_queries_config["blacklist_expert"] = (
            "Blacklist Expert",
            get_query_blacklist_expert,
            conn_src,
            (),
        )
    
    if "discagens_trestto" in datasets_to_load:
        from utils.queries import get_query_discagens_trestto
        all_queries_config["discagens_trestto"] = (
            "Discagens Trestto",
            get_query_discagens_trestto,
            conn_trc,
            (data_inicio, data_fim),
        )
    
    # Validar datasets solicitados
    invalid_datasets = set(datasets_to_load) - set(all_queries_config.keys())
    if invalid_datasets:
        raise ValueError(f"Datasets inválidos solicitados: {invalid_datasets}")
    
    salvar_log(f"📋 Datasets a serem carregados: {', '.join(datasets_to_load)}")
    salvar_log("-"*80)
    
    dataframes = {}
    
    # Carregar apenas os datasets especificados
    for nome_df in datasets_to_load:
        if nome_df not in all_queries_config:
            continue
            
        descricao, query_func, conexao, args = all_queries_config[nome_df]
        
        try:
            tempo_inicio = time.time()
            
            # Executar query
            query = query_func(*args)
            df = pd.read_sql(query, conexao)
            
            tempo_fim = time.time()
            tempo_decorrido = tempo_fim - tempo_inicio
            
            # Formatação do tempo
            minutos = int(tempo_decorrido // 60)
            segundos = int(tempo_decorrido % 60)
            tempo_str = f"{minutos}m {segundos}s" if minutos > 0 else f"{segundos}s"
            
            # Formatação da quantidade de registros
            qtd_registros = f"{len(df):,}".replace(",", ".")
            
            # Exibir no terminal (resumido)
            print(f"{descricao}: {qtd_registros} registros")
            
            # Registrar no log (completo)
            salvar_log(f"✓ {descricao}")
            salvar_log(f"   📊 Registros: {qtd_registros}")
            salvar_log(f"   ⏱️  Tempo: {tempo_str}")
            salvar_log("-"*80)
            
            dataframes[nome_df] = df
            
        except Exception as e:
            erro_msg = f"❌ ERRO ao carregar {descricao}"
            print(erro_msg)
            salvar_log(erro_msg)
            salvar_log(f"   ⚠️  Detalhes: {str(e)}")
            salvar_log("="*80)
            raise Exception(f"Falha ao carregar {descricao}: {str(e)}")
    
    return dataframes

def load_all_data_(
    csv_path: Optional[str] = None,
    where_campanhas: str = "",
    where_clientes_mailing: str = "",
    where_acionamentos: str = "",
    where_tabulacao: str = "",
    where_clientes_pagamentos: str = "",
    where_clientes_acordos: str = "",
    where_massivos: str = "",
    where_telefones: str = "",
) -> Tuple[pd.DataFrame, ...]:
    """
    Função principal para carregar todos os dados.
    
    Args:
        csv_path: Caminho do arquivo CSV para calcular range de datas.
                  Se None, usa o caminho padrão do servidor:
                  \\trc-dc-ad\Planejamento\MIS\CARTEIRAS\GetNet\df_csvBI_padronizado.csv
        where_campanhas: Cláusula WHERE para discagens
        where_clientes_mailing: Cláusula WHERE para mailing_hist
        where_acionamentos: Cláusula WHERE para acionamentos
        where_tabulacao: Cláusula WHERE para tabulação
        where_clientes_pagamentos: Cláusula WHERE para pagamentos
        where_clientes_acordos: Cláusula WHERE para acordos
        where_massivos: Cláusula WHERE para SMS, RCS e Email
        where_telefones: Cláusula WHERE para telefones
                  
    Returns:
        Tupla com 12 DataFrames
        
    Exemplo de uso:
        # Definir WHEREs
        where_campanhas_ouze = "A.GrupoPrincipal IN (SELECT G.id_grupo FROM grupo G WHERE G.ID_CAMPANHA IN (19, 30))"
        where_clientes = "COD_CLI IN(196,198,228)"
        
        # Usando CSV padrão do servidor (recomendado)
        dfs = load_all_data(
            where_campanhas=where_campanhas_ouze,
            where_clientes_mailing=where_clientes,
            where_clientes_pagamentos=where_clientes
        )
        
        # Usando CSV customizado
        dfs = load_all_data(
            csv_path='data/historico.csv',
            where_campanhas=where_campanhas_ouze
        )
        
        # Desempacotando
        (df_disc_exp, df_mail, df_tab, df_tabul, df_cal, 
         df_pag, df_acord, df_sms, df_rcs, df_email, 
         df_tel, df_black) = load_all_data(where_campanhas=where_campanhas_ouze)
    """
    # Determina o range de datas (csv_path=None usará o padrão do servidor)
    data_inicio, data_fim = get_date_range_from_csv(csv_path)
    
    # Usa context manager para gerenciar conexões
    with get_db_connections() as (conn_trc, conn_bd2, conn_src):
        return data_loader(
            conn_trc=conn_trc,
            conn_bd2=conn_bd2,
            conn_src=conn_src,
            data_inicio=data_inicio,
            data_fim=data_fim,
            where_campanhas=where_campanhas,
            where_clientes_mailing=where_clientes_mailing,
            where_acionamentos=where_acionamentos,
            where_tabulacao=where_tabulacao,
            where_clientes_pagamentos=where_clientes_pagamentos,
            where_clientes_acordos=where_clientes_acordos,
            where_massivos=where_massivos,
            where_telefones=where_telefones
        )

def data_loader(
    conn_trc, 
    conn_bd2, 
    conn_src, 
    data_inicio: str, 
    data_fim: str,
    where_campanhas: str = "",
    where_clientes_mailing: str = "",
    where_acionamentos: str = "",
    where_tabulacao: str = "",
    where_clientes_pagamentos: str = "",
    where_clientes_acordos: str = "",
    where_massivos: str = "",
    where_telefones: str = "",
    datasets_to_load: Optional[List[str]] = None
) -> Dict[str, pd.DataFrame]:
    """
    Carrega dados necessários dos bancos de dados.
    
    Args:
        conn_trc: Conexão com banco TRC
        conn_bd2: Conexão com banco BD2
        conn_src: Conexão com banco SRC
        data_inicio: Data inicial no formato 'YYYY-MM-DD'
        data_fim: Data final no formato 'YYYY-MM-DD'
        where_campanhas: Cláusula WHERE para discagens
        where_clientes_mailing: Cláusula WHERE para mailing_hist
        where_acionamentos: Cláusula WHERE para acionamentos
        where_tabulacao: Cláusula WHERE para tabulação
        where_clientes_pagamentos: Cláusula WHERE para pagamentos
        where_clientes_acordos: Cláusula WHERE para acordos
        where_massivos: Cláusula WHERE para SMS, RCS e Email
        where_telefones: Cláusula WHERE para telefones
        datasets_to_load: Lista de datasets a serem carregados. Se None, carrega todos.
        
    Returns:
        Dicionário com DataFrames carregados
    """
    print(f"\n📊 Carregando dados de {data_inicio} até {data_fim}...\n")
    salvar_log("="*80)
    salvar_log(f"📊 INÍCIO DO CARREGAMENTO DE DADOS")
    salvar_log(f"   Período: {data_inicio} até {data_fim}")
    salvar_log("="*80)
    
    # Se não especificado, carrega todos os disponíveis
    if datasets_to_load is None:
        datasets_to_load = [
            'discagens_expert', 'mailing_hist', 'tab_acionamentos',
            'tabulacao_aciona', 'dw_calendario', 'pagamentos', 'acordos',
            'sms', 'rcs', 'email', 'telefone', 'blacklist_expert',
            'discagens_trestto'
        ]
    
    # ===============================
    # CONFIGURAÇÃO DE QUERIES
    # ===============================
    all_queries_config = {
        "mailing_hist": (
            "Mailing Histórico",
            get_query_mailing_hist,
            conn_bd2,
            (data_inicio, data_fim, where_clientes_mailing),
        ),
        "dw_calendario": (
            "Calendário",
            get_query_dw_calendario,
            conn_bd2,
            (data_inicio, data_fim),
        ),
    }
    
    # Adicionar queries dinâmicas
    if "discagens_expert" in datasets_to_load:
        from utils.queries import get_query_discagens
        all_queries_config["discagens_expert"] = (
            "Discagens Expert",
            get_query_discagens,
            conn_src,
            (data_inicio, data_fim, where_campanhas),
        )
    
    if "tab_acionamentos" in datasets_to_load:
        from utils.queries import get_query_base_acionamentos
        all_queries_config["tab_acionamentos"] = (
            "Tabulação de Acionamentos",
            get_query_base_acionamentos,
            conn_src,
            (data_inicio, data_fim, where_acionamentos),
        )
    
    if "tabulacao_aciona" in datasets_to_load:
        from utils.queries import get_query_tabulacao_aciona
        all_queries_config["tabulacao_aciona"] = (
            "Tabulação Acionamentos",
            get_query_tabulacao_aciona,
            conn_bd2,
            (where_tabulacao,),
        )
    
    if "pagamentos" in datasets_to_load:
        from utils.queries import get_query_pagamentos
        all_queries_config["pagamentos"] = (
            "Pagamentos",
            get_query_pagamentos,
            conn_src,
            (data_inicio, data_fim, where_clientes_pagamentos),
        )
    
    if "acordos" in datasets_to_load:
        from utils.queries import get_query_acordos
        all_queries_config["acordos"] = (
            "Acordos",
            get_query_acordos,
            conn_src,
            (data_inicio, data_fim, where_clientes_acordos),
        )
    
    if "sms" in datasets_to_load:
        from utils.queries import get_query_sms
        all_queries_config["sms"] = (
            "SMS",
            get_query_sms,
            conn_bd2,
            (data_inicio, data_fim, where_massivos),
        )
    
    if "rcs" in datasets_to_load:
        from utils.queries import get_query_rcs
        all_queries_config["rcs"] = (
            "RCS",
            get_query_rcs,
            conn_bd2,
            (data_inicio, data_fim, where_massivos),
        )
    
    if "email" in datasets_to_load:
        from utils.queries import get_query_email
        all_queries_config["email"] = (
            "Email",
            get_query_email,
            conn_bd2,
            (data_inicio, data_fim, where_massivos),
        )
    
    if "telefone" in datasets_to_load:
        from utils.queries import get_query_telefone
        all_queries_config["telefone"] = (
            "Telefones",
            get_query_telefone,
            conn_src,
            (where_telefones,),
        )
    
    if "blacklist_expert" in datasets_to_load:
        from utils.queries import get_query_blacklist_expert
        all_queries_config["blacklist_expert"] = (
            "Blacklist Expert",
            get_query_blacklist_expert,
            conn_src,
            (),
        )
    
    if "discagens_trestto" in datasets_to_load:
        from utils.queries import get_query_discagens_trestto
        all_queries_config["discagens_trestto"] = (
            "Discagens Trestto",
            get_query_discagens_trestto,
            conn_trc,
            (data_inicio, data_fim),
        )
    
    # Validar datasets solicitados
    invalid_datasets = set(datasets_to_load) - set(all_queries_config.keys())
    if invalid_datasets:
        raise ValueError(f"Datasets inválidos solicitados: {invalid_datasets}")
    
    salvar_log(f"📋 Datasets a serem carregados: {', '.join(datasets_to_load)}")
    salvar_log("-"*80)
    
    # ===============================
    # CARREGAMENTO DE DADOS
    # ===============================
    dataframes = {}
    tempo_total_inicio = time.time()
    total_datasets = len(datasets_to_load)
    
    for idx, nome_df in enumerate(datasets_to_load, 1):
        if nome_df not in all_queries_config:
            continue
        
        descricao, query_func, conexao, args = all_queries_config[nome_df]
        
        try:
            tempo_inicio = time.time()
            
            # Executar query
            query = query_func(*args)
            df = pd.read_sql(query, conexao)
            
            tempo_fim = time.time()
            tempo_decorrido = tempo_fim - tempo_inicio
            
            # Formatação do tempo
            minutos = int(tempo_decorrido // 60)
            segundos = int(tempo_decorrido % 60)
            tempo_str = f"{minutos}m {segundos}s" if minutos > 0 else f"{segundos}s"
            
            # Formatação da quantidade de registros
            qtd_registros = f"{len(df):,}".replace(",", ".")
            
            # Exibir no terminal
            print(f"[{idx}/{total_datasets}] ✓ {descricao}: {qtd_registros} registros ({tempo_str})")
            
            # Registrar no log
            salvar_log(f"✓ [{idx}/{total_datasets}] {descricao}")
            salvar_log(f"   📊 Registros: {qtd_registros}")
            salvar_log(f"   ⏱️  Tempo: {tempo_str}")
            salvar_log("-"*80)
            
            dataframes[nome_df] = df
            
        except Exception as e:
            erro_msg = f"❌ ERRO ao carregar {descricao}"
            print(erro_msg)
            salvar_log(erro_msg)
            salvar_log(f"   ⚠️  Detalhes: {str(e)}")
            salvar_log("="*80)
            raise Exception(f"Falha ao carregar {descricao}: {str(e)}")
    
    # Tempo total
    tempo_total = time.time() - tempo_total_inicio
    minutos_total = int(tempo_total // 60)
    segundos_total = int(tempo_total % 60)
    tempo_total_str = f"{minutos_total}m {segundos_total}s" if minutos_total > 0 else f"{segundos_total}s"
    
    print(f"\n✅ Carregamento concluído em {tempo_total_str}")
    salvar_log("="*80)
    salvar_log(f"✅ CARREGAMENTO CONCLUÍDO")
    salvar_log(f"   ⏱️  Tempo total: {tempo_total_str}")
    salvar_log("="*80)
    
    return dataframes

def load_all_data(
    datasets_to_load=None,
    csv_path: Optional[str] = None,
    where_campanhas: str = "",
    where_clientes_mailing: str = "",
    where_acionamentos: str = "",
    where_tabulacao: str = "",
    where_clientes_pagamentos: str = "",
    where_clientes_acordos: str = "",
    where_massivos: str = "",
    where_telefones: str = ""
) -> Tuple[pd.DataFrame, ...]:
    """
    Função principal para carregar todos os dados.
    
    Args:
        csv_path: Caminho do arquivo CSV para calcular range de datas.
                  Se None, usa o caminho padrão do servidor:
                  \\trc-dc-ad\Planejamento\MIS\CARTEIRAS\GetNet\df_csvBI_padronizado.csv
        where_campanhas: Cláusula WHERE para discagens
        where_clientes_mailing: Cláusula WHERE para mailing_hist
        where_acionamentos: Cláusula WHERE para acionamentos
        where_tabulacao: Cláusula WHERE para tabulação
        where_clientes_pagamentos: Cláusula WHERE para pagamentos
        where_clientes_acordos: Cláusula WHERE para acordos
        where_massivos: Cláusula WHERE para SMS, RCS e Email
        where_telefones: Cláusula WHERE para telefones
                  
    Returns:
        Tupla com 12 DataFrames
        
    Exemplo de uso:
        # Definir WHEREs
        where_campanhas_ouze = "A.GrupoPrincipal IN (SELECT G.id_grupo FROM grupo G WHERE G.ID_CAMPANHA IN (19, 30))"
        where_clientes = "COD_CLI IN(196,198,228)"
        
        # Usando CSV padrão do servidor (recomendado)
        dfs = load_all_data(
            where_campanhas=where_campanhas_ouze,
            where_clientes_mailing=where_clientes,
            where_clientes_pagamentos=where_clientes
        )
        
        # Usando CSV customizado
        dfs = load_all_data(
            csv_path='data/historico.csv',
            where_campanhas=where_campanhas_ouze
        )
        
        # Desempacotando
        (df_disc_exp, df_mail, df_tab, df_tabul, df_cal, 
         df_pag, df_acord, df_sms, df_rcs, df_email, 
         df_tel, df_black) = load_all_data(where_campanhas=where_campanhas_ouze)
    """
    # Determina o range de datas (csv_path=None usará o padrão do servidor)
    data_inicio, data_fim = get_date_range_from_csv(csv_path)
    
    # Usa context manager para gerenciar conexões
    with get_db_connections() as (conn_trc, conn_bd2, conn_src):
        dataframes = data_loader(
            conn_trc=conn_trc,
            conn_bd2=conn_bd2,
            conn_src=conn_src,
            data_inicio=data_inicio,
            data_fim=data_fim,
            datasets_to_load=datasets_to_load,
            where_campanhas=where_campanhas,
            where_clientes_mailing=where_clientes_mailing,
            where_acionamentos=where_acionamentos,
            where_tabulacao=where_tabulacao,
            where_clientes_pagamentos=where_clientes_pagamentos,
            where_clientes_acordos=where_clientes_acordos,
            where_massivos=where_massivos,
            where_telefones=where_telefones
        )
        
        # Retornar na ordem esperada (tupla de 12 DataFrames)
        return (
            dataframes.get('discagens_expert', pd.DataFrame()),
            dataframes.get('mailing_hist', pd.DataFrame()),
            dataframes.get('tab_acionamentos', pd.DataFrame()),
            dataframes.get('tabulacao_aciona', pd.DataFrame()),
            dataframes.get('dw_calendario', pd.DataFrame()),
            dataframes.get('pagamentos', pd.DataFrame()),
            dataframes.get('acordos', pd.DataFrame()),
            dataframes.get('sms', pd.DataFrame()),
            dataframes.get('rcs', pd.DataFrame()),
            dataframes.get('email', pd.DataFrame()),
            dataframes.get('telefone', pd.DataFrame()),
            dataframes.get('blacklist_expert', pd.DataFrame()),
        )

# ============================================================================
# EXEMPLO DE USO
# ============================================================================

if __name__ == "__main__":
    # Opção 1: Usando arquivo CSV padrão do servidor (RECOMENDADO)
    dados = load_all_data()
    
    # Opção 2: Usando CSV customizado
    # dados = load_all_data('caminho/para/outro/arquivo.csv')
    
    # Desempacotando os dados
    (df_discagens_expert, 
     df_cad_devf, 
     df_tab_acionamentos, 
     df_maling_hist, 
     df_dw_calendario, 
     df_tabulacao_aciona, 
     df_pagamentos, 
     df_acordos, 
     df_discagens_trestto) = dados
    
    print("\n🎉 Dados prontos para uso!")
    print(f"📊 Exemplo - Discagens Expert: {len(df_discagens_expert)} registros")