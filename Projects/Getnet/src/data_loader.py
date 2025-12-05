import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Tuple, Optional
from contextlib import contextmanager

from data_wrangling_mailingHist import tratar_base_mailing_hist
from db_connection import get_connection
from queries import (
    get_query_discagens, 
    get_query_cad_devf, 
    get_query_mailing_hist, 
    get_query_discagens_trestto, 
    get_query_tabulacao_aciona, 
    get_query_base_acionamentos, 
    get_query_dw_calendario, 
    get_query_pagamentos, 
    get_query_acordos
)

def get_date_range_from_csv(csv_path: Optional[str] = None) -> Tuple[str, str]:
    """
    Lê um arquivo XLSX e retorna o range de datas baseado na última data encontrada.
    
    Lógica:
    - Se última data está no mês atual: busca do dia 1 do mês até ontem
    - Se última data está em mês anterior: busca do dia seguinte à última data até ontem
    
    Args:
        csv_path: Caminho do arquivo XLSX. Se None, usa o caminho padrão do servidor.
        
    Returns:
        Tupla com (data_inicio, data_fim) no formato 'YYYY-MM-DD'
    """
    # Define caminho padrão se não fornecido
    if csv_path is None:
        csv_path = r"\\trc-dc-ad\Planejamento\MIS\CARTEIRAS\GetNet\df_csvBI_padronizado.xlsx"
    
    try:
        print(f"📂 Lendo arquivo: {csv_path}")
        
        # Lê o XLSX da segunda aba
        df = pd.read_excel(csv_path, sheet_name='df_csvBI_padronizado')
        
        # Verifica se coluna 'data' existe
        if 'data' not in df.columns:
            date_col = df.columns[0]
            print(f"⚠️ Coluna 'data' não encontrada. Usando primeira coluna: '{date_col}'")
        else:
            date_col = 'data'
        
        # Converte para datetime e pega a última data
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        
        # Remove valores nulos
        df_validos = df[df[date_col].notna()]
        
        if len(df_validos) == 0:
            raise ValueError("Nenhuma data válida encontrada no XLSX")
        
        # Filtro: Remove datas muito antigas (antes de 2020)
        ano_minimo = 2020
        df_validos = df_validos[df_validos[date_col].dt.year >= ano_minimo]
        
        if len(df_validos) == 0:
            raise ValueError(f"Nenhuma data válida encontrada após {ano_minimo}")
        
        ultima_data = df_validos[date_col].max()
        hoje = datetime.now()
        data_fim = (hoje - timedelta(days=1)).strftime('%Y-%m-%d')
        
        # ⭐ LÓGICA CORRIGIDA ⭐
        # Verifica se última data está no mês atual
        if ultima_data.year == hoje.year and ultima_data.month == hoje.month:
            # Mesmo mês: pega do dia 1 do mês
            data_inicio = ultima_data.replace(day=1).strftime('%Y-%m-%d')
            print(f"📅 Última data no mês atual - buscando do início do mês")
        else:
            # Mês diferente: pega do dia seguinte à última data
            data_inicio = (ultima_data + timedelta(days=1)).strftime('%Y-%m-%d')
            print(f"📅 Última data em mês anterior - buscando do dia seguinte")
        
        print(f"✅ Arquivo lido com sucesso!")
        print(f"📅 Range de datas calculado:")
        print(f"   Última data no XLSX: {ultima_data.strftime('%Y-%m-%d')}")
        print(f"   Data início: {data_inicio}")
        print(f"   Data fim: {data_fim}")
        
        return data_inicio, data_fim
        
    except FileNotFoundError:
        print(f"❌ Arquivo não encontrado: {csv_path}")
        print(f"⚠️ Usando datas padrão.")
        return get_default_date_range()
        
    except PermissionError:
        print(f"❌ Sem permissão para acessar: {csv_path}")
        print(f"⚠️ Usando datas padrão.")
        return get_default_date_range()
        
    except Exception as e:
        print(f"❌ Erro ao processar XLSX: {e}")
        print(f"⚠️ Usando datas padrão.")
        return get_default_date_range()

def get_default_date_range() -> Tuple[str, str]:
    """
    Retorna range de datas padrão: primeiro dia do mês atual até ontem.
    """
    hoje = datetime.now()
    data_inicio = hoje.replace(day=1).strftime('%Y-%m-%d')
    data_fim = (hoje - timedelta(days=1)).strftime('%Y-%m-%d')
    return data_inicio, data_fim


@contextmanager
def get_db_connections():
    """
    Context manager para gerenciar conexões de banco de dados.
    Garante que as conexões sejam fechadas mesmo em caso de erro.
    """
    conn_trc = None
    conn_bd2 = None
    conn_src = None
    
    try:
        print("🔌 Conectando aos bancos de dados...")
        conn_trc = get_connection("SERVER_BD2", "DATABASE_TRC")
        conn_bd2 = get_connection("SERVER_BD2", "DATABASE_BD2")
        conn_src = get_connection("SERVER_SRC", "DATABASE_SRC")
        print("✅ Conexões estabelecidas")
        
        yield conn_trc, conn_bd2, conn_src
        
    finally:
        # Fecha as conexões
        for conn, name in [(conn_trc, 'TRC'), (conn_bd2, 'BD2'), (conn_src, 'SRC')]:
            if conn:
                try:
                    conn.close()
                    print(f"🔌 Conexão {name} fechada")
                except Exception as e:
                    print(f"⚠️ Erro ao fechar conexão {name}: {e}")


def data_loader(
    conn_trc, 
    conn_bd2, 
    conn_src, 
    data_inicio: str, 
    data_fim: str
) -> Tuple[pd.DataFrame, ...]:
    """
    Carrega todos os dados necessários dos bancos de dados.
    
    Args:
        conn_trc: Conexão com banco TRC
        conn_bd2: Conexão com banco BD2
        conn_src: Conexão com banco SRC
        data_inicio: Data inicial no formato 'YYYY-MM-DD'
        data_fim: Data final no formato 'YYYY-MM-DD'
        
    Returns:
        Tupla com 9 DataFrames
    """
    print(f"\n📊 Carregando dados de {data_inicio} até {data_fim}...\n")
    
    # DADOS SRC
    print("⏳ Carregando discagens_expert...")
    df_discagens_expert = pd.read_sql(
        get_query_discagens(data_inicio, data_fim), 
        conn_src
    )
    print(f"   ✓ {len(df_discagens_expert)} registros")
    
    print("⏳ Carregando cad_devf...")
    df_cad_devf = pd.read_sql(
        get_query_cad_devf(), 
        conn_src
    )
    print(f"   ✓ {len(df_cad_devf)} registros")
    
    print("⏳ Carregando tab_acionamentos...")
    df_tab_acionamentos = pd.read_sql(
        get_query_base_acionamentos(data_inicio, data_fim), 
        conn_src
    )
    print(f"   ✓ {len(df_tab_acionamentos)} registros")
    
    print("⏳ Carregando pagamentos...")
    df_pagamentos = pd.read_sql(
        get_query_pagamentos(data_inicio, data_fim), 
        conn_src
    )
    print(f"   ✓ {len(df_pagamentos)} registros")
    
    print("⏳ Carregando acordos...")
    df_acordos = pd.read_sql(
        get_query_acordos(data_inicio, data_fim), 
        conn_src
    )
    print(f"   ✓ {len(df_acordos)} registros")

    # DADOS BD2/PLANEJAMENTO
    print("⏳ Carregando mailing_hist...")
    df_maling_hist = pd.read_sql(
        get_query_mailing_hist(data_inicio, data_fim), 
        conn_bd2
    )
    print(f"   ✓ {len(df_maling_hist)} registros (antes do tratamento)")
    
    print("⏳ Tratando base mailing_hist...")
    df_maling_hist = tratar_base_mailing_hist(df_maling_hist, df_cad_devf)
    print(f"   ✓ {len(df_maling_hist)} registros (após tratamento)")

    print("⏳ Carregando dw_calendario...")
    df_dw_calendario = pd.read_sql(
        get_query_dw_calendario(data_inicio, data_fim), 
        conn_bd2
    )
    print(f"   ✓ {len(df_dw_calendario)} registros")
    
    print("⏳ Carregando tabulacao_aciona...")
    df_tabulacao_aciona = pd.read_sql(
        get_query_tabulacao_aciona(), 
        conn_bd2
    )
    print(f"   ✓ {len(df_tabulacao_aciona)} registros")

    # DADOS BD2/TRC
    print("⏳ Carregando discagens_trestto...")
    df_discagens_trestto = pd.read_sql(
        get_query_discagens_trestto(data_inicio, data_fim), 
        conn_trc
    )
    print(f"   ✓ {len(df_discagens_trestto)} registros")
    
    print("\n✅ Todos os dados carregados com sucesso!\n")
    
    return (
        df_discagens_expert, 
        df_cad_devf, 
        df_tab_acionamentos, 
        df_maling_hist, 
        df_dw_calendario, 
        df_tabulacao_aciona, 
        df_pagamentos, 
        df_acordos, 
        df_discagens_trestto
    )


def load_all_data(csv_path: Optional[str] = None) -> Tuple[pd.DataFrame, ...]:
    """
    Função principal para carregar todos os dados.
    
    Args:
        csv_path: Caminho do arquivo CSV para calcular range de datas.
                  Se None, usa o caminho padrão do servidor:
                  \\trc-dc-ad\Planejamento\MIS\CARTEIRAS\GetNet\df_csvBI_padronizado.csv
                  
    Returns:
        Tupla com 9 DataFrames
        
    Exemplo de uso:
        # Usando CSV padrão do servidor (recomendado)
        dfs = load_all_data()
        
        # Usando CSV customizado
        dfs = load_all_data('data/historico.csv')
        
        # Desempacotando
        df_disc_exp, df_cad, df_tab, df_mail, df_cal, df_tabul, df_pag, df_acord, df_disc_tres = load_all_data()
    """
    # Determina o range de datas (csv_path=None usará o padrão do servidor)
    data_inicio, data_fim = get_date_range_from_csv(csv_path)
    
    # Usa context manager para gerenciar conexões
    with get_db_connections() as (conn_trc, conn_bd2, conn_src):
        return data_loader(conn_trc, conn_bd2, conn_src, data_inicio, data_fim)


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