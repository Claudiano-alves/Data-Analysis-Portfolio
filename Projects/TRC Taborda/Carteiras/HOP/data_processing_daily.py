"""
Script para processar consulta HOP_DAILY.sql em loop diário
Conecta ao banco de dados, executa a consulta para cada dia do mês
e salva os resultados em arquivo Excel com múltiplas abas.
"""

import pyodbc
import pandas as pd
from datetime import datetime, timedelta
import os
from pathlib import Path
import re  # Adicionar no topo do arquivo

def ler_arquivo_sql(caminho_sql):
    """
    Lê o arquivo SQL e retorna seu conteúdo como string.
    
    Args:
        caminho_sql (str): Caminho do arquivo SQL
        
    Returns:
        str: Conteúdo do arquivo SQL
    """
    with open(caminho_sql, 'r', encoding='latin-1') as file:
        return file.read()


def substituir_variaveis_sql_(sql_content, data):
    """
    Substitui as variáveis @DATAMES e @DATA no script SQL.
    
    Args:
        sql_content (str): Conteúdo do SQL original
        data (datetime.date): Data a ser utilizada
        
    Returns:
        str: SQL com as variáveis substituídas
    """
    data_str = data.strftime('%Y-%m-%d')
    
    # Substitui as declarações de variáveis
    sql_modificado = sql_content.replace(
        "DECLARE @DATAMES\t\tDATE = '2026-01-18'",
        f"DECLARE @DATAMES\t\tDATE = '{data_str}'"
    )
    sql_modificado = sql_modificado.replace(
        "DECLARE @DATA\t\t\tDATE = '2026-01-18'",
        f"DECLARE @DATA\t\t\tDATE = '{data_str}'"
    )
    
    return sql_modificado

def substituir_variaveis_sql(sql_content, data):
    """
    Substitui as variáveis @DATAMES e @DATA no script SQL usando regex.
    
    Args:
        sql_content (str): Conteúdo do SQL original
        data (datetime.date): Data a ser utilizada
        
    Returns:
        str: SQL com as variáveis substituídas
    """
    data_str = data.strftime('%Y-%m-%d')
    
    # Substitui DECLARE @DATAMES com qualquer data existente
    sql_modificado = re.sub(
        r"DECLARE\s+@DATAMES\s+DATE\s*=\s*'[0-9]{4}-[0-9]{2}-[0-9]{2}'",
        f"DECLARE @DATAMES DATE = '{data_str}'",
        sql_content,
        flags=re.IGNORECASE
    )
    
    # Substitui DECLARE @DATA com qualquer data existente
    sql_modificado = re.sub(
        r"DECLARE\s+@DATA\s+DATE\s*=\s*'[0-9]{4}-[0-9]{2}-[0-9]{2}'",
        f"DECLARE @DATA DATE = '{data_str}'",
        sql_modificado,
        flags=re.IGNORECASE
    )
    
    return sql_modificado

def conectar_banco_dados(server, database, username, password, driver='SQL Server'):
    """
    Estabelece conexão com o banco de dados SQL Server.
    
    Args:
        server (str): Nome ou IP do servidor
        database (str): Nome do banco de dados
        username (str): Usuário do banco de dados
        password (str): Senha do usuário
        driver (str): Driver ODBC a ser utilizado
        
    Returns:
        pyodbc.Connection: Objeto de conexão
    """
    # String de conexão com usuário e senha
    connection_string = (
        f'DRIVER={{{driver}}};'
        f'SERVER={server};'
        f'DATABASE={database};'
        f'UID={username};'
        f'PWD={password};'
    )
    
    try:
        conn = pyodbc.connect(connection_string)
        print(f"✓ Conexão estabelecida com sucesso: {server}/{database}")
        return conn
    except Exception as e:
        print(f"✗ Erro ao conectar ao banco de dados: {e}")
        raise

def executar_consulta_multiplos_resultados(conn, sql):
    """
    Executa consulta SQL que retorna múltiplos resultsets.
    
    Args:
        conn (pyodbc.Connection): Conexão com o banco
        sql (str): Script SQL a ser executado
        
    Returns:
        list: Lista de DataFrames, um para cada resultset
    """
    cursor = conn.cursor()
    cursor.execute(sql)
    
    resultados = []
    
    # Lê o primeiro resultado
    if cursor.description:
        df = pd.DataFrame.from_records(
            cursor.fetchall(),
            columns=[desc[0] for desc in cursor.description]
        )
        resultados.append(df)
    
    # Lê os resultados subsequentes
    while cursor.nextset():
        if cursor.description:
            df = pd.DataFrame.from_records(
                cursor.fetchall(),
                columns=[desc[0] for desc in cursor.description]
            )
            resultados.append(df)
    
    cursor.close()
    return resultados


def obter_dias_mes(ano, mes):
    """
    Gera lista com todos os dias de um determinado mês.
    
    Args:
        ano (int): Ano
        mes (int): Mês (1-12)
        
    Returns:
        list: Lista de objetos datetime.date
    """
    primeiro_dia = datetime(ano, mes, 1).date()
    
    # Determina o último dia do mês
    if mes == 12:
        ultimo_dia = datetime(ano + 1, 1, 1).date() - timedelta(days=1)
    else:
        ultimo_dia = datetime(ano, mes + 1, 1).date() - timedelta(days=1)
    
    # Gera lista de datas
    dias = []
    data_atual = primeiro_dia
    while data_atual <= ultimo_dia:
        dias.append(data_atual)
        data_atual += timedelta(days=1)
    
    return dias


def processar_mes_completo(conn, caminho_sql, ano, mes):
    """
    Processa a consulta para todos os dias de um mês e empilha os resultados.
    
    Args:
        conn (pyodbc.Connection): Conexão com o banco
        caminho_sql (str): Caminho do arquivo SQL
        ano (int): Ano a processar
        mes (int): Mês a processar (1-12)
        
    Returns:
        tuple: (df_daily, df_volumetria, df_abandonos, df_tma_mes)
    """
    # Lê o template SQL
    sql_template = ler_arquivo_sql(caminho_sql)
    
    # Inicializa listas para empilhar os resultados
    resultados_daily = []
    resultados_volumetria = []
    resultados_abandonos = []
    resultados_tma_mes = []
    
    # Obtém todos os dias do mês
    dias_mes = obter_dias_mes(ano, mes)
    total_dias = len(dias_mes)
    
    print(f"\n{'='*60}")
    print(f"Processando {total_dias} dias de {mes:02d}/{ano}")
    print(f"{'='*60}\n")
    
    for idx, data in enumerate(dias_mes, 1):
        print(f"[{idx}/{total_dias}] Processando {data.strftime('%d/%m/%Y')}...", end=' ')
        
        try:
            # Substitui as variáveis no SQL
            sql_executar = substituir_variaveis_sql(sql_template, data)
            
            # Executa a consulta
            resultados = executar_consulta_multiplos_resultados(conn, sql_executar)
            
            # Verifica se obteve os 4 resultsets esperados
            if len(resultados) != 4:
                print(f"⚠ AVISO: Esperava 4 resultados, obteve {len(resultados)}")
            
            # Empilha os resultados
            if len(resultados) >= 1:
                resultados_daily.append(resultados[0])
            if len(resultados) >= 2:
                resultados_volumetria.append(resultados[1])
            if len(resultados) >= 3:
                resultados_abandonos.append(resultados[2])
            if len(resultados) >= 4:
                resultados_tma_mes.append(resultados[3])
            
            print("✓")
            
        except Exception as e:
            print(f"✗ ERRO: {e}")
            continue
    
    # Concatena todos os resultados em DataFrames únicos
    print("\nConsolidando resultados...")
    
    df_daily = pd.concat(resultados_daily, ignore_index=True) if resultados_daily else pd.DataFrame()
    df_volumetria = pd.concat(resultados_volumetria, ignore_index=True) if resultados_volumetria else pd.DataFrame()
    df_abandonos = pd.concat(resultados_abandonos, ignore_index=True) if resultados_abandonos else pd.DataFrame()
    df_tma_mes = pd.concat(resultados_tma_mes, ignore_index=True) if resultados_tma_mes else pd.DataFrame()
    
    print(f"✓ Daily: {len(df_daily)} registros")
    print(f"✓ Volumetria: {len(df_volumetria)} registros")
    print(f"✓ Abandonos: {len(df_abandonos)} registros")
    print(f"✓ TMA Mês: {len(df_tma_mes)} registros")
    
    return df_daily, df_volumetria, df_abandonos, df_tma_mes


def salvar_excel(df_daily, df_volumetria, df_abandonos, df_tma_mes, arquivo_saida):
    """
    Salva os DataFrames em arquivo Excel com múltiplas abas.
    
    Args:
        df_daily (pd.DataFrame): DataFrame com dados daily
        df_volumetria (pd.DataFrame): DataFrame com volumetria
        df_abandonos (pd.DataFrame): DataFrame com abandonos
        df_tma_mes (pd.DataFrame): DataFrame com TMA mês
        arquivo_saida (str): Caminho do arquivo Excel de saída
    """
    print(f"\nSalvando arquivo Excel: {arquivo_saida}")
    
    with pd.ExcelWriter(arquivo_saida, engine='openpyxl') as writer:
        df_daily.to_excel(writer, sheet_name='df_daily', index=False)
        df_volumetria.to_excel(writer, sheet_name='df_volumetria', index=False)
        df_abandonos.to_excel(writer, sheet_name='df_abandonos', index=False)
        df_tma_mes.to_excel(writer, sheet_name='df_tma_mes', index=False)
    
    print(f"✓ Arquivo salvo com sucesso!")
    print(f"  Localização: {arquivo_saida}")


def main():
    """
    Função principal do script.
    """
    # ========== CONFIGURAÇÕES ==========
    # Ajuste estas variáveis conforme seu ambiente
    
    SERVER = 'trc-dc-bd2'  # Ex: 'localhost' ou 'servidor.dominio.com'
    DATABASE = 'Vonix'   # Ex: 'HOP_Database'
    USERNAME = 'Vonix'   # Ex: 'HOP_Database'
    PASSWORD = 'VnX@20241'   # Ex: 'HOP_Database'
    CAMINHO_SQL = r'\\trc-dc-ad\Planejamento\MIS\CARTEIRAS\Hop\HOP_DAILY.sql'
    
    # Mês e ano a processar
    ANO = 2026
    MES = 1  # Janeiro
    
    # ===================================
    
    try:
        # Conecta ao banco de dados
        conn = conectar_banco_dados(SERVER, DATABASE, USERNAME, PASSWORD)
        
        # Processa todos os dias do mês
        df_daily, df_volumetria, df_abandonos, df_tma_mes = processar_mes_completo(
            conn, CAMINHO_SQL, ANO, MES
        )
        
        # Define o nome do arquivo de saída (no mesmo diretório do script)
        diretorio_script = Path(__file__).parent
        arquivo_saida = diretorio_script / f'HOP_DAILY_{ANO}_{MES:02d}.xlsx'
        
        # Salva os resultados em Excel
        salvar_excel(df_daily, df_volumetria, df_abandonos, df_tma_mes, arquivo_saida)
        
        # Fecha a conexão
        conn.close()
        print("\n✓ Processamento concluído com sucesso!")
        
    except Exception as e:
        print(f"\n✗ Erro durante a execução: {e}")
        raise


if __name__ == "__main__":
    main()