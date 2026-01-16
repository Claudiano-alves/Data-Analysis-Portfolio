import pandas as pd
from dotenv import load_dotenv
from db_connection import get_connection
from datetime import datetime, timedelta
from glob import glob
import os
import sys
import re
import logging
from openpyxl import load_workbook
from excel_utils import xlsx_file, identificar_maior_data_xlsx

conn_bd2 = get_connection("TRC-DC-BD2", "PLANEJAMENTO")
sql_file = r"\\trc-dc-ad\Planejamento\MIS\CARTEIRAS\PayJoy\Daily_PayJoy_prod.sql"

# Logging configuration: use INFO by default; set PAYJOY_DEBUG=1 to see debug messages
log_level = logging.DEBUG if os.getenv('PAYJOY_DEBUG', '0') == '1' else logging.INFO
logging.basicConfig(level=log_level, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


def processa_payjoy():
    print("="*80)
    print("INICIANDO PROCESSAMENTO PayJoy")
    print("="*80)
    from data_daily import calcular_data_inicio
    # 1. Identifica a maior data no arquivo Excel
    maior_data_xlsx = identificar_maior_data_xlsx(xlsx_file)

    # 2. Calcula data de início (dia seguinte à última data do arquivo)
    data_inicio = calcular_data_inicio(maior_data_xlsx)

    # 3. Verifica se há dados para processar
    ontem = datetime.now().date() - timedelta(days=1)

    if data_inicio > ontem:
        print(f"\n{'='*80}")
        print("Relatório já está atualizado! Nenhuma data para processar.")
        print(f"{'='*80}")
        df_resultado = pd.DataFrame()
    else:
        print(f"\nPeríodo a processar: de {data_inicio.strftime('%Y-%m-%d')} até {ontem.strftime('%Y-%m-%d')}")
        
        # 4. Executa a consulta
        df_resultado = df_consulta(conn_bd2, sql_file, data_inicio)
        
        print(f"\n{'='*80}")
        print("CONSULTA EXECUTADA COM SUCESSO!")
        print(f"{'='*80}")
        print(f"Total de linhas retornadas: {len(df_resultado)}")
        print(f"Colunas: {df_resultado.columns.tolist()}")
        print(f"{'='*80}")
        
        if len(df_resultado) > 0:
            print("\nPrimeiras 10 linhas do resultado:")
            print(df_resultado.head(10))
            
            print("\nDatas únicas no resultado:")
            if 'DATA' in df_resultado.columns:
                datas_unicas = sorted(df_resultado['DATA'].unique())
                print(datas_unicas)

    conn_bd2.close()
    print("\nConexão fechada.")
    print(f"\nDataFrame 'df_resultado' disponível com {len(df_resultado)} linhas")
    return df_resultado

def df_contratos_payjoy():
    """
    Busca os contratos PayJoy com suas respectivas faixas (DPD_BUCKET)
    
    Returns:
        DataFrame com colunas: Assigned_Portfolio, FAIXA
    """
    # Cria a conexão
    conn_bd2 = get_connection("TRC-DC-BD2", "PLANEJAMENTO")
    
    # Query SQL
    query = """
    SELECT
        Assigned_Portfolio
    ,   DPD_BUCKET as FAIXA
    FROM OPENQUERY([TRC_BD_LINKED],
    '
        SELECT 
            ltrim(rtrim(contrato_fin)) Assigned_Portfolio
        ,   DPD_BUCKET
        ,   cast(CALENDAR_DATE as date) dt_base 
        FROM SRC..AUX_PAYJOY_REMESSA 
        WHERE CALENDAR_DATE = (SELECT MAX(CALENDAR_DATE) FROM SRC..AUX_PAYJOY_REMESSA)
    '
    )
    """
    
    try:
        # Executa a query e retorna o DataFrame
        df = pd.read_sql(query, conn_bd2)
        
        logger.info(f"Total de contratos: {len(df)}")
        logger.info(f"Faixas disponíveis: {df['FAIXA'].unique()}")
        
        return df
        
    except Exception as e:
        print(f"Erro ao executar query: {e}")
        raise
    finally:
        conn_bd2.close()

def df_consulta(conn_bd2, sql_file, data_inicio):
    """
    Executa a consulta substituindo a variável @dataIni no arquivo SQL.
    
    Parâmetros:
    -----------
    conn_bd2 : pyodbc.Connection
        Conexão com o banco de dados
    sql_file : str
        Caminho do arquivo SQL
    data_inicio : datetime.date
        Data a ser substituída na variável @dataIni
    
    Retorna:
    --------
    pd.DataFrame
        DataFrame com os resultados da consulta
    """
    # Lê o conteúdo da query
    with open(sql_file, "r", encoding="utf-8") as f:
        query_original = f.read()
    
    # Formata a data no formato YYYY-MM-DD
    data_str = data_inicio.strftime('%Y-%m-%d')
    
    logger.debug(f"[SUBSTITUIÇÃO] Buscando padrão '@dataIni' no SQL...")
    logger.debug(f"[SUBSTITUIÇÃO] Formatando data: {data_str}")
    
    # Usa regex para encontrar e substituir de forma robusta
    # Padrão: declare @dataIni as date = 'YYYY-MM-DD' (com variações de espaço)
    padrao = r"declare\s+@dataIni\s+as\s+date\s*=\s*'[^']*'"
    
    # Verificar se encontrou o padrão
    if re.search(padrao, query_original, re.IGNORECASE):
        logger.info("[✓] Padrão encontrado no SQL")
        substituicao = f"declare @dataIni as date = '{data_str}'"
        query_modificada = re.sub(padrao, substituicao, query_original, flags=re.IGNORECASE)
        
        # Log das mudanças (debug)
        linhas_original = query_original.split('\n')
        linhas_modificada = query_modificada.split('\n')
        
        for i, (orig, mod) in enumerate(zip(linhas_original[:50], linhas_modificada[:50])):
            if orig != mod:
                logger.debug(f"[LINHA {i+1}] Original: {orig.strip()}")
                logger.debug(f"[LINHA {i+1}] Modificada: {mod.strip()}")
    else:
        logger.warning("[✗] Padrão '@dataIni' NÃO encontrado no SQL! A consulta será executada sem modificação da data")
        query_modificada = query_original
    
    cursor = conn_bd2.cursor()
    
    try:
        print(f"\n[EXECUÇÃO] Executando consulta com @dataIni = '{data_str}'...")
        cursor.execute(query_modificada)
        
        # Navega pelos resultados até encontrar o último SELECT
        columns = None
        data = None
        
        if cursor.description:
            columns = [column[0] for column in cursor.description]
            data = cursor.fetchall()
            print(f"[✓] Primeiro result set encontrado: {len(data)} linhas")
        
        result_set_count = 1
        while cursor.nextset():
            if cursor.description:
                result_set_count += 1
                columns = [column[0] for column in cursor.description]
                data = cursor.fetchall()
                print(f"[✓] Result set #{result_set_count} encontrado: {len(data)} linhas")
        
        if columns is None or data is None:
            raise ValueError("Nenhum resultado encontrado na query")
        
        df = pd.DataFrame.from_records(data, columns=columns)
        print(f"\n[✓] Query retornou {len(df)} linhas com {len(columns)} colunas")
        
        return df
        
    except Exception as e:
        print(f"\n[✗] Erro ao executar query: {e}")
        import traceback
        traceback.print_exc()
        raise
    finally:
        cursor.close()

def consolidar_mailings_payjoy(caminho_pasta=r"\\trc-dc-ad\Planejamento\00 - USUÁRIOS\0003_Daniel Kodama\PAYJOY\mailings payjoy"):
    """
    Consolida todos os arquivos CSV de mailings PayJoy em um único DataFrame
    """
    
    arquivos = glob(os.path.join(caminho_pasta, "*.csv"))
    
    if not arquivos:
        logger.info(f"Nenhum arquivo CSV encontrado em: {caminho_pasta}")
        return pd.DataFrame()
    
    logger.info(f"Encontrados {len(arquivos)} arquivos CSV")
    
    lista_dfs = []
    arquivos_com_erro = []
    
    for arquivo in arquivos:
        try:
            nome_arquivo = os.path.basename(arquivo)
            nome_sem_extensao = nome_arquivo.replace('.csv', '')
            
            # Busca padrão de data no nome: DD.MM ou DD/MM ou DD_MM
            # Trabalha com separadores: . / _
            match = re.search(r'(\d{1,2})[./\_](\d{1,2})', nome_sem_extensao)
            
            if match:
                dia = match.group(1).zfill(2)  # Adiciona zero à esquerda se necessário
                mes = match.group(2).zfill(2)  # Adiciona zero à esquerda se necessário
                data_str = f"{dia}/{mes}"
                logger.debug(f"  Data extraída: {match.group(0)} -> {data_str}")
            else:
                # Se não encontrar data, usa os últimos 4 caracteres (método antigo)
                ultimos_4_chars = nome_sem_extensao[-4:]
                data_str = ultimos_4_chars.replace('.', '/').replace('_', '/')
                logger.warning(f"  ⚠ Padrão DD/MM não encontrado em '{nome_sem_extensao}', usando fallback: {data_str}")
            
            # Tenta identificar se o arquivo traz 'CHAVE_POPUP' ou 'CONTRATO' e lê apenas essa coluna
            df_temp = None
            col_found = None
            sep_used = None
            enc_used = None

            def _normalize_col(c):
                return re.sub(r'[^A-Z0-9]', '', str(c).upper())

            for sep in [';', ',', '\t']:
                for enc in ['latin-1', 'utf-8', 'cp1252']:
                    try:
                        # lê apenas header para detectar colunas
                        df_try = pd.read_csv(arquivo, nrows=0, encoding=enc, sep=sep)
                        cols = [_normalize_col(c) for c in df_try.columns.tolist()]

                        if 'CHAVEPOPUP' in cols:
                            col_found = df_try.columns[cols.index('CHAVEPOPUP')]
                        elif 'CONTRATO' in cols:
                            col_found = df_try.columns[cols.index('CONTRATO')]

                        if col_found is not None:
                            sep_used = sep
                            enc_used = enc
                            break
                    except Exception:
                        continue
                if col_found is not None:
                    break

            if col_found is None:
                raise Exception("Não foi possível ler o arquivo com as colunas esperadas (CHAVE_POPUP ou CONTRATO)")

            # Lê apenas a coluna identificada
            try:
                df_temp = pd.read_csv(arquivo, usecols=[col_found], encoding=enc_used, sep=sep_used)
            except Exception as e:
                raise Exception(f"Falha ao ler coluna '{col_found}' do arquivo: {e}")

            # Normaliza para coluna padrão 'CONTRATO'
            if col_found != 'CONTRATO':
                df_temp = df_temp.rename(columns={col_found: 'CONTRATO'})

            df_temp = df_temp[['CONTRATO']].copy()
            df_temp['DATA'] = data_str
            lista_dfs.append(df_temp)

            logger.debug(f"✓ {nome_arquivo} - {len(df_temp)} registros - Data: {data_str} - Col: {col_found}")
            
        except Exception as e:
            print(f"✗ ERRO em {nome_arquivo}: {e}")
            arquivos_com_erro.append((nome_arquivo, str(e)))
            continue
    
    if arquivos_com_erro:
        logger.warning('\n' + '='*60)
        logger.warning('ARQUIVOS COM ERRO:')
        for nome, erro in arquivos_com_erro:
            logger.warning(f"  - {nome}: {erro}")
        logger.warning('='*60)
    
    if lista_dfs:
        df_consolidado = pd.concat(lista_dfs, ignore_index=True)
        
        print(f"\n{'='*60}")
        print(f"Total de registros consolidados: {len(df_consolidado)}")
        print(f"Datas únicas: {sorted(df_consolidado['DATA'].unique())}")
        if 'CONTRATO' in df_consolidado.columns:
            unique_count = df_consolidado['CONTRATO'].nunique()
        else:
            unique_count = df_consolidado.iloc[:,0].nunique()

        print(f"Total de contratos únicos: {unique_count}")
        print(f"{'='*60}")

        return df_consolidado
    else:
        print("Nenhum dado foi consolidado.")
        return pd.DataFrame()