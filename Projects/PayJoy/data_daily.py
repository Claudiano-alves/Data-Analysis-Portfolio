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

conn_bd2 = get_connection("TRC-DC-BD2", "PLANEJAMENTO")
sql_file = r"\\trc-dc-ad\Planejamento\MIS\CARTEIRAS\PayJoy\Daily_PayJoy_prod.sql"
xlsx_file = r"\\trc-dc-ad\Planejamento\MIS\CARTEIRAS\PayJoy\base_PAYJOY.xlsx"

# Logging configuration: use INFO by default; set PAYJOY_DEBUG=1 to see debug messages
log_level = logging.DEBUG if os.getenv('PAYJOY_DEBUG', '0') == '1' else logging.INFO
logging.basicConfig(level=log_level, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def identificar_maior_data_xlsx(xlsx_path):
    """
    Identifica a maior data presente nas colunas do arquivo Excel.
    Encerra a execução se o arquivo não existir.
    """
    if not os.path.exists(xlsx_path):
        print(f"\n{'='*80}")
        print(f"ERRO CRÍTICO: Arquivo não encontrado!")
        print(f"Caminho: {xlsx_path}")
        print(f"{'='*80}")
        print("\nEncerrando execução...")
        sys.exit(1)
    
    try:
        df_temp = pd.read_excel(xlsx_path, nrows=5)
        logger.info("Arquivo Excel lido com sucesso")
        
        colunas = df_temp.columns.tolist()
        logger.debug(f"Colunas encontradas no arquivo: {colunas}")
        
        ultima_data = None
        
        for col in reversed(colunas):
            if col in ['FAIXA', 'Indicador']:
                continue
            
            try:
                if isinstance(col, (pd.Timestamp, datetime)):
                    if isinstance(col, pd.Timestamp):
                        ultima_data = col.to_pydatetime()
                    else:
                        ultima_data = col
                    logger.info(f"Maior data encontrada: {ultima_data.strftime('%Y-%m-%d')} (coluna já era datetime)")
                    try:
                        data = datetime.strptime(col_limpo, formato)
                        ultima_data = data
                        print(f"\nMaior data encontrada: {ultima_data.strftime('%Y-%m-%d')} (coluna: '{col}')")
                        return ultima_data
                    except:
                        continue
            except Exception as e:
                continue
        
        if ultima_data is None:
            print("\n" + "="*80)
            print("ERRO: Nenhuma data válida encontrada nas colunas do arquivo!")
            print("Colunas analisadas:", [col for col in colunas if col not in ['FAIXA', 'Indicador']])
            print("="*80)
            print("\nEncerrando execução...")
            sys.exit(1)
        
        return ultima_data
            
    except Exception as e:
        print(f"\n{'='*80}")
        print(f"ERRO ao ler arquivo Excel: {e}")
        print(f"{'='*80}")
        import traceback
        traceback.print_exc()
        print("\nEncerrando execução...")
        sys.exit(1)

def calcular_data_inicio(maior_data_xlsx):
    """
    Calcula a data de início para a consulta (dia seguinte à última data do arquivo).
    """
    if maior_data_xlsx is None:
        print(f"\n{'='*80}")
        print("ERRO: Não foi possível identificar a data inicial!")
        print(f"{'='*80}")
        sys.exit(1)
    
    data_inicio = maior_data_xlsx.date() + timedelta(days=1)
    print(f"\nData início para consulta: {data_inicio.strftime('%Y-%m-%d')}")
    return data_inicio

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
            
            # Busca padrão de data no nome: DD.MM ou DD/MM
            match = re.search(r'(\d{1,2})[./](\d{2})', nome_sem_extensao)
            
            if match:
                dia = match.group(1).zfill(2)  # Adiciona zero à esquerda se necessário
                mes = match.group(2)
                data_str = f"{dia}/{mes}"
            else:
                # Se não encontrar data, usa os últimos 4 caracteres (método antigo)
                ultimos_4_chars = nome_sem_extensao[-4:]
                data_str = ultimos_4_chars.replace('.', '/')
            
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

            # Normaliza para coluna padrão 'CHAVE_POPUP'
            if col_found != 'CHAVE_POPUP':
                df_temp = df_temp.rename(columns={col_found: 'CHAVE_POPUP'})

            df_temp = df_temp[['CHAVE_POPUP']].copy()
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
        if 'CHAVE_POPUP' in df_consolidado.columns:
            unique_count = df_consolidado['CHAVE_POPUP'].nunique()
        elif 'CONTRATO' in df_consolidado.columns:
            unique_count = df_consolidado['CONTRATO'].nunique()
        else:
            unique_count = df_consolidado.iloc[:,0].nunique()

        print(f"Total de contratos únicos: {unique_count}")
        print(f"{'='*60}")

        # Normaliza sempre para 'CHAVE_POPUP' antes de retornar
        if 'CHAVE_POPUP' not in df_consolidado.columns and 'CONTRATO' in df_consolidado.columns:
            df_consolidado = df_consolidado.rename(columns={'CONTRATO': 'CHAVE_POPUP'})

        return df_consolidado
    else:
        print("Nenhum dado foi consolidado.")
        return pd.DataFrame()

# ============================================================================
# EXECUÇÃO PRINCIPAL
# ============================================================================

def processa_payjoy():
    print("="*80)
    print("INICIANDO PROCESSAMENTO PayJoy")
    print("="*80)

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

def transforma_dados(df_resultado):
    if df_resultado.empty:
        print("\nNenhum dado para transformar.")
        return df_resultado

    print(f"\n{'='*60}")
    print("INICIANDO TRANSFORMAÇÃO DOS DADOS")
    print(f"{'='*60}")
    # Verifica quantas datas únicas existem
    datas_unicas = df_resultado['DATA'].unique()
    print(f"Datas encontradas: {datas_unicas}")
    print(f"Total de datas: {len(datas_unicas)}")

    # Define automaticamente as colunas de indicadores
    # Todas as colunas EXCETO 'DATA' e 'FAIXA'
    colunas_todas = df_resultado.columns.tolist()
    colunas_id = ['DATA', 'FAIXA']  # Colunas identificadoras
    value_columns = [col for col in colunas_todas if col not in colunas_id]

    print(f"\nColunas identificadoras: {colunas_id}")
    print(f"Total de indicadores encontrados: {len(value_columns)}")

    # Faz o unpivot (melt) mantendo DATA e FAIXA como identificadores
    df_transposto = df_resultado.melt(
        id_vars=['DATA', 'FAIXA'],
        value_vars=value_columns,
        var_name='Indicador',
        value_name='Valor'
    )

    print(f"\nDataFrame após melt:")
    print(df_transposto.head(10))

    # Pivota para colocar as datas como colunas
    df_final = df_transposto.pivot_table(
        index=['FAIXA', 'Indicador'],
        columns='DATA',
        values='Valor',
        aggfunc='first'  # Usa o primeiro valor caso haja duplicatas
    ).reset_index()

    # Remove o nome do índice das colunas (fica mais limpo)
    df_final.columns.name = None

    # Ordena por FAIXA e Indicador
    df_final = df_final.sort_values(['FAIXA', 'Indicador']).reset_index(drop=True)

    # Visualiza o resultado
    logger.info('\n' + '='*60)
    logger.info('Estrutura final:')
    logger.info(f"Total de linhas: {len(df_final)}")
    logger.info(f"Faixas únicas: {df_final['FAIXA'].unique()}")
    logger.debug(f"Colunas: {df_final.columns.tolist()}")
    logger.info('='*60)
    logger.debug('Primeiras 30 linhas:')
    logger.debug('\n' + df_final.head(30).to_string())

    return df_final

def consolidar_mailings_por_faixa(df_mailings, df_contratos, ordenar_datas=True, contratos_unicos=True):
    """
    Cruza os mailings com os contratos para obter faixas e gera tabela pivotada
    """
    
    # NORMALIZAÇÃO: aceita tanto 'CONTRATO' quanto 'CHAVE_POPUP' como coluna de chave
    if 'CONTRATO' in df_mailings.columns:
        mail_col = 'CONTRATO'
    elif 'CHAVE_POPUP' in df_mailings.columns:
        mail_col = 'CHAVE_POPUP'
    else:
        # Pega a primeira coluna disponível como último recurso
        mail_col = df_mailings.columns[0]
        logger.warning(f"Nenhuma coluna 'CONTRATO' ou 'CHAVE_POPUP' encontrada em df_mailings. Usando '{mail_col}' como chave.")

    # LIMPEZA: Remove espaços em branco das colunas de chave
    df_mailings[mail_col] = df_mailings[mail_col].astype(str).str.strip()
    df_contratos['Assigned_Portfolio'] = df_contratos['Assigned_Portfolio'].astype(str).str.strip()

    # Cruzamento usando a coluna detectada
    df_mailings_com_faixa = df_mailings.merge(
        df_contratos,
        left_on=mail_col,
        right_on='Assigned_Portfolio',
        how='left'
    )

    # Substitui FAIXA vazia por 'SEM_FAIXA' para evitar perda de linhas durante agrupamento
    df_mailings_com_faixa['FAIXA'] = df_mailings_com_faixa['FAIXA'].fillna('SEM_FAIXA')

    # Contagem
    if contratos_unicos:
        df_contagem = df_mailings_com_faixa.groupby(['FAIXA', 'DATA'])[mail_col].nunique().reset_index()
    else:
        df_contagem = df_mailings_com_faixa.groupby(['FAIXA', 'DATA'])[mail_col].count().reset_index()

    df_contagem.columns = ['FAIXA', 'DATA', 'CONTAGEM']
    
    # Pivota
    df_pivot = df_contagem.pivot(
        index='FAIXA',
        columns='DATA',
        values='CONTAGEM'
    ).fillna(0).astype(int)
    
    # Ordena datas se solicitado
    if ordenar_datas:
        def ordenar_data(data_str):
            """Converte DD/MM para tupla (dia, mes) para ordenação"""
            try:
                dia, mes = data_str.split('/')
                return (int(mes), int(dia))
            except:
                return (99, 99)
        
        colunas_ordenadas = sorted(df_pivot.columns, key=ordenar_data)
        df_pivot = df_pivot[colunas_ordenadas]
    
    # Reset index
    df_pivot = df_pivot.reset_index()
    
    return df_pivot

def substituir_zeros_com_pivot(df_transposto, df_pivot):
    """
    Substitui valores 0 do df_transposto pelos valores do df_pivot,
    respeitando FAIXA + data.
    
    Args:
        df_transposto: DataFrame com estrutura [FAIXA, Indicador, 2025-12-04, 2025-12-05, ...]
        df_pivot: DataFrame com estrutura [FAIXA, 3/12, 4/12, 5/12, ...]
    
    Returns:
        DataFrame atualizado
    """
    
    df_resultado = df_transposto.copy()
    
    # Pegar as colunas de data do df_transposto
    colunas_data = [col for col in df_transposto.columns if col not in ['FAIXA', 'Indicador']]
    
    # Para cada linha do df_pivot
    for _, row_pivot in df_pivot.iterrows():
        faixa = row_pivot["FAIXA"]
        
        # Para cada coluna de data no df_transposto
        for col_transposto in colunas_data:
            # Converter coluna do df_transposto para formato do df_pivot
            if isinstance(col_transposto, str) and '-' in col_transposto:
                partes = col_transposto.split('-')
                dia = partes[2].lstrip('0') or '0'
                mes = partes[1].lstrip('0') or '0'
                col_pivot_format = f"{dia}/{mes}"
            elif hasattr(col_transposto, 'day') and hasattr(col_transposto, 'month'):
                dia = str(col_transposto.day)
                mes = str(col_transposto.month)
                col_pivot_format = f"{dia}/{mes}"
            else:
                continue
            
            # Se essa coluna existe no df_pivot
            if col_pivot_format in df_pivot.columns:
                valor_pivot = row_pivot[col_pivot_format]
                
                # Substituir os zeros APENAS para o indicador Reachable_Portfolio
                mask = (
                    (df_resultado["FAIXA"] == faixa) & 
                    (df_resultado["Indicador"] == "Reachable_Portfolio") &
                    (df_resultado[col_transposto] == 0)
                )
                df_resultado.loc[mask, col_transposto] = valor_pivot
    
    return df_resultado

def atualizar_arquivo_excel_por_df(xlsx_file, df_payjoy):
    """
    Atualiza o arquivo Excel (`xlsx_file`) com as colunas de data presentes em
    `df_payjoy` (formato wide: FAIXA | Indicador | 16/12 | 17/12 | ...).

    Regras:
      - Só adiciona datas **após** a última data já presente no arquivo.
      - Usa o ano da última data existente no arquivo para interpretar colunas do
        df_payjoy no formato 'DD/MM'.

    Retorna um dicionário resumo: {'updated': bool, 'colunas_adicionadas': [...], 'dados_inseridos': int}
    """
    if df_payjoy is None or df_payjoy.empty:
        logger.info("Nenhum dado em df_payjoy; nada a atualizar")
        return {'updated': False, 'reason': 'df_empty'}

    # Carrega arquivo e mapeia datas existentes
    try:
        wb = load_workbook(xlsx_file)
        ws = wb.active
        df_existente = pd.read_excel(xlsx_file)
    except Exception as e:
        logger.error(f"Erro ao abrir arquivo Excel: {e}")
        return {'updated': False, 'error': str(e)}

    colunas_fixas = ['FAIXA', 'Indicador']

    def _col_to_date_obj(col, default_year):
        if isinstance(col, (pd.Timestamp, datetime)):
            d = col.to_pydatetime() if isinstance(col, pd.Timestamp) else col
            return d.date()
        s = str(col).strip()
        for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d', '%d-%m-%Y']:
            try:
                return datetime.strptime(s, fmt).date()
            except:
                continue
        # suporte para 'DD/MM' sem ano -> usa default_year
        if '/' in s:
            try:
                dia, mes = s.split('/')[:2]
                return datetime(default_year, int(mes), int(dia)).date()
            except:
                pass
        return None

    # Existing dates
    existing_date_positions = {}
    colunas_existentes = df_existente.columns.tolist()
    for idx, col in enumerate(colunas_existentes, start=1):
        if col in colunas_fixas:
            continue
        date_obj = _col_to_date_obj(col, datetime.now().year)
        if date_obj:
            existing_date_positions[date_obj] = idx

    if existing_date_positions:
        last_existing_date = max(existing_date_positions.keys())
        logger.info(f"Última data existente no Excel: {last_existing_date.strftime('%Y-%m-%d')}")
    else:
        # Se não houver datas, usamos o ano atual como referência
        last_existing_date = datetime.now().date() - timedelta(days=1)
        logger.info("Nenhuma data existente encontrada no Excel; usando ano atual como referência")

    # Extrai datas do df_payjoy
    df_pay_dates = {}
    for col in df_payjoy.columns:
        if col in colunas_fixas:
            continue
        date_obj = _col_to_date_obj(col, last_existing_date.year)
        if date_obj:
            df_pay_dates[col] = date_obj
        else:
            logger.debug(f"Coluna do df_payjoy não reconhecida como data: {col}")

    # Determina quais datas são posteriores à última data do arquivo
    to_add = sorted([d for d in set(df_pay_dates.values()) if d > last_existing_date])

    if not to_add:
        logger.info("Nenhuma data nova após a última data do arquivo; nada a adicionar")
        return {'updated': False, 'colunas_adicionadas': [], 'dados_inseridos': 0}

    # Inserir colunas em posições corretas (ordenadas por data)
    logger.info(f"Adicionando {len(to_add)} novas datas ao arquivo: {[d.strftime('%Y-%m-%d') for d in to_add]}")
    # Reconstroi mapping para posições dinâmicas
    existing_positions = dict(existing_date_positions)
    colunas_existentes = df_existente.columns.tolist()

    for d in to_add:
        # encontra primeira existing date maior que d
        insert_before_idx = None
        for exist_date, pos in sorted(existing_positions.items(), key=lambda x: x[1]):
            if exist_date > d:
                insert_before_idx = pos
                break
        insert_idx = insert_before_idx if insert_before_idx is not None else len(colunas_existentes) + 1

        ws.insert_cols(insert_idx)
        ws.cell(row=1, column=insert_idx, value=datetime.combine(d, datetime.min.time()))
        logger.debug(f"Inserida coluna para {d.strftime('%Y-%m-%d')} na posição {insert_idx}")

        # atualizar positions
        updated = {}
        for exist_date, pos in existing_positions.items():
            updated[exist_date] = pos + 1 if pos >= insert_idx else pos
        existing_positions = updated
        existing_positions[d] = insert_idx
        colunas_existentes.insert(insert_idx - 1, d)

    # Reconstrói map date->colidx
    date_to_colidx = {date: idx for date, idx in existing_positions.items()}

    # Mapeamento FAIXA+Indicador -> linha
    faixa_indicador_para_linha = {}
    for idx in range(2, ws.max_row + 1):
        faixa = ws.cell(row=idx, column=1).value
        indicador = ws.cell(row=idx, column=2).value
        if faixa is not None and indicador is not None:
            chave = (str(faixa).strip(), str(indicador).strip())
            faixa_indicador_para_linha[chave] = idx

    # Insere valores
    dados_inseridos = 0
    for _, row in df_payjoy.iterrows():
        faixa = str(row['FAIXA']).strip()
        indicador = str(row['Indicador']).strip()
        chave = (faixa, indicador)
        if chave not in faixa_indicador_para_linha:
            continue
        linha_excel = faixa_indicador_para_linha[chave]
        for col_name, date_obj in df_pay_dates.items():
            if date_obj in date_to_colidx:
                col_idx = date_to_colidx[date_obj]
                valor = row[col_name]
                if pd.notna(valor):
                    ws.cell(row=linha_excel, column=col_idx, value=valor)
                    dados_inseridos += 1

    wb.save(xlsx_file)
    logger.info(f"Arquivo salvo com {len(to_add)} colunas adicionadas e {dados_inseridos} valores inseridos")

    return {'updated': True, 'colunas_adicionadas': [d.strftime('%Y-%m-%d') for d in to_add], 'dados_inseridos': dados_inseridos}


if __name__ == "__main__":
    # Executa o processamento principal   
    df_resultado = processa_payjoy()  
    df_final = transforma_dados(df_resultado)
    df_contratos = df_contratos_payjoy()
    df_mailings = consolidar_mailings_payjoy()
    df_pivot = consolidar_mailings_por_faixa(df_mailings, df_contratos)
    df_PAYJOY = substituir_zeros_com_pivot(df_final, df_pivot)

    # Atualiza o arquivo Excel com os novos dados a partir do df_PAYJOY
    resumo = atualizar_arquivo_excel_por_df(xlsx_file, df_PAYJOY)
    if resumo.get('updated'):
        logger.info(f"Atualização concluída: colunas adicionadas: {resumo.get('colunas_adicionadas')} - valores inseridos: {resumo.get('dados_inseridos')}")
    else:
        logger.info(f"Nenhuma atualização realizada. Motivo: {resumo.get('reason', resumo.get('error', 'sem alterações'))}")