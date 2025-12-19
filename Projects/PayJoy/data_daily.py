import pandas as pd
from dotenv import load_dotenv
from db_connection import get_connection
from datetime import datetime, timedelta
import os
import sys
import re


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
        print(f"Arquivo Excel lido com sucesso!")
        
        colunas = df_temp.columns.tolist()
        print(f"\nColunas encontradas no arquivo: {colunas}")
        
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
                    print(f"\nMaior data encontrada: {ultima_data.strftime('%Y-%m-%d')} (coluna já era datetime)")
                    return ultima_data
                
                col_limpo = str(col).strip()
                
                for formato in ['%Y-%m-%d', '%d/%m/%Y', '%Y/%m/%d', '%d-%m-%Y']:
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
    
    print(f"\n[SUBSTITUIÇÃO] Buscando padrão '@dataIni' no SQL...")
    print(f"[SUBSTITUIÇÃO] Formatando data: {data_str}")
    
    # Usa regex para encontrar e substituir de forma robusta
    # Padrão: declare @dataIni as date = 'YYYY-MM-DD' (com variações de espaço)
    padrao = r"declare\s+@dataIni\s+as\s+date\s*=\s*'[^']*'"
    
    # Verificar se encontrou o padrão
    if re.search(padrao, query_original, re.IGNORECASE):
        print(f"[✓] Padrão encontrado no SQL")
        substituicao = f"declare @dataIni as date = '{data_str}'"
        query_modificada = re.sub(padrao, substituicao, query_original, flags=re.IGNORECASE)
        
        # Log das mudanças
        linhas_original = query_original.split('\n')
        linhas_modificada = query_modificada.split('\n')
        
        for i, (orig, mod) in enumerate(zip(linhas_original[:50], linhas_modificada[:50])):
            if orig != mod:
                print(f"\n[LINHA {i+1}] Original:")
                print(f"  {orig.strip()}")
                print(f"[LINHA {i+1}] Modificada:")
                print(f"  {mod.strip()}")
    else:
        print(f"[✗] AVISO: Padrão '@dataIni' NÃO encontrado no SQL!")
        print(f"[✗] A consulta será executada sem modificação da data")
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


def processar_payjoy():
    """
    Função principal que executa todo o processamento PayJoy.
    
    Retorna:
    --------
    pd.DataFrame
        DataFrame com os resultados da consulta (vazio se não houver dados para processar)
    """
    # Garante que as variáveis de ambiente estejam carregadas
    load_dotenv()
    
    # Configurações de arquivos
    sql_file = r"\\trc-dc-ad\Planejamento\MIS\CARTEIRAS\PayJoy\Daily_PayJoy.sql"
    xlsx_file = r"\\trc-dc-ad\Planejamento\MIS\CARTEIRAS\PayJoy\base_PAYJOY.xlsx"
    
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
        return pd.DataFrame()
    
    print(f"\nPeríodo a processar: de {data_inicio.strftime('%Y-%m-%d')} até {ontem.strftime('%Y-%m-%d')}")
    
    # 4. Estabelece conexão com o banco
    conn_bd2 = get_connection("SERVER_BD2", "DATABASE_BD2")
    
    try:
        # 5. Executa a consulta
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
        
        return df_resultado
        
    finally:
        conn_bd2.close()
        print("\nConexão fechada.")


def main():
    """
    Ponto de entrada principal do script.
    """
    df_resultado = processar_payjoy()
    print(f"\nDataFrame 'df_resultado' disponível com {len(df_resultado)} linhas")
    return df_resultado


if __name__ == "__main__":
    df_resultado = main()