import pyodbc
import pandas as pd
from datetime import datetime, timedelta
import calendar
import os
import re
from pathlib import Path

# ============================================================================
# CLASSE PARA GERENCIAR RELATÓRIO DE EXECUÇÃO
# ============================================================================

class RelatorioExecucao:
    """Gerencia o relatório de execução do script"""
    
    def __init__(self):
        self.registros = []
        self.inicio_execucao = datetime.now()
    
    def adicionar(self, data, status, registros=0, arquivo=None, mensagem=None):
        """
        Adiciona um registro ao relatório
        
        Args:
            data: Data processada
            status: 'SUCESSO', 'SEM_DADOS', 'ERRO'
            registros: Número de registros processados
            arquivo: Nome do arquivo gerado
            mensagem: Mensagem adicional
        """
        self.registros.append({
            'data': data,
            'status': status,
            'registros': registros,
            'arquivo': arquivo,
            'mensagem': mensagem
        })
    
    def exibir(self):
        """Exibe o relatório formatado"""
        fim_execucao = datetime.now()
        duracao = (fim_execucao - self.inicio_execucao).total_seconds()
        
        print("\n" + "="*100)
        print("📋 RELATÓRIO DE EXECUÇÃO")
        print("="*100)
        print(f"Início: {self.inicio_execucao.strftime('%d/%m/%Y %H:%M:%S')}")
        print(f"Fim: {fim_execucao.strftime('%d/%m/%Y %H:%M:%S')}")
        print(f"Duração: {duracao:.2f} segundos")
        print("="*100)
        
        if not self.registros:
            print("\n❌ Nenhuma data foi processada")
            return
        
        # Cabeçalho da tabela
        print(f"\n{'DATA':<15} {'STATUS':<15} {'REGISTROS':<12} {'ARQUIVO':<40} {'OBSERVAÇÃO'}")
        print("-"*100)
        
        # Linhas do relatório
        sucesso_total = 0
        sem_dados_total = 0
        erro_total = 0
        registros_total = 0
        
        for reg in self.registros:
            data_str = reg['data'].strftime('%d/%m/%Y')
            status = reg['status']
            registros = reg['registros']
            arquivo = reg['arquivo'] if reg['arquivo'] else '-'
            mensagem = reg['mensagem'] if reg['mensagem'] else ''
            
            # Emoji e cor baseado no status
            if status == 'SUCESSO':
                status_display = '✅ SUCESSO'
                sucesso_total += 1
                registros_total += registros
            elif status == 'SEM_DADOS':
                status_display = '⚠️  SEM DADOS'
                sem_dados_total += 1
            else:
                status_display = '❌ ERRO'
                erro_total += 1
            
            # Exibe apenas o nome do arquivo, não o caminho completo
            if arquivo != '-':
                arquivo = os.path.basename(arquivo)
            
            print(f"{data_str:<15} {status_display:<20} {registros:<12} {arquivo:<40} {mensagem}")
        
        # Resumo
        print("="*100)
        print(f"\n📊 RESUMO:")
        print(f"   ✅ Arquivos gerados com sucesso: {sucesso_total}")
        if registros_total > 0:
            print(f"      Total de registros: {registros_total:,}")
        if sem_dados_total > 0:
            print(f"   ⚠️  Datas sem dados (arquivo não criado): {sem_dados_total}")
        if erro_total > 0:
            print(f"   ❌ Erros: {erro_total}")
        print(f"   📁 Total de datas processadas: {len(self.registros)}")
        
        print("\n" + "="*100)

# ============================================================================
# FUNÇÕES AUXILIARES
# ============================================================================

def get_data_ontem():
    """Retorna a data de ontem (data alvo máxima para processamento)"""
    return datetime.now().date() - timedelta(days=1)

def extrair_data_do_arquivo(nome_arquivo):
    """
    Extrai a data do nome do arquivo no formato TEMPOS_OPERACIONAIS_YYYYMMDD_TRC.csv
    
    Args:
        nome_arquivo: Nome do arquivo
    
    Returns:
        datetime.date ou None se não conseguir extrair
    """
    padrao = r'TEMPOS_OPERACIONAIS_(\d{8})_TRCWO\.csv'
    match = re.search(padrao, nome_arquivo)
    
    if match:
        data_str = match.group(1)
        try:
            return datetime.strptime(data_str, '%Y%m%d').date()
        except ValueError:
            return None
    return None

def obter_ultima_data_processada(caminho_destino):
    """
    Verifica o diretório e retorna a última data processada com base nos arquivos existentes
    
    Args:
        caminho_destino: Caminho do diretório onde estão os arquivos
    
    Returns:
        datetime.date ou None se não houver arquivos
    """
    try:
        if not os.path.exists(caminho_destino):
            print(f"⚠️  Diretório não existe: {caminho_destino}")
            return None
        
        arquivos = [f for f in os.listdir(caminho_destino) if f.endswith('.csv')]
        
        if not arquivos:
            print("ℹ️  Nenhum arquivo CSV encontrado no diretório")
            return None
        
        datas_encontradas = []
        for arquivo in arquivos:
            data = extrair_data_do_arquivo(arquivo)
            if data:
                datas_encontradas.append(data)
        
        if not datas_encontradas:
            print("⚠️  Nenhum arquivo com padrão de data válido encontrado")
            return None
        
        ultima_data = max(datas_encontradas)
        print(f"📅 Última data processada encontrada: {ultima_data.strftime('%d/%m/%Y')}")
        print(f"   Total de arquivos no diretório: {len(arquivos)}")
        print(f"   Arquivos com padrão válido: {len(datas_encontradas)}")
        
        return ultima_data
        
    except Exception as e:
        print(f"✗ Erro ao verificar diretório: {e}")
        import traceback
        traceback.print_exc()
        return None

def gerar_lista_datas_pendentes(ultima_data_processada, data_ontem):
    """
    Gera lista de datas que precisam ser processadas (incluindo finais de semana para verificação)
    
    Args:
        ultima_data_processada: Última data já processada
        data_ontem: Data alvo (ontem)
    
    Returns:
        list: Lista de datas (datetime.date) a processar
    """
    datas_pendentes = []
    
    # Se não há data processada, processa apenas ontem
    if ultima_data_processada is None:
        datas_pendentes.append(data_ontem)
        return datas_pendentes
    
    # Começa do dia seguinte à última data processada
    data_atual = ultima_data_processada + timedelta(days=1)
    
    while data_atual <= data_ontem:
        # Adiciona TODAS as datas (incluindo finais de semana)
        # A verificação de dados será feita na consulta
        datas_pendentes.append(data_atual)
        data_atual += timedelta(days=1)
    
    return datas_pendentes

def get_primeiro_ultimo_dia_mes(data):
    """Retorna o primeiro e último dia do mês da data informada"""
    # Garante que estamos trabalhando com datetime
    if isinstance(data, datetime):
        data_dt = data
    else:
        data_dt = datetime.combine(data, datetime.min.time())
    
    primeiro_dia = data_dt.replace(day=1)
    ultimo_dia = data_dt.replace(day=calendar.monthrange(data_dt.year, data_dt.month)[1])
    return primeiro_dia, ultimo_dia

def carregar_e_atualizar_query(caminho_sql, data_exec):
    """
    Carrega o arquivo SQL e atualiza as variáveis de data usando regex
    """
    with open(caminho_sql, 'r', encoding='utf-8') as f:
        query = f.read()
    
    # Converte date para datetime se necessário
    if isinstance(data_exec, datetime):
        data_exec_dt = data_exec
    else:
        data_exec_dt = datetime.combine(data_exec, datetime.min.time())
    
    primeiro_dia, ultimo_dia = get_primeiro_ultimo_dia_mes(data_exec_dt)
    
    # Usa regex para substituir QUALQUER data que esteja nas variáveis
    # Isso garante que funcione mesmo se a query já foi modificada antes
    query = re.sub(
        r"DECLARE @DT_INI AS DATE = '\d{4}-\d{2}-\d{2}'",
        f"DECLARE @DT_INI AS DATE = '{primeiro_dia.strftime('%Y-%m-%d')}'",
        query
    )
    query = re.sub(
        r"DECLARE @DT_FIM AS DATE = '\d{4}-\d{2}-\d{2}'",
        f"DECLARE @DT_FIM AS DATE = '{ultimo_dia.strftime('%Y-%m-%d')}'",
        query
    )
    query = re.sub(
        r"DECLARE @DT\s+AS DATE = '\d{4}-\d{2}-\d{2}'",
        f"DECLARE @DT     AS DATE = '{data_exec_dt.strftime('%Y-%m-%d')}'",
        query
    )
    query = re.sub(
        r"DECLARE @DT2\s+AS DATE = '\d{4}-\d{2}-\d{2}'",
        f"DECLARE @DT2    AS DATE = '{data_exec_dt.strftime('%Y-%m-%d')}'",
        query
    )
    
    return query

def executar_consulta(connection_string, query):
    """Executa a consulta e retorna o DataFrame com os resultados"""
    try:
        conn = pyodbc.connect(connection_string)
        cursor = conn.cursor()
        
        cursor.execute(query)
        
        df = None
        while True:
            try:
                if cursor.description:
                    columns = [column[0] for column in cursor.description]
                    rows = cursor.fetchall()
                    df = pd.DataFrame.from_records(rows, columns=columns)
                
                if not cursor.nextset():
                    break
            except pyodbc.ProgrammingError:
                break
        
        cursor.close()
        conn.close()
        
        return df
            
    except Exception as e:
        print(f"  ✗ Erro ao executar consulta: {e}")
        import traceback
        traceback.print_exc()
        return None

def salvar_csv(df, data_exec, caminho_destino):
    """
    Salva o DataFrame em CSV no formato especificado
    
    Returns:
        str: Caminho completo do arquivo salvo ou None em caso de erro
    """
    try:
        # Garante que temos um datetime
        if isinstance(data_exec, datetime):
            data_exec_dt = data_exec
        else:
            data_exec_dt = datetime.combine(data_exec, datetime.min.time())
        
        nome_arquivo = f"TEMPOS_OPERACIONAIS_{data_exec_dt.strftime('%Y%m%d')}_TRCWO.csv"
        caminho_completo = os.path.join(caminho_destino, nome_arquivo)
        
        print(f"  💾 DEBUG - Salvando arquivo: {nome_arquivo}")
        
        df.to_csv(caminho_completo, index=False, sep=';', encoding='utf-8-sig')
        
        print(f"  ✅ Arquivo salvo com sucesso!")
        print(f"     Registros: {len(df):,}")
        
        return caminho_completo
        
    except Exception as e:
        print(f"  ❌ Erro ao salvar arquivo CSV: {e}")
        import traceback
        traceback.print_exc()
        return None
    
def processar_data(data_exec, caminho_sql, connection_string, caminho_destino, relatorio):
    """
    Processa uma data específica: carrega query, executa e salva CSV apenas se houver dados
    """
    try:
        print(f"\n{'─'*80}")
        print(f"📊 Processando: {data_exec.strftime('%d/%m/%Y (%A)')}")
        print(f"{'─'*80}")
        
        # Converte date para datetime
        if isinstance(data_exec, datetime):
            data_exec_dt = data_exec
        else:
            data_exec_dt = datetime.combine(data_exec, datetime.min.time())
        
        # DEBUG: Mostra as datas que serão usadas na query
        primeiro_dia, ultimo_dia = get_primeiro_ultimo_dia_mes(data_exec_dt)
        print(f"  📅 DEBUG - Datas na query:")
        print(f"     @DT_INI = {primeiro_dia.strftime('%Y-%m-%d')}")
        print(f"     @DT_FIM = {ultimo_dia.strftime('%Y-%m-%d')}")
        print(f"     @DT     = {data_exec_dt.strftime('%Y-%m-%d')}")
        print(f"     @DT2    = {data_exec_dt.strftime('%Y-%m-%d')}")
        
        # Carrega e atualiza a query
        query_atualizada = carregar_e_atualizar_query(caminho_sql, data_exec_dt)
        
        # DEBUG: Mostra um trecho da query atualizada
        linhas_query = query_atualizada.split('\n')[:10]
        print(f"\n  🔍 DEBUG - Primeiras linhas da query:")
        for linha in linhas_query:
            if 'DECLARE @DT' in linha:
                print(f"     {linha.strip()}")
        
        # Executa a consulta
        print(f"\n  🔄 Executando consulta...")
        df_resultado = executar_consulta(connection_string, query_atualizada)
        
        # Verifica se houve erro na consulta
        if df_resultado is None:
            mensagem = "Erro na execução da consulta"
            print(f"  ❌ {mensagem}")
            relatorio.adicionar(data_exec, 'ERRO', 0, None, mensagem)
            return False
        
        # Verifica se há dados
        if len(df_resultado) == 0:
            mensagem = f"Sem dados para {data_exec_dt.strftime('%A')} - Arquivo não criado"
            print(f"  ⚠️  {mensagem}")
            relatorio.adicionar(data_exec, 'SEM_DADOS', 0, None, mensagem)
            return True
        
        # Há dados: salva o CSV
        arquivo_salvo = salvar_csv(df_resultado, data_exec_dt, caminho_destino)
        
        if arquivo_salvo:
            relatorio.adicionar(data_exec, 'SUCESSO', len(df_resultado), arquivo_salvo, 
                              f"Arquivo salvo em {caminho_destino}")
            return True
        else:
            mensagem = "Falha ao salvar arquivo"
            relatorio.adicionar(data_exec, 'ERRO', len(df_resultado), None, mensagem)
            return False
            
    except Exception as e:
        mensagem = f"Exceção: {str(e)}"
        print(f"  ❌ Erro ao processar data {data_exec}: {e}")
        import traceback
        traceback.print_exc()
        relatorio.adicionar(data_exec, 'ERRO', 0, None, mensagem)
        return False

# ============================================================================
# CONFIGURAÇÕES PRINCIPAIS
# ============================================================================

#CAMINHO_SQL = r"\\trc-dc-ad\Planejamento\MIS\CARTEIRAS\Renner\Tempos\Retorno_tempos_renner.sql"
CAMINHO_SQL = r"\\trc-dc-ad\Planejamento\MIS\CARTEIRAS\Renner\Tempos\Retorno_tempos_renner - TRCWO.sql"
#CAMINHO_DESTINO_CSV = r"\\trc-dc-ad\Planejamento\MIS\CARTEIRAS\Renner\Tempos\teste"
#CAMINHO_DESTINO_CSV = r"\\trc-dc-ad\Planejamento\00 - USUÁRIOS\104 - Lucas Bassani\Relatórios\Renner\Tempos operacionais\Tempos Geral\2025\10"
CAMINHO_DESTINO_CSV = r"\\trc-dc-ad\Planejamento\00 - USUÁRIOS\104 - Lucas Bassani\Relatórios\Renner\Tempos operacionais\tempos WO\2025\Outubro"

server = r"TRC-DC-BDM\BD"
database = "SRC"
CONNECTION_STRING = f"DRIVER={{SQL Server}};SERVER={server};DATABASE={database};Trusted_Connection=yes;"

# ============================================================================
# EXECUÇÃO PRINCIPAL
# ============================================================================

if __name__ == "__main__":
    # Inicia o relatório
    relatorio = RelatorioExecucao()
    
    print("\n" + "="*100)
    print("🚀 PROCESSAMENTO INCREMENTAL DE DADOS - TEMPOS OPERACIONAIS")
    print("="*100)
    
    # 1. Determina data alvo (ontem)
    data_ontem = get_data_ontem()
    print(f"\n📅 Data alvo (ontem): {data_ontem.strftime('%d/%m/%Y (%A)')}")
    
    # 2. Verifica última data processada
    print(f"\n📂 Verificando diretório: {CAMINHO_DESTINO_CSV}")
    ultima_data_processada = obter_ultima_data_processada(CAMINHO_DESTINO_CSV)
    
    # 3. Gera lista de datas pendentes
    datas_pendentes = gerar_lista_datas_pendentes(ultima_data_processada, data_ontem)
    
    # 4. Verifica se há algo a processar
    if not datas_pendentes:
        print("\n" + "="*100)
        print("✅ SISTEMA ATUALIZADO!")
        print("="*100)
        if ultima_data_processada:
            print(f"Última data processada: {ultima_data_processada.strftime('%d/%m/%Y')}")
        print("Não há datas pendentes para processar.")
        print("Todos os dados estão atualizados até ontem.")
        print("="*100)
    else:
        print("\n" + "="*100)
        print(f"📋 DATAS PENDENTES: {len(datas_pendentes)}")
        print("="*100)
        for data in datas_pendentes:
            print(f"  • {data.strftime('%d/%m/%Y (%A)')}")
        
        # 5. Processa cada data pendente
        print("\n" + "="*100)
        print("⚙️  INICIANDO PROCESSAMENTO")
        print("="*100)
        
        for i, data_exec in enumerate(datas_pendentes, 1):
            print(f"\n[{i}/{len(datas_pendentes)}]", end=" ")
            processar_data(data_exec, CAMINHO_SQL, CONNECTION_STRING, CAMINHO_DESTINO_CSV, relatorio)
    
    # 6. Exibe relatório final
    relatorio.exibir()