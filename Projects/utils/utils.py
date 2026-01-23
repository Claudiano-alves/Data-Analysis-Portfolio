import pandas as pd
from datetime import datetime, timedelta
from typing import Tuple, Optional
import os
from pathlib import Path
import logging
import time
from functools import wraps

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def salvar_dataframes_csv(
    processo: str,
    **dataframes
) -> dict:
    """
    Salva múltiplos DataFrames como arquivos CSV em um diretório especificado.
    
    Adiciona timestamp aos nomes dos arquivos e substitui arquivos existentes
    do mesmo mês/tipo se necessário.
    
    Args:
        processo (str): Nome do processo no dicionário PROCESS_PATHS
            Ex: "acionamentos", "pagamentos", "acordos", "discagens"
        **dataframes: DataFrames nomeados para salvar
            Ex: df_enriquecido=df1, df_sem_faixa=df2
    
    Returns:
        dict: Dicionário com status de cada arquivo salvo
            {
                'nome_df': {
                    'status': 'success'|'error',
                    'caminho': 'caminho_completo',
                    'mensagem': 'mensagem de status'
                }
            }
    
    Exemplo de uso:
        >>> from config import PROCESS_PATHS
        >>> resultados = salvar_dataframes_csv(
        ...     processo="acionamentos",
        ...     df_enriquecido=df_acionamentos_enriquecido_limpo,
        ...     df_sem_faixa=df_acion_semFaixa_humano,
        ...     df_sem_descricao=df_acion_semDescricao_humano,
        ...     df_sem_origem=df_acion_semOrigem_humano
        ... )
    """
    from .config import PROCESS_PATHS  # Importar o dicionário de caminhos
    
    resultados = {}
    
    # Validar processo
    if processo not in PROCESS_PATHS:
        erro_msg = f"Processo '{processo}' não encontrado. Processos disponíveis: {list(PROCESS_PATHS.keys())}"
        logger.error(erro_msg)
        for nome_df in dataframes.keys():
            resultados[nome_df] = {
                'status': 'error',
                'caminho': None,
                'mensagem': erro_msg
            }
        return resultados
    
    # Obter data atual
    data_atual = datetime.now()
    mes_ano = data_atual.strftime("%Y%m")  # Ex: 202501
    data_completa = data_atual.strftime("%Y%m%d_%H%M%S")  # Ex: 20250122_143025
    
    # Obter caminho do processo
    caminho_completo = PROCESS_PATHS[processo]
    
    # Criar diretório se não existir
    try:
        caminho_completo.mkdir(parents=True, exist_ok=True)
        logger.info(f"Diretório verificado/criado: {caminho_completo}")
    except Exception as e:
        logger.error(f"Erro ao criar diretório {caminho_completo}: {e}")
        for nome_df in dataframes.keys():
            resultados[nome_df] = {
                'status': 'error',
                'caminho': None,
                'mensagem': f"Erro ao criar diretório: {e}"
            }
        return resultados
    
    # Salvar cada DataFrame
    for nome_df, df in dataframes.items():
        try:
            # Validar DataFrame
            if df is None or df.empty:
                logger.warning(f"DataFrame '{nome_df}' está vazio ou None. Pulando...")
                resultados[nome_df] = {
                    'status': 'warning',
                    'caminho': None,
                    'mensagem': 'DataFrame vazio ou None'
                }
                continue
            
            # Remover arquivos antigos do mesmo mês e tipo
            padrao_antigo = f"{nome_df}_{mes_ano}_*.csv"
            arquivos_antigos = list(caminho_completo.glob(padrao_antigo))
            
            for arquivo_antigo in arquivos_antigos:
                try:
                    arquivo_antigo.unlink()
                    logger.info(f"Arquivo antigo removido: {arquivo_antigo.name}")
                except Exception as e:
                    logger.warning(f"Não foi possível remover {arquivo_antigo.name}: {e}")
            
            # Gerar nome do arquivo com timestamp
            nome_arquivo = f"{nome_df}_{mes_ano}_{data_completa}.csv"
            caminho_arquivo = caminho_completo / nome_arquivo
            
            # Salvar CSV
            df.to_csv(
                caminho_arquivo,
                index=False,
                encoding='utf-8-sig',  # Para compatibilidade com Excel
                sep=';'  # Separador padrão brasileiro
            )
            
            resultados[nome_df] = {
                'status': 'success',
                'caminho': str(caminho_arquivo),
                'mensagem': f'Arquivo salvo com sucesso ({len(df)} linhas)'
            }
            logger.info(f"✓ Salvo: {nome_arquivo} ({len(df)} linhas)")
            
        except Exception as e:
            resultados[nome_df] = {
                'status': 'error',
                'caminho': None,
                'mensagem': f'Erro ao salvar: {e}'
            }
            logger.error(f"✗ Erro ao salvar '{nome_df}': {e}")
    
    # Resumo
    sucessos = sum(1 for r in resultados.values() if r['status'] == 'success')
    logger.info(f"\n{'='*60}")
    logger.info(f"Resumo: {sucessos}/{len(dataframes)} arquivos salvos com sucesso")
    logger.info(f"{'='*60}")
    
    return resultados

def get_date_range_from_csv(csv_path: Optional[str] = None) -> Tuple[str, str]:
    """
    Lê um arquivo XLSX e retorna o range de datas baseado na última data encontrada.
    
    Lógica:
    - Encontra a última (maior) data no arquivo
    - Verifica se os dias restantes do mês são apenas finais de semana
    - Se sim, ajusta para o próximo mês
    - Define data_inicio como dia 1 do mês da última data (ajustada)
    - Define data_fim como hoje - 1 dia
    
    Args:
        csv_path: Caminho do arquivo XLSX. Se None, usa o caminho padrão do servidor.
        
    Returns:
        Tupla com (data_inicio, data_fim) no formato 'YYYY-MM-DD'
    """
    
    def ajustar_para_proximo_mes_se_necessario(data):
        """
        Verifica se os dias restantes do mês são apenas finais de semana.
        Se sim, avança para o primeiro dia do próximo mês.
        
        Args:
            data: datetime object
            
        Returns:
            datetime: Data ajustada (primeiro dia do próximo mês se necessário)
        """
        # Último dia do mês atual
        ultimo_dia_mes = pd.Timestamp(data.year, data.month, 1) + pd.offsets.MonthEnd(1)
        
        # Verifica se todos os dias restantes do mês (após a data atual) são finais de semana
        dias_restantes = pd.date_range(start=data + pd.Timedelta(days=1), end=ultimo_dia_mes, freq='D')
        
        # Se não há dias restantes no mês, retorna a data original
        if len(dias_restantes) == 0:
            return data
        
        # Verifica se TODOS os dias restantes são sábado (5) ou domingo (6)
        todos_finais_semana = all(dia.weekday() >= 5 for dia in dias_restantes)
        
        if todos_finais_semana:
            # Avança para o primeiro dia do próximo mês
            proximo_mes = data + pd.offsets.MonthBegin(1)
            print(f"⏭️ Dias restantes do mês ({data.strftime('%Y-%m')}) são apenas finais de semana.")
            print(f"   Ajustando para: {proximo_mes.strftime('%Y-%m-%d')}")
            return proximo_mes
        
        print(data)
    
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
        
        # Pega a última (maior) data do arquivo
        ultima_data = df_validos[date_col].max()
        
        print(f"📅 Última data no arquivo: {ultima_data.strftime('%Y-%m-%d')}")
        
        # ⭐ AJUSTE PARA FINAIS DE SEMANA ⭐
        # Verifica se precisa pular para o próximo mês
        ultima_data_ajustada = ajustar_para_proximo_mes_se_necessario(ultima_data)
        
        # Data início: sempre dia 1 do mês da última data ajustada
        data_inicio = ultima_data_ajustada.replace(day=1).strftime('%Y-%m-%d')
        
        # Data fim: sempre hoje - 1 dia
        hoje = datetime.now()
        data_fim = (hoje - timedelta(days=1)).strftime('%Y-%m-%d')
        
        print(f"✅ Arquivo lido com sucesso!")
        print(f"📅 Range de datas calculado:")
        print(f"   Data início (dia 1 do mês ajustado): {data_inicio}")
        print(f"   Data fim (hoje - 1): {data_fim}")
        
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

def unir_dataframes(*dfs, validar_colunas=True, colunas_esperadas=None, mapeamento_colunas=None):
    """
    Une múltiplos DataFrames verticalmente com padronização de colunas.
    
    Parameters:
    -----------
    *dfs : DataFrames
        DataFrames a serem unidos (quantidade variável)
    validar_colunas : bool, default=True
        Se True, valida se todos os DataFrames têm as mesmas colunas após padronização
    colunas_esperadas : list, optional
        Lista de colunas esperadas para validação adicional
    mapeamento_colunas : dict, optional
        Dicionário para renomear colunas {nome_antigo: nome_novo}
        Se None, usa mapeamento padrão DATA_ACIONA -> DATA
    
    Returns:
    --------
    pd.DataFrame
        DataFrame único com todos os dados concatenados
    """
    
    # Validação básica
    if len(dfs) == 0:
        raise ValueError("Nenhum DataFrame foi fornecido")
    
    # Mapeamento padrão para padronização
    if mapeamento_colunas is None:
        mapeamento_colunas = {
            'DATA_ACIONA': 'DATA'
        }
    
    # Lista para armazenar DataFrames processados
    dfs_processados = []
    
    for i, df in enumerate(dfs, start=1):
        # Pula DataFrames None ou vazios
        if df is None or df.empty:
            print(f"⚠ DataFrame {i} está vazio ou é None - ignorado")
            continue
        
        # Cria uma cópia para não modificar o original
        df_copy = df.copy()
        
        # Aplica o mapeamento de colunas
        colunas_renomeadas = []
        for col_antiga, col_nova in mapeamento_colunas.items():
            if col_antiga in df_copy.columns:
                colunas_renomeadas.append(f"{col_antiga} -> {col_nova}")
        
        if colunas_renomeadas:
            df_copy = df_copy.rename(columns=mapeamento_colunas)
            print(f"✓ DataFrame {i}: Colunas renomeadas: {', '.join(colunas_renomeadas)}")
        
        dfs_processados.append(df_copy)
    
    if len(dfs_processados) == 0:
        raise ValueError("Todos os DataFrames fornecidos estão vazios ou são None")
    
    # Validação de colunas
    if validar_colunas:
        colunas_base = set(dfs_processados[0].columns)
        
        for i, df in enumerate(dfs_processados[1:], start=2):
            colunas_atuais = set(df.columns)
            if colunas_base != colunas_atuais:
                colunas_faltando = colunas_base - colunas_atuais
                colunas_extras = colunas_atuais - colunas_base
                
                msg = f"DataFrame {i} tem colunas diferentes do primeiro DataFrame."
                if colunas_faltando:
                    msg += f"\n  Colunas faltando: {list(colunas_faltando)}"
                if colunas_extras:
                    msg += f"\n  Colunas extras: {list(colunas_extras)}"
                raise ValueError(msg)
    
    # Validação contra colunas esperadas (opcional)
    if colunas_esperadas is not None:
        colunas_esperadas_set = set(colunas_esperadas)
        colunas_reais = set(dfs_processados[0].columns)
        
        if colunas_esperadas_set != colunas_reais:
            colunas_faltando = colunas_esperadas_set - colunas_reais
            colunas_extras = colunas_reais - colunas_esperadas_set
            
            msg = "As colunas dos DataFrames não correspondem às esperadas."
            if colunas_faltando:
                msg += f"\n  Colunas esperadas faltando: {list(colunas_faltando)}"
            if colunas_extras:
                msg += f"\n  Colunas extras encontradas: {list(colunas_extras)}"
            raise ValueError(msg)
    
    # Concatenação
    df_unido = pd.concat(dfs_processados, ignore_index=True)
    
    print(f"\n{'='*60}")
    print(f"✓ {len(dfs_processados)} DataFrames unidos com sucesso")
    print(f"✓ Total de linhas: {len(df_unido):,}")
    print(f"✓ Total de colunas: {len(df_unido.columns)}")
    print(f"{'='*60}")
    
    return df_unido

def salvar_log(mensagem, arquivo='logs/default.txt'):
    """
    Salva mensagem em arquivo de log com timestamp.
    Função genérica que aceita qualquer caminho de arquivo.
    
    Args:
        mensagem (str): Mensagem a ser registrada
        arquivo (str): Caminho do arquivo de log.
                      Padrão: 'logs/default.txt'
                      Cada carteira/projeto deve definir seu próprio caminho.
    
    Exemplo:
        # Em Projects/utils/utils.py (uso genérico)
        salvar_log("Processamento iniciado", arquivo='src/logs/acionamentos.txt')
        
        # Em Projects/Getnet/src/acionamentos/log_config.py (wrapper específico)
        from ...config import LOG_ACIONAMENTOS
        def salvar_log(mensagem):
            _salvar_log(mensagem, arquivo=LOG_ACIONAMENTOS)
    """
    # Criar diretório se não existir
    diretorio = os.path.dirname(arquivo)
    if diretorio:  # Se há diretório no caminho
        os.makedirs(diretorio, exist_ok=True)
    
    # Registrar log com timestamp
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(arquivo, 'a', encoding='utf-8') as f:
        f.write(f"[{timestamp}] {mensagem}\n")

def registrar_tempo(nome_processo, arquivo_log='logs/default.txt'):
    """
    Decorator para registrar tempo de execução de funções.
    
    Args:
        nome_processo (str): Nome do processo sendo executado
        arquivo_log (str): Caminho do arquivo de log.
                          Padrão: 'logs/default.txt'
                          Cada carteira/projeto deve definir seu próprio caminho.
    
    Exemplo de uso:
        # Uso genérico
        @registrar_tempo("Processar Acionamentos", arquivo_log='logs/acionamentos.txt')
        def processar_acionamentos():
            pass
        
        # Uso com config específica
        from config import LOG_ACIONAMENTOS
        
        @registrar_tempo("Processar Acionamentos", arquivo_log=LOG_ACIONAMENTOS)
        def processar_acionamentos():
            pass
    """
    def decorator(funcao):
        @wraps(funcao)
        def wrapper(*args, **kwargs):
            salvar_log(f"▶️  Iniciando: {nome_processo}", arquivo=arquivo_log)
            
            inicio = time.time()
            try:
                resultado = funcao(*args, **kwargs)
                fim = time.time()
                
                tempo_execucao = fim - inicio
                salvar_log(
                    f"✅ Concluído: {nome_processo} - Tempo: {formatar_tempo(tempo_execucao)}", 
                    arquivo=arquivo_log
                )
                
                return resultado
            except Exception as e:
                fim = time.time()
                tempo_execucao = fim - inicio
                salvar_log(
                    f"❌ Erro em {nome_processo}: {str(e)} - Tempo até erro: {formatar_tempo(tempo_execucao)}", 
                    arquivo=arquivo_log
                )
                raise
                
        return wrapper
    return decorator

def formatar_tempo(segundos):
    """
    Formata segundos em formato legível (hh:mm:ss ou mm:ss ou ss.ms)
    
    Args:
        segundos (float): Tempo em segundos
        
    Returns:
        str: Tempo formatado
    """
    if segundos >= 3600:  # 1 hora ou mais
        horas = int(segundos // 3600)
        minutos = int((segundos % 3600) // 60)
        segs = int(segundos % 60)
        return f"{horas:02d}:{minutos:02d}:{segs:02d}"
    elif segundos >= 60:  # 1 minuto ou mais
        minutos = int(segundos // 60)
        segs = int(segundos % 60)
        return f"{minutos:02d}:{segs:02d}"
    else:  # Menos de 1 minuto
        return f"{segundos:.2f}s"