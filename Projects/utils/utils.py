import pandas as pd
from datetime import datetime, timedelta
from typing import List, Tuple, Optional
import os
from pathlib import Path
import logging
import time
from functools import wraps

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def salvar_dataframes_csv(
    caminho_base: Path | str,
    **dataframes
) -> dict:
    """
    Salva múltiplos DataFrames como arquivos CSV em um diretório especificado.
    
    Substitui automaticamente arquivos existentes com o mesmo nome base na pasta.
    Cada arquivo recebe um timestamp único no formato: {nome_df}_{YYYYMMDD_HHMMSS}.csv
    
    Args:
        caminho_base (Path | str): Caminho do diretório onde os arquivos serão salvos
            Ex: Path(r"\\servidor\Planejamento\MIS\CARTEIRAS\GetNet\Analíticos\acionamentos")
            Ex: r"C:\Projetos\Carteira_XYZ\Analíticos\discagens"
        **dataframes: DataFrames nomeados para salvar
            Ex: df_enriquecido=df1, df_sem_faixa=df2, df_principal=df3
    
    Returns:
        dict: Dicionário com status de cada arquivo salvo
            {
                'nome_df': {
                    'status': 'success'|'error'|'warning',
                    'caminho': 'caminho_completo' ou None,
                    'mensagem': 'mensagem de status',
                    'linhas': quantidade de linhas (se success)
                }
            }
    
    Exemplos de uso:
        # Exemplo 1: GetNet - Acionamentos
        >>> caminho_getnet_acion = Path(r"\\servidor\MIS\CARTEIRAS\GetNet\Analíticos\acionamentos")
        >>> resultados = salvar_dataframes_csv(
        ...     caminho_getnet_acion,
        ...     df_enriquecido=df_acionamentos_enriquecido,
        ...     df_sem_faixa=df_acion_semFaixa,
        ...     df_sem_descricao=df_acion_semDescricao
        ... )
        
        # Exemplo 2: Carteira ABC - Discagens
        >>> caminho_abc_disc = Path(r"\\servidor\MIS\CARTEIRAS\ABC\Analíticos\discagens")
        >>> resultados = salvar_dataframes_csv(
        ...     caminho_abc_disc,
        ...     df_principal=df_discagens_principal
        ... )
        
        # Exemplo 3: Carteira XYZ - SMS
        >>> caminho_xyz_sms = r"C:\Projetos\XYZ\Analíticos\sms"
        >>> resultados = salvar_dataframes_csv(
        ...     caminho_xyz_sms,
        ...     df_enviados=df_sms_enviados,
        ...     df_recebidos=df_sms_recebidos
        ... )
    """
    resultados = {}
    
    # Converter caminho para Path se necessário
    caminho_completo = Path(caminho_base)
    
    # Obter timestamp atual
    data_atual = datetime.now()
    timestamp = data_atual.strftime("%Y%m%d_%H%M%S")  # Ex: 20250211_143025
    
    # Criar diretório se não existir
    try:
        caminho_completo.mkdir(parents=True, exist_ok=True)
        logger.info(f"Diretório verificado/criado: {caminho_completo}")
    except Exception as e:
        erro_msg = f"Erro ao criar diretório: {e}"
        logger.error(f"{erro_msg} - {caminho_completo}")
        # Se falhar ao criar diretório, todos os DataFrames falham
        for nome_df in dataframes.keys():
            resultados[nome_df] = {
                'status': 'error',
                'caminho': None,
                'mensagem': erro_msg
            }
        return resultados
    
    # Salvar cada DataFrame
    for nome_df, df in dataframes.items():
        try:
            # Validar DataFrame
            if df is None:
                logger.warning(f"DataFrame '{nome_df}' é None. Pulando...")
                resultados[nome_df] = {
                    'status': 'warning',
                    'caminho': None,
                    'mensagem': 'DataFrame é None'
                }
                continue
            
            if df.empty:
                logger.warning(f"DataFrame '{nome_df}' está vazio (0 linhas). Pulando...")
                resultados[nome_df] = {
                    'status': 'warning',
                    'caminho': None,
                    'mensagem': 'DataFrame vazio (0 linhas)'
                }
                continue
            
            # Remover TODOS os arquivos antigos com o mesmo nome base
            # Padrão: {nome_df}_*.csv (qualquer timestamp)
            padrao_antigo = f"{nome_df}_*.csv"
            arquivos_antigos = list(caminho_completo.glob(padrao_antigo))
            
            arquivos_removidos = 0
            for arquivo_antigo in arquivos_antigos:
                try:
                    arquivo_antigo.unlink()
                    arquivos_removidos += 1
                    logger.info(f"Arquivo antigo removido: {arquivo_antigo.name}")
                except Exception as e:
                    logger.warning(f"Não foi possível remover {arquivo_antigo.name}: {e}")
            
            if arquivos_removidos > 0:
                logger.info(f"Total de arquivos antigos removidos para '{nome_df}': {arquivos_removidos}")
            
            # Gerar nome do arquivo com timestamp
            nome_arquivo = f"{nome_df}_{timestamp}.csv"
            caminho_arquivo = caminho_completo / nome_arquivo
            
            # Salvar CSV
            df.to_csv(
                caminho_arquivo,
                index=False,
                encoding='utf-8-sig',  # Compatibilidade com Excel
                sep=';'  # Separador padrão brasileiro
            )
            
            resultados[nome_df] = {
                'status': 'success',
                'caminho': str(caminho_arquivo),
                'mensagem': 'Arquivo salvo com sucesso',
                'linhas': len(df)
            }
            logger.info(f"✓ Salvo: {nome_arquivo} ({len(df)} linhas)")
            
        except Exception as e:
            resultados[nome_df] = {
                'status': 'error',
                'caminho': None,
                'mensagem': f'Erro ao salvar: {str(e)}'
            }
            logger.error(f"✗ Erro ao salvar '{nome_df}': {e}")
    
    # Resumo final
    total_dfs = len(dataframes)
    sucessos = sum(1 for r in resultados.values() if r['status'] == 'success')
    warnings = sum(1 for r in resultados.values() if r['status'] == 'warning')
    erros = sum(1 for r in resultados.values() if r['status'] == 'error')
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Resumo do salvamento em: {caminho_completo.name}")
    logger.info(f"  ✓ Sucessos: {sucessos}/{total_dfs}")
    if warnings > 0:
        logger.info(f"  ⚠ Avisos: {warnings}/{total_dfs}")
    if erros > 0:
        logger.info(f"  ✗ Erros: {erros}/{total_dfs}")
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

def transformar_funil_formato_long_(df_acionamentos_funil):
    """
    Transforma o DataFrame de acionamentos do formato wide para long.
    
    De: DATA | FX_ATRASO | ORIGEM | TRABALHADO | ACIONAMENTOS | CPC | CPCA | PROMESSA | ...
    Para: DATA | Indicador | qte | FX_ATRASO | ORIGEM | MesAbreviado | nr_dia_util | quartil | dt_mes | VALORPRIN_FIN
    """
    
    # Definir os indicadores que serão transformados
    indicadores = {
        'TRABALHADO': 'VALORPRIN_FIN_TRABALHADO',
        'ACIONAMENTOS': 'VALORPRIN_FIN_ACIONAMENTOS',
        'CPC': 'VALORPRIN_FIN_CPC',
        'CPCA': 'VALORPRIN_FIN_CPCA',
        'PROMESSA': 'VALORPRIN_FIN_PROMESSA'
    }
    
    resultados = []
    
    for indicador, col_valor in indicadores.items():
        # Criar um DataFrame para cada indicador
        df_temp = df_acionamentos_funil[[
            'DATA',
            'FX_ATRASO',
            'ORIGEM',
            indicador,
            col_valor,
            'mes_abreviado',
            'nr_dia_util',
            'quartil',
            'dt_mes'
        ]].copy()
        
        # Renomear colunas
        df_temp = df_temp.rename(columns={
            indicador: 'qte',
            col_valor: 'VALORPRIN_FIN',
            'mes_abreviado': 'MesAbreviado'
        })
        
        # Adicionar coluna Indicador
        df_temp['Indicador'] = indicador.upper()
        
        resultados.append(df_temp)
    
    # Concatenar todos os indicadores
    df_final = pd.concat(resultados, ignore_index=True)
    
    # Reordenar colunas conforme solicitado
    df_final = df_final[[
        'DATA',
        'Indicador',
        'qte',
        'FX_ATRASO',
        'ORIGEM',
        'MesAbreviado',
        'nr_dia_util',
        'quartil',
        'dt_mes',
        'VALORPRIN_FIN'
    ]]
    
    # Ordenar por DATA, FX_ATRASO e Indicador
    df_final = df_final.sort_values(['DATA', 'FX_ATRASO', 'Indicador']).reset_index(drop=True)
    
    return df_final

def transformar_funil_formato_long(
    df_acionamentos_funil: pd.DataFrame,
    dimensoes_manter: Optional[List[str]] = None
) -> pd.DataFrame:
    """
    Transforma o DataFrame de acionamentos do formato wide para long.
    
    De: DATA | FX_ATRASO | [ORIGEM] | TRABALHADO | ACIONAMENTOS | CPC | CPCA | PROMESSA | ...
    Para: DATA | Indicador | qte | FX_ATRASO | [ORIGEM] | MesAbreviado | nr_dia_util | quartil | dt_mes | VALORPRIN_FIN
    
    Args:
        df_acionamentos_funil: DataFrame no formato wide
        dimensoes_manter: Lista de dimensões adicionais para manter (ex: ['ORIGEM', 'CANAL'])
                         Se None, detecta automaticamente colunas disponíveis
    
    Returns:
        DataFrame transformado para formato long
    
    Example:
        # Carteira com ORIGEM
        df_long = transformar_funil_formato_long(df, dimensoes_manter=['ORIGEM'])
        
        # Carteira sem ORIGEM
        df_long = transformar_funil_formato_long(df, dimensoes_manter=[])
        
        # Auto-detectar
        df_long = transformar_funil_formato_long(df)
    """
    
    # ============================================
    # DETECTAR DIMENSÕES DISPONÍVEIS
    # ============================================
    if dimensoes_manter is None:
        # Auto-detectar dimensões além de FX_ATRASO
        dimensoes_possiveis = ['ORIGEM', 'CANAL', 'REGIAO', 'PARCEIRO', 'OPERACAO']
        dimensoes_manter = [dim for dim in dimensoes_possiveis if dim in df_acionamentos_funil.columns]
    
    # Validar dimensões solicitadas
    dimensoes_validas = [dim for dim in dimensoes_manter if dim in df_acionamentos_funil.columns]
    
    # ============================================
    # DEFINIR INDICADORES E COLUNAS BASE
    # ============================================
    indicadores = {
        'TRABALHADO': 'VALORPRIN_FIN_TRABALHADO',
        'ACIONAMENTOS': 'VALORPRIN_FIN_ACIONAMENTOS',
        'CPC': 'VALORPRIN_FIN_CPC',
        'CPCA': 'VALORPRIN_FIN_CPCA',
        'PROMESSA': 'VALORPRIN_FIN_PROMESSA'
    }
    
    # Colunas fixas sempre presentes
    colunas_fixas = ['DATA', 'FX_ATRASO']
    
    # Colunas de calendário
    colunas_calendario = ['mes_abreviado', 'nr_dia_util', 'quartil', 'dt_mes']
    
    # ============================================
    # TRANSFORMAR CADA INDICADOR
    # ============================================
    resultados = []
    
    for indicador, col_valor in indicadores.items():
        # Verificar se as colunas do indicador existem
        if indicador not in df_acionamentos_funil.columns:
            continue  # Pula indicadores ausentes
        
        if col_valor not in df_acionamentos_funil.columns:
            col_valor_temp = None  # Sem coluna de valor
        else:
            col_valor_temp = col_valor
        
        # Montar lista de colunas para este indicador
        colunas_selecionar = (
            colunas_fixas +
            dimensoes_validas +
            [indicador] +
            ([col_valor_temp] if col_valor_temp else []) +
            colunas_calendario
        )
        
        # Criar DataFrame temporário
        df_temp = df_acionamentos_funil[colunas_selecionar].copy()
        
        # Renomear colunas
        renomear = {
            indicador: 'qte',
            'mes_abreviado': 'MesAbreviado'
        }
        if col_valor_temp:
            renomear[col_valor_temp] = 'VALORPRIN_FIN'
        
        df_temp = df_temp.rename(columns=renomear)
        
        # Se não havia coluna de valor, criar com 0
        if not col_valor_temp:
            df_temp['VALORPRIN_FIN'] = 0
        
        # Adicionar coluna Indicador
        df_temp['Indicador'] = indicador.upper()
        
        resultados.append(df_temp)
    
    # ============================================
    # CONCATENAR E ORDENAR
    # ============================================
    if not resultados:
        raise ValueError("Nenhum indicador válido encontrado no DataFrame")
    
    df_final = pd.concat(resultados, ignore_index=True)
    
    # Reordenar colunas dinamicamente
    colunas_ordenadas = (
        ['DATA', 'Indicador', 'qte', 'FX_ATRASO'] +
        dimensoes_validas +
        ['MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALORPRIN_FIN']
    )
    
    df_final = df_final[colunas_ordenadas]
    
    # Ordenar por DATA, FX_ATRASO, dimensões e Indicador
    colunas_ordenacao = ['DATA', 'FX_ATRASO'] + dimensoes_validas + ['Indicador']
    df_final = df_final.sort_values(colunas_ordenacao).reset_index(drop=True)
    
    return df_final

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

def salvar_log(mensagem, arquivo_log='logs/default.txt'):
    """
    Salva mensagem em arquivo_log de log com timestamp.
    Função genérica que aceita qualquer caminho de arquivo_log.
    
    Args:
        mensagem (str): Mensagem a ser registrada
        arquivo_log (str): Caminho do arquivo_log de log.
                      Padrão: 'logs/default.txt'
                      Cada carteira/projeto deve definir seu próprio caminho.
    
    Exemplo:
        # Em Projects/utils/utils.py (uso genérico)
        salvar_log("Processamento iniciado", arquivo_log='src/logs/acionamentos.txt')
        
        # Em Projects/Getnet/src/acionamentos/log_config.py (wrapper específico)
        from ...config import LOG_ACIONAMENTOS
        def salvar_log(mensagem):
            _salvar_log(mensagem, arquivo_log=LOG_ACIONAMENTOS)
    """
    # Criar diretório se não existir
    diretorio = os.path.dirname(arquivo_log)
    if diretorio:  # Se há diretório no caminho
        os.makedirs(diretorio, exist_ok=True)
    
    # Registrar log com timestamp
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(arquivo_log, 'a', encoding='utf-8') as f:
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
            salvar_log(f"▶️  Iniciando: {nome_processo}", arquivo_log=arquivo_log)
            
            inicio = time.time()
            try:
                resultado = funcao(*args, **kwargs)
                fim = time.time()
                
                tempo_execucao = fim - inicio
                salvar_log(
                    f"✅ Concluído: {nome_processo} - Tempo: {formatar_tempo(tempo_execucao)}", 
                    arquivo_log=arquivo_log
                )
                
                return resultado
            except Exception as e:
                fim = time.time()
                tempo_execucao = fim - inicio
                salvar_log(
                    f"❌ Erro em {nome_processo}: {str(e)} - Tempo até erro: {formatar_tempo(tempo_execucao)}", 
                    arquivo_log=arquivo_log
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