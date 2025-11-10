import pandas as pd
from src.data_wrangling_acionamentos import acionamentos_humano
from src.data_wrangling_discagens_expert import acionamentos_expert
from src.data_wrangling_discagens_trestto import acionamentos_trestto
from datetime import datetime
import time
from functools import wraps
import os

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

def acionamentos_funil(df_tab_acionamentos, df_tabulacao_aciona, df_dw_calendario, df_maling_hist, df_discagens_trestto, df_discagens_expert):
    df_acionamentos_humano, df_analitico_acionamentos_humano, df_acion_semFaixa_humano, df_acion_semDescricao_humano, df_acion_semOrigem_humano = acionamentos_humano(df_tab_acionamentos, df_tabulacao_aciona, df_dw_calendario, df_maling_hist)
    df_acionamentos_expert, df_analitico_expert, df_enriquecido_discagens_expert_semFaixa, df_humano_tabulados_como_robo, df_dicagens_operacaoOutros = acionamentos_expert(df_discagens_expert, df_dw_calendario, df_maling_hist)
    df_acionamentos_trestto, df_analitico_trestto, df_enriquecido_discagens_trestto_semFaixa = acionamentos_trestto(df_discagens_trestto, df_maling_hist, df_dw_calendario)

    df_acionamentos_funil = unir_dataframes(df_acionamentos_humano, df_acionamentos_expert, df_acionamentos_trestto)

    return df_acionamentos_funil, df_analitico_acionamentos_humano, df_acion_semFaixa_humano, df_acion_semDescricao_humano, df_acion_semOrigem_humano, df_analitico_trestto, df_enriquecido_discagens_trestto_semFaixa, df_analitico_expert, df_enriquecido_discagens_expert_semFaixa, df_humano_tabulados_como_robo, df_dicagens_operacaoOutros

LOG_FILE = 'logs/acionamentos.txt'

def salvar_log(mensagem, arquivo=LOG_FILE):
    """Salva mensagem no arquivo de log com timestamp"""
    os.makedirs(os.path.dirname(arquivo), exist_ok=True)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(arquivo, 'a', encoding='utf-8') as f:
        f.write(f"[{timestamp}] {mensagem}\n")

def formatar_tempo(segundos):
    """Formata tempo em formato legível"""
    if segundos < 60:
        return f"{segundos:.2f}s"
    elif segundos < 3600:
        minutos = segundos / 60
        return f"{minutos:.2f}min ({segundos:.2f}s)"
    else:
        horas = segundos / 3600
        minutos = (segundos % 3600) / 60
        return f"{horas:.0f}h {minutos:.0f}min ({segundos:.2f}s)"
    
def registrar_tempo(nome_processo):
    """Decorator para registrar tempo de execução de funções"""
    def decorator(funcao):
        @wraps(funcao)
        def wrapper(*args, **kwargs):
            salvar_log(f"▶️  Iniciando: {nome_processo}")
            
            inicio = time.time()
            try:
                resultado = funcao(*args, **kwargs)
                fim = time.time()
                
                tempo_execucao = fim - inicio
                salvar_log(f"✅ Concluído: {nome_processo} - Tempo: {formatar_tempo(tempo_execucao)}")
                
                return resultado
            except Exception as e:
                fim = time.time()
                tempo_execucao = fim - inicio
                salvar_log(f"❌ Erro em {nome_processo}: {str(e)} - Tempo até erro: {formatar_tempo(tempo_execucao)}")
                raise
                
        return wrapper
    return decorator