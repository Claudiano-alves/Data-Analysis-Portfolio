import numpy as np
import pandas as pd
from .utils import unir_dataframes, salvar_log, registrar_tempo

FAIXAS_ATRASO_BINS = [float('-inf'), 0, 30, 60, 90, 120, 150, 180, 360, 720, float('inf')]
FAIXAS_ATRASO_LABELS = [
    'Preventivo',
    '0-30',
    '31-60',
    '61-90',
    '91-120',
    '121-150',
    '151-180',
    '181-360',
    '361-720',
    'Maior 720'
]

# ============================================
# FUNÇÕES DE TRATAMENTO - MAILING_HIST
# ============================================

def adicionar_produto(df):
    """
    Adiciona a coluna PRODUTO ao DataFrame de mailing_hist
    
    Args:
        df (pd.DataFrame): DataFrame com colunas 'COD_CLI' e 'COD_CAR'
    
    Returns:
        pd.DataFrame: DataFrame com nova coluna 'PRODUTO'
    """
    conditions = [
        (df['COD_CLI'] == 228) & (df['COD_CAR'] == 2),
        (df['COD_CLI'] == 198) & (df['COD_CAR'].isin([1, 2, 3])),
        (df['COD_CLI'] == 196) & (df['COD_CAR'].isin([1, 3, 4]))
    ]
    
    choices = [
        'API',
        'Agenda Negativa',
        'Equipamentos'
    ]
    
    df['PRODUTO'] = np.select(conditions, choices, default='Outros')
    return df

def adicionar_faixa_atraso(df, coluna_atraso='ATRASO'):
    """
    Adiciona a coluna FX_ATRASO ao DataFrame
    
    Args:
        df (pd.DataFrame): DataFrame com coluna de atraso
        coluna_atraso (str): Nome da coluna que contém o valor de atraso
    
    Returns:
        pd.DataFrame: DataFrame com nova coluna 'FX_ATRASO'
    """
    df['FX_ATRASO'] = pd.cut(
        df[coluna_atraso], 
        bins=FAIXAS_ATRASO_BINS, 
        labels=FAIXAS_ATRASO_LABELS, 
        right=True
    )
    return df

def adicionar_valor_principal(df_mailing_hist, df_cad_devf):
    """
    Adiciona a coluna VALORPRIN_FIN ao DataFrame de mailing_hist através de join com CAD_DEVF
    O valor principal é o mesmo para o contrato independente da data
    
    Args:
        df_mailing_hist (pd.DataFrame): DataFrame de mailing_hist (com coluna CONTRATO)
        df_cad_devf (pd.DataFrame): DataFrame de CAD_DEVF (com colunas CONTRATO_FIN e VALORPRIN_FIN)
    
    Returns:
        pd.DataFrame: DataFrame de mailing_hist com nova coluna VALORPRIN_FIN
    """
    # Fazer cópia para não alterar o original
    df_resultado = df_mailing_hist.copy()
    
    # Garantir que os contratos estão no mesmo formato
    df_resultado['CONTRATO'] = df_resultado['CONTRATO'].astype(str)
    df_cad_devf_temp = df_cad_devf[['CONTRATO_FIN', 'VALORPRIN_FIN']].copy()
    df_cad_devf_temp['CONTRATO_FIN'] = df_cad_devf_temp['CONTRATO_FIN'].astype(str)
    
    print(f"📊 Antes do join - Mailing: {len(df_resultado):,} | CAD_DEVF: {len(df_cad_devf_temp):,}")
    
    # Fazer o join apenas por CONTRATO
    df_resultado = df_resultado.merge(
        df_cad_devf_temp,
        left_on='CONTRATO',
        right_on='CONTRATO_FIN',
        how='left'  # left join para manter todos os registros do mailing
    )
    
    # Remover a coluna auxiliar CONTRATO_FIN
    df_resultado = df_resultado.drop(columns=['CONTRATO_FIN'])
    
    print(f"📊 Após join: {len(df_resultado):,}")
    print(f"📊 Contratos com valor: {df_resultado['VALORPRIN_FIN'].notna().sum():,}")
    print(f"📊 Contratos sem valor: {df_resultado['VALORPRIN_FIN'].isna().sum():,}")
    
    return df_resultado

def tratar_base_mailing_hist(df):
    """
    Aplica todos os tratamentos padrão para base de mailing_hist
    
    Args:
        df (pd.DataFrame): DataFrame de mailing_hist
    
    Returns:
        pd.DataFrame: DataFrame tratado
    """
    df = adicionar_produto(df)
    df = adicionar_faixa_atraso(df)
    #df = adicionar_valor_principal(df, df_cad_devf)
    return df

@registrar_tempo("Acumulado mailing hist por faixa de atraso")
def gerar_acumulado_maling_hist_fxAtraso(df_maling_hist, df_dw_calendario):
    """
    Gera DataFrame com acumulado de contratos e CPFs do maling_hist por dia útil.
    """
    salvar_log("="*60)
    salvar_log("INÍCIO DO PROCESSAMENTO DE MALING_HIST")
    salvar_log("="*60)  
    salvar_log(f"Total de registros em df_maling_hist: {len(df_maling_hist)}")
    
    # Selecionar apenas as colunas necessárias
    df_reduzido = df_maling_hist[['DATA', 'CONTRATO', 'CPF', 'FX_ATRASO', 'VALORPRIN_FIN']].copy()
    
    salvar_log(f"Memória reduzida - trabalhando com {len(df_reduzido.columns)} colunas")
    
    # Converter DATA para datetime (não para date)
    df_reduzido['DATA'] = pd.to_datetime(df_reduzido['DATA'])
    
    # Preparar calendário
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    df_calendario_reduzido = df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']]
    
    # Merge com calendário
    df_reduzido = df_reduzido.merge(
        df_calendario_reduzido,
        left_on='DATA',
        right_on='dt_data',
        how='inner'
    ).drop(columns=['dt_data'])
    
    salvar_log(f"Registros após merge com calendário: {len(df_reduzido)}")
    
    resultados = []
    
    # Obter datas únicas ordenadas
    datas_unicas = sorted(df_reduzido['DATA'].unique())
    
    salvar_log(f"Total de datas únicas a processar: {len(datas_unicas)}")
    
    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...")
        
        # Obter o início do mês da data atual
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        
        # Filtrar do início do mês até a data atual
        df_intervalo = df_reduzido[
            (df_reduzido['DATA'] >= inicio_mes) & 
            (df_reduzido['DATA'] <= data)
        ].copy()
        
        # Pegar informações do calendário da data atual
        info_data = df_reduzido[df_reduzido['DATA'] == data][
            ['mes_abreviado', 'nr_dia_util', 'quartil', 'dt_mes']
        ].drop_duplicates()
        
        if len(info_data) == 0:
            continue
            
        info_data = info_data.iloc[0]
        
        # CORREÇÃO: Remover duplicatas antes de agrupar por FX_ATRASO
        # Para CONTRATOS - pegar o último registro de cada contrato por faixa
        df_contratos_unicos = df_intervalo.sort_values('DATA').groupby(['CONTRATO', 'FX_ATRASO']).tail(1)
        
        # Agrupar por FX_ATRASO - CONTRATOS ÚNICOS
        agrupado_contratos = df_contratos_unicos.groupby('FX_ATRASO').agg({
            'CONTRATO': 'nunique',
            'VALORPRIN_FIN': 'sum'
        }).reset_index()
        
        agrupado_contratos['DATA'] = data
        agrupado_contratos['Indicador'] = 'Contrato'
        agrupado_contratos['MesAbreviado'] = info_data['mes_abreviado']
        agrupado_contratos['nr_dia_util'] = info_data['nr_dia_util']
        agrupado_contratos['quartil'] = info_data['quartil']
        agrupado_contratos['dt_mes'] = info_data['dt_mes']
        agrupado_contratos = agrupado_contratos.rename(columns={'CONTRATO': 'qte'})
        
        # CORREÇÃO: Para CPFs - pegar o último registro de cada CPF por faixa
        df_cpfs_unicos = df_intervalo.sort_values('DATA').groupby(['CPF', 'FX_ATRASO']).tail(1)
        
        # Agrupar por FX_ATRASO - CPFs ÚNICOS
        agrupado_cpfs = df_cpfs_unicos.groupby('FX_ATRASO').agg({
            'CPF': 'nunique',
            'VALORPRIN_FIN': 'sum'
        }).reset_index()
        
        agrupado_cpfs['DATA'] = data
        agrupado_cpfs['Indicador'] = 'Carteira (CPFs)'
        agrupado_cpfs['MesAbreviado'] = info_data['mes_abreviado']
        agrupado_cpfs['nr_dia_util'] = info_data['nr_dia_util']
        agrupado_cpfs['quartil'] = info_data['quartil']
        agrupado_cpfs['dt_mes'] = info_data['dt_mes']
        agrupado_cpfs = agrupado_cpfs.rename(columns={'CPF': 'qte'})
        
        resultados.append(agrupado_contratos)
        resultados.append(agrupado_cpfs)
    
    # Concatenar todos os resultados
    df_acumulado = pd.concat(resultados, ignore_index=True)
    
    # Reordenar colunas
    df_acumulado = df_acumulado[[
        'DATA',
        'Indicador',
        'qte',
        'FX_ATRASO',
        'MesAbreviado',
        'nr_dia_util',
        'quartil',
        'dt_mes',
        'VALORPRIN_FIN'
    ]]
    
    salvar_log("="*60)
    salvar_log("RESUMO FINAL MALING_HIST")
    salvar_log("="*60)
    salvar_log(f"Total de registros acumulados gerados: {len(df_acumulado)}")
    salvar_log(f"Registros de Contratos: {len(df_acumulado[df_acumulado['Indicador'] == 'Contrato'])}")
    salvar_log(f"Registros de CPFs: {len(df_acumulado[df_acumulado['Indicador'] == 'CPF'])}")
    salvar_log(f"Valor total final: R$ {df_acumulado['VALORPRIN_FIN'].sum():,.2f}")
    salvar_log("="*60)
    
    return df_acumulado

@registrar_tempo("Acumulado mailing hist unique")
def gerar_acumulado_maling_hist_unique(df_maling_hist, df_dw_calendario):
    """
    Gera DataFrame com acumulado de contratos e CPFs do maling_hist por dia útil.
    Conta todos os registros únicos sem agrupamento por faixa de atraso.
    """
    salvar_log("="*60)
    salvar_log("INÍCIO DO PROCESSAMENTO DE MALING_HIST")
    salvar_log("="*60)  
    salvar_log(f"Total de registros em df_maling_hist: {len(df_maling_hist)}")
    
    # Selecionar apenas as colunas necessárias
    df_reduzido = df_maling_hist[['DATA', 'CONTRATO', 'CPF', 'VALORPRIN_FIN']].copy()
    
    salvar_log(f"Memória reduzida - trabalhando com {len(df_reduzido.columns)} colunas")
    
    # Converter DATA para datetime
    df_reduzido['DATA'] = pd.to_datetime(df_reduzido['DATA'])
    
    # Preparar calendário
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    df_calendario_reduzido = df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']]
    
    # Merge com calendário
    df_reduzido = df_reduzido.merge(
        df_calendario_reduzido,
        left_on='DATA',
        right_on='dt_data',
        how='inner'
    ).drop(columns=['dt_data'])
    
    salvar_log(f"Registros após merge com calendário: {len(df_reduzido)}")
    
    resultados = []
    
    # Obter datas únicas ordenadas
    datas_unicas = sorted(df_reduzido['DATA'].unique())
    
    salvar_log(f"Total de datas únicas a processar: {len(datas_unicas)}")
    
    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...")
        
        # Obter o início do mês da data atual
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        
        # Filtrar do início do mês até a data atual
        df_intervalo = df_reduzido[
            (df_reduzido['DATA'] >= inicio_mes) & 
            (df_reduzido['DATA'] <= data)
        ].copy()
        
        # Pegar informações do calendário da data atual
        info_data = df_reduzido[df_reduzido['DATA'] == data][
            ['mes_abreviado', 'nr_dia_util', 'quartil', 'dt_mes']
        ].drop_duplicates()
        
        if len(info_data) == 0:
            continue
            
        info_data = info_data.iloc[0]
        
        # Para CONTRATOS - pegar o último registro de cada contrato único
        df_contratos_unicos = df_intervalo.sort_values('DATA').groupby('CONTRATO').tail(1)
        
        # Calcular totais de CONTRATOS ÚNICOS
        total_contratos = df_contratos_unicos['CONTRATO'].nunique()
        valor_contratos = df_contratos_unicos['VALORPRIN_FIN'].sum()
        
        resultado_contratos = {
            'DATA': data,
            'Indicador': 'Contratos',
            'qte': total_contratos,
            'FX_ATRASO': 'Unique',
            'MesAbreviado': info_data['mes_abreviado'],
            'nr_dia_util': info_data['nr_dia_util'],
            'quartil': info_data['quartil'],
            'dt_mes': info_data['dt_mes'],
            'VALORPRIN_FIN': valor_contratos
        }
        
        # Para CPFs - pegar o último registro de cada CPF único
        df_cpfs_unicos = df_intervalo.sort_values('DATA').groupby('CPF').tail(1)
        
        # Calcular totais de CPFs ÚNICOS
        total_cpfs = df_cpfs_unicos['CPF'].nunique()
        valor_cpfs = df_cpfs_unicos['VALORPRIN_FIN'].sum()
        
        resultado_cpfs = {
            'DATA': data,
            'Indicador': 'Carteira (CPFs)',
            'qte': total_cpfs,
            'FX_ATRASO': 'Unique',
            'MesAbreviado': info_data['mes_abreviado'],
            'nr_dia_util': info_data['nr_dia_util'],
            'quartil': info_data['quartil'],
            'dt_mes': info_data['dt_mes'],
            'VALORPRIN_FIN': valor_cpfs
        }
        
        resultados.append(resultado_contratos)
        resultados.append(resultado_cpfs)
    
    # Criar DataFrame final
    df_acumulado = pd.DataFrame(resultados)
    
    # Reordenar colunas
    df_acumulado = df_acumulado[[
        'DATA',
        'Indicador',
        'qte',
        'FX_ATRASO',
        'MesAbreviado',
        'nr_dia_util',
        'quartil',
        'dt_mes',
        'VALORPRIN_FIN'
    ]]
    
    salvar_log("="*60)
    salvar_log("RESUMO FINAL MALING_HIST")
    salvar_log("="*60)
    salvar_log(f"Total de registros acumulados gerados: {len(df_acumulado)}")
    salvar_log(f"Registros de Contratos: {len(df_acumulado[df_acumulado['Indicador'] == 'Contrato'])}")
    salvar_log(f"Registros de CPFs: {len(df_acumulado[df_acumulado['Indicador'] == 'CPF'])}")
    salvar_log(f"Valor total final: R$ {df_acumulado['VALORPRIN_FIN'].sum():,.2f}")
    salvar_log("="*60)
    
    return df_acumulado
# ============================================
# FUNÇÃO GENÉRICA
# ============================================

def gerar_acumulado_mailing_hist(df_maling_hist, df_dw_calendario):
    
    df_maling_acumulado_fxAtraso = gerar_acumulado_maling_hist_fxAtraso(df_maling_hist, df_dw_calendario)
    df_maling_acumulado_unique = gerar_acumulado_maling_hist_unique(df_maling_hist, df_dw_calendario)

    df_maling_acumulado = unir_dataframes(df_maling_acumulado_fxAtraso, df_maling_acumulado_unique)
    
    return df_maling_acumulado 

def criar_faixa_customizada(df, coluna, bins, labels, nome_nova_coluna=None):
    """
    Cria uma coluna de faixa customizada
    
    Args:
        df (pd.DataFrame): DataFrame
        coluna (str): Nome da coluna para categorizar
        bins (list): Lista de bins para pd.cut
        labels (list): Lista de labels para as faixas
        nome_nova_coluna (str, optional): Nome da nova coluna. Default: 'FX_{coluna}'
    
    Returns:
        pd.DataFrame: DataFrame com nova coluna
    """
    if nome_nova_coluna is None:
        nome_nova_coluna = f'FX_{coluna}'
    
    df[nome_nova_coluna] = pd.cut(
        df[coluna], 
        bins=bins, 
        labels=labels, 
        right=True
    )
    return df