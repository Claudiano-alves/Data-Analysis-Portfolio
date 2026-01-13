import numpy as np
import pandas as pd
from utils import unir_dataframes, salvar_log, registrar_tempo

# ============================================
# DICIONÁRIOS E CONSTANTES
# ============================================

DDD_ESTADO = {
    '11': 'SP', '12': 'SP', '13': 'SP', '14': 'SP', '15': 'SP', '16': 'SP', '17': 'SP', '18': 'SP', '19': 'SP',
    '21': 'RJ', '22': 'RJ', '24': 'RJ',
    '27': 'ES', '28': 'ES',
    '31': 'MG', '32': 'MG', '33': 'MG', '34': 'MG', '35': 'MG', '37': 'MG', '38': 'MG',
    '41': 'PR', '42': 'PR', '43': 'PR', '44': 'PR', '45': 'PR', '46': 'PR',
    '47': 'SC', '48': 'SC', '49': 'SC',
    '51': 'RS', '53': 'RS', '54': 'RS', '55': 'RS',
    '61': 'DF', '62': 'GO', '63': 'TO', '64': 'GO', '65': 'MT', '66': 'MT', '67': 'MS',
    '68': 'AC', '69': 'RO',
    '71': 'BA', '73': 'BA', '74': 'BA', '75': 'BA', '77': 'BA',
    '79': 'SE',
    '81': 'PE', '82': 'AL', '83': 'PB', '84': 'RN', '85': 'CE', '86': 'PI', '87': 'PE', '88': 'CE', '89': 'PI',
    '91': 'PA', '92': 'AM', '93': 'PA', '94': 'PA', '95': 'RR', '96': 'AP', '97': 'AM', '98': 'MA', '99': 'MA'
}

# ============================================
# FUNÇÕES DE TRATAMENTO - DISCAGENS
# ============================================

def adicionar_operacao(df):
    """
    Adiciona a coluna OPERACAO ao DataFrame de discagens
    
    Args:
        df (pd.DataFrame): DataFrame com coluna 'GrupoPrincipal'
    
    Returns:
        pd.DataFrame: DataFrame com nova coluna 'OPERACAO'
    """
    conditions = [
        df['GrupoPrincipal'] == 4118,
        df['GrupoPrincipal'] == 4022,
        df['GrupoPrincipal'] == 4017,
        df['GrupoPrincipal'].isin([4047, 4679, 4681, 4683, 4671]),
        df['GrupoPrincipal'].isin([4433, 4504]),
        df['GrupoPrincipal'].isin([4326, 4636, 4637, 4649])
    ]
    
    choices = [
        'ATIVO',
        'MANUAL',
        'RECEPTIVO',
        'URA CPC',
        'PREVENTIVO',
        'AGV NEGOCIADORA'
    ]
    
    df['OPERACAO'] = np.select(conditions, choices, default='Outros')
    return df


def adicionar_estado_por_ddd(df, coluna_ddd='ddd'):
    """
    Adiciona a coluna ESTADO baseada no DDD
    
    Args:
        df (pd.DataFrame): DataFrame com coluna de DDD
        coluna_ddd (str): Nome da coluna que contém o DDD
    
    Returns:
        pd.DataFrame: DataFrame com nova coluna 'ESTADO'
    """
    df['ESTADO'] = df[coluna_ddd].astype(str).str.zfill(2).map(DDD_ESTADO)
    return df

# TRATANDO DISCAGENS EXPERT - ADICIONAR ORIGEM (ROBO OU HUMANO)
def adicionar_definir_humano_robo(df_discagens_expert):
    """
    Adiciona a coluna ESTADO baseada no DDD
    
    Args:
        df (pd.DataFrame): DataFrame com coluna de DDD
        coluna_ddd (str): Nome da coluna que contém o DDD
    
    Returns:
        pd.DataFrame: DataFrame com nova coluna 'ESTADO'
    """
    df_discagens_expert = df_discagens_expert.copy()
    df_discagens_expert['ORIGEM'] = df_discagens_expert['OPERACAO'].apply(
        lambda x: 'Robô' if x == 'AGV NEGOCIADORA' else 'HUMANO'
    )
    return df_discagens_expert

@registrar_tempo("Enriquecimento base de discagens expert")
def tratar_base_discagens(df):
    """
    Aplica todos os tratamentos padrão para base de discagens
    
    Args:
        df (pd.DataFrame): DataFrame de discagens
    
    Returns:
        pd.DataFrame: DataFrame tratado
    """
    df = adicionar_operacao(df)
    df = adicionar_estado_por_ddd(df)
    df = adicionar_definir_humano_robo(df)

    salvar_log(f"✅ Enriquecido!")
    return df

# MONTA DF COM AS TABULAÇÕES DO ROBÔ

@registrar_tempo("DF com as tabulações do Robô")
def df_acionamento_robo():
    """
    Cria um DataFrame com classificação de códigos de tabulação.
    
    Returns:
        pd.DataFrame: DataFrame com colunas:
            - COD_TABULACAO: código de tabulação
            - ALO: 1 se o código é classificado como Alô, 0 caso contrário
            - CPC: 1 se o código é classificado como CPC, 0 caso contrário
            - CPCA: 1 se o código é classificado como CPCA, 0 caso contrário
            - PROMESSA: 1 se o código é classificado como Promessa, 0 caso contrário
    """
    # Definir os códigos de cada categoria
    codigos_alo = [
        '10','13','15','20','21','23','24','4','45','6','16','120','152','159','161',
        '19','28','31','32','37','38','48','51','7','30','42','160','27','36','39',
        '47','1','12','14','2','5','8','83','84','85','86','88','89','9','99','113',
        '136','50','101','103','114','115','25','43','46','52','87'
    ]
    
    codigos_cpc = [
        '10','13','15','20','21','23','24','4','45','6','16','120','152','159','161',
        '19','28','31','32','37','38','48','51','7','30','42','160','27','36','39',
        '47','86','25','43','46','87'
    ]
    
    codigos_cpca = [
        '10','13','15','20','21','23','24','4','45','6','16','120','152','159','161',
        '19','28','31','32','37','38','48','51','7','42','160','25','43','46'
    ]
    
    codigos_promessa = [
        '10','13','15','20','21','23','24','4','45','6','160','25','43','46'
    ]
    
    # Obter todos os códigos únicos
    todos_codigos = sorted(set(codigos_alo + codigos_cpc + codigos_cpca + codigos_promessa))
    
    # Criar o DataFrame
    df_tabulacoes = pd.DataFrame({
        'COD_TABULACAO': todos_codigos
    })
    
    # Adicionar as colunas de classificação
    df_tabulacoes['ACIONAMENTOS'] = df_tabulacoes['COD_TABULACAO'].isin(codigos_alo).astype(int)
    df_tabulacoes['CPC'] = df_tabulacoes['COD_TABULACAO'].isin(codigos_cpc).astype(int)
    df_tabulacoes['CPCA'] = df_tabulacoes['COD_TABULACAO'].isin(codigos_cpca).astype(int)
    df_tabulacoes['PROMESSA'] = df_tabulacoes['COD_TABULACAO'].isin(codigos_promessa).astype(int)
    
    salvar_log("="*80)
    salvar_log(f"✅ DataFrame de classificação de tabulações criado!")
    salvar_log(f"   📊 Total de códigos: {len(df_tabulacoes)}")
    salvar_log(f"   ✓ ACIONAMENTOS: {df_tabulacoes['ACIONAMENTOS'].sum()} códigos")
    salvar_log(f"   ✓ CPC: {df_tabulacoes['CPC'].sum()} códigos")
    salvar_log(f"   ✓ CPCA: {df_tabulacoes['CPCA'].sum()} códigos")
    salvar_log(f"   ✓ PROMESSA: {df_tabulacoes['PROMESSA'].sum()} códigos")
    salvar_log("="*80)
    
    return df_tabulacoes

# CRUZAMENTO QUE DEFINE AS TABULAÇÕES
@registrar_tempo("Discagens Expert")
def enriquecer_discagens_expert(df_discagens_expert, df_acionamentos_tabulacaoRobo):
    """
    Enriquece o DataFrame de discagens_expert com classificações de tabulação.
    
    Args:
        df_discagens_expert (pd.DataFrame): DataFrame com discagens do expert,
            deve conter a coluna 'codtabulacao'
        df_acionamentos_tabulacaoRobo (pd.DataFrame): DataFrame com classificações,
            deve conter as colunas 'COD_TABULACAO', 'ACIONAMENTOS', 'CPC', 'CPCA', 'PROMESSA'
    
    Returns:
        pd.DataFrame: DataFrame enriquecido com as colunas TRABALHADO, ACIONAMENTOS, CPC, CPCA e PROMESSA
    """
    salvar_log("="*80)
    salvar_log(f"📊 Merge com classificações de tabulação...")
    salvar_log(f"   Registros antes: {len(df_discagens_expert):,}")
    
    # Realizar o merge
    df_resultado = df_discagens_expert.merge(
        df_acionamentos_tabulacaoRobo[['COD_TABULACAO', 'ACIONAMENTOS', 'CPC', 'CPCA', 'PROMESSA']],
        left_on='codtabulacao',
        right_on='COD_TABULACAO',
        how='left'
    ).drop(columns=['COD_TABULACAO'])  # Remove coluna duplicada após merge
    
    # Adicionar coluna TRABALHADO com valor 1 para todas as linhas
    # Inserir antes da coluna ACIONAMENTOS
    col_idx = df_resultado.columns.get_loc('ACIONAMENTOS')
    df_resultado.insert(col_idx, 'TRABALHADO', 1)
    
    # Substituir valores NaN por 0 nas colunas de indicadores
    colunas_indicadores = ['ACIONAMENTOS', 'CPC', 'CPCA', 'PROMESSA']
    df_resultado[colunas_indicadores] = df_resultado[colunas_indicadores].fillna(0)
    
    salvar_log(f"   ✓ Registros após merge: {len(df_resultado):,}")
    salvar_log(f"   ✓ Registros perdidos: {len(df_discagens_expert) - len(df_resultado):,}")
    
    # Estatísticas das classificações
    salvar_log(f"\n📈 Distribuição das classificações:")
    salvar_log(f"   ✓ TRABALHADO: {df_resultado['TRABALHADO'].sum():,} registros")
    salvar_log(f"   ✓ ACIONAMENTOS: {int(df_resultado['ACIONAMENTOS'].sum()):,} registros")
    salvar_log(f"   ✓ CPC: {int(df_resultado['CPC'].sum()):,} registros")
    salvar_log(f"   ✓ CPCA: {int(df_resultado['CPCA'].sum()):,} registros")
    salvar_log(f"   ✓ PROMESSA: {int(df_resultado['PROMESSA'].sum()):,} registros")
    salvar_log("="*80)

    return df_resultado

@registrar_tempo("Enriquecimento maling hist e calendário")
def enriquecer_com_mailing_calendario(df_discagens, df_mailing_hist, df_dw_calendario):
    """
    Enriquece o DataFrame de discagens com dados de mailing_hist e calendário.
    
    Args:
        df_discagens (pd.DataFrame): DataFrame com discagens enriquecidas,
            deve conter as colunas 'CONTRATO' e 'DATA'
        df_mailing_hist (pd.DataFrame): DataFrame com histórico de mailing,
            deve conter as colunas 'CONTRATO', 'DATA', 'FX_ATRASO'
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário,
            deve conter as colunas 'dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    
    Returns:
        tuple: (df_com_fx_atraso, df_sem_fx_atraso)
            - df_com_fx_atraso: DataFrame enriquecido com FX_ATRASO preenchido
            - df_sem_fx_atraso: DataFrame enriquecido sem FX_ATRASO (valores nulos)
    """
    
    df_resultado = df_discagens.copy()
    
    # Padronizar CONTRATO como maiúscula
    df_resultado['CONTRATO'] = df_resultado['CONTRATO'].astype(str).str.upper().str.strip()
    
    # ============================================
    # ENRIQUECER COM MAILING_HIST
    # ============================================
    df_mailing_temp = df_mailing_hist[['CONTRATO', 'DATA', 'FX_ATRASO', 'VALORPRIN_FIN']].copy()
    df_mailing_temp['CONTRATO'] = df_mailing_temp['CONTRATO'].astype(str).str.upper().str.strip()
    
    # Padronizar data antes do merge
    df_resultado['DATA'] = pd.to_datetime(df_resultado['DATA']).dt.date
    df_mailing_temp['DATA'] = pd.to_datetime(df_mailing_temp['DATA']).dt.date
    
    salvar_log("="*80)
    salvar_log(f"📊 Merge com mailing_hist...")
    salvar_log(f"   Registros antes: {len(df_resultado):,}")
    
    df_resultado = df_resultado.merge(
        df_mailing_temp,
        on=['CONTRATO', 'DATA'],
        how='left'
    )
    
    salvar_log(f"   ✓ Registros após merge: {len(df_resultado):,}")
    registros_sem_fx = df_resultado['FX_ATRASO'].isna().sum()
    salvar_log(f"   ⚠️  Registros sem FX_ATRASO: {registros_sem_fx:,}")
    
    # ============================================
    # ENRIQUECER COM DW_CALENDARIO
    # ============================================
    df_resultado['DATA'] = pd.to_datetime(df_resultado['DATA']).dt.date
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data']).dt.date
    
    df_calendario_reduzido = df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']]
    
    salvar_log(f"\n📅 Merge com dw_calendario...")
    salvar_log(f"   Registros antes: {len(df_resultado):,}")
    
    df_resultado = df_resultado.merge(
        df_calendario_reduzido,
        left_on='DATA',
        right_on='dt_data',
        how='left'
    ).drop(columns=['dt_data'])  # Remove coluna duplicada após merge
    
    salvar_log(f"   ✓ Registros após merge: {len(df_resultado):,}")
    registros_sem_calendario = df_resultado['nr_dia_util'].isna().sum()
    salvar_log(f"   ⚠️  Registros sem dados de calendário: {registros_sem_calendario:,}")
    
    # ============================================
    # SEPARAR EM DOIS DATAFRAMES
    # ============================================
    df_com_fx_atraso = df_resultado[df_resultado['FX_ATRASO'].notna()].copy()
    df_sem_fx_atraso = df_resultado[df_resultado['FX_ATRASO'].isna()].copy()
    
    salvar_log(f"\n📦 Separação dos DataFrames:")
    salvar_log(f"   ✓ Registros COM FX_ATRASO: {len(df_com_fx_atraso):,}")
    salvar_log(f"   ✓ Registros SEM FX_ATRASO: {len(df_sem_fx_atraso):,}")
    salvar_log("="*80)
    return df_com_fx_atraso, df_sem_fx_atraso

@registrar_tempo("Segmentando base de discagens enriquecida")
def segmentacao_discagens(df):
    """
    Separa o DataFrame em 3 grupos conforme critérios específicos:
    1. ORIGEM = 'Humano' E ACIONAMENTOS = 1
    2. OPERACAO = 'Outros'
    3. Restante (demais registros)
    
    Parâmetros:
    -----------
    df : pandas.DataFrame
        DataFrame original com os dados de discagens
        
    Retorna:
    --------
    dict : Dicionário com 3 DataFrames
        - 'humano_primeiro_acionamento': Registros com ORIGEM='Humano' e ACIONAMENTOS=1
        - 'operacao_outros': Registros com OPERACAO='Outros'
        - 'restante': Demais registros
    """
    
    # Criar cópias para evitar warnings do pandas
    df_trabalho = df.copy()
    
    # 1. Filtrar casos: ORIGEM = "Humano" E ACIONAMENTOS = 1
    condicao_humano = (df_trabalho['ORIGEM'] == 'Humano') & (df_trabalho['ACIONAMENTOS'] == 1)
    df_humano_primeiro = df_trabalho[condicao_humano].copy()
    
    # Remover esses registros do DataFrame de trabalho
    df_trabalho = df_trabalho[~condicao_humano].copy()
    
    # 2. Filtrar casos: OPERACAO = "Outros"
    condicao_outros = df_trabalho['OPERACAO'] == 'Outros'
    df_operacao_outros = df_trabalho[condicao_outros].copy()
    
    # 3. Restante dos dados
    df_restante = df_trabalho[~condicao_outros].copy()
    
    # salvar_log("=" * 60)
    # salvar_log("RESUMO DA SEPARAÇÃO DE DADOS")
    # salvar_log("=" * 60)

    # total = len(df)

    # salvar_log(f"Total de registros original: {total:,}")

    # if total == 0:
    #     salvar_log("\n1. Humano + Primeiro Acionamento: 0 registros (0.00%)")
    #     salvar_log("2. Operação 'Outros': 0 registros (0.00%)")
    #     salvar_log("3. Restante: 0 registros (0.00%)")
    # else:
    #     salvar_log(
    #         f"\n1. Humano + Primeiro Acionamento: "
    #         f"{len(df_humano_primeiro):,} registros "
    #         f"({len(df_humano_primeiro) / total * 100:.2f}%)"
    #     )
    #     salvar_log(
    #         f"2. Operação 'Outros': "
    #         f"{len(df_operacao_outros):,} registros "
    #         f"({len(df_operacao_outros) / total * 100:.2f}%)"
    #     )
    #     salvar_log(
    #         f"3. Restante: "
    #         f"{len(df_restante):,} registros "
    #         f"({len(df_restante) / total * 100:.2f}%)"
    #     )

    # salvar_log(
    #     f"\nVerificação: "
    #     f"{len(df_humano_primeiro) + len(df_operacao_outros) + len(df_restante):,} registros"
    # )

    # salvar_log("=" * 60)


    # Exibir resumo da separação
    salvar_log("="*60)
    salvar_log("RESUMO DA SEPARAÇÃO DE DADOS")
    salvar_log("="*60)
    salvar_log(f"Total de registros original: {len(df):,}")
    salvar_log(f"\n1. Humano + Primeiro Acionamento: {len(df_humano_primeiro):,} registros ({len(df_humano_primeiro)/len(df)*100:.2f}%)")
    salvar_log(f"2. Operação 'Outros': {len(df_operacao_outros):,} registros ({len(df_operacao_outros)/len(df)*100:.2f}%)")
    salvar_log(f"3. Restante: {len(df_restante):,} registros ({len(df_restante)/len(df)*100:.2f}%)")
    salvar_log(f"\nVerificação: {len(df_humano_primeiro) + len(df_operacao_outros) + len(df_restante):,} registros")
    salvar_log("="*60)
    
    # Retornar os três DataFrames separados
    return df_restante, df_humano_primeiro, df_operacao_outros


# FUNIL

@registrar_tempo("Funil unique expert")
def acionamentos_unique_expert(df_com_fx_atraso, df_dw_calendario):
    """
    Gera contagem acumulada mensal de acionamentos únicos por CPF (melhor score) por FX_ATRASO e ORIGEM.
    
    Args:
        df_com_fx_atraso (pd.DataFrame): DataFrame enriquecido com FX_ATRASO preenchido
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais únicas por faixa de atraso e origem
    """
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    df = df_com_fx_atraso.copy()
    
    # Criar coluna ORIGEM com valor "Robô"
    
    df['DATA'] = pd.to_datetime(df['DATA'])

    # Datas únicas ordenadas
    datas_unicas = df['DATA'].sort_values().unique()
    resultados = []

    salvar_log("="*80)
    salvar_log(f"📊 Processando acumulado mensal ÚNICO (melhor score por CPF) para {len(datas_unicas)} datas...")

    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...")
        
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        
        # 1. Filtrar intervalo do início do mês até a data atual
        df_intervalo = df[(df['DATA'] >= inicio_mes) & (df['DATA'] <= data)].copy()
        
        # 2. Calcular score de tabulação
        df_intervalo['TABULACAO_SCORE'] = (
            df_intervalo['PROMESSA'].astype(int) * 4 +
            df_intervalo['CPCA'].astype(int) * 3 +
            df_intervalo['CPC'].astype(int) * 2 +
            df_intervalo['ACIONAMENTOS'].astype(int) * 1
        )
        
        # 3. Ordenar por CPF e score (maior score primeiro)
        df_intervalo = df_intervalo.sort_values(
            ['CPF', 'TABULACAO_SCORE'],
            ascending=[True, False]
        )
        
        # 4. Manter apenas o melhor score por CPF (unique)
        df_unique = df_intervalo.drop_duplicates(
            subset=['CPF'],
            keep='first'
        ).copy()
        
        # 5. Agrupar por FX_ATRASO e ORIGEM
        agrupado = df_unique.groupby(['FX_ATRASO', 'ORIGEM']).apply(lambda g: pd.Series({
            'TRABALHADO': g['TRABALHADO'].sum(),
            'VALORPRIN_FIN_TRABALHADO': g.loc[g['TRABALHADO'] == 1, 'VALORPRIN_FIN'].sum(),
            'ACIONAMENTOS': g['ACIONAMENTOS'].sum(),
            'VALORPRIN_FIN_ACIONAMENTOS': g.loc[g['ACIONAMENTOS'] == 1, 'VALORPRIN_FIN'].sum(),
            'CPC': g['CPC'].sum(),
            'VALORPRIN_FIN_CPC': g.loc[g['CPC'] == 1, 'VALORPRIN_FIN'].sum(),
            'CPCA': g['CPCA'].sum(),
            'VALORPRIN_FIN_CPCA': g.loc[g['CPCA'] == 1, 'VALORPRIN_FIN'].sum(),
            'PROMESSA': g['PROMESSA'].sum(),
            'VALORPRIN_FIN_PROMESSA': g.loc[g['PROMESSA'] == 1, 'VALORPRIN_FIN'].sum()
        })).reset_index()
        
        agrupado['DATA'] = data
        
        resultados.append(agrupado)

    # Concatenar tudo
    df_final = pd.concat(resultados, ignore_index=True)

    # Cruzar com calendário
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    
    salvar_log(f"\n📅 Merge com dw_calendario...")
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA',
        right_on='dt_data',
        how='inner'
    ).drop(columns=['dt_data'])

    # Identificar colunas numéricas
    colunas_numericas = df_final.select_dtypes(include=['number']).columns

    # Preencher NaN com 0 apenas nas colunas numéricas
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)

    # Reordenar colunas
    colunas_ordenadas = [
        'DATA', 'FX_ATRASO', 'ORIGEM',
        'TRABALHADO', 'VALORPRIN_FIN_TRABALHADO',
        'ACIONAMENTOS', 'VALORPRIN_FIN_ACIONAMENTOS',
        'CPC', 'VALORPRIN_FIN_CPC',
        'CPCA', 'VALORPRIN_FIN_CPCA',
        'PROMESSA', 'VALORPRIN_FIN_PROMESSA',
        'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    ]
    df_final = df_final[colunas_ordenadas]

    salvar_log(f"   ✓ Registros finais: {len(df_final):,}")
    salvar_log(f"\n📈 Totais acumulados ÚNICOS (última data):")
    salvar_log(f"   ✓ TRABALHADO: {df_final[df_final['DATA'] == df_final['DATA'].max()]['TRABALHADO'].sum():,}")
    salvar_log(f"   ✓ ACIONAMENTOS: {df_final[df_final['DATA'] == df_final['DATA'].max()]['ACIONAMENTOS'].sum():,}")
    salvar_log(f"   ✓ CPC: {df_final[df_final['DATA'] == df_final['DATA'].max()]['CPC'].sum():,}")
    salvar_log(f"   ✓ CPCA: {df_final[df_final['DATA'] == df_final['DATA'].max()]['CPCA'].sum():,}")
    salvar_log(f"   ✓ PROMESSA: {df_final[df_final['DATA'] == df_final['DATA'].max()]['PROMESSA'].sum():,}")
    salvar_log("="*80)

    df_final['FX_ATRASO'] = 'Unique'
    return df_final

#def acionamentos_unique_expert(df_com_fx_atraso, df_dw_calendario):
    """
    Gera contagem acumulada mensal de acionamentos únicos por CPF (melhor score) por FX_ATRASO e ORIGEM.
    
    Args:
        df_com_fx_atraso (pd.DataFrame): DataFrame enriquecido com FX_ATRASO preenchido
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais únicas por faixa de atraso e origem
    """
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    df = df_com_fx_atraso.copy()
    
    # Criar coluna ORIGEM com valor "Robô"
    df['ORIGEM'] = 'Robô'
    
    df['DATA'] = pd.to_datetime(df['DATA'])

    # Datas únicas ordenadas
    datas_unicas = df['DATA'].sort_values().unique()
    resultados = []

    print(f"📊 Processando acumulado mensal ÚNICO (melhor score por CPF) para {len(datas_unicas)} datas...")

    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            print(f"   Processando {i}/{len(datas_unicas)} datas...")
        
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        
        # 1. Filtrar intervalo do início do mês até a data atual
        df_intervalo = df[(df['DATA'] >= inicio_mes) & (df['DATA'] <= data)].copy()
        
        # 2. Calcular score de tabulação
        df_intervalo['TABULACAO_SCORE'] = (
            df_intervalo['PROMESSA'].astype(int) * 3 +
            df_intervalo['CPCA'].astype(int) * 2 +
            df_intervalo['CPC'].astype(int) * 1
        )
        
        # 3. Ordenar por CPF e score (maior score primeiro)
        df_intervalo = df_intervalo.sort_values(
            ['CPF', 'TABULACAO_SCORE'],
            ascending=[True, False]
        )
        
        # 4. Manter apenas o melhor score por CPF (unique)
        df_unique = df_intervalo.drop_duplicates(
            subset=['CPF'],
            keep='first'
        ).copy()
        
        # 5. Agrupar por FX_ATRASO e ORIGEM
        agrupado = df_unique.groupby(['FX_ATRASO', 'ORIGEM']).apply(lambda g: pd.Series({
            'ACIONAMENTOS': g['ACIONAMENTOS'].sum(),
            'VALORPRIN_FIN_ACIONAMENTOS': g.loc[g['ACIONAMENTOS'] == 1, 'VALORPRIN_FIN'].sum(),
            'CPC': g['CPC'].sum(),
            'VALORPRIN_FIN_CPC': g.loc[g['CPC'] == 1, 'VALORPRIN_FIN'].sum(),
            'CPCA': g['CPCA'].sum(),
            'VALORPRIN_FIN_CPCA': g.loc[g['CPCA'] == 1, 'VALORPRIN_FIN'].sum(),
            'PROMESSA': g['PROMESSA'].sum(),
            'VALORPRIN_FIN_PROMESSA': g.loc[g['PROMESSA'] == 1, 'VALORPRIN_FIN'].sum()
        })).reset_index()
        
        agrupado['DATA'] = data
        
        resultados.append(agrupado)

    # Concatenar tudo
    df_final = pd.concat(resultados, ignore_index=True)

    # Cruzar com calendário
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    
    print(f"\n📅 Merge com dw_calendario...")
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA',
        right_on='dt_data',
        how='left'
    ).drop(columns=['dt_data'])

    # Identificar colunas numéricas
    colunas_numericas = df_final.select_dtypes(include=['number']).columns

    # Preencher NaN com 0 apenas nas colunas numéricas
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)

    # Reordenar colunas
    colunas_ordenadas = [
        'DATA', 'FX_ATRASO', 'ORIGEM',
        'ACIONAMENTOS', 'VALORPRIN_FIN_ACIONAMENTOS',
        'CPC', 'VALORPRIN_FIN_CPC',
        'CPCA', 'VALORPRIN_FIN_CPCA',
        'PROMESSA', 'VALORPRIN_FIN_PROMESSA',
        'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    ]
    df_final = df_final[colunas_ordenadas]

    print(f"   ✓ Registros finais: {len(df_final):,}")
    print(f"\n📈 Totais acumulados ÚNICOS (última data):")
    print(f"   ✓ ACIONAMENTOS: {df_final[df_final['DATA'] == df_final['DATA'].max()]['ACIONAMENTOS'].sum():,}")
    print(f"   ✓ CPC: {df_final[df_final['DATA'] == df_final['DATA'].max()]['CPC'].sum():,}")
    print(f"   ✓ CPCA: {df_final[df_final['DATA'] == df_final['DATA'].max()]['CPCA'].sum():,}")
    print(f"   ✓ PROMESSA: {df_final[df_final['DATA'] == df_final['DATA'].max()]['PROMESSA'].sum():,}")

    df_final['FX_ATRASO'] = 'Unique'
    return df_final

@registrar_tempo("Funil esforço expert")
def acionamentos_esforco_expert(df_com_fx_atraso, df_dw_calendario):
    """
    Gera contagem acumulada mensal de acionamentos por FX_ATRASO e ORIGEM.
    
    Args:
        df_com_fx_atraso (pd.DataFrame): DataFrame enriquecido com FX_ATRASO preenchido
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais por faixa de atraso e origem
    """
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    df = df_com_fx_atraso.copy()
    
    # Criar coluna ORIGEM com valor "Robô"
    
    df['DATA'] = pd.to_datetime(df['DATA'])

    # Datas únicas ordenadas
    datas_unicas = df['DATA'].sort_values().unique()
    resultados = []

    salvar_log("="*80)
    salvar_log(f"📊 Processando acumulado mensal para {len(datas_unicas)} datas...")

    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...")
        
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        
        # 1. Filtrar intervalo do início do mês até a data atual
        df_intervalo = df[(df['DATA'] >= inicio_mes) & (df['DATA'] <= data)].copy()
        
        # 2. Agrupar por FX_ATRASO e ORIGEM (sem deduplicação, soma tudo)
        agrupado = df_intervalo.groupby(['FX_ATRASO', 'ORIGEM']).apply(lambda g: pd.Series({
            'TRABALHADO': g['TRABALHADO'].sum(),
            'VALORPRIN_FIN_TRABALHADO': g.loc[g['TRABALHADO'] == 1, 'VALORPRIN_FIN'].sum(),
            'ACIONAMENTOS': g['ACIONAMENTOS'].sum(),
            'VALORPRIN_FIN_ACIONAMENTOS': g.loc[g['ACIONAMENTOS'] == 1, 'VALORPRIN_FIN'].sum(),
            'CPC': g['CPC'].sum(),
            'VALORPRIN_FIN_CPC': g.loc[g['CPC'] == 1, 'VALORPRIN_FIN'].sum(),
            'CPCA': g['CPCA'].sum(),
            'VALORPRIN_FIN_CPCA': g.loc[g['CPCA'] == 1, 'VALORPRIN_FIN'].sum(),
            'PROMESSA': g['PROMESSA'].sum(),
            'VALORPRIN_FIN_PROMESSA': g.loc[g['PROMESSA'] == 1, 'VALORPRIN_FIN'].sum()
        })).reset_index()
        
        agrupado['DATA'] = data
        resultados.append(agrupado)

    # Concatenar tudo
    df_final = pd.concat(resultados, ignore_index=True)

    # Cruzar com calendário
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    
    salvar_log(f"\n📅 Merge com dw_calendario...")
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA',
        right_on='dt_data',
        how='inner'
    ).drop(columns=['dt_data'])

    # Identificar colunas numéricas
    colunas_numericas = df_final.select_dtypes(include=['number']).columns

    # Preencher NaN com 0 apenas nas colunas numéricas
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)

    # Reordenar colunas
    colunas_ordenadas = [
        'DATA', 'FX_ATRASO', 'ORIGEM',
        'TRABALHADO', 'VALORPRIN_FIN_TRABALHADO',
        'ACIONAMENTOS', 'VALORPRIN_FIN_ACIONAMENTOS',
        'CPC', 'VALORPRIN_FIN_CPC',
        'CPCA', 'VALORPRIN_FIN_CPCA',
        'PROMESSA', 'VALORPRIN_FIN_PROMESSA',
        'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    ]
    df_final = df_final[colunas_ordenadas]

    salvar_log(f"   ✓ Registros finais: {len(df_final):,}")
    salvar_log(f"\n📈 Totais acumulados:")
    salvar_log(f"   ✓ TRABALHADO: {df_final[df_final['DATA'] == df_final['DATA'].max()]['TRABALHADO'].sum():,}")
    salvar_log(f"   ✓ ACIONAMENTOS: {df_final[df_final['DATA'] == df_final['DATA'].max()]['ACIONAMENTOS'].sum():,}")
    salvar_log(f"   ✓ CPC: {df_final[df_final['DATA'] == df_final['DATA'].max()]['CPC'].sum():,}")
    salvar_log(f"   ✓ CPCA: {df_final[df_final['DATA'] == df_final['DATA'].max()]['CPCA'].sum():,}")
    salvar_log(f"   ✓ PROMESSA: {df_final[df_final['DATA'] == df_final['DATA'].max()]['PROMESSA'].sum():,}")
    salvar_log("="*80)

    df_final['FX_ATRASO'] = 'Esforço'
    return df_final

#def acionamentos_esforco_expert(df_com_fx_atraso, df_dw_calendario):
    """
    Gera contagem acumulada mensal de acionamentos por FX_ATRASO e ORIGEM.
    
    Args:
        df_com_fx_atraso (pd.DataFrame): DataFrame enriquecido com FX_ATRASO preenchido
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais por faixa de atraso e origem
    """
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    df = df_com_fx_atraso.copy()
    
    # Criar coluna ORIGEM com valor "Robô"
    df['ORIGEM'] = 'Robô'
    
    df['DATA'] = pd.to_datetime(df['DATA'])

    # Datas únicas ordenadas
    datas_unicas = df['DATA'].sort_values().unique()
    resultados = []

    print(f"📊 Processando acumulado mensal para {len(datas_unicas)} datas...")

    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            print(f"   Processando {i}/{len(datas_unicas)} datas...")
        
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        
        # 1. Filtrar intervalo do início do mês até a data atual
        df_intervalo = df[(df['DATA'] >= inicio_mes) & (df['DATA'] <= data)].copy()
        
        # 2. Agrupar por FX_ATRASO e ORIGEM (sem deduplicação, soma tudo)
        agrupado = df_intervalo.groupby(['FX_ATRASO', 'ORIGEM']).apply(lambda g: pd.Series({
            'ACIONAMENTOS': g['ACIONAMENTOS'].sum(),
            'VALORPRIN_FIN_ACIONAMENTOS': g.loc[g['ACIONAMENTOS'] == 1, 'VALORPRIN_FIN'].sum(),
            'CPC': g['CPC'].sum(),
            'VALORPRIN_FIN_CPC': g.loc[g['CPC'] == 1, 'VALORPRIN_FIN'].sum(),
            'CPCA': g['CPCA'].sum(),
            'VALORPRIN_FIN_CPCA': g.loc[g['CPCA'] == 1, 'VALORPRIN_FIN'].sum(),
            'PROMESSA': g['PROMESSA'].sum(),
            'VALORPRIN_FIN_PROMESSA': g.loc[g['PROMESSA'] == 1, 'VALORPRIN_FIN'].sum()
        })).reset_index()
        
        agrupado['DATA'] = data
        resultados.append(agrupado)

    # Concatenar tudo
    df_final = pd.concat(resultados, ignore_index=True)

    # Cruzar com calendário
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    
    print(f"\n📅 Merge com dw_calendario...")
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA',
        right_on='dt_data',
        how='left'
    ).drop(columns=['dt_data'])

    # Identificar colunas numéricas
    colunas_numericas = df_final.select_dtypes(include=['number']).columns

    # Preencher NaN com 0 apenas nas colunas numéricas
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)

    # Reordenar colunas
    colunas_ordenadas = [
        'DATA', 'FX_ATRASO', 'ORIGEM',
        'ACIONAMENTOS', 'VALORPRIN_FIN_ACIONAMENTOS',
        'CPC', 'VALORPRIN_FIN_CPC',
        'CPCA', 'VALORPRIN_FIN_CPCA',
        'PROMESSA', 'VALORPRIN_FIN_PROMESSA',
        'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    ]
    df_final = df_final[colunas_ordenadas]

    print(f"   ✓ Registros finais: {len(df_final):,}")
    print(f"\n📈 Totais acumulados:")
    print(f"   ✓ ACIONAMENTOS: {df_final[df_final['DATA'] == df_final['DATA'].max()]['ACIONAMENTOS'].sum():,}")
    print(f"   ✓ CPC: {df_final[df_final['DATA'] == df_final['DATA'].max()]['CPC'].sum():,}")
    print(f"   ✓ CPCA: {df_final[df_final['DATA'] == df_final['DATA'].max()]['CPCA'].sum():,}")
    print(f"   ✓ PROMESSA: {df_final[df_final['DATA'] == df_final['DATA'].max()]['PROMESSA'].sum():,}")

    df_final['FX_ATRASO'] = 'Esforço'
    return df_final

@registrar_tempo("Funil fxAtraso e Origem expert")
def acionamentos_fxAtraso_origem_expert(df_com_fx_atraso, df_dw_calendario):
    """
    Gera contagem acumulada mensal de acionamentos únicos por CPF e FX_ATRASO (melhor score) por ORIGEM.
    Permite que o mesmo CPF seja contado em faixas de atraso diferentes.
    
    Args:
        df_com_fx_atraso (pd.DataFrame): DataFrame enriquecido com FX_ATRASO preenchido
        df_dw_calendario (pd.DataFrame): DataFrame com dados de calendário
    
    Returns:
        pd.DataFrame: DataFrame com contagens acumuladas mensais únicas por CPF e faixa de atraso
    """
    import warnings
    warnings.filterwarnings("ignore", category=FutureWarning)

    df = df_com_fx_atraso.copy()
    
    df['DATA'] = pd.to_datetime(df['DATA'])

    # Datas únicas ordenadas
    datas_unicas = df['DATA'].sort_values().unique()
    resultados = []

    salvar_log("="*80)
    salvar_log(f"📊 Processando acumulado mensal ÚNICO por CPF + FX_ATRASO (melhor score) para {len(datas_unicas)} datas...")

    for i, data in enumerate(datas_unicas, 1):
        if i % 10 == 0 or i == len(datas_unicas):
            salvar_log(f"   Processando {i}/{len(datas_unicas)} datas...")
        
        inicio_mes = pd.Timestamp(data.year, data.month, 1)
        
        # 1. Filtrar intervalo do início do mês até a data atual
        df_intervalo = df[(df['DATA'] >= inicio_mes) & (df['DATA'] <= data)].copy()
        
        # 2. Calcular score de tabulação
        df_intervalo['TABULACAO_SCORE'] = (
            df_intervalo['PROMESSA'].astype(int) * 4 +
            df_intervalo['CPCA'].astype(int) * 3 +
            df_intervalo['CPC'].astype(int) * 2 +
            df_intervalo['ACIONAMENTOS'].astype(int) * 1
        )
        
        # 3. Ordenar por CPF, FX_ATRASO e score (maior score primeiro)
        df_intervalo = df_intervalo.sort_values(
            ['CPF', 'FX_ATRASO', 'TABULACAO_SCORE'],
            ascending=[True, True, False]
        )
        
        # 4. Manter apenas o melhor score por CPF + FX_ATRASO (unique por faixa)
        df_unique = df_intervalo.drop_duplicates(
            subset=['CPF', 'FX_ATRASO'],
            keep='first'
        ).copy()
        
        # 5. Agrupar por FX_ATRASO e ORIGEM
        agrupado = df_unique.groupby(['FX_ATRASO', 'ORIGEM']).apply(lambda g: pd.Series({
            'TRABALHADO': g['TRABALHADO'].sum(),
            'VALORPRIN_FIN_TRABALHADO': g.loc[g['TRABALHADO'] == 1, 'VALORPRIN_FIN'].sum(),
            'ACIONAMENTOS': g['ACIONAMENTOS'].sum(),
            'VALORPRIN_FIN_ACIONAMENTOS': g.loc[g['ACIONAMENTOS'] == 1, 'VALORPRIN_FIN'].sum(),
            'CPC': g['CPC'].sum(),
            'VALORPRIN_FIN_CPC': g.loc[g['CPC'] == 1, 'VALORPRIN_FIN'].sum(),
            'CPCA': g['CPCA'].sum(),
            'VALORPRIN_FIN_CPCA': g.loc[g['CPCA'] == 1, 'VALORPRIN_FIN'].sum(),
            'PROMESSA': g['PROMESSA'].sum(),
            'VALORPRIN_FIN_PROMESSA': g.loc[g['PROMESSA'] == 1, 'VALORPRIN_FIN'].sum()
        })).reset_index()
        
        agrupado['DATA'] = data
        resultados.append(agrupado)

    # Concatenar tudo
    df_final = pd.concat(resultados, ignore_index=True)

    # Cruzar com calendário
    df_dw_calendario_temp = df_dw_calendario.copy()
    df_dw_calendario_temp['dt_data'] = pd.to_datetime(df_dw_calendario_temp['dt_data'])
    
    salvar_log(f"\n📅 Merge com dw_calendario...")
    df_final = df_final.merge(
        df_dw_calendario_temp[['dt_data', 'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado']],
        left_on='DATA',
        right_on='dt_data',
        how='inner'
    ).drop(columns=['dt_data'])

    # Identificar colunas numéricas
    colunas_numericas = df_final.select_dtypes(include=['number']).columns

    # Preencher NaN com 0 apenas nas colunas numéricas
    df_final[colunas_numericas] = df_final[colunas_numericas].fillna(0)

    # Reordenar colunas
    colunas_ordenadas = [
        'DATA', 'FX_ATRASO', 'ORIGEM',
        'TRABALHADO', 'VALORPRIN_FIN_TRABALHADO',
        'ACIONAMENTOS', 'VALORPRIN_FIN_ACIONAMENTOS',
        'CPC', 'VALORPRIN_FIN_CPC',
        'CPCA', 'VALORPRIN_FIN_CPCA',
        'PROMESSA', 'VALORPRIN_FIN_PROMESSA',
        'nr_dia_util', 'quartil', 'dt_mes', 'mes_abreviado'
    ]
    df_final = df_final[colunas_ordenadas]

    salvar_log(f"   ✓ Registros finais: {len(df_final):,}")
    salvar_log(f"\n📈 Totais acumulados ÚNICOS por CPF+FAIXA (última data):")
    salvar_log(f"   ✓ TRABALHADO: {df_final[df_final['DATA'] == df_final['DATA'].max()]['TRABALHADO'].sum():,}")
    salvar_log(f"   ✓ ACIONAMENTOS: {df_final[df_final['DATA'] == df_final['DATA'].max()]['ACIONAMENTOS'].sum():,}")
    salvar_log(f"   ✓ CPC: {df_final[df_final['DATA'] == df_final['DATA'].max()]['CPC'].sum():,}")
    salvar_log(f"   ✓ CPCA: {df_final[df_final['DATA'] == df_final['DATA'].max()]['CPCA'].sum():,}")
    salvar_log(f"   ✓ PROMESSA: {df_final[df_final['DATA'] == df_final['DATA'].max()]['PROMESSA'].sum():,}")
    salvar_log("="*80)

    return df_final

def vadicao_unique(df_acionamentos_unique_expert, df_enriquecer_discagens_expert_limpo):
    soma_trabalhado_0930 = df_acionamentos_unique_expert.loc[
    df_acionamentos_unique_expert['DATA'] == '2025-09-30', 'TRABALHADO'].sum()
    print(f"🔹 Soma de 'trabalhado' em df_acionamentos_unique_expert na data 2025-09-30: {soma_trabalhado_0930:,}")
    total_cpfs_unicos = df_enriquecer_discagens_expert_limpo['CPF'].nunique()
    print(f"🔹 Total de CPFs únicos: {total_cpfs_unicos:,}")

def vadicao_fxAtraso(df_acionamentos_origem_fxAtraso_expert, df_enriquecer_discagens_expert_limpo):
    soma_trabalhado_0930 = df_acionamentos_origem_fxAtraso_expert.loc[
    df_acionamentos_origem_fxAtraso_expert['DATA'] == '2025-09-30', 'TRABALHADO'].sum()
    print(f"🔹 Soma de 'trabalhado' em df_acionamentos_origem_fxAtraso_expert na data 2025-09-30: {soma_trabalhado_0930:,}")
    total_cpfs_unicos = df_enriquecer_discagens_expert_limpo[['CPF', 'FX_ATRASO']].drop_duplicates().shape[0]
    print(f"🔹 Total de combinações únicas CPF + FX_ATRASO: {total_cpfs_unicos:,}")

def validacao_esforco(df_acionamentos_esforco_expert, df_enriquecer_discagens_expert_limpo):
    df_acionamentos_esforco_expert['DATA'] = pd.to_datetime(df_acionamentos_esforco_expert['DATA'])
    soma_trabalhado_0930 = df_acionamentos_esforco_expert.loc[
        df_acionamentos_esforco_expert['DATA'] == '2025-09-30', 'TRABALHADO'
    ].sum()
    print(f"🔹 Soma de 'trabalhado' em df_acionamentos_esforco_expert na data 2025-09-30: {soma_trabalhado_0930:,}")
    total_cpfs = df_enriquecer_discagens_expert_limpo['CPF'].count()
    print(f"🔹 Total de combinações de CPFs: {total_cpfs:,}")

def acionamentos_expert(df_discagens_expert, df_dw_calendario, df_maling_hist):

    df_acionamentos_tabulacaoRobo = df_acionamento_robo()
    df_enriquecido_discagens_expert = enriquecer_discagens_expert(df_discagens_expert, df_acionamentos_tabulacaoRobo)
    df_enriquecido_discagens_expert = tratar_base_discagens(df_enriquecido_discagens_expert)
    df_enriquecido_discagens_expert_comFaixa, df_enriquecido_discagens_expert_semFaixa = enriquecer_com_mailing_calendario(df_enriquecido_discagens_expert, df_maling_hist, df_dw_calendario)
    df_enriquecido_discagens_expert_limpo, df_humano_tabulados_como_robo, df_dicagens_operacaoOutros = segmentacao_discagens(df_enriquecido_discagens_expert_comFaixa)

    df_acionamentos_esforco_expert = acionamentos_esforco_expert(df_enriquecido_discagens_expert_limpo, df_dw_calendario)
    df_acionamentos_unique_expert = acionamentos_unique_expert(df_enriquecido_discagens_expert_limpo, df_dw_calendario)
    df_acionamentos_origem_fxAtraso_expert = acionamentos_fxAtraso_origem_expert(df_enriquecido_discagens_expert_limpo, df_dw_calendario)

    df_analitico_expert = df_enriquecido_discagens_expert_limpo.copy()
    df_acionamentos_expert = unir_dataframes(df_acionamentos_origem_fxAtraso_expert, df_acionamentos_unique_expert, df_acionamentos_esforco_expert)

    return df_acionamentos_expert, df_analitico_expert, df_enriquecido_discagens_expert_semFaixa, df_humano_tabulados_como_robo, df_dicagens_operacaoOutros
