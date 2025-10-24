
import pandas as pd

def tratar_discagens_trestto(df, df_mailing_hist):
    """
    Aplica tratamentos para base de discagens Trestto com segmentação por PRODUTO e FX_ATRASO
    Retorna dois DataFrames: esforço total e únicos (CPFs únicos por métrica)
    """
    df = df.copy()
    df_mailing_hist = df_mailing_hist.copy()
    
    # Converter DATA para o mesmo tipo
    df['DATA'] = pd.to_datetime(df['DATA']).dt.date
    df_mailing_hist['DATA'] = pd.to_datetime(df_mailing_hist['DATA']).dt.date
    
    # Garantir que CPF está como string
    df['CPF'] = df['CPF'].astype(str)
    df_mailing_hist['CPF'] = df_mailing_hist['CPF'].astype(str)
    
    print(f"📊 Antes da consolidação - Trestto: {len(df):,}")
    
    # ✅ CONSOLIDAR TRESTTO ANTES DO MERGE (por DATA + CPF)
    colunas_metricas = ['DISCAGEM', 'ALO', 'CPC', 'CPCA', 'PROMESSA']
    df_consolidado = df.groupby(['DATA', 'CPF'], as_index=False)[colunas_metricas].sum()
    
    print(f"📊 Após consolidação - Trestto: {len(df_consolidado):,}")
    print(f"📊 Mailing: {len(df_mailing_hist):,}")
    
    # Fazer o join com mailing_hist por CPF E DATA
    df_join = df_consolidado.merge(
        df_mailing_hist[['DATA', 'CPF', 'PRODUTO', 'FX_ATRASO']].drop_duplicates(), 
        on=['CPF', 'DATA'], 
        how='inner'
    )
    
    print(f"📊 Após join: {len(df_join):,}")
    
    # TOTAL TRESTTO ESFORÇO DIÁRIO (segmentado por PRODUTO e FX_ATRASO)
    df_esforco = df_join.groupby(['DATA', 'PRODUTO', 'FX_ATRASO'], as_index=False)[colunas_metricas].sum()
    
    # TOTAL TRESTTO UNIQUE DIÁRIO (segmentado por PRODUTO e FX_ATRASO)
    df_unique = df_join.copy()
    
    # Aplica a transformação: > 0 vira 1
    for col in colunas_metricas:
        df_unique[col] = (df_unique[col] > 0).astype(int)
    
    # Agrupa por data, produto e faixa de atraso
    df_unique = df_unique.groupby(['DATA', 'PRODUTO', 'FX_ATRASO'], as_index=False)[colunas_metricas].sum()
    
    return df_esforco, df_unique

# def tratar_discagens_trestto(df):
#     """
#     Aplica tratamentos para base de discagens Trestto
#     Retorna dois DataFrames: esforço total e únicos (CPFs únicos por métrica)
    
#     Args:
#         df (pd.DataFrame): DataFrame de discagens Trestto
    
#     Returns:
#         tuple: (df_esforco, df_unique)
#             - df_esforco: Soma total de discagens por dia
#             - df_unique: Contagem de CPFs únicos que tiveram cada métrica por dia
#     """
#     # TOTAL TRESTTO ESFORÇO DIÁRIO
#     df_esforco = df.groupby('DATA').agg({
#         'DISCAGEM': 'sum', 
#         'ALO': 'sum',  
#         'CPC': 'sum',     
#         'CPCA': 'sum',  
#         'PROMESSA': 'sum'
#     }).reset_index()
    
#     # TOTAL TRESTTO UNIQUE DIÁRIO
#     df_unique = df.copy()
    
#     # Colunas para transformar em binário
#     colunas_para_binario = ['DISCAGEM', 'ALO', 'CPC', 'CPCA', 'PROMESSA']
    
#     # Aplica a transformação: > 0 vira 1
#     df_unique[colunas_para_binario] = df_unique[colunas_para_binario].applymap(
#         lambda x: 1 if x > 0 else 0
#     )
    
#     # Agrupa por data
#     df_unique = df_unique.groupby('DATA').agg({
#         'DISCAGEM': 'sum', 
#         'ALO': 'sum',  
#         'CPC': 'sum',     
#         'CPCA': 'sum',  
#         'PROMESSA': 'sum'
#     }).reset_index()
    
#     return df_esforco, df_unique