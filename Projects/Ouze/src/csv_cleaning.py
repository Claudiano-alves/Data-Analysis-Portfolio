from src.utils import unir_dataframes
import pandas as pd
import numpy as np


def csv_pagamentos(df_funil_powerBI):
    # df_funil_powerBI['qte'] = (
    # df_funil_powerBI['qte']
    # .astype(str)                # garante string
    # .str.replace('.', '', regex=False)   # remove separador de milhar
    # .str.replace(',', '.', regex=False)  # troca vírgula decimal por ponto
    # )

    # Lista de indicadores
    indicadores_pagamentos = ['($) Pagamentos', 'Pagamentos']
    indicadores_total = ['Total pagamentos', '($) Total pagamentos']

    # Filtrar apenas os indicadores desejados
    df_filtrado = df_funil_powerBI[df_funil_powerBI['indicador'].isin(indicadores_pagamentos + indicadores_total)]

    # Remover coluna 'poroduto'
    df_sem_poroduto = df_filtrado.drop(columns=['poroduto'])

    # -------------------------------
    # 1. Agrupar "Pagamentos" (mantendo coluna tipo)
    colunas_pagamentos = [
        'data', 'indicador', 'fx_atraso', 'tipo',
        'MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes'
    ]

    df_pagamentos = df_sem_poroduto[df_sem_poroduto['indicador'].isin(indicadores_pagamentos)]
    df_pagamentos_agrupado = df_pagamentos.groupby(colunas_pagamentos, as_index=False)['qte'].sum()

    # -------------------------------
    # 2. Agrupar "Total pagamentos" (sem coluna tipo)
    colunas_total = [
        'data', 'indicador', 'fx_atraso',
        'MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes'
    ]

    df_total = df_sem_poroduto[df_sem_poroduto['indicador'].isin(indicadores_total)]
    df_total_agrupado = df_total.groupby(colunas_total, as_index=False)['qte'].sum()

    #======================================================================================

    # Separar os dois indicadores
    df_pagamentos = df_pagamentos_agrupado[df_pagamentos_agrupado['indicador'] == 'Pagamentos'].copy()
    df_valorprin = df_pagamentos_agrupado[df_pagamentos_agrupado['indicador'] == '($) Pagamentos'].copy()

    # Renomear colunas para diferenciar
    df_pagamentos = df_pagamentos.rename(columns={'qte': 'qte'})
    df_valorprin = df_valorprin.rename(columns={'qte': 'VALORPRIN_FIN'})

    # Definir chaves de correspondência
    chaves = ['data', 'fx_atraso', 'tipo', 'MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']

    # Fazer merge alinhando Pagamentos com ($) Pagamentos
    df_unificado = pd.merge(
        df_pagamentos[chaves + ['qte']],
        df_valorprin[chaves + ['VALORPRIN_FIN']],
        on=chaves,
        how='left'
    )

    # Adicionar coluna Indicador (fixa como "Pagamentos")
    df_unificado['Indicador'] = 'Pagamentos'

    # Reordenar colunas conforme solicitado
    df_unificado = df_unificado[['data','Indicador','qte','fx_atraso','tipo',
                                'MesAbreviado','nr_dia_util','quartil','dt_mes','VALORPRIN_FIN']]

    #==============================================================================================

    # Separar os dois indicadores de Total
    df_total = df_total_agrupado[df_total_agrupado['indicador'] == 'Total pagamentos'].copy()
    df_total_valor = df_total_agrupado[df_total_agrupado['indicador'] == '($) Total pagamentos'].copy()

    # Renomear colunas
    df_total = df_total.rename(columns={'qte': 'qte'})
    df_total_valor = df_total_valor.rename(columns={'qte': 'VALORPRIN_FIN'})

    # Definir chaves de correspondência (sem 'tipo')
    chaves_total = ['data', 'fx_atraso', 'MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes']

    # Merge alinhando Total pagamentos com ($) Total pagamentos
    df_total_unificado = pd.merge(
        df_total[chaves_total + ['qte']],
        df_total_valor[chaves_total + ['VALORPRIN_FIN']],
        on=chaves_total,
        how='left'
    )

    # Adicionar coluna Indicador (fixa como "Total pagamentos")
    df_total_unificado['Indicador'] = 'Total pagamentos'

    # Reordenar colunas
    df_total_unificado = df_total_unificado[['data','Indicador','qte','fx_atraso',
                                            'MesAbreviado','nr_dia_util','quartil','dt_mes','VALORPRIN_FIN']]

    # -------------------------------
    # Agora unir com o df_unificado de Pagamentos
    df_pagamentosBI = pd.concat([df_unificado, df_total_unificado], ignore_index=True)

    return df_pagamentosBI

def csv_carteira_saldoDevedor(df_funil_powerBI):
    # Lista de indicadores
    indicadores_carteira = ['Carteira (CPFs)']
    indicadores_saldo = ['Saldo Devedor']

    # Filtrar apenas os indicadores desejados
    df_filtrado_carteira_saldo = df_funil_powerBI[df_funil_powerBI['indicador'].isin(indicadores_carteira + indicadores_saldo)]

    # Remover coluna 'poroduto'
    df_sem_poroduto_cs = df_filtrado_carteira_saldo.drop(columns=['poroduto'])

    # Colunas de agrupamento
    colunas_carteira = [
        'data', 'fx_atraso', 'MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes'
    ]

    # -------------------------------
    # Agrupar Carteira (CPFs)
    df_carteira = df_sem_poroduto_cs[df_sem_poroduto_cs['indicador'] == 'Carteira (CPFs)']
    # REMOVE DUPLICADOS ANTES DE AGRUPAR (mantém a linha com maior valor)
    df_carteira = df_carteira.sort_values('qte', ascending=False).drop_duplicates(subset=colunas_carteira, keep='first')
    df_carteira_agrupado = df_carteira.groupby(colunas_carteira, as_index=False)['qte'].sum()

    # -------------------------------
    # Agrupar Saldo Devedor
    df_saldo = df_sem_poroduto_cs[df_sem_poroduto_cs['indicador'] == 'Saldo Devedor']
    # REMOVE DUPLICADOS ANTES DE AGRUPAR (mantém a linha com maior valor)
    df_saldo = df_saldo.sort_values('qte', ascending=False).drop_duplicates(subset=colunas_carteira, keep='first')
    df_saldo_agrupado = df_saldo.groupby(colunas_carteira, as_index=False)['qte'].sum()

    # Renomear colunas
    df_carteira_agrupado = df_carteira_agrupado.rename(columns={'qte': 'qte'})
    df_saldo_agrupado = df_saldo_agrupado.rename(columns={'qte': 'VALORPRIN_FIN'})

    # -------------------------------
    # Merge alinhando Carteira (CPFs) com Saldo Devedor
    df_carteira_saldo_unificado = pd.merge(
        df_carteira_agrupado[colunas_carteira + ['qte']],
        df_saldo_agrupado[colunas_carteira + ['VALORPRIN_FIN']],
        on=colunas_carteira,
        how='left'
    )

    # Adicionar coluna Indicador e tipo
    df_carteira_saldo_unificado['Indicador'] = 'Carteira (CPFs)'
    df_carteira_saldo_unificado['tipo'] = ''

    # Reordenar colunas
    df_carteira_saldo_unificado = df_carteira_saldo_unificado[[
        'data', 'Indicador', 'qte', 'fx_atraso', 'tipo',
        'MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALORPRIN_FIN'
    ]]

    return df_carteira_saldo_unificado

def csv_promessa(df_funil_powerBI):

    # Lista de indicadores
    df_funil_powerBI['indicador'] = df_funil_powerBI['indicador'].str.strip()
    indicadores_carteira = ['Promessa']
    indicadores_saldo = ['($) PROMESSA']

    # Filtrar apenas os indicadores desejados e remover qte = 0
    df_filtrado_carteira_saldo = df_funil_powerBI[df_funil_powerBI['indicador'].isin(indicadores_carteira + indicadores_saldo)]

    # Remover coluna 'poroduto'
    df_sem_poroduto_cs = df_filtrado_carteira_saldo.drop(columns=['poroduto'])

    # -------------------------------
    # Agrupar Promessa
    colunas_carteira = [
        'data', 'fx_atraso', 'tipo', 'MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes'
    ]

    df_promessa = df_sem_poroduto_cs[df_sem_poroduto_cs['indicador'] == 'Promessa']
    df_promessa_agrupado = df_promessa.groupby(colunas_carteira, as_index=False)['qte'].sum()

    # -------------------------------
    # Agrupar ($) Promessa
    df_saldo = df_sem_poroduto_cs[df_sem_poroduto_cs['indicador'] == '($) PROMESSA']
    df_saldo_agrupado = df_saldo.groupby(colunas_carteira, as_index=False)['qte'].sum()

    # Renomear colunas
    df_promessa_agrupado = df_promessa_agrupado.rename(columns={'qte': 'qte'})
    df_saldo_agrupado = df_saldo_agrupado.rename(columns={'qte': 'VALORPRIN_FIN'})

    # -------------------------------
    # Merge alinhando Carteira (CPFs) com Saldo Devedor
    df_promessa_unificado = pd.merge(
        df_promessa_agrupado[colunas_carteira + ['qte']],
        df_saldo_agrupado[colunas_carteira + ['VALORPRIN_FIN']],
        on=colunas_carteira,
        how='left'
    )

    # Adicionar coluna Indicador (fixa como "Carteira (CPFs)")
    df_promessa_unificado['Indicador'] = 'Promessa'

    # Reordenar colunas
    df_promessa_unificado = df_promessa_unificado[['data','Indicador','qte','fx_atraso', 'tipo',
                                                            'MesAbreviado','nr_dia_util','quartil','dt_mes','VALORPRIN_FIN']]

    return df_promessa_unificado

def csv_outrosIndicadores(df_funil_powerBI):

    #df_funil_powerBI['qte'] = df_funil_powerBI['qte'].str.replace(',', '.', regex=False)
    #df_funil_powerBI['qte'] = pd.to_numeric(df_funil_powerBI['qte'], errors='coerce')
    # Lista de indicadores desejados
    indicadores_desejados = [
        '(#) EMAIL', '(#) SMS', '(#) WHATS',
        'Acionamentos', 'Contratos', 'CPC', 'CPCA', 'Trabalhado'
    ]

    # Filtrar apenas os indicadores desejados
    df_filtrado = df_funil_powerBI[df_funil_powerBI['indicador'].isin(indicadores_desejados)].copy()

    # Remover coluna 'poroduto'
    df_sem_poroduto = df_filtrado.drop(columns=['poroduto'])

    # Definir colunas de agrupamento (todas exceto qte)
    colunas_agrupamento = [
        'data', 'indicador', 'fx_atraso', 'tipo',
        'MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes'
    ]

    # Agrupar e somar qte
    df_agrupado = df_sem_poroduto.groupby(colunas_agrupamento, as_index=False)['qte'].sum()

    # Adicionar coluna 'valor' preenchida com 0
    df_agrupado['VALORPRIN_FIN'] = 0
    # Suponha que sua coluna se chame 'indicador'
    df_agrupado = df_agrupado.rename(columns={'indicador': 'Indicador'})

    # Reordenar colunas conforme solicitado
    df_agrupado = df_agrupado[['data','Indicador','qte','fx_atraso','tipo',
                            'MesAbreviado','nr_dia_util','quartil','dt_mes','VALORPRIN_FIN']]

    return df_agrupado

def tratar_base_csv():
    # Carregar o arquivo
    caminho = r"\\trc-dc-ad\Planejamento\MIS\CARTEIRAS\GetNet\funil_powerbi.csv"
    df_funil_powerBI = pd.read_csv(caminho, encoding='windows-1252', sep=';')

    # Ajustar coluna qte
    df_funil_powerBI['qte'] = (
        df_funil_powerBI['qte']
        .astype(str)                # garante string
        .str.strip()                # remove espaços extras
        .str.replace(r'\.(?=\d{3}(,|$))', '', regex=True)  # remove pontos de milhar
        .str.replace(',', '.', regex=False)                # troca vírgula decimal por ponto
    )

    # Converter para número
    df_funil_powerBI['qte'] = pd.to_numeric(df_funil_powerBI['qte'], errors='coerce')

    df_pagamentos = csv_pagamentos(df_funil_powerBI)
    df_carteira_saldoDevedor = csv_carteira_saldoDevedor(df_funil_powerBI)
    df_promessa = csv_promessa(df_funil_powerBI)
    df_outrosIndicadores = csv_outrosIndicadores(df_funil_powerBI)

    df_csvBI_padronizado = unir_dataframes(df_pagamentos, df_carteira_saldoDevedor, df_promessa, df_outrosIndicadores)

    return df_csvBI_padronizado

