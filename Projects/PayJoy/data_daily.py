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
from excel_utils import atualizar_arquivo_excel_por_df, xlsx_file

# Logging configuration: use INFO by default; set PAYJOY_DEBUG=1 to see debug messages
log_level = logging.DEBUG if os.getenv('PAYJOY_DEBUG', '0') == '1' else logging.INFO
logging.basicConfig(level=log_level, format='[%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def calcular_data_inicio(maior_data_xlsx):
    """
    Calcula a data de início para a consulta (dia seguinte à última data do arquivo).
    """
    if maior_data_xlsx is None:
        print(f"\n{'='*80}")
        print("ERRO: Não foi possível identificar a data inicial!")
        print(f"{'='*80}")
        sys.exit(1)
    
    # Se for datetime, converter para date; se já for date, usar direto
    if isinstance(maior_data_xlsx, datetime):
        data_maior = maior_data_xlsx.date()
    else:
        data_maior = maior_data_xlsx
    
    data_inicio = data_maior + timedelta(days=1)
    print(f"\nData início para consulta: {data_inicio.strftime('%Y-%m-%d')}")
    return data_inicio

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

    # *** CRÍTICO: Converte a coluna DATA para o formato DD/MM/YYYY ***
    df_resultado['DATA'] = pd.to_datetime(df_resultado['DATA']).dt.strftime('%d/%m/%Y')

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

    # *** NOVO: Ordena as colunas de data cronologicamente ***
    colunas_fixas = ['FAIXA', 'Indicador']
    colunas_data = [col for col in df_final.columns if col not in colunas_fixas]

    # Ordena as datas convertendo de volta para datetime temporariamente
    colunas_data_ordenadas = sorted(colunas_data, key=lambda x: pd.to_datetime(x, format='%d/%m/%Y'))

    # Reorganiza o DataFrame com as colunas ordenadas
    df_final = df_final[colunas_fixas + colunas_data_ordenadas]

    # Ordena por FAIXA e Indicador
    df_final = df_final.sort_values(['FAIXA', 'Indicador']).reset_index(drop=True)

    # Visualiza o resultado
    print(f"\n{'='*60}")
    print(f"Estrutura final:")
    print(f"Total de linhas: {len(df_final)}")
    print(f"Faixas únicas: {df_final['FAIXA'].unique()}")
    print(f"Colunas: {df_final.columns.tolist()}")
    print(f"{'='*60}")
    print("\nPrimeiras 30 linhas:")
    print(df_final.head(30))

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

    # AJUSTE: Adiciona ano 2026 à coluna DATA se ainda não tiver
    def adicionar_ano(data_str):
        """Adiciona /2026 se a data estiver no formato DD/MM"""
        data_str = str(data_str).strip()
        if '/' in data_str:
            partes = data_str.split('/')
            if len(partes) == 2:  # formato DD/MM
                return f"{partes[0]}/{partes[1]}/2026"
            elif len(partes) == 3:  # formato DD/MM/YYYY
                return data_str
        return data_str
    
    df_mailings_com_faixa['DATA'] = df_mailings_com_faixa['DATA'].apply(adicionar_ano)

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
            """Converte DD/MM/YYYY para tupla (ano, mes, dia) para ordenação"""
            try:
                partes = data_str.split('/')
                if len(partes) == 3:  # DD/MM/YYYY
                    return (int(partes[2]), int(partes[1]), int(partes[0]))
                elif len(partes) == 2:  # DD/MM (fallback)
                    return (2026, int(partes[1]), int(partes[0]))
                else:
                    return (9999, 99, 99)
            except:
                return (9999, 99, 99)
        
        colunas_ordenadas = sorted(df_pivot.columns, key=ordenar_data)
        df_pivot = df_pivot[colunas_ordenadas]
    
    # Reset index
    df_pivot = df_pivot.reset_index()
    
    return df_pivot

def converter_data_para_formato_pivot(col_data):
    """
    Converte uma coluna de data para o formato DD/MM usado pelo df_pivot.
    
    Args:
        col_data: pandas.Timestamp, datetime, ou string (formatos suportados: YYYY-MM-DD)
    
    Returns:
        String no formato 'D/M' ou 'DD/MM' (ex: '4/12', '15/3')
        None se não conseguir converter
    """
    try:
        # Se for timestamp/datetime do pandas ou Python
        if hasattr(col_data, 'day') and hasattr(col_data, 'month'):
            dia = col_data.day
            mes = col_data.month
            return f"{dia}/{mes}"
        
        # Se for string no formato YYYY-MM-DD
        if isinstance(col_data, str):
            if '-' in col_data:
                partes = col_data.split('-')
                if len(partes) >= 3:
                    ano, mes, dia = int(partes[0]), int(partes[1]), int(partes[2])
                    return f"{dia}/{mes}"
            # Se já está em formato DD/MM, retorna como está
            elif '/' in col_data:
                return col_data
        
        return None
    except Exception as e:
        logger.debug(f"Erro ao converter data {col_data}: {e}")
        return None

def substituir_zeros_com_pivot(df_transposto, df_pivot):
    """
    Substitui valores 0 do df_transposto pelos valores do df_pivot,
    respeitando FAIXA + data.
    
    Args:
        df_transposto: DataFrame com estrutura [DATA, FAIXA, Indicador, Valor] (formato longo)
        df_pivot: DataFrame com estrutura [FAIXA, 02/01/2026, 05/01/2026, ...] (formato largo)
    
    Returns:
        DataFrame atualizado no formato original
    """
    
    print("Iniciando substituição...")
    
    # Verificar estrutura
    if 'DATA' in df_transposto.columns and 'Valor' in df_transposto.columns:
        # DataFrame está em formato longo - converter para largo primeiro
        df_largo = df_transposto.pivot_table(
            index=['FAIXA', 'Indicador'],
            columns='DATA',
            values='Valor',
            aggfunc='first'
        ).reset_index()
        
        # Remove o nome do índice das colunas
        df_largo.columns.name = None
        
        print(f"Convertido para formato largo: {df_largo.shape}")
        print(f"Colunas: {df_largo.columns.tolist()[:5]}...")
        
        formato_longo = True
        df_trabalho = df_largo.copy()
    else:
        # Já está em formato largo
        formato_longo = False
        df_trabalho = df_transposto.copy()
    
    # Pegar as colunas de data
    colunas_data = [col for col in df_trabalho.columns if col not in ['FAIXA', 'Indicador']]
    
    print(f"Colunas de data identificadas: {len(colunas_data)}")
    print(f"Primeiras datas: {colunas_data[:3]}")
    
    substituicoes = 0
    
    # Para cada linha do df_pivot
    for _, row_pivot in df_pivot.iterrows():
        faixa = row_pivot["FAIXA"]
        
        # Para cada coluna de data no df_pivot (exceto FAIXA)
        for col_pivot in df_pivot.columns:
            if col_pivot == 'FAIXA':
                continue
            
            # Verificar se essa data existe no df_trabalho
            if col_pivot in colunas_data:
                valor_pivot = row_pivot[col_pivot]
                
                # Substituir os zeros APENAS para o indicador Reachable_Portfolio
                mask = (
                    (df_trabalho["FAIXA"] == faixa) & 
                    (df_trabalho["Indicador"] == "Reachable_Portfolio") &
                    (df_trabalho[col_pivot] == 0)
                )
                
                linhas_afetadas = mask.sum()
                
                if linhas_afetadas > 0:
                    df_trabalho.loc[mask, col_pivot] = valor_pivot
                    substituicoes += 1
                    
                    if substituicoes <= 3:  # Mostra apenas as primeiras 3
                        print(f"  ✓ FAIXA {faixa}, Data {col_pivot}: 0 -> {valor_pivot}")
    
    print(f"\nTotal de substituições: {substituicoes}")
    
    # Se estava em formato longo, converter de volta
    if formato_longo:
        # Converter de volta para formato longo
        df_resultado = df_trabalho.melt(
            id_vars=['FAIXA', 'Indicador'],
            var_name='DATA',
            value_name='Valor'
        )
        
        # Ordenar como estava antes
        df_resultado = df_resultado.sort_values(['DATA', 'FAIXA', 'Indicador']).reset_index(drop=True)
        
        print(f"Convertido de volta para formato longo: {df_resultado.shape}")
        
        return df_resultado
    else:
        return df_trabalho

if __name__ == "__main__":
    from data_loader import consolidar_mailings_payjoy, df_contratos_payjoy, processa_payjoy
    # Executa o processamento principal   
    df_resultado = processa_payjoy()  
    df_final = transforma_dados(df_resultado)
    df_contratos = df_contratos_payjoy()
    df_mailings = consolidar_mailings_payjoy()
    df_pivot = consolidar_mailings_por_faixa(df_mailings, df_contratos)
    df_PAYJOY = substituir_zeros_com_pivot(df_final, df_pivot)

    # DEBUG: Filtrar e exibir apenas Reachable_Portfolio
    print(f"\n{'='*80}")
    print("ANÁLISE: Reachable_Portfolio no df_PAYJOY")
    print(f"{'='*80}")
    df_filtrado = df_PAYJOY[df_PAYJOY["Indicador"] == "Reachable_Portfolio"]
    print(f"\nTotal de linhas com Reachable_Portfolio: {len(df_filtrado)}")
    print(f"\nFaixas com essa informação: {df_filtrado['FAIXA'].unique().tolist()}")
    print(f"\nDataframe completo (Reachable_Portfolio):")
    print(df_final.to_string())
    print(df_pivot.to_string())
    print(df_filtrado.to_string())
    print(f"{'='*80}\n")

    # Atualiza o arquivo Excel com os novos dados a partir do df_PAYJOY
    resumo = atualizar_arquivo_excel_por_df(xlsx_file, df_PAYJOY)
    if resumo.get('updated'):
        logger.info(f"Atualização concluída: colunas adicionadas: {resumo.get('colunas_adicionadas')} - valores inseridos: {resumo.get('dados_inseridos')}")
    else:
        logger.info(f"Nenhuma atualização realizada. Motivo: {resumo.get('reason', resumo.get('error', 'sem alterações'))}")