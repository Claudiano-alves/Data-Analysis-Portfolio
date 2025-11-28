import pandas as pd
from datetime import datetime
import time
from functools import wraps
import os
from pathlib import Path
from typing import Dict

def salvar_analiticos_acionamentos(
    df_acionamentos_funil,
    df_analitico_acionamentos_humano,
    df_acion_semFaixa_humano,
    df_acion_semDescricao_humano,
    df_acion_semOrigem_humano,
    df_analitico_trestto,
    df_enriquecido_discagens_trestto_semFaixa,
    df_analitico_expert,
    df_enriquecido_discagens_expert_semFaixa,
    df_humano_tabulados_como_robo,
    df_dicagens_operacaoOutros,
    df_acionamentos_funil_long,
    encoding: str = "utf-8-sig",
    sep: str = ";",
    decimal: str = ","
) -> Dict[str, str]:
    """
    Salva todos os analíticos de acionamentos na pasta específica da GetNet.
    Arquivos são salvos com a data de hoje e substituem versões anteriores do mesmo dia.
    
    Parâmetros:
    -----------
    df_* : DataFrames retornados pela função acionamentos_funil()
    encoding : str
        Encoding do CSV (utf-8-sig para Excel BR)
    sep : str
        Separador de colunas (padrão ";")
    decimal : str
        Separador decimal (padrão ",")
    
    Retorna:
    --------
    Dict[str, str] : Dicionário com {nome_arquivo: caminho_completo}
    """
    
    # Caminho base dos analíticos
    base_path = Path(r"\\trc-dc-ad\Planejamento\MIS\CARTEIRAS\GetNet\Analíticos\Acionamentos")
    
    # Criar diretório se não existir
    base_path.mkdir(parents=True, exist_ok=True)
    
    # Data de hoje (apenas data, sem hora)
    data_hoje = datetime.now().strftime('%Y%m%d')
    
    # Mapeamento dos DataFrames com seus nomes de arquivo
    analiticos = {
        f"acionamentos_funil_{data_hoje}.csv": df_acionamentos_funil,
        f"analitico_acionamentos_humano_{data_hoje}.csv": df_analitico_acionamentos_humano,
        f"acion_semFaixa_humano_{data_hoje}.csv": df_acion_semFaixa_humano,
        f"acion_semDescricao_humano_{data_hoje}.csv": df_acion_semDescricao_humano,
        f"acion_semOrigem_humano_{data_hoje}.csv": df_acion_semOrigem_humano,
        f"analitico_trestto_{data_hoje}.csv": df_analitico_trestto,
        f"enriquecido_discagens_trestto_semFaixa_{data_hoje}.csv": df_enriquecido_discagens_trestto_semFaixa,
        f"analitico_expert_{data_hoje}.csv": df_analitico_expert,
        f"enriquecido_discagens_expert_semFaixa_{data_hoje}.csv": df_enriquecido_discagens_expert_semFaixa,
        f"humano_tabulados_como_robo_{data_hoje}.csv": df_humano_tabulados_como_robo,
        f"dicagens_operacaoOutros_{data_hoje}.csv": df_dicagens_operacaoOutros,
        f"acionamentos_funil_long_{data_hoje}.csv": df_acionamentos_funil_long,
    }
    
    arquivos_salvos = {}
    
    print("=" * 80)
    print("SALVANDO ANALÍTICOS DE ACIONAMENTOS")
    print("=" * 80)
    print(f"Destino: {base_path}")
    print(f"Data: {datetime.now().strftime('%d/%m/%Y')}\n")
    
    # Salvar cada DataFrame (substituirá se já existir)
    for nome_arquivo, df in analiticos.items():
        try:
            caminho_completo = base_path / nome_arquivo
            
            # Verificar se arquivo já existe (será substituído)
            existe = caminho_completo.exists()
            acao = "SUBSTITUÍDO" if existe else "CRIADO"
            
            df.to_csv(caminho_completo, index=False, sep=sep, decimal=decimal, encoding=encoding)
            arquivos_salvos[nome_arquivo] = str(caminho_completo)
            print(f"✓ {nome_arquivo:<60} ({len(df):>6} linhas) [{acao}]")
        except Exception as e:
            print(f"✗ {nome_arquivo:<60} ERRO: {str(e)}")
    
    print(f"\n{'=' * 80}")
    print(f"✅ {len(arquivos_salvos)}/{len(analiticos)} arquivos salvos com sucesso")
    print("=" * 80)
    
    return arquivos_salvos

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
    from data_wrangling_acionamentos import acionamentos_humano
    from data_wrangling_discagens_expert import acionamentos_expert
    from data_wrangling_discagens_trestto import acionamentos_trestto

    df_acionamentos_humano, df_analitico_acionamentos_humano, df_acion_semFaixa_humano, df_acion_semDescricao_humano, df_acion_semOrigem_humano = acionamentos_humano(df_tab_acionamentos, df_tabulacao_aciona, df_dw_calendario, df_maling_hist)

    df_acionamentos_expert, df_analitico_expert, df_enriquecido_discagens_expert_semFaixa, df_humano_tabulados_como_robo, df_dicagens_operacaoOutros = acionamentos_expert(df_discagens_expert, df_dw_calendario, df_maling_hist)
    df_acionamentos_trestto, df_analitico_trestto, df_enriquecido_discagens_trestto_semFaixa = acionamentos_trestto(df_discagens_trestto, df_maling_hist, df_dw_calendario)

    df_acionamentos_funil = unir_dataframes(df_acionamentos_humano, df_acionamentos_expert, df_acionamentos_trestto)
    df_acionamentos_funil_long = transformar_funil_formato_long(df_acionamentos_funil)

    return df_acionamentos_funil, df_analitico_acionamentos_humano, df_acion_semFaixa_humano, df_acion_semDescricao_humano, df_acion_semOrigem_humano, df_analitico_trestto, df_enriquecido_discagens_trestto_semFaixa, df_analitico_expert, df_enriquecido_discagens_expert_semFaixa, df_humano_tabulados_como_robo, df_dicagens_operacaoOutros, df_acionamentos_funil_long

def consolidar_dataframes(df_mailing_acumulado, df_pagamentos_funil, df_acionamentos_funil_long):
    """
    Consolida três DataFrames em um único DataFrame unificado.
    
    Parâmetros:
    -----------
    df_mailing_acumulado : DataFrame
        DataFrame com colunas: DATA, Indicador, qte, FX_ATRASO, MesAbreviado, 
        nr_dia_util, quartil, dt_mes, VALORPRIN_FIN
    
    df_pagamentos_funil : DataFrame
        DataFrame com colunas: DATA_PAGTO, Indicador, qte, FX_ATRASO, TIPO, 
        MesAbreviado, nr_dia_util, quartil, dt_mes, VALOR_PARC
    
    df_acionamentos_funil_long : DataFrame
        DataFrame com colunas: DATA, Indicador, qte, FX_ATRASO, ORIGEM, 
        MesAbreviado, nr_dia_util, quartil, dt_mes, VALORPRIN_FIN
    
    Retorna:
    --------
    DataFrame consolidado com colunas padronizadas:
        DATA, Indicador, qte, FX_ATRASO, TIPO_ORIGEM, MesAbreviado, 
        nr_dia_util, quartil, dt_mes, VALOR
    """
    import pandas as pd
    
    # Preparar df_mailing_acumulado
    df_mailing = df_mailing_acumulado.copy()
    df_mailing['TIPO_ORIGEM'] = ''
    df_mailing = df_mailing.rename(columns={'VALORPRIN_FIN': 'VALOR'})

    # Preparar df_pagamentos_funil
    df_pagamentos = df_pagamentos_funil.copy()
    df_pagamentos = df_pagamentos.rename(columns={
        'DATA_PAGTO': 'DATA',
        'TIPO': 'TIPO_ORIGEM',
        'VALOR_PARC': 'VALOR'
    })

    # Preparar df_acionamentos_funil_long
    df_acionamentos = df_acionamentos_funil_long.copy()
    df_acionamentos = df_acionamentos.rename(columns={
        'ORIGEM': 'TIPO_ORIGEM',
        'VALORPRIN_FIN': 'VALOR'
    })
    

    # Garantir que todos tenham as mesmas colunas na mesma ordem
    colunas_padrao = ['DATA', 'Indicador', 'qte', 'FX_ATRASO', 'TIPO_ORIGEM', 
                      'MesAbreviado', 'nr_dia_util', 'quartil', 'dt_mes', 'VALOR']
    
    df_mailing = df_mailing[colunas_padrao]
    df_pagamentos = df_pagamentos[colunas_padrao]
    df_acionamentos = df_acionamentos[colunas_padrao]
    
    # Concatenar os três DataFrames
    df_consolidado = pd.concat([df_mailing, df_pagamentos, df_acionamentos], ignore_index=True)
    
    df_consolidado['DATA'] = pd.to_datetime(df_consolidado['DATA']).dt.date

    return df_consolidado

def transformar_funil_formato_long(df_acionamentos_funil):
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
# Exemplo de uso:
# df_resultado = consolidar_dataframes(df_mailing_acumulado, df_pagamentos_funil, df_acionamentos_funil_long)

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