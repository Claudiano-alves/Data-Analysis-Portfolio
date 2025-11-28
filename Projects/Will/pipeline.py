import pandas as pd
from datetime import datetime, timedelta
from db_connection import get_connection

def obter_ultima_data_arquivo(caminho_csv):
    """
    Lê o arquivo CSV e retorna a última data de atendimento.
    
    Parâmetros:
    -----------
    caminho_csv : str
        Caminho do arquivo CSV
    
    Retorna:
    --------
    datetime : Última data encontrada no arquivo
    """
    try:
        # Ler CSV com delimitador ponto e vírgula
        df = pd.read_csv(caminho_csv, encoding='utf-8-sig', sep=';')
        
        # Verificar se a coluna existe
        print(f"Colunas encontradas no arquivo ({len(df.columns)} colunas):")
        print(list(df.columns)[:10])  # Mostrar apenas as 10 primeiras
        
        if 'data_atendimento' not in df.columns:
            raise ValueError("Coluna 'data_atendimento' não encontrada no arquivo")
        
        # Mostrar exemplo de como a data está no arquivo
        print(f"Exemplo de data no arquivo: {df['data_atendimento'].iloc[0]}")
        
        # Tentar converter para datetime com dayfirst=True (formato brasileiro)
        df['data_atendimento'] = pd.to_datetime(df['data_atendimento'], dayfirst=True, errors='coerce')
        
        # Verificar se conseguiu converter
        if df['data_atendimento'].isna().all():
            raise ValueError("Não foi possível converter nenhuma data. Verifique o formato das datas no arquivo.")
        
        # Obter a última data
        ultima_data = df['data_atendimento'].max()
        
        # Verificar se a última data é válida
        if pd.isna(ultima_data):
            raise ValueError("Última data é inválida (NaT)")
        
        print(f"Última data encontrada no arquivo: {ultima_data.strftime('%d/%m/%Y')}")
        
        return ultima_data
        
    except Exception as e:
        print(f"Erro ao ler arquivo: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

def consultar_tempos_operacionais(conn, data_inicio, data_fim):
    """
    Consulta dados de tempos operacionais do SQL Server.
    
    Parâmetros:
    -----------
    conn : pyodbc.Connection
        Conexão ativa com o banco de dados
    data_inicio : datetime ou str
        Data inicial do período (formato: 'YYYY-MM-DD' ou datetime)
    data_fim : datetime ou str
        Data final do período (formato: 'YYYY-MM-DD' ou datetime)
    
    Retorna:
    --------
    DataFrame : Dados consultados
    """
    
    # Converter para datetime se necessário
    if isinstance(data_inicio, str):
        data_inicio = datetime.strptime(data_inicio, '%Y-%m-%d')
    if isinstance(data_fim, str):
        data_fim = datetime.strptime(data_fim, '%Y-%m-%d')
    
    query = """
    SELECT
        COALESCE(CAST(data_insert AS DATE), '1900-01-01') as data_insert,
        COALESCE(CAST(hora_insert AS TIME), '00:00:00') as hora_insert,
        COALESCE(CAST(id_agente AS BIGINT), 0) as id_agente,
        LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(COALESCE(nome_agente, ''), 'JOÃO', 'JOAO'), CHAR(9), ''), CHAR(13), ''), CHAR(10), ''))) as nome_agente,
        COALESCE(cpf_agente, '') as cpf_agente,
        COALESCE(nome_supervisor, '') as nome_supervisor,
        1 as coluna_fixa,
        COALESCE(CAST(data_atendimento AS DATE), '1900-01-01') as data_atendimento,
        COALESCE(CAST(DATEADD(MINUTE, 60, CAST(COALESCE(tempo_logado, '00:00:00') AS DATETIME)) AS TIME), '00:00:00') as tempo_logado,
        COALESCE(CAST(hora_login AS TIME), '00:00:00') as hora_login,
        COALESCE(
            CAST(
                CASE 
                    WHEN DATEADD(MINUTE, 60, CAST(COALESCE(hora_logout, '00:00:00') AS DATETIME)) > CAST('20:40:00' AS DATETIME)
                         THEN CAST('20:40:00' AS DATETIME)
                    ELSE DATEADD(MINUTE, 60, CAST(COALESCE(hora_logout, '00:00:00') AS DATETIME))
                END
            AS TIME),
        '00:00:00') as hora_logout,
        COALESCE(CAST(tempo_trabalhado AS TIME), '00:00:00') as tempo_trabalhado,
        COALESCE(CAST(DATEADD(MINUTE, 90, CAST(COALESCE(tempo_falado, '00:00:00') AS DATETIME)) AS TIME), '00:00:00') as tempo_falado,
        CASE
            WHEN DATEADD(HOUR, -1, CAST(COALESCE(tempo_disponivel, '00:00:00') AS DATETIME)) < '1900-01-01 00:00:00'
            THEN CAST('00:00:00' AS TIME)
            ELSE CAST(DATEADD(MINUTE, -30, CAST(COALESCE(tempo_disponivel, '00:00:00') AS DATETIME)) AS TIME)
        END as tempo_disponivel,
        COALESCE(CAST(tempo_pos_atendimento AS TIME), '00:00:00') as tempo_pos_atendimento,
        COALESCE(CAST(pausa_descanso1 AS TIME), '00:00:00') as pausa_descanso1,
        COALESCE(CAST(pausa_lanche AS TIME), '00:00:00') as pausa_lanche,
        COALESCE(CAST(pausa_descanso2 AS TIME), '00:00:00') as pausa_descanso2,
        CAST(
            DATEADD(SECOND, 
                (
                    DATEDIFF(SECOND, '00:00:00', COALESCE(pausa_descanso1, '00:00:00')) +
                    DATEDIFF(SECOND, '00:00:00', COALESCE(pausa_lanche, '00:00:00')) +
                    DATEDIFF(SECOND, '00:00:00', COALESCE(pausa_descanso2, '00:00:00'))
                ),
                '00:00:00'
            )
        AS TIME) as pausa_nr17,
        COALESCE(CAST(pausa_reuniao AS TIME), '00:00:00') as pausa_reuniao,
        COALESCE(CAST(pausa_treinamento AS TIME), '00:00:00') as pausa_treinamento,
        COALESCE(CAST(pausa_feedback AS TIME), '00:00:00') as pausa_feedback,
        COALESCE(CAST(pausa_sistema AS TIME), '00:00:00') as pausa_sistema,
        COALESCE(CAST(pausa_outros AS TIME), '00:00:00') as pausa_outros,
        COALESCE(tipo_de_operacao, '') as tipo_de_operacao,
        COALESCE(nome_da_assessoria, '') as nome_da_assessoria,
        CASE
            WHEN DATEADD(HOUR, -1, CAST(COALESCE(tempo_idle, '00:00:00') AS DATETIME)) < '1900-01-01 00:00:00'
            THEN CAST('00:00:00' AS TIME)
            ELSE CAST(DATEADD(MINUTE, -30, CAST(COALESCE(tempo_idle, '00:00:00') AS DATETIME)) AS TIME)
        END as tempo_idle,
        COALESCE(CAST(pausa_digital AS TIME), '00:00:00') as pausa_digital,
        COALESCE(CAST(tma_total AS TIME), '00:00:00') as tma_total,
        COALESCE(CAST(tma_produtivo AS TIME), '00:00:00') as tma_produtivo,
        COALESCE(atendimentos_total, 0) as atendimentos_total,
        COALESCE(atendimentos_produtivo, 0) as atendimentos_produtivo,
        CASE 
            WHEN COALESCE(atendimentos_total, 0) = 0 THEN CAST('00:00:00' AS TIME)
            ELSE CAST(
                DATEADD(SECOND, 
                    DATEDIFF(SECOND, '00:00:00', COALESCE(tempo_pos_atendimento, '00:00:00')) / atendimentos_total,
                    '00:00:00'
                )
            AS TIME)
        END as med_pos,
        CASE 
            WHEN COALESCE(atendimentos_total, 0) = 0 THEN CAST('00:00:00' AS TIME)
            ELSE 
                CASE
                    WHEN DATEADD(HOUR, -1, CAST(COALESCE(tempo_idle, '00:00:00') AS DATETIME)) < '1900-01-01 00:00:00'
                    THEN CAST('00:00:00' AS TIME)
                    ELSE CAST(
                        DATEADD(SECOND, 
                            DATEDIFF(SECOND, '00:00:00', 
                                CAST(DATEADD(MINUTE, -30, CAST(COALESCE(tempo_idle, '00:00:00') AS DATETIME)) AS TIME)
                            ) / atendimentos_total,
                            '00:00:00'
                        )
                    AS TIME)
                END
        END as med_idle
    FROM WillCenter_TemposOperacionais WITH (NOLOCK)
    WHERE data_atendimento >= ? AND data_atendimento <= ?
    ORDER BY data_atendimento, id_agente
    """
    
    print(f"Consultando dados de {data_inicio.strftime('%Y-%m-%d')} até {data_fim.strftime('%Y-%m-%d')}...")
    
    df = pd.read_sql(query, conn, params=[
        data_inicio.strftime('%Y-%m-%d'),
        data_fim.strftime('%Y-%m-%d')
    ])
    
    print(f"Consulta concluída: {len(df)} linhas retornadas")
    
    return df

def processar_atualizacao_tempos():
    """
    Função principal que orquestra o processo de atualização.
    
    1. Mapeia a última data no arquivo
    2. Elabora o range de datas (última data + 1 até ontem)
    3. Consulta dados do banco
    4. Adiciona ao arquivo CSV
    """
    
    # Configurações
    caminho_csv = r"\\trc-dc-ad\Planejamento\MIS\CARTEIRAS\WILLBANK\Automations\base_tempos.csv"
    
    try:
        print("="*60)
        print("INICIANDO PROCESSO DE ATUALIZAÇÃO DE TEMPOS OPERACIONAIS")
        print("="*60)
        
        # 1. Mapear última data no arquivo
        print("\n[1/4] Mapeando última data no arquivo...")
        ultima_data_arquivo = obter_ultima_data_arquivo(caminho_csv)
        
        # 2. Elaborar range de datas
        print("\n[2/4] Elaborando range de datas...")
        data_inicio = ultima_data_arquivo + timedelta(days=1)
        data_fim = datetime.now() - timedelta(days=1)
        
        print(f"Última data no arquivo: {ultima_data_arquivo.strftime('%d/%m/%Y')}")
        print(f"Período a buscar: {data_inicio.strftime('%d/%m/%Y')} até {data_fim.strftime('%d/%m/%Y')}")
        
        # Validar se há dados a buscar
        if data_inicio > data_fim:
            print(f"\nArquivo já está atualizado!")
            return {
                'sucesso': True,
                'mensagem': 'Arquivo já está atualizado',
                'linhas_adicionadas': 0
            }
        
        # 3. Conectar e consultar banco
        print("\n[3/4] Conectando ao banco e consultando dados...")
        conn_bd2 = get_connection("TRC-DC-BD2", "PLANEJAMENTO")
        
        df_novos = consultar_tempos_operacionais(conn_bd2, data_inicio, data_fim)
        
        conn_bd2.close()
        
        if len(df_novos) == 0:
            print("\nNenhum dado novo encontrado para o período.")
            return {
                'sucesso': True,
                'mensagem': 'Nenhum dado novo encontrado',
                'linhas_adicionadas': 0,
                'periodo': f"{data_inicio.strftime('%d/%m/%Y')} até {data_fim.strftime('%d/%m/%Y')}"
            }
        
        # 4. Adicionar ao arquivo CSV (apenas append, sem modificar dados existentes)
        print("\n[4/4] Adicionando dados ao arquivo CSV...")
        
        print(f"Linhas a adicionar: {len(df_novos)}")
        
        # Adicionar os novos dados ao final do arquivo CSV com delimitador ponto e vírgula
        df_novos.to_csv(caminho_csv, mode='a', header=False, index=False, encoding='utf-8-sig', sep=';')
        
        print(f"Dados adicionados com sucesso ao arquivo CSV!")
        
        print("\n" + "="*60)
        print("PROCESSO CONCLUÍDO COM SUCESSO!")
        print("="*60)
        
        return {
            'sucesso': True,
            'mensagem': 'Dados atualizados com sucesso',
            'linhas_adicionadas': len(df_novos),
            'periodo': f"{data_inicio.strftime('%Y-%m-%d')} até {data_fim.strftime('%Y-%m-%d')}"
        }
        
    except Exception as e:
        print(f"\nERRO: {str(e)}")
        return {
            'sucesso': False,
            'mensagem': f'Erro ao processar: {str(e)}',
            'erro': str(e)
        }


# Exemplo de uso
if __name__ == "__main__":
    resultado = processar_atualizacao_tempos()
    
    print("\n" + "="*60)
    print("RESUMO DA EXECUÇÃO")
    print("="*60)
    for chave, valor in resultado.items():
        print(f"{chave}: {valor}")