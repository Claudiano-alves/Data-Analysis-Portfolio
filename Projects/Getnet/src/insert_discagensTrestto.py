import pandas as pd
from datetime import datetime, timedelta
from src.db_connection import get_connection
conn_trc = get_connection("SERVER_BD2", "DATABASE_TRC")

def get_max_date(conn_trc):
    """Retorna a última data na tabela DISCAGENS_TRESTTO"""
    query = """
    SELECT MAX(data) as max_data
    FROM DISCAGENS_TRESTTO
    """
    df = pd.read_sql(query, conn_trc)
    return df['max_data'].iloc[0]

def get_insert_query(data_consulta):
    """
    Gera a query de INSERT com a data formatada para o OPENQUERY
    data_consulta: objeto datetime ou string no formato YYYY-MM-DD
    """
    # Converte para datetime se necessário
    if isinstance(data_consulta, str):
        data_consulta = datetime.strptime(data_consulta, '%Y-%m-%d')
    
    # Formata a data inicial (00:00:00) e final (00:00:00 do dia seguinte)
    data_inicio = data_consulta.strftime('%Y%m%d')
    data_fim = (data_consulta + timedelta(days=1)).strftime('%Y%m%d')
    
    query = f"""
    INSERT INTO DISCAGENS_TRESTTO (
        DATA,
        CPF,
        SUBSTATUSURA,
        DISCAGEM,
        ALO,
        CPC,
        CPCA,
        PROMESSA
    )
    SELECT
        *
    FROM OPENQUERY([Trestto],
    '
        select
            DATA
        ,   CPF
        ,   SUBSTATUSURA AS SUBSTATUSURA
        ,   SUM(DISCAGEM) AS DISCAGEM
        ,   SUM(ALO) AS ALO
        ,   SUM(CPC) AS CPC
        ,   SUM(CPCA) AS CPCA
        ,   SUM(PP) AS PROMESSA
        from(
            select
                cast(datahoraligacao as date) data
            ,   CPF
            ,   DISCAGEM = 1
            ,   SUBSTATUSURA
            ,   TELEFONELIGACAO
            ,   CASE
                    WHEN SUBSTATUSURA IN (
                        ''ClienteAlegaPagamentoMais5Dias'',''NaoEfetuouPagamentoMais5Dias'',''QuestionaValor'',
                        ''SemAgentesVirtuaisDisponiveis'',''CPFValidadoBase'',''DesejaBoletoWhatsApp'',''ErroWSDividas'',
                        ''Mudou'',''NaoEsta'',''Ocupado'',''SegundaRecusa'',''AgendamentoQualquerHorario'',''AlegaPagamento'',
                        ''ClienteDesejaFatura'',''DificuldadeFinanceira'',''EfetuouPagamentoMais5Dias'',''Morreu'',
                        ''NaoConfirmouCliente'',''NaoDesejaAcordo'',''PrimeiraRecusa'',''Abandonada'',''ConfirmouAcordo'',
                        ''ConfirmouCliente'',''Conhece'',''DesconheceDivida'',''NaoConhece'',''NaoDesejaFatura'',
                        ''NaoQuerInformarCPF'',''VouChamar'',''DesejaAcordo'',''DesejaAcordoAVista'',''DesejaFalarAtendente'',
                        ''ErroWSAcordosPlanos'',''ErroWSBuscarTitulos'',''Falecido'',''NaoRealizouPagamento'',
                        ''PagamentoNaoAgendado'',''Parente'',''QualAssunto'',''SelecionouCanalWhatsApp'',''Aguardar'',
                        ''DesejaParcelar'',''EfetuouPagamento'',''NaoDesejaReceberOferta'',''NaoResponsavelMatricula'',
                        ''OpcaoLigacao'',''PagamentoAgendado'',''QuemFala'',''ResponsavelMatricula'',''AgendamentoNoite'',
                        ''ClienteAlegaPagamentoMenos5Dias'',''DesejaNegociarWhatsApp'',''Espera'',''NaoRecebeuFatura'',
                        ''QuestionaVencimento'',''RecusaAcordo'',''Transferida'',''AgendamentoTarde'',''ClienteAgresivo'',
                        ''ErroTransferencia'',''TerceiraRecusa''
                    ) THEN 1 ELSE 0
                END AS ALO
            ,   CASE
                    WHEN SUBSTATUSURA IN (
                        ''ClienteAlegaPagamentoMais5Dias'',''NaoEfetuouPagamentoMais5Dias'',''QuestionaValor'',
                        ''CPFValidadoBase'',''DesejaBoletoWhatsApp'',''ErroWSDividas'',''SegundaRecusa'',''AlegaPagamento'',
                        ''ClienteDesejaFatura'',''DificuldadeFinanceira'',''EfetuouPagamentoMais5Dias'',
                        ''NaoDesejaAcordo'',''PrimeiraRecusa'',''ConfirmouAcordo'',''ConfirmouCliente'',
                        ''DesconheceDivida'',''NaoDesejaFatura'',''DesejaAcordo'',''DesejaAcordoAVista'',
                        ''ErroWSAcordosPlanos'',''NaoRealizouPagamento'',''PagamentoNaoAgendado'',
                        ''DesejaParcelar'',''EfetuouPagamento'',''NaoDesejaReceberOferta'',
                        ''PagamentoAgendado'',''ResponsavelMatricula'',''ClienteAlegaPagamentoMenos5Dias'',
                        ''DesejaNegociarWhatsApp'',''NaoRecebeuFatura'',''QuestionaVencimento'',''RecusaAcordo'',
                        ''ClienteAgresivo'',''TerceiraRecusa''
                    ) THEN 1 ELSE 0
                END AS CPC
            ,   CASE
                    WHEN SUBSTATUSURA IN (
                        ''NaoEfetuouPagamentoMais5Dias'',''QuestionaValor'',''DesejaBoletoWhatsApp'',''ErroWSDividas'',
                        ''SegundaRecusa'',''ClienteDesejaFatura'',''DificuldadeFinanceira'',''NaoDesejaAcordo'',''ConfirmouAcordo'',
                        ''ConfirmouCliente'',''NaoDesejaFatura'',''DesejaAcordo'',''ErroWSAcordosPlanos'',''NaoRealizouPagamento'',
                        ''PagamentoNaoAgendado'',''DesejaParcelar'',''PagamentoAgendado'',''ResponsavelMatricula'',''DesejaNegociarWhatsApp'',
                        ''NaoRecebeuFatura'',''QuestionaVencimento'',''RecusaAcordo'',''TerceiraRecusa'', ''DesejaAcordoAVista''
                    ) THEN 1 ELSE 0
                END AS CPCA
            ,   CASE
                    WHEN SUBSTATUSURA IN (
                        ''DesejaBoletoWhatsApp'',''ClienteDesejaFatura'',''ConfirmouAcordo'',
                        ''DesejaAcordo'',''DesejaAcordoAVista'',''ErroWSAcordosPlanos''
                    ) THEN 1 ELSE 0
                END AS PP
            from Analitico_TRC
            where datahoraligacao >= ''{data_inicio}'' 
              and datahoraligacao < ''{data_fim}''
        )a
        GROUP BY
            DATA
        ,   CPF
        ,   SUBSTATUSURA
        ORDER BY
            DATA
    '
    )
    """
    return query

def executar_carga_incremental(conn_trc):
    """
    Executa a carga incremental:
    1. Consulta a última data na tabela
    2. Define D-1 (ontem) como data alvo
    3. Processa TODOS os dias faltantes até D-1
    """
    try:
        # Consulta a última data
        max_data = get_max_date(conn_trc)
        print(f"Última data na tabela: {max_data}")
        
        # Define a data alvo como ONTEM (D-1)
        hoje = datetime.now().date()
        data_alvo = hoje - timedelta(days=1)
        print(f"Data alvo (D-1): {data_alvo}")
        
        # Se tabela vazia, define data inicial manualmente
        if max_data is None:
            print("⚠ Tabela vazia. Defina uma data inicial manualmente.")
            return
        
        # Converte max_data para date
        max_data = pd.to_datetime(max_data).date()
        
        # Calcula a próxima data a ser inserida (dia seguinte ao max_data)
        proxima_data = max_data + timedelta(days=1)
        
        # Verifica se já está atualizado
        if proxima_data > data_alvo:
            print(f"✓ Banco está atualizado! Última data ({max_data}) já contempla D-1 ({data_alvo})")
            print(f"Nenhuma inserção necessária.")
            return
        
        # Calcula quantos dias faltam
        dias_faltantes = (data_alvo - max_data).days
        print(f"\n⚠ Faltam {dias_faltantes} dia(s) para atualizar até D-1")
        print(f"Processando de {proxima_data} até {data_alvo}...\n")
        
        # Loop para processar cada dia faltante
        data_atual = proxima_data
        total_linhas = 0
        dias_processados = 0
        
        while data_atual <= data_alvo:
            print(f"▶ Processando data: {data_atual}")
            
            # Gera a query de INSERT
            query_insert = get_insert_query(data_atual)
            
            # Executa o INSERT
            cursor = conn_trc.cursor()
            cursor.execute(query_insert)
            conn_trc.commit()
            
            linhas_inseridas = cursor.rowcount
            total_linhas += linhas_inseridas
            dias_processados += 1
            
            print(f"  ✓ {linhas_inseridas} linhas inseridas")
            
            cursor.close()
            
            # Avança para o próximo dia
            data_atual += timedelta(days=1)
        
        print(f"\n{'='*60}")
        print(f"✓ CARGA COMPLETA!")
        print(f"  - Dias processados: {dias_processados}")
        print(f"  - Total de linhas inseridas: {total_linhas}")
        print(f"  - Período: {proxima_data} até {data_alvo}")
        print(f"{'='*60}")
        
    except Exception as e:
        print(f"\n✗ Erro durante a execução: {str(e)}")
        conn_trc.rollback()
        raise

# Uso exemplo:
executar_carga_incremental(conn_trc)