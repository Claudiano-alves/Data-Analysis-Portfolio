import pandas as pd
import pyodbc
import os
import subprocess
import platform

# Configurações de conexão
server = r"trc-dc-bd"
database = "SRC"

conn = pyodbc.connect(
    f"DRIVER={{SQL Server}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    "Trusted_Connection=yes;"
)

query = '''
    SELECT DISTINCT
        C.CONTRATO_FIN,
        CPF_DEV,
        C.ATRASO_FIN,
        CASE
            WHEN C.ATRASO_FIN BETWEEN 1 AND 7           THEN 'A - 01 - 07'
            WHEN C.ATRASO_FIN BETWEEN 8 AND 15          THEN 'B - 08 - 15'
            WHEN C.ATRASO_FIN BETWEEN 16 AND 30         THEN 'C - 16 - 30'
            WHEN C.ATRASO_FIN BETWEEN 31 AND 60         THEN 'D - 31 - 60'
            WHEN C.ATRASO_FIN BETWEEN 61 AND 90         THEN 'E - 61 - 90'
            WHEN C.ATRASO_FIN BETWEEN 91 AND 180        THEN 'F - 91 - 180'
            WHEN C.ATRASO_FIN BETWEEN 181 AND 270       THEN 'G - 181 - 270'
            WHEN C.ATRASO_FIN BETWEEN 271 AND 360       THEN 'H - 271 - 360'
            WHEN C.ATRASO_FIN BETWEEN 361 AND 540       THEN 'I - 361 - 540'
            WHEN C.ATRASO_FIN BETWEEN 541 AND 720       THEN 'J - 541 - 720'
            WHEN C.ATRASO_FIN BETWEEN 721 AND 1080      THEN 'K - 721 - 1080'
            WHEN C.ATRASO_FIN BETWEEN 1081 AND 1440     THEN 'L - 1081 - 1440'
            WHEN C.ATRASO_FIN BETWEEN 1441 AND 1800     THEN 'M - 1441 - 1800'
            WHEN C.ATRASO_FIN > 1800                    THEN 'N - Maior 1800'
        ELSE '0 - Menor 8'
        END FX_ATRASO,
        C.VALORPRIN_FIN,
        CASE
            WHEN C.VALORPRIN_FIN BETWEEN 0 AND 50       THEN 'A - 0 a 50'
            WHEN C.VALORPRIN_FIN BETWEEN 50 AND 100     THEN 'B - 51 a 100'
            WHEN C.VALORPRIN_FIN BETWEEN 100 AND 250    THEN 'C - 101 a 250'
            WHEN C.VALORPRIN_FIN BETWEEN 250 AND 500    THEN 'D - 251 a 500'
            WHEN C.VALORPRIN_FIN BETWEEN 500 AND 750    THEN 'E - 501 a 750 '
            WHEN C.VALORPRIN_FIN BETWEEN 750 AND 1000   THEN 'F - 751 a 1000'
            WHEN C.VALORPRIN_FIN BETWEEN 1000 AND 1500  THEN 'G - 1001 a 1500'
            WHEN C.VALORPRIN_FIN BETWEEN 1500 AND 2000  THEN 'H - 1501 a 2000'
            WHEN C.VALORPRIN_FIN BETWEEN 2000 AND 3000  THEN 'I - 2001 a 3000'
            WHEN C.VALORPRIN_FIN BETWEEN 3000 AND 5000  THEN 'J - 3001 a 5000'
            WHEN C.VALORPRIN_FIN BETWEEN 5000 AND 10000 THEN 'K - 5001 a 10000'
            WHEN C.VALORPRIN_FIN > 10000                THEN 'L - Maior 10000'
        ELSE '0 - Menor 0'
        END TKM,
        Y.REGIONAL,
        Y.GRUPO,
        Y.SPD,
        Y.BU,
        Y.MODALIDE,
        Y.STDEBITO,
        Y.STALUNO,
        Y.CURSO,
        Y.APROACAD
    FROM CAD_DEVF C
    LEFT JOIN AUX_TRC_ROTINA S ON C.CONTRATO_FIN = S.CONTRATO_FIN
    LEFT JOIN (
        SELECT
            a.CONTRATO_FIN,
            MAX(CASE WHEN a.COD_INDICADOR = 'REGIONAL' THEN a.VL_INDICADOR ELSE NULL END) AS REGIONAL,
            MAX(CASE WHEN a.COD_INDICADOR = 'GRUPO' THEN a.VL_INDICADOR ELSE NULL END) AS GRUPO,
            MAX(CASE WHEN a.COD_INDICADOR = 'SPD' THEN a.VL_INDICADOR ELSE NULL END) AS SPD,
            MAX(CASE WHEN a.COD_INDICADOR = 'BU' THEN a.VL_INDICADOR ELSE NULL END) AS BU,
            MAX(CASE WHEN a.COD_INDICADOR = 'MODALIDE' THEN a.VL_INDICADOR ELSE NULL END) AS MODALIDE,
            MAX(CASE WHEN a.COD_INDICADOR = 'STDEBITO' THEN a.VL_INDICADOR ELSE NULL END) AS STDEBITO,
            MAX(CASE WHEN a.COD_INDICADOR = 'STALUNO' THEN STA.SITUACAO_FINAL ELSE NULL END) AS STALUNO,
            MAX(CASE WHEN a.COD_INDICADOR = 'APROACAD' THEN a.VL_INDICADOR ELSE NULL END) AS APROACAD,
            MAX(CASE WHEN a.COD_INDICADOR = 'CURSO' THEN a.DESC_INDICADOR ELSE NULL END) AS CURSO,
            MAX(CASE WHEN b.CONTRATO_FIN IS NOT NULL THEN 'RENOVACAO' ELSE 'MENSALIDADE' END) AS PRODUTO,
            MAX(CASE WHEN a.COD_INDICADOR = 'ULTRENOV' THEN a.VL_INDICADOR ELSE NULL END) AS ULTRENOV,
            MAX(CASE WHEN a.COD_INDICADOR = 'LTCOMER' THEN a.VL_INDICADOR ELSE NULL END) AS LTCOMER
        FROM AUX_SYSOPENINDICADOR_YDUQS a
        LEFT JOIN AUX_SYSOPENSTATUSALUNO_YDUQS STA ON a.COD_INDICADOR = 'STALUNO' AND TRY_CAST(a.VL_INDICADOR AS INT) = STA.COD_SITUACAO
        LEFT JOIN AUX_SYSOPENINDICADOR_YDUQS b ON a.ID_CLIENTE = b.ID_CLIENTE AND a.COD_PRODUT = 1 AND b.COD_PRODUT = 2
        GROUP BY
            a.CONTRATO_FIN
    ) Y ON C.CONTRATO_FIN = Y.CONTRATO_FIN 
    WHERE C.COD_CLI = 252 AND C.STATCONT_FIN = 0
'''

# Define o caminho completo do arquivo
arquivo_csv = os.path.join(os.path.dirname(__file__), "resultado.csv")

# Remove arquivo anterior se existir
if os.path.exists(arquivo_csv):
    os.remove(arquivo_csv)

print("Iniciando exportação...")

# Processa em chunks e salva no CSV
first_chunk = True
for chunk in pd.read_sql(query, conn, chunksize=200_000):
    chunk.to_csv(arquivo_csv, mode='a', sep=';', index=False, decimal=',', encoding='utf-8-sig', header=first_chunk)
    first_chunk = False

conn.close()

print(f"Exportação concluída! Arquivo salvo em: {arquivo_csv}")

# Abre o arquivo automaticamente
try:
    if platform.system() == 'Windows':
        os.startfile(arquivo_csv)
    elif platform.system() == 'Darwin':  # macOS
        subprocess.call(['open', arquivo_csv])
    else:  # Linux
        subprocess.call(['xdg-open', arquivo_csv])
    print("Arquivo aberto com sucesso!")
except Exception as e:
    print(f"Não foi possível abrir o arquivo automaticamente: {e}")
    print(f"Por favor, abra manualmente: {arquivo_csv}")