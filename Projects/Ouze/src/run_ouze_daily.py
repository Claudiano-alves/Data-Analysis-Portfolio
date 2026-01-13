"""run_ouze_daily.py

Uso:
 - python run_ouze_daily.py

O que faz:
 - Lê `base_daily_ouze.csv` (primeira coluna) para encontrar a data mais recente já processada.
 - Constrói uma lista de datas úteis (seg-sex) da última data até ontem (inclusive), incluindo a última data para ser reprocessada.
 - Para cada data a processar, calcula:
     @DT_INI: primeiro dia do mês atual (mês de hoje)
     @DT_FIM: último dia do mês atual
     @DT e @DT2: a data sendo processada
     @DU: diferença em dias entre hoje e a data processada (ex: segunda processando sexta -> 3)
 - Lê `OUZE_DAILY.sql`, remove as DECLAREs existentes dessas variáveis e antepõe um bloco DECLARE com os valores calculados.
 - Executa o SQL (conexão via string configurada no arquivo). O script espera dois result sets. O primeiro é adicionado à aba 'Result1' e o segundo à aba 'Result2' de um arquivo Excel `base_daily_ouze_results.xlsx` na mesma pasta.

Notas / premissas:
 - CSV é separado por ponto-e-vírgula e a primeira coluna contém datas no formato dd/mm/YYYY.
 - Fins de semana (sábado/domingo) são ignorados.
 - Se a execução falhar, o SQL é salvo em ./sql_runs/ para inspeção.

Dependências:
 pip install pandas pyodbc openpyxl

"""

from pathlib import Path
import os
import re
import sys
import argparse
import calendar
from datetime import datetime, date, timedelta
import pandas as pd

try:
    import pyodbc
except Exception:
    pyodbc = None


# ============================================================================
# Configurações de conexão com o banco de dados
# ============================================================================
SERVER = r"TRC-DC-BDM\BD"
DATABASE = "SRC"
CONN_STR = f"DRIVER={{SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"
# ============================================================================


ROOT = Path(__file__).resolve().parent
CSV_PATH = ROOT / "base_daily_ouze.csv"
SQL_PATH = ROOT / "OUZE_DAILY.sql"
OUT_XLSX = ROOT / "base_daily_ouze_results.xlsx"
SQL_RUNS_DIR = ROOT / "sql_runs"


def read_latest_date_from_csv(csv_path: Path) -> date | None:
    # CSV é separado por ponto-e-vírgula; primeira coluna com cabeçalho 'DATA'
    if not csv_path.exists():
        return None
    # lê apenas a primeira coluna
    try:
        df = pd.read_csv(csv_path, sep=';', usecols=[0], encoding='utf-8', dtype=str)
    except Exception:
        # tenta com outra codificação caso haja caracteres especiais
        df = pd.read_csv(csv_path, sep=';', usecols=[0], encoding='latin-1', dtype=str)

    # extrai a primeira coluna como série de strings (compatível com diferentes versões do pandas)
    if df.shape[1] >= 1:
        s = df.iloc[:, 0].astype(str)
    else:
        # caso improvável: pega a primeira coluna pelo nome
        s = df[df.columns[0]].astype(str)

    # analisa datas (dayfirst=True para formato dd/mm/yyyy)
    parsed = pd.to_datetime(s, dayfirst=True, errors='coerce')
    parsed = parsed.dropna()
    if parsed.empty:
        return None
    return parsed.max().date()


def business_days_between(start_date: date, end_date: date) -> list:
    # intervalo inclusivo de start_date a end_date, apenas seg-sex
    if start_date > end_date:
        return []
    result = []
    cur = start_date
    while cur <= end_date:
        if cur.weekday() < 5:  # 0=segunda, ..., 4=sexta
            result.append(cur)
        cur = cur + timedelta(days=1)
    return result


def compute_month_bounds(today: date):
    first = today.replace(day=1)
    last_day = calendar.monthrange(today.year, today.month)[1]
    last = today.replace(day=last_day)
    return first, last


def build_declare_block(dt_ini: date, dt_fim: date, dt: date, du: int) -> str:
    return (
        f"DECLARE @DT_INI AS DATE = '{dt_ini.isoformat()}' -- PRIMEIRO DIA DO MÊS\n"
        f"DECLARE @DT_FIM AS DATE = '{dt_fim.isoformat()}' -- ÚLTIMO DIA DO MÊS\n"
        f"DECLARE @DT     AS DATE = '{dt.isoformat()}' -- PRIMEIRO DIA DESEJADO\n"
        f"DECLARE @DT2    AS DATE = '{dt.isoformat()}' -- ÚLTIMO DIA DESEJADO\n"
        f"-- Variáveis de data/hora concatenadas\n"
        f"DECLARE @DATETIME_INI AS VARCHAR(19) = CONVERT(VARCHAR(10), @DT_INI, 120) + ' 00:00:00'\n"
        f"DECLARE @DATETIME_FIM AS VARCHAR(19) = CONVERT(VARCHAR(10), @DT_FIM, 120) + ' 23:59:59'\n"
        f"DECLARE @DU INT = {du}\n"
    )


def remove_existing_declares(sql_text: str) -> str:
    # Remove linhas que declaram as variáveis que vamos definir
    pattern = re.compile(r"^\s*DECLARE\s+@DT_INI.*$|^\s*DECLARE\s+@DT_FIM.*$|^\s*DECLARE\s+@DT\s+.*$|^\s*DECLARE\s+@DT2\s+.*$|^\s*DECLARE\s+@DU\s+.*$|^\s*DECLARE\s+@DATETIME_INI.*$|^\s*DECLARE\s+@DATETIME_FIM.*$", flags=re.IGNORECASE | re.MULTILINE)
    return pattern.sub('', sql_text)


def execute_sql_and_fetch_two_results(conn_str: str, sql_text: str):
    if pyodbc is None:
        raise RuntimeError('pyodbc não instalado; não é possível executar SQL. Instale com: pip install pyodbc')
    
    print('  → Conectando ao banco de dados...')
    with pyodbc.connect(conn_str, autocommit=True) as conn:
        cur = conn.cursor()
        print('  → Conexão estabelecida. Executando SQL...')
        cur.execute(sql_text)
        print('  → SQL executado com sucesso.')
        
        # primeiro result set
        print('  → Coletando primeiro result set...')
        if cur.description:
            cols1 = [c[0] for c in cur.description]
            print(f'    ├─ Colunas: {len(cols1)} - {cols1[:5]}{"..." if len(cols1) > 5 else ""}')
        else:
            cols1 = []
            print('    ├─ Sem descrição (nenhuma coluna)')
        
        rows1 = cur.fetchall()
        print(f'    └─ Linhas coletadas: {len(rows1)}')
        
        if cols1 and rows1:
            df1 = pd.DataFrame.from_records(rows1, columns=cols1)
            print(f'  → DataFrame 1 criado: {df1.shape[0]} linhas × {df1.shape[1]} colunas')
        else:
            df1 = pd.DataFrame()
            print('  → DataFrame 1: vazio')

        # move para o próximo result set
        print('  → Tentando avançar para próximo result set...')
        has_next = cur.nextset()
        print(f'    └─ Próximo result set disponível: {has_next}')
        
        df2 = pd.DataFrame()
        if has_next:
            print('  → Coletando segundo result set...')
            if cur.description:
                cols2 = [c[0] for c in cur.description]
                print(f'    ├─ Colunas: {len(cols2)} - {cols2[:5]}{"..." if len(cols2) > 5 else ""}')
            else:
                cols2 = []
                print('    ├─ Sem descrição (nenhuma coluna)')
            
            rows2 = cur.fetchall()
            print(f'    └─ Linhas coletadas: {len(rows2)}')
            
            if cols2 and rows2:
                df2 = pd.DataFrame.from_records(rows2, columns=cols2)
                print(f'  → DataFrame 2 criado: {df2.shape[0]} linhas × {df2.shape[1]} colunas')
            else:
                df2 = pd.DataFrame()
                print('  → DataFrame 2: vazio')
        else:
            print('  → Sem próximo result set (consultado apenas um)')

        return df1, df2


def append_df_to_excel(path: Path, sheet_name: str, df: pd.DataFrame):
    # Usa pandas ExcelWriter com openpyxl e if_sheet_exists='replace' para adicionar/substituir abas com segurança
    mode = 'a' if path.exists() else 'w'
    with pd.ExcelWriter(path, engine='openpyxl', mode=mode, if_sheet_exists='replace') as writer:
        df.to_excel(writer, sheet_name=sheet_name, index=False)


def main(argv=None):
    parser = argparse.ArgumentParser(description='Executa consulta OUZE_DAILY.sql com datas dinâmicas')
    parser.add_argument('--dry-run', action='store_true', help='Não executa SQL; apenas escreve arquivos SQL gerados em ./sql_runs/')
    args = parser.parse_args(argv)

    print('=' * 80)
    print('INICIANDO PROCESSAMENTO OUZE_DAILY')
    print('=' * 80)

    print('\n[1/5] Lendo data mais recente do CSV...')
    latest = read_latest_date_from_csv(CSV_PATH)
    today = date.today()
    yesterday = today - timedelta(days=1)

    print(f'  → Hoje: {today.isoformat()}')
    print(f'  → Ontem: {yesterday.isoformat()}')

    if latest is None:
        print(f'  ⚠ Nenhuma data válida encontrada em {CSV_PATH}')
        print(f'  → Usando ontem como ponto de partida: {yesterday.isoformat()}')
        latest = yesterday
    else:
        print(f'  → Última data no CSV: {latest.isoformat()}')

    print('\n[2/5] Calculando datas úteis para processar...')
    to_process = business_days_between(latest, yesterday)
    print(f'  → Total de dias úteis a processar: {len(to_process)}')
    for d in to_process:
        print(f'     - {d.isoformat()} ({["seg", "ter", "qua", "qui", "sex"][d.weekday()]})')

    if not to_process:
        print('  ⚠ Nenhuma data útil para processar (última >= ontem ou apenas fins de semana).')
        print('\n✗ NADA A FAZER')
        return

    print('\n[3/5] Lendo template SQL...')
    try:
        sql_text_orig = SQL_PATH.read_text(encoding='utf-8')
        print(f'  ✓ Template SQL carregado ({len(sql_text_orig)} caracteres)')
    except Exception as e:
        print(f'  ✗ ERRO ao ler SQL: {e}')
        return

    print('\n[4/5] Processando cada data...')
    processed_count = 0
    for proc_date in to_process:
        print(f'\n  ► Data: {proc_date.isoformat()}')
        
        # calcula variáveis
        dt_ini, dt_fim = compute_month_bounds(today)
        du = (today - proc_date).days
        print(f'    ├─ DT_INI: {dt_ini.isoformat()}')
        print(f'    ├─ DT_FIM: {dt_fim.isoformat()}')
        print(f'    ├─ DT/DT2: {proc_date.isoformat()}')
        print(f'    └─ DU: {du}')

        declare_block = build_declare_block(dt_ini, dt_fim, proc_date, du)

        # remove declares existentes do template sql e antepõe nosso bloco
        sql_body = remove_existing_declares(sql_text_orig)
        final_sql = declare_block + '\n' + sql_body

        if args.dry_run:
            # escreve SQL em arquivo para inspeção
            SQL_RUNS_DIR.mkdir(exist_ok=True)
            out_file = SQL_RUNS_DIR / f'ouze_{proc_date.isoformat()}.sql'
            out_file.write_text(final_sql, encoding='utf-8')
            print(f'    ✓ SQL escrito em {out_file}')
            continue

        # executa
        print(f'    ► Executando SQL contra banco de dados...')
        try:
            df1, df2 = execute_sql_and_fetch_two_results(CONN_STR, final_sql)
            processed_count += 1
        except Exception as e:
            print(f'    ✗ ERRO ao executar SQL: {e}')
            # em caso de erro, salva SQL para debug
            SQL_RUNS_DIR.mkdir(exist_ok=True)
            (SQL_RUNS_DIR / f'error_ouze_{proc_date.isoformat()}.sql').write_text(final_sql, encoding='utf-8')
            print(f'    → SQL de erro salvo para debug')
            continue

        # adiciona resultados às abas do Excel
        print(f'    ► Gravando resultados em Excel...')
        if not df1.empty:
            append_df_to_excel(OUT_XLSX, 'Result1', df1)
            print(f'    ✓ Result1 gravado ({len(df1)} linhas)')
        else:
            print(f'    ⚠ Result1 vazio (nenhuma linha)')

        if not df2.empty:
            append_df_to_excel(OUT_XLSX, 'Result2', df2)
            print(f'    ✓ Result2 gravado ({len(df2)} linhas)')
        else:
            print(f'    ⚠ Result2 vazio (nenhuma linha)')

    print('\n[5/5] Resumo final...')
    print(f'  ✓ Datas processadas com sucesso: {processed_count}/{len(to_process)}')
    print(f'  → Arquivo de saída: {OUT_XLSX}')
    print('=' * 80)
    print('✓ PROCESSAMENTO CONCLUÍDO')
    print('=' * 80)

if __name__ == '__main__':
    main()
