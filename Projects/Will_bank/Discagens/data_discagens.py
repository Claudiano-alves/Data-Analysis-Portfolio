import os
import pyodbc
import pandas as pd
from datetime import datetime, timedelta
import re
from dotenv import load_dotenv

# Se o .env está na raiz do projeto
#load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))
load_dotenv(r"C:\Users\claudiano.alves\Documents\Claudiano\repository\Data-Analysis-Portfolio\Projects\.env")

class TemposWillBankAutomation:
    def __init__(self):
        # Configurações de conexão
        self.server = os.getenv("DB_SERVER")
        self.database = os.getenv("DB_DATABASE")
        self.username = os.getenv("DB_USERNAME")
        self.password = os.getenv("DB_PASSWORD")

        # Caminhos
        self.sql_file_path = r"\\trc-dc-ad\Planejamento\MIS\CARTEIRAS\WILLBANK\Automations\Discagens\temposWill.sql"
        self.output_base_path = r"\\trc-dc-ad\Grades\willBank\DISCONNECTED"
        
        # Conexão
        self.conn = None
        
    def connect_database(self):
        """Estabelece conexão com o banco de dados"""
        try:
            conn_string = (
                f"DRIVER={{SQL Server}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"UID={self.username};"
                f"PWD={self.password}"
            )
            self.conn = pyodbc.connect(conn_string)
            print("✓ Conexão estabelecida com sucesso!")
            return True
        except Exception as e:
            print(f"✗ Erro ao conectar ao banco: {e}")
            return False
    
    def get_month_folder(self, date_obj):
        """Retorna o nome da pasta do mês (01, 02, etc.)"""
        return date_obj.strftime("%m")
    
    def ensure_month_folder_exists(self, month_folder):
        """Garante que a pasta do mês existe"""
        folder_path = os.path.join(self.output_base_path, month_folder)
        if not os.path.exists(folder_path):
            os.makedirs(folder_path)
            print(f"✓ Pasta criada: {folder_path}")
        return folder_path
    
    def get_last_processed_date(self, month_folder_path):
        """Encontra a última data processada na pasta"""
        if not os.path.exists(month_folder_path):
            return None
        
        # Padrão: tempos_DDMMYYYY.csv
        pattern = re.compile(r'discagens_(\d{8})\.csv')
        dates = []
        
        for filename in os.listdir(month_folder_path):
            match = pattern.match(filename)
            if match:
                date_str = match.group(1)
                try:
                    # Converte DDMMYYYY para objeto datetime
                    date_obj = datetime.strptime(date_str, "%d%m%Y")
                    dates.append(date_obj)
                except ValueError:
                    continue
        
        if dates:
            last_date = max(dates)
            print(f"✓ Última data encontrada: {last_date.strftime('%d/%m/%Y')}")
            return last_date
        else:
            print("ℹ Nenhuma data anterior encontrada")
            return None
    
    def read_sql_template(self):
        """Lê o arquivo SQL template"""
        try:
            with open(self.sql_file_path, 'r', encoding='latin-1') as file:
                sql_template = file.read()
            print("✓ Template SQL carregado")
            return sql_template
        except Exception as e:
            print(f"✗ Erro ao ler arquivo SQL: {e}")
            return None
    
    def generate_sql_for_range(self, sql_template, start_date, end_date):
        """Gera SQL para um intervalo de datas"""
        dt_ini = start_date.strftime('%Y-%m-%d')
        dt_fim = end_date.strftime('%Y-%m-%d')
        
        # Substitui as datas no template
        sql = sql_template.replace("DECLARE @DT_INI AS DATE = '2026-01-01'", 
                                   f"DECLARE @DT_INI AS DATE = '{dt_ini}'")
        sql = sql.replace("DECLARE @DT_FIM AS DATE = '2026-01-31'", 
                         f"DECLARE @DT_FIM AS DATE = '{dt_fim}'")
        
        return sql
    
    def identify_date_column(self, df):
        """Identifica a coluna de data no DataFrame"""
        # Procura por colunas comuns de data
        possible_names = ['DATA', 'DT', 'DATE', 'DT_PROCESSAMENTO', 'DT_PROCESS', 'DT_REF', 'DATA_REF']
        
        for col in df.columns:
            if col.upper() in possible_names:
                return col
        
        # Se não encontrar, procura por colunas do tipo datetime
        for col in df.columns:
            if pd.api.types.is_datetime64_any_dtype(df[col]):
                return col
        
        # Tenta converter colunas para datetime
        for col in df.columns:
            try:
                pd.to_datetime(df[col], errors='raise')
                return col
            except:
                continue
        
        return None
    
    def split_and_save_by_date(self, df, start_date, end_date):
        """Divide o DataFrame por data e salva em arquivos individuais"""
        # Identifica a coluna de data
        date_column = self.identify_date_column(df)
        
        if date_column is None:
            print("✗ Não foi possível identificar a coluna de data no resultado")
            print(f"Colunas disponíveis: {list(df.columns)}")
            return False
        
        print(f"✓ Coluna de data identificada: {date_column}")
        
        # Garante que a coluna está em formato datetime
        if not pd.api.types.is_datetime64_any_dtype(df[date_column]):
            df[date_column] = pd.to_datetime(df[date_column])
        
        # Normaliza para remover horário
        df['_data_normalized'] = df[date_column].dt.normalize()
        
        success_count = 0
        error_count = 0
        
        # Itera sobre cada data no intervalo
        current_date = start_date
        while current_date <= end_date:
            try:
                # Filtra dados da data específica
                date_normalized = pd.Timestamp(current_date).normalize()
                df_date = df[df['_data_normalized'] == date_normalized].copy()
                
                # Remove a coluna auxiliar
                df_date = df_date.drop(columns=['_data_normalized'])
                
                # Define pasta do mês
                month_folder = self.get_month_folder(current_date)
                month_folder_path = self.ensure_month_folder_exists(month_folder)
                
                # Nome do arquivo: tempos_DDMMYYYY.csv
                filename = f"discagens_{current_date.strftime('%d%m%Y')}.csv"
                filepath = os.path.join(month_folder_path, filename)
                
                # Verifica se o arquivo já existe
                if os.path.exists(filepath):
                    print(f"ℹ Arquivo já existe: {filename} - pulando")
                    current_date += timedelta(days=1)
                    continue
                
                # Salva apenas se houver dados
                if len(df_date) > 0:
                    df_date.to_csv(filepath, index=False, encoding='utf-8-sig', sep=';')
                    print(f"✓ Arquivo salvo: {filename} ({len(df_date)} registros)")
                    success_count += 1
                else:
                    print(f"ℹ Nenhum registro encontrado para {current_date.strftime('%d/%m/%Y')}")
                
            except Exception as e:
                print(f"✗ Erro ao processar data {current_date.strftime('%d/%m/%Y')}: {e}")
                error_count += 1
            
            current_date += timedelta(days=1)
        
        print(f"\n{'='*50}")
        print(f"Divisão e salvamento concluídos!")
        print(f"✓ Sucesso: {success_count} arquivo(s)")
        print(f"✗ Erros: {error_count} arquivo(s)")
        print(f"{'='*50}")
        
        return True
    
    def process_date_range(self, start_date, end_date):
        """Processa o intervalo de datas (executa SQL apenas UMA vez)"""
        sql_template = self.read_sql_template()
        if not sql_template:
            return False
        
        # Gera SQL para o intervalo completo
        sql = self.generate_sql_for_range(sql_template, start_date, end_date)
        
        print(f"\n🔄 Executando consulta SQL de {start_date.strftime('%d/%m/%Y')} até {end_date.strftime('%d/%m/%Y')}...")
        
        try:
            # Executa a query UMA ÚNICA VEZ
            df = pd.read_sql(sql, self.conn)
            print(f"✓ Consulta executada com sucesso! Total de registros: {len(df)}")
            
            # Divide e salva os dados por data
            self.split_and_save_by_date(df, start_date, end_date)
            
            return True
            
        except Exception as e:
            print(f"✗ Erro ao executar consulta SQL: {e}")
            return False
    
    def run(self):
        """Executa o processo completo"""
        print("="*50)
        print("AUTOMAÇÃO TEMPOS WILLBANK")
        print("="*50)
        
        # Conecta ao banco
        if not self.connect_database():
            return False
        
        try:
            # Data atual e ontem
            today = datetime.now()
            yesterday = today - timedelta(days=1)
            
            print(f"\nData atual: {today.strftime('%d/%m/%Y')}")
            print(f"Processar até: {yesterday.strftime('%d/%m/%Y')}")
            
            # Verifica a pasta do mês atual
            current_month_folder = self.get_month_folder(today)
            month_folder_path = self.ensure_month_folder_exists(current_month_folder)
            
            # Busca última data processada
            last_date = self.get_last_processed_date(month_folder_path)
            
            # Define data inicial
            if last_date:
                # Começa no dia seguinte à última data encontrada
                start_date = last_date + timedelta(days=1)
            else:
                # Se não houver dados, começa no primeiro dia do mês atual
                start_date = today.replace(day=1)
            
            print(f"\n📅 Intervalo a processar: {start_date.strftime('%d/%m/%Y')} até {yesterday.strftime('%d/%m/%Y')}")
            
            # Verifica se há algo a processar
            if start_date > yesterday:
                print("\nℹ Não há novas datas para processar!")
                return True
            
            # Processa as datas (SQL executado apenas UMA vez)
            self.process_date_range(start_date, yesterday)
            
        finally:
            if self.conn:
                self.conn.close()
                print("\n✓ Conexão fechada")
        
        return True


# Execução
if __name__ == "__main__":
    automation = TemposWillBankAutomation()
    automation.run()