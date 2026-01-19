import pyodbc
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging
import os
from dotenv import load_dotenv

# Se o .env está na raiz do projeto
#load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), "..", ".env"))
load_dotenv(r"C:\Users\claudiano.alves\Documents\Claudiano\repository\Data-Analysis-Portfolio\Projects\.env")

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Configuração de logs em arquivo - caminho absoluto relativo ao script
LOG_FILE = Path(__file__).parent / 'logs.txt'

def salvar_log(mensagem, arquivo=LOG_FILE):
    """Salva mensagem no arquivo de log com timestamp"""
    os.makedirs(os.path.dirname(arquivo) or '.', exist_ok=True)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(arquivo, 'a', encoding='utf-8') as f:
        f.write(f"[{timestamp}] {mensagem}\n")

def limpar_logs(arquivo=LOG_FILE):
    """Limpa o arquivo de log, mantendo apenas o log atual"""
    try:
        if Path(arquivo).exists():
            with open(arquivo, 'w', encoding='utf-8') as f:
                f.write("")
    except Exception as e:
        print(f"Aviso: Não foi possível limpar logs anteriores: {e}")

class GeradorBaseAtiva:
    def __init__(self):
        self.sql_path = Path(r"\\trc-dc-ad\Planejamento\MIS\CARTEIRAS\YDUQS\base_ativa.sql")
        self.base_output_path = Path(r"\\trc-dc-ad\OperacionalEstacio\PA provida\base_ativa")
        
    def ler_sql(self):
        """Lê o arquivo SQL do caminho especificado"""
        try:
            with open(self.sql_path, 'r', encoding='utf-8') as f:
                query = f.read()
            logging.info(f"Query SQL lida com sucesso de {self.sql_path}")
            return query
        except Exception as e:
            logging.error(f"Erro ao ler arquivo SQL: {e}")
            raise
    
    def verificar_criar_estrutura_pastas(self):
        """
        Verifica e cria a estrutura de pastas necessária baseada no mês atual
        Retorna o caminho completo onde o arquivo deve ser salvo
        """
        data_atual = datetime.now()
        ano_atual = data_atual.strftime("%Y")
        mes_atual = data_atual.strftime("%m")
        
        # Caminho: base_ativa\YYYY\MM
        caminho_ano = self.base_output_path / ano_atual
        caminho_mes = caminho_ano / mes_atual
        
        # Verifica se a pasta do ano existe
        if not caminho_ano.exists():
            logging.info(f"Criando pasta do ano: {caminho_ano}")
            caminho_ano.mkdir(parents=True, exist_ok=True)
        
        # Verifica se a pasta do mês existe
        if not caminho_mes.exists():
            logging.info(f"Criando pasta do mês: {caminho_mes}")
            caminho_mes.mkdir(parents=True, exist_ok=True)
        else:
            logging.info(f"Pasta do mês já existe: {caminho_mes}")
        
        return caminho_mes
    
    def gerar_nome_arquivo(self):
        """Gera o nome do arquivo no formato base_ativa_DD-MM.xlsx"""
        data_atual = datetime.now()
        dia = data_atual.strftime("%d")
        mes = data_atual.strftime("%m")
        nome_arquivo = f"base_ativa_{dia}-{mes}.xlsx"
        return nome_arquivo
    
    def executar_query(self, query, connection_string):
        """
        Executa a query SQL e retorna um DataFrame
        
        Parâmetros:
        - query: string com a consulta SQL
        - connection_string: string de conexão com o banco de dados
        
        Exemplo de connection_string:
        "DRIVER={SQL Server};SERVER=seu_servidor;DATABASE=seu_banco;UID=usuario;PWD=senha"
        ou
        "DRIVER={SQL Server};SERVER=seu_servidor;DATABASE=seu_banco;Trusted_Connection=yes"
        """
        try:
            logging.info("Conectando ao banco de dados...")
            conn = pyodbc.connect(connection_string)
            
            logging.info("Executando query...")
            df = pd.read_sql(query, conn)
            
            conn.close()
            logging.info(f"Query executada com sucesso. {len(df)} registros recuperados.")
            return df
        
        except Exception as e:
            logging.error(f"Erro ao executar query: {e}")
            raise
    
    def salvar_excel(self, df, caminho_completo):
        """Salva o DataFrame em um arquivo Excel"""
        try:
            df.to_excel(caminho_completo, index=False, engine='openpyxl')
            logging.info(f"Arquivo salvo com sucesso em: {caminho_completo}")
        except Exception as e:
            logging.error(f"Erro ao salvar arquivo Excel: {e}")
            raise
    
    def executar_processo_completo(self, connection_string):
        """
        Executa o processo completo:
        1. Lê a query SQL
        2. Verifica/cria estrutura de pastas
        3. Executa a query
        4. Salva o resultado em Excel
        """
        try:
            # 1. Ler query SQL
            query = self.ler_sql()
            
            # 2. Verificar e criar estrutura de pastas
            caminho_destino = self.verificar_criar_estrutura_pastas()
            
            # 3. Gerar nome do arquivo
            nome_arquivo = self.gerar_nome_arquivo()
            caminho_completo = caminho_destino / nome_arquivo
            
            # 4. Executar query
            df = self.executar_query(query, connection_string)
            
            # 5. Salvar em Excel
            self.salvar_excel(df, caminho_completo)
            
            logging.info("Processo concluído com sucesso!")
            return caminho_completo
            
        except Exception as e:
            logging.error(f"Erro no processo: {e}")
            raise

# Exemplo de uso
if __name__ == "__main__":
    # Icons Unicode para logs
    ICON_SUCCESS = "✓"
    ICON_ERROR = "✗"
    ICON_WARNING = "⚠"
    
    # Acumula mensagens de erro
    erros = []
    resultado_final = None
    
    # Limpar logs anteriores no início da execução
    limpar_logs()
    
    try:
        # Recuperar credenciais do arquivo .env
        server = os.getenv("SERVER_SRC")
        database = os.getenv("DATABASE_SRC")
        
        # Validar se as credenciais foram carregadas
        if not server or not database:
            missing = []
            if not server: missing.append("SERVER_SRC")
            if not database: missing.append("DATABASE_SRC")
            
            msg_erro = f"Credenciais faltando no arquivo .env: {', '.join(missing)}"
            erros.append(msg_erro)
            raise ValueError(msg_erro)
        
        # Construir string de conexão com autenticação do Windows
        conn_string = (
            f"DRIVER={{SQL Server}};"
            f"SERVER={server};"
            f"DATABASE={database};"
            f"Trusted_Connection=yes"
        )
        
        # Testar conexão antes de executar o processo
        try:
            test_conn = pyodbc.connect(conn_string)
            test_conn.close()
            
        except Exception as conn_error:
            msg_erro = f"Conexão ao banco de dados"
            erros.append(msg_erro)
            raise
        
        # Criar instância e executar
        try:
            gerador = GeradorBaseAtiva()
            caminho_arquivo = gerador.executar_processo_completo(conn_string)
            resultado_final = "sucesso"
            
        except Exception as exec_error:
            msg_erro = f"Geração da base ativa"
            erros.append(msg_erro)
            resultado_final = "erro"
        
    except ValueError as e:
        resultado_final = "erro"
        
    except Exception as e:
        resultado_final = "erro"
    
    finally:
        # Salvar apenas o resumo final no log
        if not erros:
            salvar_log(f"{ICON_SUCCESS} Sucesso - Will Bank - tempos")
            print(f"{ICON_SUCCESS} Sucesso - Will Bank - tempos")
        else:
            for erro in erros:
                salvar_log(f"{ICON_ERROR} Erro - {erro}")
                print(f"{ICON_ERROR} Erro - {erro}")