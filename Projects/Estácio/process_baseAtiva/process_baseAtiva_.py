import pyodbc
import pandas as pd
from pathlib import Path
from datetime import datetime
import logging

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

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
    # Configurar a string de conexão com o banco de dados
    # IMPORTANTE: Ajuste os parâmetros conforme seu ambiente
    
    # Opção 2: Com autenticação Windows (Trusted Connection)
    connection_string = (
        "DRIVER={SQL Server};"
        "SERVER=seu_servidor;"
        "DATABASE=seu_banco;"
        "Trusted_Connection=yes"
    )
    
    # Criar instância e executar
    gerador = GeradorBaseAtiva()
    
    try:
        caminho_arquivo = gerador.executar_processo_completo(connection_string)
        print(f"\n✓ Arquivo gerado com sucesso!")
        print(f"  Localização: {caminho_arquivo}")
    except Exception as e:
        print(f"\n✗ Erro ao executar o processo: {e}")