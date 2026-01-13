import pandas as pd
from datetime import datetime, timedelta
from typing import Tuple, Optional
import os

def get_date_range_from_csv(csv_path: Optional[str] = None) -> Tuple[str, str]:
    """
    Lê um arquivo XLSX e retorna o range de datas baseado na última data encontrada.
    
    Lógica:
    - Encontra a última (maior) data no arquivo
    - Verifica se os dias restantes do mês são apenas finais de semana
    - Se sim, ajusta para o próximo mês
    - Define data_inicio como dia 1 do mês da última data (ajustada)
    - Define data_fim como hoje - 1 dia
    
    Args:
        csv_path: Caminho do arquivo XLSX. Se None, usa o caminho padrão do servidor.
        
    Returns:
        Tupla com (data_inicio, data_fim) no formato 'YYYY-MM-DD'
    """
    
    def ajustar_para_proximo_mes_se_necessario(data):
        """
        Verifica se os dias restantes do mês são apenas finais de semana.
        Se sim, avança para o primeiro dia do próximo mês.
        
        Args:
            data: datetime object
            
        Returns:
            datetime: Data ajustada (primeiro dia do próximo mês se necessário)
        """
        # Último dia do mês atual
        ultimo_dia_mes = pd.Timestamp(data.year, data.month, 1) + pd.offsets.MonthEnd(1)
        
        # Verifica se todos os dias restantes do mês (após a data atual) são finais de semana
        dias_restantes = pd.date_range(start=data + pd.Timedelta(days=1), end=ultimo_dia_mes, freq='D')
        
        # Se não há dias restantes no mês, retorna a data original
        if len(dias_restantes) == 0:
            return data
        
        # Verifica se TODOS os dias restantes são sábado (5) ou domingo (6)
        todos_finais_semana = all(dia.weekday() >= 5 for dia in dias_restantes)
        
        if todos_finais_semana:
            # Avança para o primeiro dia do próximo mês
            proximo_mes = data + pd.offsets.MonthBegin(1)
            print(f"⏭️ Dias restantes do mês ({data.strftime('%Y-%m')}) são apenas finais de semana.")
            print(f"   Ajustando para: {proximo_mes.strftime('%Y-%m-%d')}")
            return proximo_mes
        
        print(data)
    
    # Define caminho padrão se não fornecido
    if csv_path is None:
        csv_path = r"\\trc-dc-ad\Planejamento\MIS\CARTEIRAS\GetNet\df_csvBI_padronizado.xlsx"
    
    try:
        print(f"📂 Lendo arquivo: {csv_path}")
        
        # Lê o XLSX da segunda aba
        df = pd.read_excel(csv_path, sheet_name='df_csvBI_padronizado')
        
        # Verifica se coluna 'data' existe
        if 'data' not in df.columns:
            date_col = df.columns[0]
            print(f"⚠️ Coluna 'data' não encontrada. Usando primeira coluna: '{date_col}'")
        else:
            date_col = 'data'
        
        # Converte para datetime e pega a última data
        df[date_col] = pd.to_datetime(df[date_col], errors='coerce')
        
        # Remove valores nulos
        df_validos = df[df[date_col].notna()]
        
        if len(df_validos) == 0:
            raise ValueError("Nenhuma data válida encontrada no XLSX")
        
        # Filtro: Remove datas muito antigas (antes de 2020)
        ano_minimo = 2020
        df_validos = df_validos[df_validos[date_col].dt.year >= ano_minimo]
        
        if len(df_validos) == 0:
            raise ValueError(f"Nenhuma data válida encontrada após {ano_minimo}")
        
        # Pega a última (maior) data do arquivo
        ultima_data = df_validos[date_col].max()
        
        print(f"📅 Última data no arquivo: {ultima_data.strftime('%Y-%m-%d')}")
        
        # ⭐ AJUSTE PARA FINAIS DE SEMANA ⭐
        # Verifica se precisa pular para o próximo mês
        ultima_data_ajustada = ajustar_para_proximo_mes_se_necessario(ultima_data)
        
        # Data início: sempre dia 1 do mês da última data ajustada
        data_inicio = ultima_data_ajustada.replace(day=1).strftime('%Y-%m-%d')
        
        # Data fim: sempre hoje - 1 dia
        hoje = datetime.now()
        data_fim = (hoje - timedelta(days=1)).strftime('%Y-%m-%d')
        
        print(f"✅ Arquivo lido com sucesso!")
        print(f"📅 Range de datas calculado:")
        print(f"   Data início (dia 1 do mês ajustado): {data_inicio}")
        print(f"   Data fim (hoje - 1): {data_fim}")
        
        return data_inicio, data_fim
        
    except FileNotFoundError:
        print(f"❌ Arquivo não encontrado: {csv_path}")
        print(f"⚠️ Usando datas padrão.")
        return get_default_date_range()
        
    except PermissionError:
        print(f"❌ Sem permissão para acessar: {csv_path}")
        print(f"⚠️ Usando datas padrão.")
        return get_default_date_range()
        
    except Exception as e:
        print(f"❌ Erro ao processar XLSX: {e}")
        print(f"⚠️ Usando datas padrão.")
        return get_default_date_range()
    
def get_default_date_range() -> Tuple[str, str]:
    """
    Retorna range de datas padrão: primeiro dia do mês atual até ontem.
    """
    hoje = datetime.now()
    data_inicio = hoje.replace(day=1).strftime('%Y-%m-%d')
    data_fim = (hoje - timedelta(days=1)).strftime('%Y-%m-%d')
    return data_inicio, data_fim

LOG_FILE = 'logs/acionamentos.txt'

def salvar_log(mensagem, arquivo=LOG_FILE):
    """Salva mensagem no arquivo de log com timestamp"""
    os.makedirs(os.path.dirname(arquivo), exist_ok=True)
    timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    with open(arquivo, 'a', encoding='utf-8') as f:
        f.write(f"[{timestamp}] {mensagem}\n")