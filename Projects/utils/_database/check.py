# utils/database/check.py

from datetime import date


def ja_inserido_hoje(tabela: str, conn) -> bool:
    """
    Verifica se já existem registros com dt_carga de hoje na tabela.
    Convenção: toda tabela destino deve ter a coluna dt_carga (DATETIME2 DEFAULT GETDATE()).
    """
    hoje = date.today().isoformat()
    cursor = conn.cursor()
    cursor.execute(
        f"SELECT TOP 1 1 FROM {tabela} WHERE CAST(dt_carga AS DATE) = ?",
        hoje
    )
    return cursor.fetchone() is not None