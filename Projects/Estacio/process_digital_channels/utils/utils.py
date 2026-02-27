import pandas as pd


def tratar_base_auxiliar(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica os tratamentos e filtros necessários na base da carteira
    antes do cruzamento com outra base.

    Etapa 1 - Filtros:
        - COD_PRODUT: apenas 1 ou 2
        - GRUPO: apenas ESTACIO, WYDEN, UNITOLEDO ou ATHENAS
        - STALUNO: apenas ATIVO ou INATIVO

    Etapa 2 - Deduplicação e enriquecimento:
        - Quando o mesmo CONTRATO_FIN aparecer nos dois COD_PRODUT (1 e 2),
          mantém apenas o registro com COD_PRODUT = 2.
        - Cria coluna GRUPO_SEGMENTADO
        - Cria coluna PRODUTO_SEGMENTADO

    Parâmetros
    ----------
    df : pd.DataFrame
        Base bruta com as colunas da carteira.

    Retorna
    -------
    pd.DataFrame
        Base tratada, pronta para o cruzamento.
    """

    df = df.copy()

    # ------------------------------------------------------------------ #
    # Etapa 1 – Filtros                                                    #
    # ------------------------------------------------------------------ #

    # Garantir que COD_PRODUT seja numérico para comparação segura
    df["COD_PRODUT"] = pd.to_numeric(df["COD_PRODUT"], errors="coerce")

    # Aplicar strip em todas as colunas do tipo string/object
    str_cols = df.select_dtypes(include="object").columns
    df[str_cols] = df[str_cols].apply(lambda col: col.str.strip())

    # Normalizar para upper apenas nas colunas usadas nos filtros
    for col in ["GRUPO", "STALUNO"]:
        df[col] = df[col].str.upper()

    filtro_produto = df["COD_PRODUT"].isin([1, 2])
    filtro_grupo   = df["GRUPO"].isin(["ESTACIO", "WYDEN", "UNITOLEDO", "ATHENAS"])
    filtro_staluno = df["STALUNO"].isin(["ATIVO", "INATIVO"])

    df = df[filtro_produto & filtro_grupo & filtro_staluno].copy()

    # ------------------------------------------------------------------ #
    # Etapa 2 – Deduplicação: prioriza COD_PRODUT = 2                     #
    # ------------------------------------------------------------------ #

    # Ordena de forma que COD_PRODUT = 2 sempre venha antes do 1.
    # Essa ordenação vale para os dois passos abaixo.
    df = df.sort_values("COD_PRODUT", ascending=False)

    # Passo 1 – deduplica por CONTRATO_FIN
    # Se o mesmo contrato existir em COD_PRODUT 1 e 2, mantém o 2.
    df = df.drop_duplicates(subset=["CONTRATO_FIN"], keep="first")

    # Passo 2 – deduplica por ID_CLIENTE
    # Após a deduplicação por contrato, se o mesmo cliente ainda aparecer
    # em COD_PRODUT 1 e 2 (via contratos distintos), mantém o 2.
    df = (
        df.drop_duplicates(subset=["ID_CLIENTE"], keep="first")
          .reset_index(drop=True)
    )

    # ------------------------------------------------------------------ #
    # Etapa 2 – Colunas derivadas                                          #
    # ------------------------------------------------------------------ #

    # GRUPO_SEGMENTADO
    condicoes_seg = [
        df["GRUPO"].isin(["ESTACIO", "ATHENAS"]) & (df["STALUNO"] == "ATIVO"),
        df["GRUPO"].isin(["ESTACIO", "ATHENAS"]) & (df["STALUNO"] == "INATIVO"),
        df["GRUPO"].isin(["WYDEN", "UNITOLEDO"]) & (df["STALUNO"] == "ATIVO"),
        df["GRUPO"].isin(["WYDEN", "UNITOLEDO"]) & (df["STALUNO"] == "INATIVO"),
    ]
    valores_seg = [
        "ESTACIO ATIVO",
        "ESTACIO INATIVO",
        "WYDEN ATIVO",
        "WYDEN INATIVO",
    ]
    import numpy as np
    df["GRUPO_SEGMENTADO"] = np.select(condicoes_seg, valores_seg, default="OUTROS")

    # PRODUTO_SEGMENTADO
    df["PRODUTO_SEGMENTADO"] = np.select(
        [df["COD_PRODUT"] == 2, df["COD_PRODUT"] == 1],
        ["RENOVAÇÃO", "BASE LIQUIDA"],
        default="OUTROS",
    )

    return df


def cruzar_mailing_com_base(
    df_mailing_hist: pd.DataFrame,
    df_base_auxiliar_tratado: pd.DataFrame,
) -> pd.DataFrame:
    """
    Cruza df_mailing_hist com df_base_auxiliar_tratado pelo número de contrato
    (inner join), trazendo todas as colunas de ambas as bases.

    Chaves de cruzamento:
        df_mailing_hist          → CONTRATO
        df_base_auxiliar_tratado → CONTRATO_FIN

    Parâmetros
    ----------
    df_mailing_hist : pd.DataFrame
        Base de mailing histórico com as colunas:
        DATA, CONTRATO, CPF, ATRASO, COD_CLI, COD_CAR, VALOR, FX_ATRASO

    df_base_auxiliar_tratado : pd.DataFrame
        Base auxiliar já tratada pela função tratar_base_carteira(), com as colunas:
        CONTRATO_FIN, COD_PRODUT, ID_CLIENTE, REGIONAL, GRUPO, SPD, BU, MODALIDE,
        STDEBITO, STALUNO, APROACAD, CURSO, PRODUTO, ULTRENOV, LTCOMER,
        GRUPO_SEGMENTADO, PRODUTO_SEGMENTADO

    Retorna
    -------
    pd.DataFrame
        Base cruzada (inner join) com todas as colunas das duas bases.
        A coluna CONTRATO_FIN é removida após o merge para evitar redundância,
        mantendo CONTRATO como referência.
    """

    df_mailing = df_mailing_hist.copy()
    df_base    = df_base_auxiliar_tratado.copy()

    # Garantir que as chaves sejam string sem espaços para o join não falhar
    df_mailing["CONTRATO"]  = df_mailing["CONTRATO"].astype(str).str.strip()
    df_base["CONTRATO_FIN"] = df_base["CONTRATO_FIN"].astype(str).str.strip()

    df_cruzado = df_mailing.merge(
        df_base,
        left_on="CONTRATO",
        right_on="CONTRATO_FIN",
        how="inner",
    ).drop(columns=["CONTRATO_FIN"])

    return df_cruzado