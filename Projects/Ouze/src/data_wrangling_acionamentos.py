def tratar_acionamentos(df_tabualacao_aciona):
    colunas_binarias = ['CPC', 'CPCA', 'PROMESSA']

    # Converte todas para int
    df_tabualacao_aciona[colunas_binarias] = df_tabualacao_aciona[colunas_binarias].astype(int)
    return df_tabualacao_aciona