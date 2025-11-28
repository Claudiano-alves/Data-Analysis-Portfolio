# pipeline_funil.py

from data_wrangling_mailingHist import (
    gerar_acumulado_mailing_hist,
    gerar_acumulado_maling_hist_unique
)
from data_wrangling_pagamentos import data_pagamentos, tratar_pagamentos
from data_wrangling_discagens_expert import acionamentos_expert
from data_loader import load_all_data
from utils import acionamentos_funil, consolidar_dataframes, salvar_analiticos_acionamentos


def executar_pipeline_funil():
    """
    Executa o pipeline completo de transformação dos dados do funil.

    Parâmetros
    ----------
    df_maling_hist : DataFrame
        Histórico de mailings.
    df_dw_calendario : DataFrame
        Dimensão calendário.
    df_tab_acionamentos : DataFrame
        Dados de acionamentos tabulados.
    df_tabulacao_aciona : DataFrame
        Dados de tabulação de acionamentos.
    df_discagens_trestto : DataFrame
        Dados de discagens Trestto.
    df_discagens_expert : DataFrame
        Dados de discagens Expert.
    df_pagamentos : DataFrame
        Dados de pagamentos.
    df_acordos : DataFrame
        Dados de acordos.

    Retorno
    -------
    df_funil_final : DataFrame
        DataFrame consolidado final do funil.
    """
    dados = load_all_data()

    (df_discagens_expert, 
     df_cad_devf, 
     df_tab_acionamentos, 
     df_maling_hist, 
     df_dw_calendario, 
     df_tabulacao_aciona, 
     df_pagamentos, 
     df_acordos, 
     df_discagens_trestto) = dados

    # 1. Monta mailing acumulado
    df_mailing_acumulado = gerar_acumulado_mailing_hist(df_maling_hist, df_dw_calendario)

    # 2. Executa tratamento de acionamentos
    (
        df_acionamentos_funil,
        df_analitico_acionamentos_humano,
        df_acion_semFaixa_humano,
        df_acion_semDescricao_humano,
        df_acion_semOrigem_humano,
        df_analitico_trestto,
        df_enriquecido_discagens_trestto_semFaixa,
        df_analitico_expert,
        df_enriquecido_discagens_expert_semFaixa,
        df_humano_tabulados_como_robo,
        df_dicagens_operacaoOutros,
        df_acionamentos_funil_long
    ) = acionamentos_funil(
        df_tab_acionamentos,
        df_tabulacao_aciona,
        df_dw_calendario,
        df_maling_hist,
        df_discagens_trestto,
        df_discagens_expert
    )

    arquivos_salvos = salvar_analiticos_acionamentos(
        df_acionamentos_funil,
        df_analitico_acionamentos_humano,
        df_acion_semFaixa_humano,
        df_acion_semDescricao_humano,
        df_acion_semOrigem_humano,
        df_analitico_trestto,
        df_enriquecido_discagens_trestto_semFaixa,
        df_analitico_expert,
        df_enriquecido_discagens_expert_semFaixa,
        df_humano_tabulados_como_robo,
        df_dicagens_operacaoOutros,
        df_acionamentos_funil_long
    )

    # 4. Executa tratamento de pagamentos
    df_pagamentos_funil, df_sem_fx_atraso, df_pagamento_analitico = tratar_pagamentos(
        df_pagamentos,
        df_acordos,
        df_maling_hist,
        df_dw_calendario
    )

    # 5. Consolida tudo
    df_funil_final = consolidar_dataframes(
        df_mailing_acumulado,
        df_pagamentos_funil,
        df_acionamentos_funil_long
    )

    return df_funil_final
