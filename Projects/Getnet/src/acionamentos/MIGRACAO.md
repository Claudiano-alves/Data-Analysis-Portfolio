"""
GUIA DE MIGRAÇÃO - Novo Sistema de Acionamentos
================================================

A estrutura antiga (arquivo único data_wrangling_acionamentos.py) foi reorganizada 
em uma pasta de módulos para melhor manutenção e escalabilidade.

ESTRUTURA NOVA:
===============

src/acionamentos/
├── __init__.py              # Exporta todas as funções públicas
├── tratamentos.py           # Funções de limpeza e enriquecimento
├── metricas_acumuladas.py   # Funções de métricas mensais
├── metricas_diarias.py      # Funções de métricas diárias
└── pipelines.py             # Orquestração completa

COMO USAR:
==========

1. IMPORTAÇÃO SIMPLES (recomendado):
   
   from Getnet.src.acionamentos import acionamentos_humano
   
   df_acionamentos, df_analitico, df_sem_fx, df_sem_desc, df_sem_origem = acionamentos_humano(
       df_tab_acionamentos,
       df_tabulacao_aciona,
       df_dw_calendario,
       df_maling_hist
   )


2. IMPORTAÇÕES ESPECÍFICAS POR CATEGORIA:

   # Tratamentos
   from Getnet.src.acionamentos.tratamentos import (
       tratar_acionamentos_tabulacao,
       confere_tabulacao_acionamentos,
       enriquecer_acionamentos,
       separar_inconsistencias
   )

   # Métricas acumuladas
   from Getnet.src.acionamentos.metricas_acumuladas import (
       acionamentos_fxAtraso_origem_humano,
       acionamentos_unique_humano,
       acionamentos_esforco_humano
   )

   # Métricas diárias
   from Getnet.src.acionamentos.metricas_diarias import (
       acionamentos_unique_origem_fxAtraso,
       acionamentos_unique_fxAtraso,
       acionamentos_esforco_origem_fxAtraso
   )


3. FLUXO CUSTOMIZADO:

   from Getnet.src.acionamentos.tratamentos import (
       tratar_acionamentos_tabulacao,
       confere_tabulacao_acionamentos,
       enriquecer_acionamentos
   )
   from Getnet.src.acionamentos.metricas_acumuladas import (
       acionamentos_fxAtraso_origem_humano
   )

   # Step 1: Tratamento
   df_tabulacao = tratar_acionamentos_tabulacao(df_tabulacao_aciona)
   df_tabulados = confere_tabulacao_acionamentos(df_tab_acionamentos, df_tabulacao)
   
   # Step 2: Enriquecimento
   df_limpo, df_sem_fx, df_sem_desc, df_sem_orig = enriquecer_acionamentos(
       df_tabulados, df_mailing_hist, df_dw_calendario
   )
   
   # Step 3: Métrica específica
   df_metrica = acionamentos_fxAtraso_origem_humano(df_limpo, df_dw_calendario)


DEPENDÊNCIAS ENTRE MÓDULOS:
============================

tratamentos.py
    ├── Importa: utils (salvar_log, registrar_tempo)
    └── Exporta: dados enriquecidos

metricas_acumuladas.py
    ├── Importa: utils, tratamentos (indiretamente via dados)
    └── Exporta: séries acumuladas por mês

metricas_diarias.py
    ├── Importa: utils
    └── Exporta: séries diárias

pipelines.py
    ├── Importa: tratamentos, metricas_acumuladas, utils
    └── Exporta: pipeline completo orquestrado

__init__.py
    ├── Importa: todos os módulos
    └── Exporta: interface única e consistente


MANUTENÇÃO FUTURA:
==================

Adicionar uma nova função de métrica?
   → Crie em metricas_acumuladas.py ou metricas_diarias.py
   → Adicione ao __all__ em __init__.py

Alterar lógica de tratamento?
   → Modifique em tratamentos.py
   → Testes em test_tratamentos.py (recomendado)

Criar novo pipeline?
   → Adicione função em pipelines.py
   → Exporte em __init__.py


TESTES RECOMENDADOS:
====================

tests/
├── test_tratamentos.py
├── test_metricas_acumuladas.py
├── test_metricas_diarias.py
└── test_pipelines.py


ARQUIVO ANTIGO:
===============

O arquivo data_wrangling_acionamentos.py pode ser removido após verificar
se todas as dependências foram migradas para os novos módulos.
"""
