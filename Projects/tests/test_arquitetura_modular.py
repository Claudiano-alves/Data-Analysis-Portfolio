"""
TESTES DE VALIDAÇÃO - Arquitetura Modular
Valida que todos os módulos foram criados corretamente.

Execute com: python -m pytest tests/test_arquitetura_modular.py -v
"""

import sys
from pathlib import Path

# Adicionar src ao path
src_path = Path(__file__).parent.parent / 'src'
sys.path.insert(0, str(src_path))


def test_imports_mailing():
    """Validar imports do módulo mailing"""
    from mailing import (
        adicionar_produto,
        adicionar_faixa_atraso,
        tratar_base_mailing_hist,
        processar_mailing_completo
    )
    assert callable(adicionar_produto)
    assert callable(processar_mailing_completo)
    print("✓ Mailing imports validados")


def test_imports_pagamentos():
    """Validar imports do módulo pagamentos"""
    from pagamentos import (
        data_pagamentos,
        gerar_acumulado_por_dia_util,
        processar_pagamentos_completo,
        tratar_pagamentos
    )
    assert callable(data_pagamentos)
    assert callable(processar_pagamentos_completo)
    print("✓ Pagamentos imports validados")


def test_imports_acionamentos():
    """Validar imports do módulo acionamentos"""
    from acionamentos import (
        confere_tabulacao_acionamentos,
        enriquecer_acionamentos,
        acionamentos_humano
    )
    assert callable(confere_tabulacao_acionamentos)
    assert callable(acionamentos_humano)
    print("✓ Acionamentos imports validados")


def test_imports_consolidacao():
    """Validar imports do módulo consolidacao"""
    from consolidacao import (
        executar_pipeline_funil_completo,
        consolidar_dataframes_funil
    )
    assert callable(executar_pipeline_funil_completo)
    assert callable(consolidar_dataframes_funil)
    print("✓ Consolidacao imports validados")


def test_diretorio_estrutura():
    """Validar que todas as pastas foram criadas"""
    from pathlib import Path
    
    base = Path(__file__).parent.parent / 'src'
    
    pastas_esperadas = [
        'mailing',
        'pagamentos',
        'acionamentos',
        'consolidacao'
    ]
    
    for pasta in pastas_esperadas:
        caminho = base / pasta
        assert caminho.exists(), f"Pasta {pasta} não encontrada"
        assert (caminho / '__init__.py').exists(), f"__init__.py não encontrado em {pasta}"
    
    print("✓ Estrutura de diretórios validada")


def test_arquivos_necessarios():
    """Validar que todos os arquivos foram criados"""
    from pathlib import Path
    
    base = Path(__file__).parent.parent / 'src'
    
    arquivos_esperados = {
        'mailing': ['__init__.py', 'tratamentos.py', 'metricas_acumuladas.py', 'pipelines.py'],
        'pagamentos': ['__init__.py', 'tratamentos.py', 'metricas_acumuladas.py', 'pipelines.py'],
        'acionamentos': ['__init__.py', 'tratamentos.py', 'metricas_acumuladas.py', 'metricas_diarias.py', 'pipelines.py'],
        'consolidacao': ['__init__.py', 'pipelines.py']
    }
    
    for pasta, arquivos in arquivos_esperados.items():
        for arquivo in arquivos:
            caminho = base / pasta / arquivo
            assert caminho.exists(), f"Arquivo {pasta}/{arquivo} não encontrado"
    
    print("✓ Todos os arquivos necessários foram criados")


def test_documentacao():
    """Validar que documentação foi criada"""
    from pathlib import Path
    
    doc_path = Path(__file__).parent.parent / 'src' / 'ARQUITETURA_MODULAR.md'
    assert doc_path.exists(), "ARQUITETURA_MODULAR.md não encontrado"
    
    print("✓ Documentação criada")


if __name__ == '__main__':
    """Executar testes manualmente"""
    print("\n" + "=" * 60)
    print("VALIDANDO ARQUITETURA MODULAR")
    print("=" * 60 + "\n")
    
    testes = [
        test_diretorio_estrutura,
        test_arquivos_necessarios,
        test_imports_mailing,
        test_imports_pagamentos,
        test_imports_acionamentos,
        test_imports_consolidacao,
        test_documentacao
    ]
    
    sucesso = 0
    falhas = 0
    
    for teste in testes:
        try:
            teste()
            sucesso += 1
        except Exception as e:
            print(f"✗ {teste.__name__}: {str(e)}")
            falhas += 1
    
    print("\n" + "=" * 60)
    print(f"RESULTADO: {sucesso} sucesso, {falhas} falhas")
    print("=" * 60 + "\n")
    
    if falhas == 0:
        print("✅ ARQUITETURA VALIDADA COM SUCESSO!\n")
    else:
        print("❌ EXISTEM PROBLEMAS NA ARQUITETURA\n")
        sys.exit(1)
