#!/usr/bin/env python3
"""
Script de exemplo para testar o novo sistema ETL normalizado
"""

import os
import sys
from datetime import datetime, timedelta

# Adiciona o diretório do projeto ao path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from etl_orchestrator import ServiceNowETLOrchestrator


def test_reference_sync():
    """Testa sincronização de dados de referência"""
    print("🧪 TESTE: Sincronização de Referências")
    print("=" * 50)

    orchestrator = ServiceNowETLOrchestrator()

    # Teste incremental
    print("\n1️⃣ Testando sincronização incremental...")
    success = orchestrator.sync_reference_data(force_full_sync=False)
    print(f"Resultado: {'✅ Sucesso' if success else '❌ Falha'}")

    return success


def test_incident_extraction():
    """Testa extração de incidentes"""
    print("\n🧪 TESTE: Extração de Incidentes")
    print("=" * 50)

    orchestrator = ServiceNowETLOrchestrator()

    # Testa extração de ontem
    yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"\n2️⃣ Testando extração de incidentes de {yesterday}...")
    success = orchestrator.extract_incidents(yesterday, yesterday)
    print(f"Resultado: {'✅ Sucesso' if success else '❌ Falha'}")

    return success


def test_full_workflow():
    """Testa fluxo completo"""
    print("\n🧪 TESTE: Fluxo ETL Completo")
    print("=" * 50)

    orchestrator = ServiceNowETLOrchestrator()

    # Testa fluxo dos últimos 2 dias
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    print(f"\n3️⃣ Testando fluxo completo de {start_date} até {end_date}...")
    success = orchestrator.full_etl_workflow(
        start_date, end_date, force_full_reference_sync=False
    )
    print(f"Resultado: {'✅ Sucesso' if success else '❌ Falha'}")

    return success


def test_quick_sync():
    """Testa sincronização rápida"""
    print("\n🧪 TESTE: Sincronização Rápida")
    print("=" * 50)

    orchestrator = ServiceNowETLOrchestrator()

    print("\n4️⃣ Testando sincronização rápida (1 dia)...")
    success = orchestrator.quick_incident_sync(days_back=1)
    print(f"Resultado: {'✅ Sucesso' if success else '❌ Falha'}")

    return success


def test_company_sync():
    """Testa sincronização de empresas por tipo"""
    print("\n🧪 TESTE: Sincronização de Empresas")
    print("=" * 50)

    orchestrator = ServiceNowETLOrchestrator()

    print("\n5️⃣ Testando sincronização de empresas cliente...")
    success = orchestrator.sync_specific_companies("customer")
    print(f"Resultado: {'✅ Sucesso' if success else '❌ Falha'}")

    return success


def run_all_tests():
    """Executa todos os testes"""
    print("🚀 INICIANDO TESTES DO SISTEMA ETL NORMALIZADO")
    print("=" * 60)
    print(f"⏰ Início: {datetime.now()}")

    tests = [
        ("Sincronização de Referências", test_reference_sync),
        ("Extração de Incidentes", test_incident_extraction),
        ("Fluxo ETL Completo", test_full_workflow),
        ("Sincronização Rápida", test_quick_sync),
        ("Sincronização de Empresas", test_company_sync),
    ]

    results = []

    for test_name, test_func in tests:
        try:
            success = test_func()
            results.append((test_name, success))
        except Exception as e:
            print(f"❌ ERRO no teste {test_name}: {e}")
            results.append((test_name, False))

        print("\n" + "-" * 60)

    # Resumo final
    print("\n🎯 RESUMO DOS TESTES")
    print("=" * 60)

    passed = sum(1 for _, success in results if success)
    total = len(results)

    for test_name, success in results:
        status = "✅ PASSOU" if success else "❌ FALHOU"
        print(f"   {status} - {test_name}")

    print(f"\n📊 RESULTADO GERAL: {passed}/{total} testes passaram")

    if passed == total:
        print("🎉 TODOS OS TESTES PASSARAM! Sistema está funcionando!")
    else:
        print("⚠️ Alguns testes falharam. Verifique os logs acima.")

    print(f"⏰ Fim: {datetime.now()}")


def main():
    """Função principal"""
    if len(sys.argv) < 2:
        print("🧪 TESTADOR DO SISTEMA ETL NORMALIZADO")
        print("=" * 50)
        print("\nComandos disponíveis:")
        print("  python test_etl.py all         - Executa todos os testes")
        print(
            "  python test_etl.py ref         - Testa sincronização de referências"
        )
        print(
            "  python test_etl.py incidents   - Testa extração de incidentes"
        )
        print("  python test_etl.py full        - Testa fluxo completo")
        print("  python test_etl.py quick       - Testa sincronização rápida")
        print(
            "  python test_etl.py companies   - Testa sincronização de empresas"
        )
        return

    command = sys.argv[1].lower()

    if command == "all":
        run_all_tests()
    elif command == "ref":
        test_reference_sync()
    elif command == "incidents":
        test_incident_extraction()
    elif command == "full":
        test_full_workflow()
    elif command == "quick":
        test_quick_sync()
    elif command == "companies":
        test_company_sync()
    else:
        print(f"❌ Comando desconhecido: {command}")
        print("Execute sem argumentos para ver os comandos disponíveis")


if __name__ == "__main__":
    main()
