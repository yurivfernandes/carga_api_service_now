#!/usr/bin/env python3
"""
Script de exemplo para testar o novo sistema de ETL com logs
"""

import os
import sys

# Adiciona o diretório atual ao path para importar os módulos
sys.path.append(os.path.dirname(os.path.abspath(__file__)))


def test_new_interface():
    """Testa a nova interface do sistema"""
    print("🧪 TESTANDO NOVA INTERFACE DO SERVICENOW ETL")
    print("=" * 60)

    # Simula chamadas que serão feitas pelo usuário
    test_commands = [
        "help",
        "logs",
        # Descomente para testar com dados reais:
        # "today --json",
        # "range 2025-09-01 2025-09-15",
    ]

    for command in test_commands:
        print(f"\n🔧 Testando comando: python main.py {command}")
        print("-" * 40)

        # Aqui você pode executar os comandos se quiser testar
        # os.system(f"python main.py {command}")

        if command == "help":
            print("✅ Comando 'help' mostraria as instruções de uso completas")
        elif command == "logs":
            print("✅ Comando 'logs' mostraria as últimas execuções")
        else:
            print(
                f"✅ Comando '{command}' executaria extração com log detalhado"
            )

    print("\n🎉 Teste da interface concluído!")
    print("\n💡 COMANDOS MAIS ÚTEIS:")
    print("   python main.py help                    # Ver todas as opções")
    print("   python main.py today                   # Dados de hoje")
    print(
        "   python main.py range 2025-09-01 2025-09-15  # Período específico"
    )
    print(
        "   python main.py daily 7 --json          # Últimos 7 dias com JSON"
    )
    print(
        "   python main.py logs                    # Ver execuções anteriores"
    )


if __name__ == "__main__":
    test_new_interface()
