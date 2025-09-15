"""
Script de teste para validar a estrutura do projeto
"""

import sys
import os
sys.path.append(os.path.dirname(__file__))

from config import Config

def test_config():
    """Testa se as configurações estão sendo carregadas corretamente"""
    print("🧪 Testando configurações...")
    
    print(f"URL Base: {Config.SERVICENOW_BASE_URL}")
    print(f"Username: {Config.SERVICENOW_USERNAME}")
    print(f"DB Server: {Config.DB_SERVER}")
    print(f"DB Name: {Config.DB_NAME}")
    
    # Testa string de conexão
    conn_str = Config.get_db_connection_string()
    print(f"Connection String: {conn_str[:50]}...")
    
    # Testa headers
    headers = Config.get_servicenow_headers()
    print(f"Headers: {headers}")
    
    print("✅ Configurações OK")

def test_extractors():
    """Testa se os extractors podem ser importados"""
    print("\n🧪 Testando imports dos extractors...")
    
    try:
        from extractors.base_extractor import BaseServiceNowExtractor
        print("✅ BaseServiceNowExtractor importado")
        
        from extractors.incident_extractor import IncidentExtractor  
        print("✅ IncidentExtractor importado")
        
        from extractors.task_extractor import TaskExtractor
        print("✅ TaskExtractor importado")
        
        from extractors.sla_extractor import SLAExtractor
        print("✅ SLAExtractor importado")
        
        from extractors.time_worked_extractor import TimeWorkedExtractor
        print("✅ TimeWorkedExtractor importado")
        
        from extractors.contract_group_extractor import ContractSLAExtractor, GroupExtractor
        print("✅ ContractSLAExtractor e GroupExtractor importados")
        
        print("✅ Todos os extractors importados com sucesso")
        
    except ImportError as e:
        print(f"❌ Erro ao importar: {e}")
        return False
    
    return True

def test_database_manager():
    """Testa se o database manager pode ser importado"""
    print("\n🧪 Testando DatabaseManager...")
    
    try:
        from database_manager import DatabaseManager
        print("✅ DatabaseManager importado")
        
        # Cria instância (mas não se conecta ao banco)
        db_manager = DatabaseManager()
        print("✅ DatabaseManager instanciado")
        
    except ImportError as e:
        print(f"❌ Erro ao importar DatabaseManager: {e}")
        return False
    
    return True

def test_main():
    """Testa se o main pode ser importado"""
    print("\n🧪 Testando ServiceNowETL...")
    
    try:
        from main import ServiceNowETL
        print("✅ ServiceNowETL importado")
        
        # Cria instância (mas não executa ETL)
        etl = ServiceNowETL()
        print("✅ ServiceNowETL instanciado")
        
    except ImportError as e:
        print(f"❌ Erro ao importar ServiceNowETL: {e}")
        return False
    
    return True

if __name__ == "__main__":
    print("🚀 Executando testes da estrutura do projeto...")
    print("=" * 50)
    
    try:
        test_config()
        
        if test_extractors() and test_database_manager() and test_main():
            print("\n" + "=" * 50)
            print("🎉 Todos os testes passaram! A estrutura está correta.")
            print("\n📋 Próximos passos:")
            print("1. Configure suas credenciais no arquivo .env")
            print("2. Instale as dependências: pip install -r requirements.txt")
            print("3. Execute: python main.py")
        else:
            print("\n❌ Alguns testes falharam. Verifique os imports.")
            
    except Exception as e:
        print(f"\n❌ Erro durante os testes: {e}")
        sys.exit(1)