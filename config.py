"""
Configurações e utilitários para o projeto ServiceNow API Extractor
"""

import os

from dotenv import load_dotenv

# Carrega variáveis do arquivo .env
load_dotenv()

class Config:
    """Classe para gerenciar configurações da aplicação"""
    
    # ServiceNow API
    SERVICENOW_BASE_URL = os.getenv('SERVICENOW_BASE_URL')
    SERVICENOW_USERNAME = os.getenv('SERVICENOW_USERNAME')
    SERVICENOW_PASSWORD = os.getenv('SERVICENOW_PASSWORD')
    
    # Database
    DB_DRIVER = os.getenv('DB_DRIVER')
    DB_SERVER = os.getenv('DB_SERVER')
    DB_NAME = os.getenv('DB_NAME')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    
    @classmethod
    def get_db_connection_string(cls) -> str:
        """Retorna a string de conexão do banco de dados"""
        return f"DRIVER={{{cls.DB_DRIVER}}};SERVER={cls.DB_SERVER};DATABASE={cls.DB_NAME};UID={cls.DB_USER};PWD={cls.DB_PASSWORD}"
    
    @classmethod
    def get_servicenow_auth(cls) -> tuple:
        """Retorna as credenciais do ServiceNow"""
        return (cls.SERVICENOW_USERNAME, cls.SERVICENOW_PASSWORD)
    
    @classmethod
    def get_servicenow_headers(cls) -> dict:
        """Retorna os headers padrão para requisições ServiceNow"""
        return {"Content-Type": "application/json"}

def get_db_connection():
    """Retorna uma conexão com o banco de dados"""
    try:
        import pyodbc
    except Exception as e:
        # Não falha no import do módulo para permitir testes sem dependência de DB
        raise ImportError(
            "pyodbc não está disponível no ambiente. Instale a dependência ou configure um ambiente com ODBC."
        ) from e

    return pyodbc.connect(Config.get_db_connection_string())

def flatten_reference_fields(data: dict) -> dict:
    """Converte campos de referência do ServiceNow para valores simples"""
    for key in list(data.keys()):
        value = data.get(key)
        if isinstance(value, dict) and "value" in value:
            data[key] = value.get("value")
            data[f"dv_{key}"] = ""
    return data    return data    return data