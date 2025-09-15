
from typing import Dict, Optional

import polars as pl
import pyodbc

from config import Config


class DatabaseManager:
    def __init__(self):
        self.db_metrics = {
            "total_operations": 0,
            "total_db_time": 0.0,
            "total_records_processed": 0,
            "failed_operations": 0,
            "operations_detail": [],
        }

    def save_dataframes_to_database(
        self,
        dataframes: Dict[str, pl.DataFrame]
    ) -> bool:
        """
        Salva DataFrames no banco de dados configurado via .env/config.py (SQL Server, etc).
        """
        import pyodbc

        from config import Config
        success = True
        conn_str = Config.get_db_connection_string()
        try:
            conn = pyodbc.connect(conn_str)
            cursor = conn.cursor()
            for table_name, df in dataframes.items():
                if df is None or df.is_empty():
                    continue
                columns = df.columns
                # Cria a tabela se não existir (schema simples, pode ser adaptado)
                col_defs = ", ".join([f'[{col}] NVARCHAR(MAX)' for col in columns])
                pk = "id" if "id" in columns else columns[0]
                cursor.execute(f"IF OBJECT_ID('{table_name}', 'U') IS NULL CREATE TABLE {table_name} ({col_defs}, PRIMARY KEY ([{pk}]))")

                # Upsert (MERGE para SQL Server)
                for row in df.iter_rows(named=True):
                    values = [row[col] if row[col] is not None else None for col in columns]
                    # Monta comando MERGE para upsert
                    merge_sql = f"MERGE INTO {table_name} AS target USING (SELECT "
                    merge_sql += ", ".join([f'? AS [{col}]' for col in columns])
                    merge_sql += f") AS source ON target.[{pk}] = source.[{pk}] "
                    merge_sql += "WHEN MATCHED THEN UPDATE SET "
                    merge_sql += ", ".join([f"target.[{col}] = source.[{col}]" for col in columns if col != pk])
                    merge_sql += " WHEN NOT MATCHED THEN INSERT ("
                    merge_sql += ", ".join([f'[{col}]' for col in columns])
                    merge_sql += ") VALUES ("
                    merge_sql += ", ".join([f'source.[{col}]' for col in columns])
                    merge_sql += ");"
                    cursor.execute(merge_sql, values)
                conn.commit()
            conn.close()
        except Exception as e:
            print(f"Erro ao salvar no banco: {e}")
            success = False
        return success

    def print_db_metrics(self):
        print("🗄️  Métricas Banco de Dados: (implementação em desenvolvimento)")

    def get_db_metrics_data(self) -> Dict:
        """Retorna dados das métricas para uso externo"""
        return {
            "total_transactions": self.db_metrics.get("total_operations", 0),
            "total_records": self.db_metrics.get("total_records_processed", 0),
            "total_time": self.db_metrics.get("total_db_time", 0.0),
        }