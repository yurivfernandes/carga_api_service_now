from typing import Dict, Optional

import polars as pl


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
        Salva DataFrames no banco de dados com upsert (insert ou update) para sys_company e sys_user.
        """
        import sqlite3
        success = True
        db_path = "service_now.db"  # Nome do arquivo do banco SQLite
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            for table_name, df in dataframes.items():
                if df is None or df.is_empty():
                    continue
                # Cria a tabela se não existir (schema simples, pode ser adaptado)
                columns = df.columns
                col_defs = ", ".join([f'{col} TEXT' for col in columns])
                pk = "id" if "id" in columns else columns[0]
                cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} ({col_defs}, PRIMARY KEY ({pk}))")

                # Upsert (insert or update)
                for row in df.iter_rows(named=True):
                    placeholders = ", ".join(["?" for _ in columns])
                    update_clause = ", ".join([f"{col}=excluded.{col}" for col in columns if col != pk])
                    sql = (
                        f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders}) "
                        f"ON CONFLICT({pk}) DO UPDATE SET {update_clause}"
                    )
                    values = [str(row[col]) if row[col] is not None else None for col in columns]
                    cursor.execute(sql, values)
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
