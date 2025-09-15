"""
Gerenciador de dados JSON compactados para ServiceNow
"""

import gzip
import json
import time
from datetime import date, datetime
from typing import Any, Dict, Optional

import polars as pl

from config import get_db_connection


class JSONDataManager:
    """Gerenciador para armazenamento de dados ServiceNow em formato JSON compactado"""

    def __init__(self):
        self.compression_enabled = True
        self.json_metrics = {
            "total_saves": 0,
            "total_json_size_kb": 0.0,
            "total_compressed_size_kb": 0.0,
            "total_records": 0,
            "avg_compression_ratio": 0.0,
        }

    def compact_json_data(self, data: Dict[str, Any]) -> str:
        """Converte dados para JSON no formato mais compacto possível"""
        # Configurações para JSON mínimo
        return json.dumps(
            data,
            separators=(",", ":"),  # Remove espaços
            ensure_ascii=False,  # Permite caracteres UTF-8
            default=self._json_serializer,
        )

    def _json_serializer(self, obj):
        """Serializa objetos especiais para JSON"""
        if isinstance(obj, (datetime, date)):
            return obj.isoformat()
        elif hasattr(obj, "to_dict"):  # Para objetos Polars
            return obj.to_dict()
        elif hasattr(obj, "__dict__"):
            return obj.__dict__
        return str(obj)

    def compress_data(self, json_string: str) -> bytes:
        """Comprime dados JSON usando gzip"""
        return gzip.compress(json_string.encode("utf-8"), compresslevel=9)

    def decompress_data(self, compressed_data: bytes) -> str:
        """Descomprime dados JSON"""
        return gzip.decompress(compressed_data).decode("utf-8")

    def calculate_sizes(
        self, json_string: str, compressed_data: bytes = None
    ) -> Dict[str, float]:
        """Calcula tamanhos e razão de compressão"""
        json_size_kb = len(json_string.encode("utf-8")) / 1024

        if compressed_data:
            compressed_size_kb = len(compressed_data) / 1024
            compression_ratio = (1 - compressed_size_kb / json_size_kb) * 100
        else:
            compressed_size_kb = None
            compression_ratio = None

        return {
            "json_size_kb": round(json_size_kb, 2),
            "compressed_size_kb": round(compressed_size_kb, 2)
            if compressed_size_kb
            else None,
            "compression_ratio": round(compression_ratio, 2)
            if compression_ratio
            else None,
        }

    def prepare_servicenow_data(
        self, dataframes: Dict[str, pl.DataFrame]
    ) -> Dict[str, Any]:
        """Prepara dados do ServiceNow para armazenamento JSON"""
        prepared_data = {
            "metadata": {
                "extraction_timestamp": datetime.now().isoformat(),
                "table_count": len(dataframes),
                "total_records": sum(len(df) for df in dataframes.values()),
            },
            "data": {},
        }

        for table_name, df in dataframes.items():
            if not df.is_empty():
                # Converte DataFrame para lista de dicionários de forma compacta
                records = df.to_dicts()

                # Remove valores None/null para economizar espaço
                cleaned_records = []
                for record in records:
                    cleaned_record = {
                        k: v for k, v in record.items() if v is not None
                    }
                    cleaned_records.append(cleaned_record)

                prepared_data["data"][table_name] = {
                    "count": len(cleaned_records),
                    "records": cleaned_records,
                }

        return prepared_data

    def save_json_data_to_db(
        self,
        dataframes: Dict[str, pl.DataFrame],
        extraction_type: str = "daily",
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        extraction_metrics: Optional[Dict] = None,
    ) -> bool:
        """Salva dados em formato JSON compactado no banco"""

        start_time = time.time()

        try:
            # 1. Prepara dados para JSON
            prepared_data = self.prepare_servicenow_data(dataframes)
            json_string = self.compact_json_data(prepared_data)

            # 2. Comprime dados se habilitado
            compressed_data = None
            if self.compression_enabled:
                compressed_data = self.compress_data(json_string)

            # 3. Calcula métricas de tamanho
            size_metrics = self.calculate_sizes(json_string, compressed_data)

            # 4. Prepara dados para inserção
            data_extraction = date.today()
            record_count = prepared_data["metadata"]["total_records"]

            # 5. Remove dados existentes para o mesmo período
            with get_db_connection() as conn:
                cursor = conn.cursor()
                conn.autocommit = False

                try:
                    # Delete dados existentes baseado no tipo de extração
                    if start_date and end_date:
                        delete_query = """
                        DELETE FROM servicenow_data_json 
                        WHERE start_date = ? AND end_date = ? AND extraction_type = ?
                        """
                        cursor.execute(
                            delete_query, start_date, end_date, extraction_type
                        )
                    elif extraction_type == "backlog":
                        delete_query = """
                        DELETE FROM servicenow_data_json 
                        WHERE extraction_type = 'backlog'
                        """
                        cursor.execute(delete_query)
                    else:
                        delete_query = """
                        DELETE FROM servicenow_data_json 
                        WHERE data_extraction = ? AND extraction_type = ?
                        """
                        cursor.execute(
                            delete_query, data_extraction, extraction_type
                        )

                    # Insert novos dados
                    insert_query = """
                    INSERT INTO servicenow_data_json (
                        data_extraction, extraction_type, start_date, end_date,
                        data_json, data_compressed, record_count, json_size_kb,
                        compressed_size_kb, compression_ratio, extraction_duration_seconds,
                        api_requests_count
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """

                    duration = time.time() - start_time
                    api_requests = (
                        extraction_metrics.get("total_requests", 0)
                        if extraction_metrics
                        else 0
                    )

                    cursor.execute(
                        insert_query,
                        data_extraction,
                        extraction_type,
                        start_date,
                        end_date,
                        json_string,
                        compressed_data,
                        record_count,
                        size_metrics["json_size_kb"],
                        size_metrics["compressed_size_kb"],
                        size_metrics["compression_ratio"],
                        round(duration, 2),
                        api_requests,
                    )

                    conn.commit()

                    # Atualiza métricas internas
                    self._update_metrics(size_metrics, record_count)

                    self._print_save_summary(
                        size_metrics, record_count, duration
                    )

                    return True

                except Exception as e:
                    conn.rollback()
                    print(f"❌ Erro ao salvar dados JSON: {e}")
                    return False

        except Exception as e:
            print(f"❌ Erro ao preparar dados JSON: {e}")
            return False

    def _update_metrics(self, size_metrics: Dict, record_count: int):
        """Atualiza métricas internas"""
        self.json_metrics["total_saves"] += 1
        self.json_metrics["total_json_size_kb"] += size_metrics["json_size_kb"]
        self.json_metrics["total_records"] += record_count

        if size_metrics["compressed_size_kb"]:
            self.json_metrics["total_compressed_size_kb"] += size_metrics[
                "compressed_size_kb"
            ]

            # Calcula média da razão de compressão
            if self.json_metrics["avg_compression_ratio"] == 0:
                self.json_metrics["avg_compression_ratio"] = size_metrics[
                    "compression_ratio"
                ]
            else:
                self.json_metrics["avg_compression_ratio"] = (
                    self.json_metrics["avg_compression_ratio"]
                    + size_metrics["compression_ratio"]
                ) / 2

    def _print_save_summary(
        self, size_metrics: Dict, record_count: int, duration: float
    ):
        """Imprime resumo do salvamento"""
        print("\n💾 Dados salvos em formato JSON:")
        print(f"   ├── Registros: {record_count:,}")
        print(f"   ├── Tamanho JSON: {size_metrics['json_size_kb']:.2f} KB")

        if size_metrics["compressed_size_kb"]:
            print(
                f"   ├── Tamanho comprimido: {size_metrics['compressed_size_kb']:.2f} KB"
            )
            print(f"   ├── Economia: {size_metrics['compression_ratio']:.1f}%")

        print(f"   └── Tempo de processamento: {duration:.2f}s")

    def load_json_data_from_db(
        self, data_extraction: str, extraction_type: str = "daily"
    ) -> Optional[Dict]:
        """Carrega dados JSON do banco"""
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()

                query = """
                SELECT data_json, data_compressed 
                FROM servicenow_data_json 
                WHERE data_extraction = ? AND extraction_type = ?
                ORDER BY created_at DESC
                """

                cursor.execute(query, data_extraction, extraction_type)
                result = cursor.fetchone()

                if result:
                    json_data, compressed_data = result

                    if compressed_data:
                        # Dados estão comprimidos
                        json_string = self.decompress_data(compressed_data)
                    else:
                        # Dados estão em JSON puro
                        json_string = json_data

                    return json.loads(json_string)

                return None

        except Exception as e:
            print(f"❌ Erro ao carregar dados JSON: {e}")
            return None

    def get_json_metrics(self) -> Dict:
        """Retorna métricas do gerenciador JSON"""
        return self.json_metrics.copy()

    def print_json_metrics(self):
        """Imprime métricas do armazenamento JSON"""
        metrics = self.json_metrics

        if metrics["total_saves"] == 0:
            print("📊 Nenhum dado JSON foi salvo ainda.")
            return

        avg_json_size = metrics["total_json_size_kb"] / metrics["total_saves"]
        avg_compressed_size = (
            metrics["total_compressed_size_kb"] / metrics["total_saves"]
            if metrics["total_compressed_size_kb"] > 0
            else 0
        )

        print("\n💾 Métricas Armazenamento JSON:")
        print(f"   ├── Total de salvamentos: {metrics['total_saves']}")
        print(f"   ├── Total de registros: {metrics['total_records']:,}")
        print(
            f"   ├── Tamanho total JSON: {metrics['total_json_size_kb']:.2f} KB"
        )
        print(f"   ├── Tamanho médio por save: {avg_json_size:.2f} KB")

        if avg_compressed_size > 0:
            print(
                f"   ├── Tamanho total comprimido: {metrics['total_compressed_size_kb']:.2f} KB"
            )
            print(
                f"   ├── Tamanho médio comprimido: {avg_compressed_size:.2f} KB"
            )
            print(
                f"   └── Razão média de compressão: {metrics['avg_compression_ratio']:.1f}%"
            )
        else:
            print("   └── Compressão: Desabilitada")
