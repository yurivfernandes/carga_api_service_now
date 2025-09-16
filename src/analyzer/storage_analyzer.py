"""
Script para análise de espaço entre armazenamento normalizado vs JSON comprimido
"""

from datetime import datetime
from typing import Any, Dict

import polars as pl

from settings.config import get_db_connection
from teste_json.json_data_manager import JSONDataManager


class StorageAnalyzer:
    """Analisador de espaço para comparar diferentes modelos de armazenamento"""

    def __init__(self):
        self.json_manager = JSONDataManager()
        self.normalized_tables = [
            "incident",
            "sys_user",
            "core_company",
            "task_sla",
            "time_worked",
        ]

    def get_table_sizes(self) -> Dict[str, Dict]:
        """Obtém tamanhos das tabelas normalizadas"""
        table_sizes = {}

        with get_db_connection() as conn:
            cursor = conn.cursor()

            for table in self.normalized_tables:
                query = f"""
                SELECT 
                    COUNT(*) as record_count,
                    SUM(DATALENGTH(*)) / 1024.0 as size_kb
                FROM {table}
                """
                try:
                    cursor.execute(query)
                    result = cursor.fetchone()

                    if result:
                        table_sizes[table] = {
                            "records": result[0] or 0,
                            "size_kb": round(result[1] or 0, 2),
                        }
                    else:
                        table_sizes[table] = {"records": 0, "size_kb": 0}

                except Exception as e:
                    print(f"⚠️ Erro ao analisar tabela {table}: {e}")
                    table_sizes[table] = {"records": 0, "size_kb": 0}

        return table_sizes

    def get_json_data_sizes(self) -> Dict[str, Any]:
        """Obtém tamanhos dos dados JSON"""
        json_sizes = {
            "total_records": 0,
            "total_json_size_kb": 0,
            "total_compressed_size_kb": 0,
            "average_compression_ratio": 0,
            "entries": [],
        }

        with get_db_connection() as conn:
            cursor = conn.cursor()

            query = """
            SELECT 
                data_extraction,
                extraction_type,
                record_count,
                json_size_kb,
                compressed_size_kb,
                compression_ratio,
                created_at
            FROM servicenow_data_json
            ORDER BY created_at DESC
            """

            try:
                cursor.execute(query)
                results = cursor.fetchall()

                for row in results:
                    entry = {
                        "date": row[0],
                        "type": row[1],
                        "records": row[2] or 0,
                        "json_size_kb": row[3] or 0,
                        "compressed_size_kb": row[4] or 0,
                        "compression_ratio": row[5] or 0,
                        "created_at": row[6],
                    }

                    json_sizes["entries"].append(entry)
                    json_sizes["total_records"] += entry["records"]
                    json_sizes["total_json_size_kb"] += entry["json_size_kb"]
                    json_sizes["total_compressed_size_kb"] += entry[
                        "compressed_size_kb"
                    ]

                # Calcula média da compressão
                if json_sizes["entries"]:
                    valid_ratios = [
                        e["compression_ratio"]
                        for e in json_sizes["entries"]
                        if e["compression_ratio"] > 0
                    ]
                    if valid_ratios:
                        json_sizes["average_compression_ratio"] = sum(
                            valid_ratios
                        ) / len(valid_ratios)

            except Exception as e:
                print(f"⚠️ Erro ao analisar dados JSON: {e}")

        return json_sizes

    def calculate_space_efficiency(self) -> Dict[str, Any]:
        """Calcula eficiência de espaço entre os modelos"""
        normalized_sizes = self.get_table_sizes()
        json_sizes = self.get_json_data_sizes()

        # Total do modelo normalizado
        normalized_total_records = sum(
            table["records"] for table in normalized_sizes.values()
        )
        normalized_total_size_kb = sum(
            table["size_kb"] for table in normalized_sizes.values()
        )

        # Calcula eficiência
        efficiency = {
            "normalized": {
                "total_records": normalized_total_records,
                "total_size_kb": round(normalized_total_size_kb, 2),
                "avg_bytes_per_record": round(
                    (normalized_total_size_kb * 1024)
                    / normalized_total_records,
                    2,
                )
                if normalized_total_records > 0
                else 0,
                "tables": normalized_sizes,
            },
            "json": {
                "total_records": json_sizes["total_records"],
                "json_size_kb": round(json_sizes["total_json_size_kb"], 2),
                "compressed_size_kb": round(
                    json_sizes["total_compressed_size_kb"], 2
                ),
                "avg_compression_ratio": round(
                    json_sizes["average_compression_ratio"], 2
                ),
                "avg_bytes_per_record_json": round(
                    (json_sizes["total_json_size_kb"] * 1024)
                    / json_sizes["total_records"],
                    2,
                )
                if json_sizes["total_records"] > 0
                else 0,
                "avg_bytes_per_record_compressed": round(
                    (json_sizes["total_compressed_size_kb"] * 1024)
                    / json_sizes["total_records"],
                    2,
                )
                if json_sizes["total_records"] > 0
                else 0,
                "entries": json_sizes["entries"],
            },
        }

        # Calcula comparações se há dados suficientes
        if (
            normalized_total_size_kb > 0
            and json_sizes["total_compressed_size_kb"] > 0
        ):
            space_saved_vs_normalized = (
                (
                    normalized_total_size_kb
                    - json_sizes["total_compressed_size_kb"]
                )
                / normalized_total_size_kb
            ) * 100
            efficiency["comparison"] = {
                "space_saved_vs_normalized_percent": round(
                    space_saved_vs_normalized, 2
                ),
                "json_vs_normalized_ratio": round(
                    json_sizes["total_compressed_size_kb"]
                    / normalized_total_size_kb,
                    3,
                ),
                "recommendation": self._get_recommendation(
                    space_saved_vs_normalized
                ),
            }
        else:
            efficiency["comparison"] = {
                "space_saved_vs_normalized_percent": 0,
                "json_vs_normalized_ratio": 0,
                "recommendation": "Dados insuficientes para comparação",
            }

        return efficiency

    def _get_recommendation(self, space_saved_percent: float) -> str:
        """Fornece recomendação baseada na economia de espaço"""
        if space_saved_percent > 50:
            return "🟢 JSON comprimido é MUITO mais eficiente - Recomendado para arquivamento"
        elif space_saved_percent > 20:
            return "🟡 JSON comprimido oferece boa economia - Considerar para dados históricos"
        elif space_saved_percent > 0:
            return "🟠 JSON comprimido oferece pequena economia - Avaliar trade-offs"
        elif space_saved_percent > -20:
            return "🟠 Tamanhos similares - Considerar outras vantagens (flexibilidade)"
        else:
            return "🔴 Modelo normalizado é mais eficiente - Manter estrutura atual"

    def print_detailed_analysis(self):
        """Imprime análise detalhada de espaço"""
        efficiency = self.calculate_space_efficiency()

        print("=" * 80)
        print("📊 ANÁLISE COMPARATIVA DE ARMAZENAMENTO - SERVICENOW")
        print("=" * 80)

        # Modelo Normalizado
        print("\n🏗️ MODELO NORMALIZADO (Atual):")
        print("-" * 50)
        normalized = efficiency["normalized"]

        print(f"📈 Total de registros: {normalized['total_records']:,}")
        print(
            f"💾 Tamanho total: {normalized['total_size_kb']:,.2f} KB ({normalized['total_size_kb'] / 1024:.2f} MB)"
        )
        print(
            f"⚖️ Média por registro: {normalized['avg_bytes_per_record']:.1f} bytes"
        )

        print("\n📋 Detalhamento por tabela:")
        for table, data in normalized["tables"].items():
            print(
                f"   ├── {table:20} {data['records']:>8,} registros  {data['size_kb']:>8.1f} KB"
            )

        # Modelo JSON
        print("\n🗜️ MODELO JSON COMPRIMIDO:")
        print("-" * 50)
        json_data = efficiency["json"]

        if json_data["total_records"] > 0:
            print(f"📈 Total de registros: {json_data['total_records']:,}")
            print(
                f"💾 Tamanho JSON: {json_data['json_size_kb']:,.2f} KB ({json_data['json_size_kb'] / 1024:.2f} MB)"
            )
            print(
                f"🗜️ Tamanho comprimido: {json_data['compressed_size_kb']:,.2f} KB ({json_data['compressed_size_kb'] / 1024:.2f} MB)"
            )
            print(
                f"📊 Compressão média: {json_data['avg_compression_ratio']:.1f}%"
            )
            print(
                f"⚖️ Média por registro (JSON): {json_data['avg_bytes_per_record_json']:.1f} bytes"
            )
            print(
                f"⚖️ Média por registro (comprimido): {json_data['avg_bytes_per_record_compressed']:.1f} bytes"
            )

            if json_data["entries"]:
                print("\n📋 Extrações JSON realizadas:")
                for entry in json_data["entries"][-5:]:  # Últimas 5 extrações
                    compression_display = (
                        f"{entry['compression_ratio']:.1f}%"
                        if entry["compression_ratio"] > 0
                        else "N/A"
                    )
                    print(
                        f"   ├── {entry['date']} ({entry['type']}) - {entry['records']:,} reg. - {entry['compressed_size_kb']:.1f} KB - {compression_display}"
                    )
        else:
            print("ℹ️ Nenhum dado JSON encontrado no banco")

        # Comparação
        print("\n⚖️ COMPARAÇÃO E RECOMENDAÇÕES:")
        print("-" * 50)
        comparison = efficiency.get("comparison", {})

        if comparison.get("space_saved_vs_normalized_percent", 0) != 0:
            space_saved = comparison["space_saved_vs_normalized_percent"]
            ratio = comparison["json_vs_normalized_ratio"]

            print(f"💡 Economia de espaço: {space_saved:+.1f}%")
            print(
                f"📏 Razão JSON/Normalizado: {ratio:.3f} ({ratio * 100:.1f}%)"
            )
            print(f"🎯 Recomendação: {comparison['recommendation']}")

            # Projeções
            if space_saved > 0:
                print("\n📊 PROJEÇÕES:")
                annual_savings_mb = (
                    (normalized["total_size_kb"] * space_saved / 100)
                    / 1024
                    * 12
                )  # Assumindo crescimento mensal
                print(
                    f"   ├── Economia anual estimada: {annual_savings_mb:.1f} MB"
                )
                print(
                    f"   └── Em 5 anos: {annual_savings_mb * 5:.1f} MB ({annual_savings_mb * 5 / 1024:.2f} GB)"
                )
        else:
            print("ℹ️ Comparação não disponível - dados insuficientes")

        # Vantagens e desvantagens
        print("\n💭 CONSIDERAÇÕES TÉCNICAS:")
        print("   🟢 Vantagens JSON comprimido:")
        print("      ├── Flexibilidade de schema")
        print("      ├── Facilidade para arquivamento")
        print("      └── Menor complexidade de queries simples")
        print("   🟠 Desvantagens JSON comprimido:")
        print("      ├── Queries complexas menos eficientes")
        print("      ├── Maior uso de CPU (compressão/descompressão)")
        print("      └── Menos otimizado para joins")

        print("\n" + "=" * 80)

    def export_analysis_to_csv(
        self, output_file: str = "storage_analysis.csv"
    ):
        """Exporta análise para CSV"""
        efficiency = self.calculate_space_efficiency()

        # Prepara dados para CSV
        rows = []

        # Tabelas normalizadas
        for table, data in efficiency["normalized"]["tables"].items():
            rows.append(
                {
                    "model": "normalized",
                    "table_type": table,
                    "date": datetime.now().strftime("%Y-%m-%d"),
                    "records": data["records"],
                    "size_kb": data["size_kb"],
                    "compressed_size_kb": None,
                    "compression_ratio": None,
                }
            )

        # Dados JSON
        for entry in efficiency["json"]["entries"]:
            rows.append(
                {
                    "model": "json_compressed",
                    "table_type": entry["type"],
                    "date": entry["date"],
                    "records": entry["records"],
                    "size_kb": entry["json_size_kb"],
                    "compressed_size_kb": entry["compressed_size_kb"],
                    "compression_ratio": entry["compression_ratio"],
                }
            )

        # Cria DataFrame e exporta
        if rows:
            df = pl.DataFrame(rows)
            df.write_csv(output_file)
            print(f"📁 Análise exportada para: {output_file}")
        else:
            print("⚠️ Nenhum dado para exportar")


def main():
    """Executa análise completa de armazenamento"""
    analyzer = StorageAnalyzer()
    analyzer.print_detailed_analysis()
    analyzer.export_analysis_to_csv()


if __name__ == "__main__":
    main()
