"""
Extrator de SLAs de incidentes do ServiceNow
"""

from datetime import datetime

import polars as pl

from .base_extractor import BaseServiceNowExtractor


class SLAExtractor(BaseServiceNowExtractor):
    """Extrator específico para SLAs de incidentes"""

    def get_slas_for_incidents(self, start_date: str, end_date: str) -> list:
        """Busca SLAs para uma lista de incidentes"""
        all_slas = []

        query = f"sys_created_on>={start_date} 00:00:00^sys_created_on<={end_date} 23:59:59^taskISNOTEMPTY"
        params = {"sysparm_query": query, "sysparm_limit": "1000"}

        slas = self.paginated_request("task_sla", params)

        all_slas.extend(slas)

        return all_slas

    def extract_data(self, start_date: str, end_date: str) -> pl.DataFrame:
        """Extrai dados de SLAs para os incidentes especificados e retorna como DataFrame Polars"""
        slas = self.get_slas_for_incidents(start_date, end_date)
        print(f"✅ Total de {len(slas)} SLAs extraídos")
        if slas:
            slas = self._process_sla(slas=slas)
            return pl.DataFrame(data=slas)
        else:
            return pl.DataFrame()

    def _process_sla(self, slas: list) -> list:
        """Processa dados básicos dos incidentes sem enriquecimento"""
        processed_slas = []

        for sla in slas:
            processed_sla = self.process_data([sla])[0]
            processed_sla["etl_created_at"] = datetime.now()
            processed_sla["etl_updated_at"] = datetime.now()
            processed_slas.append(processed_sla)

        return processed_slas
