"""
Extrator de tempo trabalhado de incidentes do ServiceNow
"""

from typing import List, Optional

import polars as pl

from .base_extractor import BaseServiceNowExtractor


class TimeWorkedExtractor(BaseServiceNowExtractor):
    """Extrator específico para tempo trabalhado em incidentes"""

    def get_time_worked_for_incidents(
        self, start_date: str, end_date: str
    ) -> list:
        """Busca tempo trabalhado para uma lista de incidentes"""
        all_time_worked = []

        query = f"sys_created_on>={start_date} 00:00:00^sys_created_on<={end_date} 23:59:59^taskISNOTEMPTY"
        params = {"sysparm_query": query}

        time_worked = self.paginated_request("task_time_worked", params)
        all_time_worked.extend(time_worked)
        return all_time_worked

    def extract_data(self, start_date: str, end_date: str) -> pl.DataFrame:
        """Extrai dados de tempo trabalhado para os incidentes especificados e retorna como DataFrame Polars"""

        # Busca registros de tempo trabalhado
        time_worked = self.get_time_worked_for_incidents(start_date, end_date)

        if time_worked:
            # Processa os dados
            processed_time_worked = self.process_data(time_worked)
            df = pl.DataFrame(processed_time_worked)
        else:
            df = pl.DataFrame()

        print(
            f"✅ Total de {len(time_worked)} registros de tempo trabalhado extraídos"
        )
        return df
