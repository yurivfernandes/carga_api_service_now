"""
Extrator de incidentes do ServiceNow
"""

from typing import Optional

import polars as pl

from .base_extractor import BaseServiceNowExtractor


class IncidentExtractor(BaseServiceNowExtractor):
    """Extrator específico para incidentes"""

    def extract_data(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        include_backlog: bool = False,
    ) -> pl.DataFrame:
        """Extrai dados de incidentes e retorna como DataFrame Polars"""
        start_date, end_date = self.get_date_range(start_date, end_date)

        print(
            f"📅 Processando incidentes do período: {start_date} - {end_date}"
        )

        all_incidents = []

        # Busca incidentes por range de datas
        incidents = self.get_incidents_by_date_range(start_date, end_date)
        all_incidents.extend(incidents)

        # Enriquece os dados
        if all_incidents:
            enriched_incidents = self.enrich_incident_data(all_incidents)
            df = pl.DataFrame(enriched_incidents)
        else:
            df = pl.DataFrame()

        print(f"✅ Total de {len(all_incidents)} incidentes extraídos")
        return df
    
    def get_incidents_by_date_range(
        self, start_date: str, end_date: str
    ) -> list:
        """Busca incidentes fechados dentro do range de datas especificado"""
        query = (
            f"closed_at>={start_date} 00:00:00^closed_at<={end_date} 23:59:59"
        )

        params = {"sysparm_query": query}
        all_incidents = self.paginated_request("incident", params)

        print(
            f"✅ Total de incidentes para período {start_date} - {end_date}: {len(all_incidents)}"
        )
        return all_incidents

    def enrich_incident_data(self, incidents: list) -> list:
        """Enriquece os dados dos incidentes com informações detalhadas"""
        enriched_incidents = []

        for inc in incidents:
            # Processa campos de referência
            inc = self.process_data([inc])[0]

            # Busca detalhes da empresa
            if inc.get("company"):
                company_detail = self.make_request(
                    f"core_company/{inc.get('company')}", {}
                )
                if company_detail:
                    inc["dv_company"] = company_detail.get("name", "")

            # Busca detalhes do usuário que resolveu
            if inc.get("resolved_by"):
                user_detail = self.make_request(
                    f"sys_user/{inc.get('resolved_by')}", {}
                )
                if user_detail:
                    inc["dv_resolved_by"] = user_detail.get("name", "")

            # Busca detalhes do usuário que abriu
            if inc.get("opened_by"):
                user_detail = self.make_request(
                    f"sys_user/{inc.get('opened_by')}", {}
                )
                if user_detail:
                    inc["dv_opened_by"] = user_detail.get("name", "")

            enriched_incidents.append(inc)

        return enriched_incidents

    
