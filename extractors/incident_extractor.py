"""
Extrator de incidentes do ServiceNow - versão normalizada sem enriquecimento
"""

from datetime import datetime
from typing import Optional

import polars as pl

from .base_extractor import BaseServiceNowExtractor


class IncidentExtractor(BaseServiceNowExtractor):
    """Extrator específico para incidentes - trabalha apenas com IDs de referência"""

    def extract_data(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pl.DataFrame:
        """Extrai dados de incidentes sem enriquecimento e retorna como DataFrame Polars"""
        start_date, end_date = self.get_date_range(start_date, end_date)

        print(
            f"📅 Processando incidentes do período: {start_date} - {end_date}"
        )

        # Busca incidentes por range de datas
        incidents = self.get_incidents_by_date_range(start_date, end_date)

        # Processa dados básicos (sem enriquecimento)
        if incidents:
            processed_incidents = self._process_incidents(incidents)
            df = pl.DataFrame(processed_incidents)
        else:
            df = pl.DataFrame()

        print(f"✅ Total de {len(incidents)} incidentes extraídos")
        return df

    def get_incidents_by_date_range(
        self, start_date: str, end_date: str
    ) -> list:
        """Busca incidentes fechados dentro do range de datas especificado"""
        query = (
            f"closed_at>={start_date} 00:00:00^closed_at<={end_date} 23:59:59"
        )

        params = {
            "sysparm_query": query,
            "sysparm_fields": self._get_incident_fields(),
        }

        all_incidents = self.paginated_request("incident", params)

        print(
            f"✅ Total de incidentes para período {start_date} - {end_date}: {len(all_incidents)}"
        )
        return all_incidents

    def _get_incident_fields(self) -> str:
        """Define campos a serem extraídos da API de incidentes"""
        fields = [
            # Identificação
            "sys_id",
            "number",
            # Status e Estado
            "state",
            "incident_state",
            "active",
            "resolved_at",
            "closed_at",
            # Prioridade e Impacto
            "priority",
            "urgency",
            "impact",
            "severity",
            # Classificação
            "category",
            "subcategory",
            "u_subcategory_detail",
            # Atribuição - apenas IDs (não enriquecer)
            "company",
            "assignment_group",
            "assigned_to",
            "caller_id",
            # Resolução - apenas IDs (não enriquecer)
            "resolved_by",
            "opened_by",
            "closed_by",
            # Descrição
            "short_description",
            "description",
            "close_notes",
            "resolution_notes",
            # Localização
            "location",
            # Configuração
            "cmdb_ci",
            "business_service",
            # SLA
            "business_stc",
            "calendar_stc",
            "resolve_time",
            # Reopen
            "reopen_count",
            "reopened_time",
            # Relacionamento
            "parent_incident",
            "problem_id",
            "change_request",
            # Auditoria
            "sys_created_on",
            "sys_created_by",
            "sys_updated_on",
            "sys_updated_by",
            "opened_at",
            "time_worked",
        ]

        return ",".join(fields)

    def _process_incidents(self, incidents: list) -> list:
        """Processa dados básicos dos incidentes sem enriquecimento"""
        processed_incidents = []

        for incident in incidents:
            # Processa campos de referência básicos
            processed_incident = self.process_data([incident])[0]

            # Adiciona timestamps ETL
            processed_incident["etl_created_at"] = datetime.now()
            processed_incident["etl_updated_at"] = datetime.now()

            processed_incidents.append(processed_incident)

        return processed_incidents
