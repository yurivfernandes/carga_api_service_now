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
            df = pl.DataFrame(
                data=self._process_incidents(incidents), schema=self._schema
            )

        else:
            df = pl.DataFrame()

        print(f"✅ Total de {len(incidents)} incidentes extraídos")
        return df

    def get_incidents_by_date_range(
        self, start_date: str, end_date: str
    ) -> list:
        """Busca incidentes fechados dentro do range de datas especificado"""
        query = (
            f"opened_at>={start_date} 00:00:00^opened_at<={end_date} 23:59:59"
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
        """Define campos a serem extraídos da API de incidentes com base no schema"""
        return ",".join(self._schema.keys())

    @property
    def _schema(self) -> dict:
        """Retorna o schema dos campos de incidente como dicionário com tipo pl.String"""
        return {
            # Identificação
            "sys_id": pl.String,
            "number": pl.String,
            # Status e Estado
            "state": pl.String,
            "incident_state": pl.String,
            "active": pl.String,
            "resolved_at": pl.String,
            "closed_at": pl.String,
            # Prioridade e Impacto
            "priority": pl.String,
            "urgency": pl.String,
            "impact": pl.String,
            "severity": pl.String,
            # Classificação
            "category": pl.String,
            "subcategory": pl.String,
            "u_subcategory_detail": pl.String,
            # Atribuição - apenas IDs (não enriquecer)
            "company": pl.String,
            "assignment_group": pl.String,
            "assigned_to": pl.String,
            "caller_id": pl.String,
            # Resolução - apenas IDs (não enriquecer)
            "resolved_by": pl.String,
            "opened_by": pl.String,
            "closed_by": pl.String,
            # Descrição
            "short_description": pl.String,
            "description": pl.String,
            "close_notes": pl.String,
            "resolution_notes": pl.String,
            # Localização
            "location": pl.String,
            # Configuração
            "cmdb_ci": pl.String,
            "business_service": pl.String,
            # SLA
            "business_stc": pl.String,
            "calendar_stc": pl.String,
            "resolve_time": pl.String,
            # Reopen
            "reopen_count": pl.String,
            "reopened_time": pl.String,
            # Relacionamento
            "parent_incident": pl.String,
            "problem_id": pl.String,
            "change_request": pl.String,
            # Auditoria
            "sys_created_on": pl.String,
            "sys_created_by": pl.String,
            "sys_updated_on": pl.String,
            "sys_updated_by": pl.String,
            "opened_at": pl.String,
            "time_worked": pl.String,
        }

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
