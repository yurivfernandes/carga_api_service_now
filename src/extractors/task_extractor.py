"""
Extrator de tarefas de incidentes do ServiceNow
"""

from datetime import datetime
from typing import Optional

import polars as pl

from .base_extractor import BaseServiceNowExtractor


class TaskExtractor(BaseServiceNowExtractor):
    """Extrator específico para tarefas de incidentes"""

    def get_tasks_for_incidents(self, start_date: str, end_date: str) -> list:
        """Busca tarefas para uma lista de incidentes"""
        all_tasks = []

        query = f"opened_at>={start_date} 00:00:00^opened_at<={end_date} 23:59:59^parentISNOTEMPTY"
        params = {"sysparm_query": query}

        tasks = self.paginated_request("incident_task", params)
        all_tasks.extend(tasks)
        return all_tasks

    def extract_data(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pl.DataFrame:
        """Extrai dados de tarefas para os incidentes especificados e retorna como DataFrame Polars"""
        print(f"📅 Processando tarefas")

        # Busca tarefas
        tasks = self.get_tasks_for_incidents(start_date, end_date)
        print(f"✅ Total de {len(tasks)} tarefas extraídas")
        if tasks:
            # Enriquece os dados
            tasks = self._process_tasks(tasks=tasks)
            return pl.DataFrame(data=tasks)
        else:
            return pl.DataFrame()

    def _process_tasks(self, tasks: list) -> list:
        """Processa dados básicos dos incidentes sem enriquecimento"""
        processed_tasks = []

        for tasks in tasks:
            processed_task = self.process_data([tasks])[0]
            processed_task["etl_created_at"] = datetime.now()
            processed_task["etl_updated_at"] = datetime.now()
            processed_tasks.append(processed_task)

        return processed_tasks
