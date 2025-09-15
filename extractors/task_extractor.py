"""
Extrator de tarefas de incidentes do ServiceNow
"""

import polars as pl
from typing import List, Optional
from .base_extractor import BaseServiceNowExtractor


class TaskExtractor(BaseServiceNowExtractor):
    """Extrator específico para tarefas de incidentes"""
    
    def get_tasks_for_incidents(self, incident_ids: List[str]) -> list:
        """Busca tarefas para uma lista de incidentes"""
        all_tasks = []
        
        for incident_id in incident_ids:
            query = f"parent={incident_id}"
            params = {"sysparm_query": query}
            
            tasks = self.make_request("incident_task", params)
            
            # Adiciona o ID do incidente para cada tarefa
            for task in tasks:
                task["incident"] = incident_id
            
            all_tasks.extend(tasks)
        
        return all_tasks
    
    def enrich_task_data(self, tasks: list) -> list:
        """Enriquece os dados das tarefas com informações detalhadas"""
        enriched_tasks = []
        
        for task in tasks:
            # Processa campos de referência
            task = self.process_data([task])[0]
            
            # Busca detalhes do usuário que fechou
            if task.get("closed_by"):
                user_detail = self.make_request(
                    f"sys_user/{task.get('closed_by')}", {}
                )
                if user_detail:
                    task["dv_closed_by"] = user_detail.get("name", "")
            
            # Busca detalhes do usuário atribuído
            if task.get("assigned_to"):
                user_detail = self.make_request(
                    f"sys_user/{task.get('assigned_to')}", {}
                )
                if user_detail:
                    task["dv_assigned_to"] = user_detail.get("name", "")
            
            # Busca detalhes do usuário que abriu
            if task.get("opened_by"):
                user_detail = self.make_request(
                    f"sys_user/{task.get('opened_by')}", {}
                )
                if user_detail:
                    task["dv_opened_by"] = user_detail.get("name", "")
            
            enriched_tasks.append(task)
        
        return enriched_tasks
    
    def extract_data(self, incident_ids: List[str]) -> pl.DataFrame:
        """Extrai dados de tarefas para os incidentes especificados e retorna como DataFrame Polars"""
        print(f"📅 Processando tarefas para {len(incident_ids)} incidentes")
        
        # Busca tarefas
        tasks = self.get_tasks_for_incidents(incident_ids)
        
        if tasks:
            # Enriquece os dados
            enriched_tasks = self.enrich_task_data(tasks)
            df = pl.DataFrame(enriched_tasks)
        else:
            df = pl.DataFrame()
        
        print(f"✅ Total de {len(tasks)} tarefas extraídas")
        return df