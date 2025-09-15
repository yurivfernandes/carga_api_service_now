"""
Extrator de tempo trabalhado de incidentes do ServiceNow
"""

import polars as pl
from typing import List, Optional
from .base_extractor import BaseServiceNowExtractor


class TimeWorkedExtractor(BaseServiceNowExtractor):
    """Extrator específico para tempo trabalhado em incidentes"""
    
    def get_time_worked_for_incidents(self, incident_ids: List[str]) -> list:
        """Busca tempo trabalhado para uma lista de incidentes"""
        all_time_worked = []
        
        for incident_id in incident_ids:
            query = f"task={incident_id}"
            params = {"sysparm_query": query, "sysparm_limit": "10000"}
            
            time_worked = self.make_request("task_time_worked", params)
            
            # Adiciona o ID do incidente para cada registro
            for time_record in time_worked:
                time_record["incident"] = incident_id
            
            all_time_worked.extend(time_worked)
        
        return all_time_worked
    
    def extract_data(self, incident_ids: List[str]) -> pl.DataFrame:
        """Extrai dados de tempo trabalhado para os incidentes especificados e retorna como DataFrame Polars"""
        print(f"📅 Processando tempo trabalhado para {len(incident_ids)} incidentes")
        
        # Busca registros de tempo trabalhado
        time_worked = self.get_time_worked_for_incidents(incident_ids)
        
        if time_worked:
            # Processa os dados
            processed_time_worked = self.process_data(time_worked)
            df = pl.DataFrame(processed_time_worked)
        else:
            df = pl.DataFrame()
        
        print(f"✅ Total de {len(time_worked)} registros de tempo trabalhado extraídos")
        return df