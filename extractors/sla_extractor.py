"""
Extrator de SLAs de incidentes do ServiceNow
"""

import polars as pl
from typing import List, Optional
from .base_extractor import BaseServiceNowExtractor


class SLAExtractor(BaseServiceNowExtractor):
    """Extrator específico para SLAs de incidentes"""
    
    def get_slas_for_incidents(self, incident_ids: List[str]) -> list:
        """Busca SLAs para uma lista de incidentes"""
        all_slas = []
        
        for incident_id in incident_ids:
            query = f"task={incident_id}"
            params = {"sysparm_query": query, "sysparm_limit": "1000"}
            
            slas = self.make_request("task_sla", params)
            
            # Adiciona o ID do incidente para cada SLA
            for sla in slas:
                sla["incident"] = incident_id
            
            all_slas.extend(slas)
        
        return all_slas
    
    def enrich_sla_data(self, slas: list) -> list:
        """Enriquece os dados dos SLAs com informações detalhadas"""
        enriched_slas = []
        
        for sla in slas:
            # Processa campos de referência
            sla = self.process_data([sla])[0]
            
            # Busca detalhes do contrato SLA
            sla_sys_id = sla.get("sla")
            if sla_sys_id:
                sla_detail = self.make_request(f"contract_sla/{sla_sys_id}", {})
                if sla_detail:
                    sla["dv_sla"] = sla_detail.get("name", "")
            else:
                sla["dv_sla"] = ""
            
            enriched_slas.append(sla)
        
        return enriched_slas
    
    def extract_data(self, incident_ids: List[str], start_date: Optional[str] = None, end_date: Optional[str] = None) -> pl.DataFrame:
        """Extrai dados de SLAs para os incidentes especificados e retorna como DataFrame Polars"""
        print(f"📅 Processando SLAs para {len(incident_ids)} incidentes")
        
        # Busca SLAs
        slas = self.get_slas_for_incidents(incident_ids)
        
        if slas:
            # Enriquece os dados
            enriched_slas = self.enrich_sla_data(slas)
            df = pl.DataFrame(enriched_slas)
        else:
            df = pl.DataFrame()
        
        print(f"✅ Total de {len(slas)} SLAs extraídos")
        return df