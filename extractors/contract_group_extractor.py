"""
Extrator de contratos SLA e grupos do ServiceNow
"""

import polars as pl
from typing import Optional
from .base_extractor import BaseServiceNowExtractor


class ContractSLAExtractor(BaseServiceNowExtractor):
    """Extrator específico para contratos SLA"""
    
    def extract_data(self) -> pl.DataFrame:
        """Extrai dados de contratos SLA e retorna como DataFrame Polars"""
        print("📅 Processando contratos SLA")
        
        params = {"sysparm_limit": 10000}
        contracts = self.make_request("contract_sla", params)
        
        if contracts:
            processed_contracts = self.process_data(contracts)
            df = pl.DataFrame(processed_contracts)
        else:
            df = pl.DataFrame()
        
        print(f"✅ Total de {len(contracts)} contratos SLA extraídos")
        return df


class GroupExtractor(BaseServiceNowExtractor):
    """Extrator específico para grupos de usuários"""
    
    def extract_data(self) -> pl.DataFrame:
        """Extrai dados de grupos e retorna como DataFrame Polars"""
        print("📅 Processando grupos de usuários")
        
        params = {"sysparm_limit": 10000}
        groups = self.make_request("sys_user_group", params)
        
        if groups:
            processed_groups = self.process_data(groups)
            df = pl.DataFrame(processed_groups)
        else:
            df = pl.DataFrame()
        
        print(f"✅ Total de {len(groups)} grupos extraídos")
        return df