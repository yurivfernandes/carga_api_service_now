"""
Extrator de empresas do ServiceNow com sincronização incremental
"""

import hashlib
from datetime import datetime, timedelta
from typing import List, Set

import polars as pl

from settings.config import get_db_connection

from .base_extractor import BaseServiceNowExtractor


class CompanyExtractor(BaseServiceNowExtractor):
    """Extrator específico para empresas (sys_company/core_company) com sincronização inteligente"""

    def __init__(self):
        super().__init__()
        self.table_name = "sys_company"
        self.api_endpoint = (
            "core_company"  # Endpoint do ServiceNow para empresas
        )

    def extract_data(self, force_full_sync: bool = False) -> pl.DataFrame:
        """
        Extrai dados de empresas de forma incremental

        Args:
            force_full_sync: Se True, força sincronização completa
        """
        print("🏢 Iniciando extração de empresas (core_company)")
        force_full_sync = True
        if force_full_sync:
            print("🔄 Sincronização COMPLETA forçada")
            return self._extract_all_companies()
        else:
            print("⚡ Sincronização INCREMENTAL (apenas mudanças)")
            return self._extract_incremental_companies()

    def _extract_all_companies(self) -> pl.DataFrame:
        """Extrai todas as empresas (sincronização completa)"""
        print("📥 Buscando todas as empresas...")

        # Busca todas as empresas ativas primeiro
        params = {
            "sysparm_query": "active=true",
            "sysparm_fields": self._get_company_fields(),
        }

        active_companies = self.paginated_request(self.api_endpoint, params)
        print(f"✅ {len(active_companies)} empresas ativas encontradas")

        # Busca também empresas inativas modificadas recentemente (últimos 30 dias)
        cutoff_date = (datetime.now() - timedelta(days=30)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        params_inactive = {
            "sysparm_query": f"active=false^sys_updated_on>={cutoff_date}",
            "sysparm_fields": self._get_company_fields(),
        }

        inactive_companies = self.paginated_request(
            self.api_endpoint, params_inactive
        )
        print(
            f"✅ {len(inactive_companies)} empresas inativas modificadas recentemente"
        )

        # Combina todas as empresas
        all_companies = active_companies + inactive_companies

        # Remove duplicatas baseado no sys_id
        seen_ids = set()
        unique_companies = []
        for company in all_companies:
            if company.get("sys_id") not in seen_ids:
                unique_companies.append(company)
                seen_ids.add(company.get("sys_id"))

        print(f"✅ Total de {len(unique_companies)} empresas únicas extraídas")

        if unique_companies:
            processed_companies = self._process_and_hash_companies(
                unique_companies
            )
            return pl.DataFrame(processed_companies)
        else:
            return pl.DataFrame()

    def _extract_incremental_companies(self) -> pl.DataFrame:
        """Extrai apenas empresas modificadas recentemente"""

        # 1. Determina data de última sincronização
        last_sync_date = self._get_last_sync_date()
        print(f"📅 Última sincronização: {last_sync_date}")

        # 2. Busca empresas modificadas desde a última sincronização
        query = f"sys_updated_on>={last_sync_date}"

        params = {
            "sysparm_query": query,
            "sysparm_fields": self._get_company_fields(),
        }

        modified_companies = self.paginated_request(self.api_endpoint, params)
        print(
            f"✅ {len(modified_companies)} empresas modificadas desde {last_sync_date}"
        )

        if modified_companies:
            processed_companies = self._process_and_hash_companies(
                modified_companies
            )

            # 3. Identifica quais realmente mudaram (usando hash)
            companies_to_update = self._filter_changed_companies(
                processed_companies
            )
            print(
                f"🔄 {len(companies_to_update)} empresas com mudanças reais detectadas"
            )

            if companies_to_update:
                return pl.DataFrame(companies_to_update)

        print("ℹ️ Nenhuma mudança detectada nas empresas")
        return pl.DataFrame()

    def _get_company_fields(self) -> str:
        """Define campos a serem extraídos da API de empresas"""
        fields = [
            # Campos básicos
            "sys_id",
            "name",
            "parent",
            # Tipos de empresa
            "customer",
            "vendor",
            "manufacturer",
            # Contato
            "phone",
            "fax",
            "website",
            # Endereço
            "street",
            "city",
            "state",
            "zip",
            "country",
            # Fiscal
            "federal_tax_id",
            # Status
            "active",
            # Auditoria
            "sys_created_on",
            "sys_created_by",
            "sys_updated_on",
            "sys_updated_by",
        ]

        return ",".join(fields)

    def _process_and_hash_companies(self, companies: List[dict]) -> List[dict]:
        """Processa empresas e adiciona hash para controle de mudanças"""
        processed_companies = []

        for company in companies:
            # Processa campos de referência
            processed_company = self.process_data([company])[0]

            # Converte campos booleanos para formato do banco
            bool_fields = ["customer", "vendor", "manufacturer", "active"]
            for field in bool_fields:
                if field in processed_company:
                    processed_company[field] = (
                        1
                        if processed_company[field] in [True, "true", "1"]
                        else 0
                    )

            # Adiciona timestamps ETL
            processed_company["etl_created_at"] = datetime.now()
            processed_company["etl_updated_at"] = datetime.now()

            # Calcula hash dos dados principais
            hash_data = {
                k: v
                for k, v in processed_company.items()
                if not k.startswith("etl_")
                and k not in ["sys_created_on", "sys_updated_on"]
            }

            data_string = str(sorted(hash_data.items()))
            processed_company["etl_hash"] = hashlib.md5(
                data_string.encode()
            ).hexdigest()

            processed_companies.append(processed_company)

        return processed_companies

    def _filter_changed_companies(self, companies: List[dict]) -> List[dict]:
        """Filtra apenas empresas que realmente mudaram (baseado no hash)"""
        if not companies:
            return []

        changed_companies = []

        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()

                for company in companies:
                    # Busca hash atual no banco
                    query = "SELECT etl_hash FROM sys_company WHERE sys_id = ?"
                    cursor.execute(query, company["sys_id"])
                    result = cursor.fetchone()

                    if result is None:
                        # Empresa nova
                        print(
                            f"➕ Nova empresa: {company.get('name', company.get('sys_id'))}"
                        )
                        changed_companies.append(company)
                    elif result[0] != company["etl_hash"]:
                        # Empresa modificada
                        print(
                            f"🔄 Empresa modificada: {company.get('name', company.get('sys_id'))}"
                        )
                        changed_companies.append(company)

        except Exception as e:
            print(f"⚠️ Erro ao filtrar empresas alteradas: {e}")
            print("ℹ️ Retornando todas as empresas por segurança")
            return companies

        return changed_companies

    def _get_last_sync_date(self) -> str:
        """Obtém data da última sincronização de empresas"""
        default_date = (datetime.now() - timedelta(days=30)).strftime(
            "%Y-%m-%d %H:%M:%S"
        )

        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()

                # Busca última atualização na tabela de empresas
                query = "SELECT MAX(etl_updated_at) FROM sys_company"
                cursor.execute(query)
                result = cursor.fetchone()

                if result and result[0]:
                    # Subtrai 2 horas para garantir sobreposição
                    last_update = result[0] - timedelta(hours=2)
                    return last_update.strftime("%Y-%m-%d %H:%M:%S")

        except Exception as e:
            print(f"⚠️ Erro ao obter última sincronização: {e}")

        print(f"ℹ️ Usando data padrão: {default_date}")
        return default_date

    def get_companies_by_ids(self, company_ids: Set[str]) -> pl.DataFrame:
        """
        Busca empresas específicas por IDs (para resolver referências)

        Args:
            company_ids: Set de IDs de empresas para buscar
        """
        if not company_ids:
            return pl.DataFrame()

        print(f"🔍 Buscando {len(company_ids)} empresas específicas por ID...")

        # Divide em lotes para evitar URLs muito longas
        batch_size = 50
        all_companies = []

        company_ids_list = list(company_ids)
        for i in range(0, len(company_ids_list), batch_size):
            batch_ids = company_ids_list[i : i + batch_size]
            ids_query = "^OR".join([f"sys_id={cid}" for cid in batch_ids])

            params = {
                "sysparm_query": ids_query,
                "sysparm_fields": self._get_company_fields(),
            }

            batch_companies = self.paginated_request(self.api_endpoint, params)
            all_companies.extend(batch_companies)
            print(
                f"📥 Lote {i // batch_size + 1}: {len(batch_companies)} empresas"
            )

        print(
            f"✅ Total de {len(all_companies)} empresas específicas extraídas"
        )

        if all_companies:
            processed_companies = self._process_and_hash_companies(
                all_companies
            )
            return pl.DataFrame(processed_companies)
        else:
            return pl.DataFrame()

    def get_missing_company_ids_from_incidents(self) -> Set[str]:
        """Identifica IDs de empresas referenciadas em incidentes mas não presentes na tabela sys_company"""
        missing_ids = set()

        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()

                # Busca IDs únicos de empresas referenciadas em incidentes
                query = """
                SELECT DISTINCT company
                FROM incident 
                WHERE company IS NOT NULL 
                AND company NOT IN (SELECT sys_id FROM sys_company)
                """

                cursor.execute(query)
                results = cursor.fetchall()

                for row in results:
                    missing_ids.add(row[0])

                print(
                    f"🔍 {len(missing_ids)} empresas referenciadas mas não encontradas na tabela"
                )

        except Exception as e:
            print(f"⚠️ Erro ao identificar empresas em falta: {e}")

        return missing_ids

    def sync_missing_companies(self) -> pl.DataFrame:
        """Sincroniza empresas que estão referenciadas em incidentes mas faltam na tabela"""
        print("🔄 Sincronizando empresas em falta...")

        missing_ids = self.get_missing_company_ids_from_incidents()

        if missing_ids:
            return self.get_companies_by_ids(missing_ids)
        else:
            print("✅ Todas as empresas referenciadas já estão sincronizadas")
            return pl.DataFrame()

    def get_companies_by_type(
        self, company_type: str = "customer"
    ) -> pl.DataFrame:
        """
        Extrai empresas por tipo específico (customer, vendor, manufacturer)

        Args:
            company_type: Tipo de empresa ('customer', 'vendor', 'manufacturer')
        """
        print(f"🏢 Extraindo empresas do tipo: {company_type}")

        params = {
            "sysparm_query": f"{company_type}=true^active=true",
            "sysparm_fields": self._get_company_fields(),
        }

        companies = self.paginated_request(self.api_endpoint, params)
        print(
            f"✅ {len(companies)} empresas do tipo {company_type} encontradas"
        )

        if companies:
            processed_companies = self._process_and_hash_companies(companies)
            return pl.DataFrame(processed_companies)
        else:
            return pl.DataFrame()
