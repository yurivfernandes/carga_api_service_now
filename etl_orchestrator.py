"""
Orquestrador ETL do ServiceNow - versão com dados normalizados
"""

import sys
from datetime import datetime, timedelta
from typing import Optional

from database_manager import DatabaseManager
from execution_logger import ExecutionLogger
from extractors.company_extractor import CompanyExtractor
from extractors.incident_extractor import IncidentExtractor
from extractors.user_extractor import UserExtractor


class ServiceNowETLOrchestrator:
    def sync_reference_data(self, force_full_sync: bool = False) -> bool:
        """
        Sincroniza todos os usuários e empresas (full) ou apenas os referenciados nos incidentes mais recentes (incremental).
        Args:
            force_full_sync: Se True, sincroniza todos os usuários e empresas. Se False, sincroniza apenas os referenciados nos incidentes recentes.
        """
        print("🔄 SINCRONIZAÇÃO DE DADOS DE REFERÊNCIA")
        print("=" * 50)
        success = True
        try:
            if force_full_sync:
                print("🏢 Sincronizando TODAS as empresas...")
                df_companies = self.company_extractor.get_all_companies()
                if df_companies is not None and not df_companies.is_empty():
                    self._save_df(df_companies, "sys_company")
                    print(f"✅ {len(df_companies)} empresas salvas no banco")
                else:
                    print("ℹ️ Nenhuma empresa encontrada")

                print("👤 Sincronizando TODOS os usuários...")
                df_users = self.user_extractor.get_all_users()
                if df_users is not None and not df_users.is_empty():
                    self._save_df(df_users, "sys_user")
                    print(f"✅ {len(df_users)} usuários salvos no banco")
                else:
                    print("ℹ️ Nenhum usuário encontrado")
            else:
                # Busca incidentes recentes para sincronizar apenas referenciados
                print("📋 Buscando incidentes recentes para sincronizar referências...")
                df_incidents = self.incident_extractor.extract_data()
                self.sync_reference_data_from_incidents(df_incidents)
            print("\n✅ SINCRONIZAÇÃO DE REFERÊNCIA CONCLUÍDA!")
        except Exception as e:
            print(f"\n❌ ERRO na sincronização de referência: {e}")
            success = False
        return success
    """Orquestrador principal do ETL com dados normalizados"""

    def __init__(self):
        self.user_extractor = UserExtractor()
        self.company_extractor = CompanyExtractor()
        self.incident_extractor = IncidentExtractor()
        self.execution_logger = ExecutionLogger()
        # Gerenciador de banco de dados para gravar DataFrames
        self.db_manager = DatabaseManager()

    def _save_df(
        self,
        df,
        table_name: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> bool:
        """Wrapper para salvar um único DataFrame no DatabaseManager"""
        if df is None:
            return False
        try:
            if hasattr(df, "is_empty") and df.is_empty():
                return False
        except Exception:
            # Se não tiver método is_empty, assume que há dados
            pass

        data = {table_name: df}
        return self.db_manager.save_dataframes_to_database(
            data, start_date, end_date
        )

    def sync_reference_data_from_incidents(self, df_incidents) -> bool:
        """
        Sincroniza apenas usuários e empresas referenciados nos incidentes fornecidos.
        Args:
            df_incidents: DataFrame de incidentes extraídos
        """
        print("🔄 SINCRONIZAÇÃO DE DADOS DE REFERÊNCIA (baseada nos incidentes)")
        print("=" * 50)
        success = True
        try:
            # Extrai IDs únicos de companys e usuários dos incidentes
            company_ids = set()
            user_ids = set()
            if df_incidents is not None and not df_incidents.is_empty():
                if "company" in df_incidents.columns:
                    company_ids = set(df_incidents["company"].dropna().unique())
                for col in ["resolved_by", "opened_by", "updated_by"]:
                    if col in df_incidents.columns:
                        user_ids.update(df_incidents[col].dropna().unique())

            # Busca e salva apenas as empresas necessárias
            print(f"\n🏢 Buscando {len(company_ids)} empresas referenciadas nos incidentes...")
            if company_ids:
                df_companies = self.company_extractor.get_companies_by_ids(list(company_ids))
                if df_companies is not None and not df_companies.is_empty():
                    self._save_df(df_companies, "sys_company")
                    print(f"✅ {len(df_companies)} empresas salvas no banco")
                else:
                    print("ℹ️ Nenhuma empresa encontrada para os IDs informados")
            else:
                print("ℹ️ Nenhuma empresa referenciada nos incidentes")

            # Busca e salva apenas os usuários necessários
            print(f"\n� Buscando {len(user_ids)} usuários referenciados nos incidentes...")
            if user_ids:
                df_users = self.user_extractor.get_users_by_ids(list(user_ids))
                if df_users is not None and not df_users.is_empty():
                    self._save_df(df_users, "sys_user")
                    print(f"✅ {len(df_users)} usuários salvos no banco")
                else:
                    print("ℹ️ Nenhum usuário encontrado para os IDs informados")
            else:
                print("ℹ️ Nenhum usuário referenciado nos incidentes")

            print("\n✅ SINCRONIZAÇÃO DE REFERÊNCIA CONCLUÍDA COM SUCESSO!")
        except Exception as e:
            print(f"\n❌ ERRO na sincronização de referência: {e}")
            success = False
        return success

    def extract_incidents(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None
    ) -> bool:
        """
        Extrai incidentes do período especificado (sem enriquecimento)

        Args:
            start_date: Data de início (YYYY-MM-DD)
            end_date: Data de fim (YYYY-MM-DD)
        """
        print("📋 EXTRAÇÃO DE INCIDENTES")
        print("=" * 50)

        success = True

        try:
            # Extrai incidentes
            df_incidents = self.incident_extractor.extract_data(
                start_date, end_date
            )

            if not df_incidents.is_empty():
                self._save_df(df_incidents, "incident")
                print(f"✅ {len(df_incidents)} incidentes salvos no banco")
            else:
                print("ℹ️ Nenhum incidente encontrado para o período")

            print("\n✅ EXTRAÇÃO DE INCIDENTES CONCLUÍDA!")

        except Exception as e:
            print(f"\n❌ ERRO na extração de incidentes: {e}")
            success = False

        return success

    def full_etl_workflow(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> bool:
        """
        Executa o fluxo ETL completo: incidentes + referências (apenas referenciados)

        Args:
            start_date: Data de início para incidentes (YYYY-MM-DD)
            end_date: Data de fim para incidentes (YYYY-MM-DD)
        """
        print("🚀 INICIANDO FLUXO ETL COMPLETO")
        print("=" * 60)

        execution_id = self.execution_logger.start_execution("full_etl")

        try:
            # 1. Extrai incidentes primeiro
            print("\n📋 ETAPA 1: Extração de Incidentes")
            df_incidents = self.incident_extractor.extract_data(start_date, end_date)
            if df_incidents is not None and not df_incidents.is_empty():
                self._save_df(df_incidents, "incident", start_date, end_date)
                print(f"✅ {len(df_incidents)} incidentes salvos no banco")
            else:
                print("ℹ️ Nenhum incidente encontrado para o período")
                self.execution_logger.finish_execution(
                    execution_id, success=True
                )
                return True

            # 2. Sincroniza apenas referências presentes nos incidentes
            print("\n📋 ETAPA 2: Sincronização de Referências dos Incidentes")
            ref_success = self.sync_reference_data_from_incidents(df_incidents)

            overall_success = ref_success

            if overall_success:
                print("\n🎉 FLUXO ETL CONCLUÍDO COM SUCESSO!")
                self.execution_logger.finish_execution(
                    execution_id, success=True
                )
            else:
                print("\n⚠️ FLUXO ETL CONCLUÍDO COM PROBLEMAS!")
                self.execution_logger.finish_execution(
                    execution_id,
                    success=False,
                    error="Problemas em algumas etapas",
                )

            return overall_success

        except Exception as e:
            print(f"\n❌ ERRO CRÍTICO no fluxo ETL: {e}")
            self.execution_logger.finish_execution(
                execution_id, success=False, error=str(e)
            )
            return False

    def quick_incident_sync(self, days_back: int = 1) -> bool:
        """
        Sincronização rápida de incidentes recentes (para execução diária)

        Args:
            days_back: Quantos dias para trás buscar
        """
        print(f"⚡ SINCRONIZAÇÃO RÁPIDA - Últimos {days_back} dias")
        print("=" * 50)

        # Calcula datas
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=days_back)).strftime(
            "%Y-%m-%d"
        )

        execution_id = self.execution_logger.start_execution("quick_sync")

        try:
            # 1. Sincronização incremental de referências (rápida)
            print("\n🔄 Sincronização incremental de referências...")
            self.sync_reference_data(force_full_sync=False)

            # 2. Extração de incidentes recentes
            print(f"\n📋 Extraindo incidentes de {start_date} até {end_date}...")
            success = self.extract_incidents(start_date, end_date)

            if success:
                print("\n⚡ SINCRONIZAÇÃO RÁPIDA CONCLUÍDA!")
                self.execution_logger.finish_execution(execution_id, success=True)
            else:
                print("\n⚠️ PROBLEMAS na sincronização rápida!")
                self.execution_logger.finish_execution(execution_id, success=False)

            return success

        except Exception as e:
            print(f"\n❌ ERRO na sincronização rápida: {e}")
            self.execution_logger.finish_execution(execution_id, success=False, error=str(e))
            return False

    def sync_specific_companies(self, company_type: str = "customer") -> bool:
        """
        Sincroniza empresas de um tipo específico

        Args:
            company_type: Tipo de empresa ('customer', 'vendor', 'manufacturer')
        """
        print(f"🏢 SINCRONIZAÇÃO DE EMPRESAS: {company_type.upper()}")
        print("=" * 50)

        try:
            df_companies = self.company_extractor.get_companies_by_type(
                company_type
            )

            if not df_companies.is_empty():
                self._save_df(df_companies, "sys_company")
                print(
                    f"✅ {len(df_companies)} empresas {company_type} sincronizadas"
                )
                return True
            else:
                print(f"ℹ️ Nenhuma empresa {company_type} encontrada")
                return True

        except Exception as e:
            print(f"❌ ERRO ao sincronizar empresas {company_type}: {e}")
            return False


def main():
    """Função para testar o orquestrador"""
    if len(sys.argv) < 2:
        print("Uso: python etl_orchestrator.py <comando> [argumentos]")
        print("Comandos disponíveis:")
        print("  ref-sync [--full]     - Sincronizar dados de referência")
        print(
            "  incidents YYYY-MM-DD YYYY-MM-DD - Extrair incidentes do período"
        )
        print("  full-etl YYYY-MM-DD YYYY-MM-DD [--full-ref] - ETL completo")
        print("  quick-sync [dias]     - Sincronização rápida")
        print("  companies [tipo]      - Sincronizar empresas por tipo")
        sys.exit(1)

    orchestrator = ServiceNowETLOrchestrator()
    command = sys.argv[1]

    if command == "ref-sync":
        print("Este comando foi descontinuado. Use o full-etl para sincronizar apenas referências presentes nos incidentes extraídos.")

    elif command == "incidents":
        if len(sys.argv) < 4:
            print(
                "Uso: python etl_orchestrator.py incidents YYYY-MM-DD YYYY-MM-DD"
            )
            sys.exit(1)

        start_date = sys.argv[2]
        end_date = sys.argv[3]
        orchestrator.extract_incidents(start_date, end_date)

    elif command == "full-etl":
        if len(sys.argv) < 4:
            print(
                "Uso: python etl_orchestrator.py full-etl YYYY-MM-DD YYYY-MM-DD"
            )
            sys.exit(1)

        start_date = sys.argv[2]
        end_date = sys.argv[3]
        orchestrator.full_etl_workflow(start_date, end_date)

    elif command == "quick-sync":
        days_back = int(sys.argv[2]) if len(sys.argv) > 2 else 1
        orchestrator.quick_incident_sync(days_back)

    elif command == "companies":
        company_type = sys.argv[2] if len(sys.argv) > 2 else "customer"
        orchestrator.sync_specific_companies(company_type)

    else:
        print(f"Comando desconhecido: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
