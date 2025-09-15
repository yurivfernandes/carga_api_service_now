"""
Orquestrador ETL do ServiceNow - versão com dados normalizados
"""

import sys
from datetime import datetime, timedelta
from typing import Optional

from config import save_to_database
from execution_logger import ExecutionLogger
from extractors.company_extractor import CompanyExtractor
from extractors.incident_extractor import IncidentExtractor
from extractors.user_extractor import UserExtractor


class ServiceNowETLOrchestrator:
    """Orquestrador principal do ETL com dados normalizados"""

    def __init__(self):
        self.user_extractor = UserExtractor()
        self.company_extractor = CompanyExtractor()
        self.incident_extractor = IncidentExtractor()
        self.execution_logger = ExecutionLogger()

    def sync_reference_data(self, force_full_sync: bool = False) -> bool:
        """
        Sincroniza dados de referência (usuários e empresas) de forma inteligente

        Args:
            force_full_sync: Se True, força sincronização completa de tudo
        """
        print("🔄 SINCRONIZAÇÃO DE DADOS DE REFERÊNCIA")
        print("=" * 50)

        success = True

        try:
            # 1. Sincroniza usuários
            print("\n👥 Sincronizando usuários...")
            df_users = self.user_extractor.extract_data(
                force_full_sync=force_full_sync
            )

            if not df_users.is_empty():
                save_to_database(df_users, "sys_user")
                print(f"✅ {len(df_users)} usuários salvos no banco")
            else:
                print("ℹ️ Nenhum usuário para atualizar")

            # 2. Sincroniza usuários em falta (referenciados em incidentes)
            print("\n🔍 Verificando usuários em falta...")
            df_missing_users = self.user_extractor.sync_missing_users()

            if not df_missing_users.is_empty():
                save_to_database(df_missing_users, "sys_user")
                print(
                    f"✅ {len(df_missing_users)} usuários em falta sincronizados"
                )

            # 3. Sincroniza empresas
            print("\n🏢 Sincronizando empresas...")
            df_companies = self.company_extractor.extract_data(
                force_full_sync=force_full_sync
            )

            if not df_companies.is_empty():
                save_to_database(df_companies, "sys_company")
                print(f"✅ {len(df_companies)} empresas salvas no banco")
            else:
                print("ℹ️ Nenhuma empresa para atualizar")

            # 4. Sincroniza empresas em falta (referenciadas em incidentes)
            print("\n🔍 Verificando empresas em falta...")
            df_missing_companies = (
                self.company_extractor.sync_missing_companies()
            )

            if not df_missing_companies.is_empty():
                save_to_database(df_missing_companies, "sys_company")
                print(
                    f"✅ {len(df_missing_companies)} empresas em falta sincronizadas"
                )

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
                save_to_database(df_incidents, "incident")
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
        force_full_reference_sync: bool = False,
    ) -> bool:
        """
        Executa o fluxo ETL completo: referências + incidentes

        Args:
            start_date: Data de início para incidentes (YYYY-MM-DD)
            end_date: Data de fim para incidentes (YYYY-MM-DD)
            force_full_reference_sync: Se True, força sync completa das referências
        """
        print("🚀 INICIANDO FLUXO ETL COMPLETO")
        print("=" * 60)

        # Inicia logging da execução
        execution_id = self.execution_logger.start_execution("full_etl")

        try:
            # 1. Sincroniza dados de referência primeiro
            print("\n📋 ETAPA 1: Dados de Referência")
            ref_success = self.sync_reference_data(
                force_full_sync=force_full_reference_sync
            )

            if not ref_success:
                print(
                    "⚠️ Aviso: Problemas na sincronização de referência, mas continuando..."
                )

            # 2. Extrai incidentes
            print("\n📋 ETAPA 2: Incidentes")
            incident_success = self.extract_incidents(start_date, end_date)

            # 3. Verifica resultado final
            overall_success = ref_success and incident_success

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
            print(
                f"\n📋 Extraindo incidentes de {start_date} até {end_date}..."
            )
            success = self.extract_incidents(start_date, end_date)

            if success:
                print("\n⚡ SINCRONIZAÇÃO RÁPIDA CONCLUÍDA!")
                self.execution_logger.finish_execution(
                    execution_id, success=True
                )
            else:
                print("\n⚠️ PROBLEMAS na sincronização rápida!")
                self.execution_logger.finish_execution(
                    execution_id, success=False
                )

            return success

        except Exception as e:
            print(f"\n❌ ERRO na sincronização rápida: {e}")
            self.execution_logger.finish_execution(
                execution_id, success=False, error=str(e)
            )
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
                save_to_database(df_companies, "sys_company")
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
        force_full = "--full" in sys.argv
        orchestrator.sync_reference_data(force_full_sync=force_full)

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
                "Uso: python etl_orchestrator.py full-etl YYYY-MM-DD YYYY-MM-DD [--full-ref]"
            )
            sys.exit(1)

        start_date = sys.argv[2]
        end_date = sys.argv[3]
        force_full_ref = "--full-ref" in sys.argv
        orchestrator.full_etl_workflow(start_date, end_date, force_full_ref)

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
