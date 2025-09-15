"""
Script principal para execução da extração e carga de dados do ServiceNow - Versão Normalizada
"""

import datetime
import sys
import time
from typing import Optional

from database_manager import DatabaseManager
from etl_orchestrator import ServiceNowETLOrchestrator
from execution_logger import ExecutionLogger, print_recent_executions
from extractors.contract_group_extractor import (
    ContractSLAExtractor,
    GroupExtractor,
)
from extractors.incident_extractor import IncidentExtractor
from extractors.sla_extractor import SLAExtractor
from extractors.task_extractor import TaskExtractor
from extractors.time_worked_extractor import TimeWorkedExtractor
from json_data_manager import JSONDataManager


class ServiceNowETL:
    """Classe principal para orquestrar o processo ETL do ServiceNow"""

    def __init__(
        self,
        enable_json_storage: bool = False,
        logger: Optional["ExecutionLogger"] = None,
    ):
        self.db_manager = DatabaseManager()
        self.json_manager = JSONDataManager() if enable_json_storage else None
        self.enable_json_storage = enable_json_storage
        self.logger = logger
        self.incident_extractor = IncidentExtractor()
        self.task_extractor = TaskExtractor()
        self.sla_extractor = SLAExtractor()
        self.time_worked_extractor = TimeWorkedExtractor()
        self.contract_extractor = ContractSLAExtractor()
        self.group_extractor = GroupExtractor()

    def extract_configuration_data(self):
        """Extrai dados de configuração (contratos SLA e grupos)"""
        print("🔧 Iniciando extração de dados de configuração...")
        start_time = time.time()

        # Extrai contratos SLA
        contract_df = self.contract_extractor.extract_data()

        # Extrai grupos
        groups_df = self.group_extractor.extract_data()

        # Imprime métricas da API
        self.contract_extractor.print_api_metrics("Contratos SLA")
        self.group_extractor.print_api_metrics("Grupos")

        # Salva no banco
        config_dataframes = {"contract_sla": contract_df, "groups": groups_df}

        db_start_time = time.time()
        success = self.db_manager.save_dataframes_to_database(
            config_dataframes
        )
        db_end_time = time.time()

        # Atualiza logger se disponível
        if self.logger:
            self.logger.add_processed_table("contract_sla", len(contract_df))
            self.logger.add_processed_table("groups", len(groups_df))

        total_time = time.time() - start_time
        db_time = db_end_time - db_start_time

        print(
            f"\n⏱️  Tempo configuração - Total: {total_time:.2f}s | BD: {db_time:.2f}s"
        )

        if success:
            print("✅ Dados de configuração salvos com sucesso")
        else:
            print("❌ Erro ao salvar dados de configuração")

        return success

    def extract_incident_data(
        self, start_date: Optional[str] = None, end_date: Optional[str] = None
    ):
        """Extrai dados de incidentes e dados relacionados"""
        print("📊 Iniciando extração de dados de incidentes...")
        start_time = time.time()

        # 1. Extrai incidentes
        incidents_df = self.incident_extractor.extract_data(
            start_date, end_date
        )

        if incidents_df.is_empty():
            print("⚠️  Nenhum incidente encontrado para o período especificado")
            return True

        # 2. Obtém IDs dos incidentes para buscar dados relacionados
        incident_ids = incidents_df.select("sys_id").to_series().to_list()

        # 3. Extrai dados relacionados
        print("🔗 Extraindo dados relacionados aos incidentes...")

        tasks_df = self.task_extractor.extract_data(incident_ids)
        slas_df = self.sla_extractor.extract_data(incident_ids)
        time_worked_df = self.time_worked_extractor.extract_data(incident_ids)

        extraction_time = time.time() - start_time

        # Imprime métricas da API
        self.incident_extractor.print_api_metrics("Incidentes")
        self.task_extractor.print_api_metrics("Tarefas")
        self.sla_extractor.print_api_metrics("SLAs")
        self.time_worked_extractor.print_api_metrics("Tempo Trabalhado")

        # 4. Prepara DataFrames para salvamento
        incident_dataframes = {
            "incident": incidents_df,
            "incident_task": tasks_df,
            "incident_sla": slas_df,
            "task_time_worked": time_worked_df,
        }

        # 5. Salva no banco
        db_start_time = time.time()
        success = self.db_manager.save_dataframes_to_database(
            incident_dataframes, start_date, end_date
        )

        # 6. Salva em JSON comprimido se habilitado
        if self.enable_json_storage and success:
            extraction_metrics = {
                "total_requests": (
                    self.incident_extractor.get_api_metrics()["total_requests"]
                    + self.task_extractor.get_api_metrics()["total_requests"]
                    + self.sla_extractor.get_api_metrics()["total_requests"]
                    + self.time_worked_extractor.get_api_metrics()[
                        "total_requests"
                    ]
                )
            }

            json_success = self.json_manager.save_json_data_to_db(
                incident_dataframes,
                extraction_type="daily" if start_date == end_date else "range",
                start_date=start_date,
                end_date=end_date,
                extraction_metrics=extraction_metrics,
            )

            if json_success:
                print("✅ Dados também salvos em formato JSON comprimido")

        db_end_time = time.time()

        # Atualiza logger se disponível
        if self.logger:
            self.logger.add_processed_table("incident", len(incidents_df))
            self.logger.add_processed_table("incident_task", len(tasks_df))
            self.logger.add_processed_table("incident_sla", len(slas_df))
            self.logger.add_processed_table(
                "task_time_worked", len(time_worked_df)
            )

        total_time = time.time() - start_time
        db_time = db_end_time - db_start_time
        api_time = extraction_time

        print(
            f"\n⏱️  Tempo incidentes - Total: {total_time:.2f}s | API: {api_time:.2f}s | BD: {db_time:.2f}s"
        )

        if success:
            print("✅ Dados de incidentes salvos com sucesso")
        else:
            print("❌ Erro ao salvar dados de incidentes")

        return success

    def run_full_etl(
        self,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        include_backlog: bool = False,
    ):
        """Executa o processo completo de ETL"""
        print("🚀 Iniciando processo completo de ETL do ServiceNow")
        print(f"⏰ INÍCIO: {datetime.datetime.now()}")

        etl_start_time = time.time()

        try:
            # 1. Extrai dados de configuração
            if not self.extract_configuration_data():
                print("❌ Falha na extração de dados de configuração")
                return False

            # 2. Extrai dados de incidentes
            if not self.extract_incident_data(start_date, end_date):
                print("❌ Falha na extração de dados de incidentes")
                return False

            etl_end_time = time.time()
            total_etl_time = etl_end_time - etl_start_time

            # 3. Imprime métricas finais
            self.print_final_metrics(total_etl_time)

            print(f"⏰ FIM: {datetime.datetime.now()}")
            print("🎉 Processo ETL concluído com sucesso!")
            return True

        except Exception as e:
            print(f"❌ Erro durante execução do ETL: {e}")
            return False

    def print_final_metrics(self, total_etl_time: float):
        """Imprime um resumo final das métricas de performance"""
        print("\n" + "=" * 60)
        print("📊 RESUMO FINAL DE PERFORMANCE")
        print("=" * 60)

        # Métricas gerais
        print(
            f"⏱️  Tempo total do ETL: {total_etl_time:.2f}s ({total_etl_time / 60:.1f} minutos)"
        )

        # Coleta métricas de todos os extractors
        all_extractors = [
            ("Incidentes", self.incident_extractor),
            ("Tarefas", self.task_extractor),
            ("SLAs", self.sla_extractor),
            ("Tempo Trabalhado", self.time_worked_extractor),
            ("Contratos SLA", self.contract_extractor),
            ("Grupos", self.group_extractor),
        ]

        total_api_requests = 0
        total_api_time = 0.0
        total_failed_requests = 0

        for name, extractor in all_extractors:
            metrics = extractor.get_api_metrics()
            total_api_requests += metrics["total_requests"]
            total_api_time += metrics["total_api_time"]
            total_failed_requests += metrics["failed_requests"]

        # Métricas consolidadas da API
        if total_api_requests > 0:
            avg_api_time = total_api_time / total_api_requests
            api_success_rate = (
                (total_api_requests - total_failed_requests)
                / total_api_requests
            ) * 100

            print("\n🌐 Resumo API ServiceNow:")
            print(f"   ├── Total de requisições: {total_api_requests:,}")
            print(f"   ├── Requisições falharam: {total_failed_requests}")
            print(f"   ├── Taxa de sucesso: {api_success_rate:.1f}%")
            print(f"   ├── Tempo total API: {total_api_time:.2f}s")
            print(f"   └── Tempo médio/requisição: {avg_api_time:.3f}s")

        # Métricas do banco de dados
        self.db_manager.print_db_metrics()

        # Métricas JSON se habilitado
        if self.enable_json_storage:
            self.json_manager.print_json_metrics()

        # Atualiza métricas no logger se disponível
        if self.logger:
            self.logger.update_api_metrics(all_extractors)
            self.logger.update_db_metrics(self.db_manager)
            if self.enable_json_storage:
                self.logger.update_json_metrics(self.json_manager)

        # Cálculo de eficiência
        if total_etl_time > 0:
            api_percentage = (total_api_time / total_etl_time) * 100
            print("\n📈 Análise de Performance:")
            print(
                f"   ├── Tempo gasto com API: {api_percentage:.1f}% do total"
            )
            print(
                f"   └── Throughput: {total_api_requests / total_etl_time:.1f} requisições/segundo"
            )

        print("=" * 60)

    def run_daily_etl(self, days_back: int = 3):
        """Executa ETL para os últimos N dias"""
        today = datetime.date.today()

        print(f"📅 Executando ETL diário para os últimos {days_back} dias")

        for i in range(days_back, -1, -1):
            day = today - datetime.timedelta(days=i)
            date_str = day.strftime("%Y-%m-%d")

            print(f"\n📅 Processando dia: {date_str}")

            success = self.extract_incident_data(
                start_date=date_str, end_date=date_str, include_backlog=False
            )

            if not success:
                print(f"❌ Falha ao processar dia {date_str}")
                return False

        return True


def main():
    """Função principal com interface melhorada"""

    # Ajuda e validação de argumentos
    if len(sys.argv) < 2:
        print_usage()
        return

    command = sys.argv[1].lower()

    # Comandos especiais
    if command in ["help", "--help", "-h"]:
        print_usage()
        return
    elif command == "logs":
        print_recent_executions()
        return
    elif command == "analyze":
        from storage_analyzer import StorageAnalyzer

        analyzer = StorageAnalyzer()
        analyzer.print_detailed_analysis()
        return

    # Novos comandos do orquestrador normalizado
    elif command in ["sync-ref", "sync-references"]:
        orchestrator = ServiceNowETLOrchestrator()
        force_full = "--full" in sys.argv
        execution_id = orchestrator.execution_logger.start_execution(
            "sync_references"
        )

        try:
            success = orchestrator.sync_reference_data(
                force_full_sync=force_full
            )
            orchestrator.execution_logger.finish_execution(
                execution_id, success=success
            )
        except Exception as e:
            orchestrator.execution_logger.finish_execution(
                execution_id, success=False, error=str(e)
            )

        return

    elif command in ["etl-new", "normalized-etl"]:
        if len(sys.argv) < 4:
            print(
                "❌ Erro: Para ETL normalizado, forneça data de INÍCIO e FIM"
            )
            print(
                "   Uso: python main.py etl-new YYYY-MM-DD YYYY-MM-DD [--full-ref]"
            )
            print("   Exemplo: python main.py etl-new 2025-09-01 2025-09-15")
            return

        orchestrator = ServiceNowETLOrchestrator()
        start_date = sys.argv[2]
        end_date = sys.argv[3]
        force_full_ref = "--full-ref" in sys.argv

        if not validate_date_format(start_date) or not validate_date_format(
            end_date
        ):
            print("❌ Erro: Formato de data inválido. Use YYYY-MM-DD")
            return

        orchestrator.full_etl_workflow(start_date, end_date, force_full_ref)
        return

    elif command in ["quick-sync", "quick"]:
        orchestrator = ServiceNowETLOrchestrator()
        days_back = (
            int(sys.argv[2])
            if len(sys.argv) > 2 and sys.argv[2].isdigit()
            else 1
        )
        orchestrator.quick_incident_sync(days_back)
        return

    elif command in ["sync-companies", "companies"]:
        orchestrator = ServiceNowETLOrchestrator()
        company_type = sys.argv[2] if len(sys.argv) > 2 else "customer"
        orchestrator.sync_specific_companies(company_type)
        return

    # Verifica se deve habilitar armazenamento JSON (comandos antigos)
    enable_json = "--json" in sys.argv or "-j" in sys.argv

    # Cria logger de execução
    logger = ExecutionLogger()

    try:
        etl = ServiceNowETL(enable_json_storage=enable_json, logger=logger)

        if enable_json:
            print("📄 Armazenamento JSON comprimido HABILITADO")

        success = False

        if command == "config":
            logger.set_execution_params("config", json_enabled=enable_json)
            success = etl.extract_configuration_data()

        elif command == "daily":
            days_back = 3
            if len(sys.argv) > 2 and sys.argv[2].isdigit():
                days_back = int(sys.argv[2])

            logger.set_execution_params("daily", json_enabled=enable_json)
            success = etl.run_daily_etl(days_back=days_back)

        elif command == "backlog":
            logger.set_execution_params("backlog", json_enabled=enable_json)
            success = etl.run_full_etl(include_backlog=True)

        elif command in ["range", "period", "custom"]:
            if len(sys.argv) < 4:
                print(
                    "❌ Erro: Para extração por período, forneça data de INÍCIO e FIM"
                )
                print(
                    "   Uso: python main.py range YYYY-MM-DD YYYY-MM-DD [--json]"
                )
                print("   Exemplo: python main.py range 2025-09-01 2025-09-15")
                return

            start_date = sys.argv[2]
            end_date = sys.argv[3]

            # Validação básica do formato de data
            if not validate_date_format(
                start_date
            ) or not validate_date_format(end_date):
                print("❌ Erro: Formato de data inválido. Use YYYY-MM-DD")
                print("   Exemplo: 2025-09-15")
                return

            logger.set_execution_params(
                "range", start_date, end_date, enable_json
            )
            success = etl.run_full_etl(
                start_date=start_date, end_date=end_date
            )

        elif command == "today":
            today = datetime.date.today().strftime("%Y-%m-%d")
            logger.set_execution_params("today", today, today, enable_json)
            success = etl.run_full_etl(start_date=today, end_date=today)

        elif command == "yesterday":
            yesterday = (
                datetime.date.today() - datetime.timedelta(days=1)
            ).strftime("%Y-%m-%d")
            logger.set_execution_params(
                "yesterday", yesterday, yesterday, enable_json
            )
            success = etl.run_full_etl(
                start_date=yesterday, end_date=yesterday
            )

        else:
            print(f"❌ Comando desconhecido: {command}")
            print_usage()
            return

        # Finaliza log baseado no sucesso
        if success:
            logger.set_success()
        else:
            logger.set_error("Execução falhou")

    except KeyboardInterrupt:
        logger.set_error("Execução cancelada pelo usuário")
        print("\n⚠️ Execução cancelada pelo usuário")
    except Exception as e:
        logger.set_error(f"Erro inesperado: {str(e)}")
        print(f"❌ Erro inesperado: {e}")
        import traceback

        traceback.print_exc()
    finally:
        # Sempre finaliza o log
        logger.finish_execution()
        logger.save_to_database()
        logger.print_execution_summary()


def validate_date_format(date_string: str) -> bool:
    """Valida formato de data YYYY-MM-DD"""
    try:
        datetime.datetime.strptime(date_string, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def print_usage():
    """Imprime instruções de uso"""
    print("\n🚀 SERVICENOW ETL - SISTEMA DE EXTRAÇÃO DE DADOS")
    print("=" * 60)
    print("\n📋 COMANDOS DISPONÍVEIS:")
    print()

    # Novos comandos normalizados
    print("🆕 NOVA ARQUITETURA NORMALIZADA:")
    print("   python main.py sync-ref [--full]")
    print("      └── Sincroniza dados de referência (usuários e empresas)")
    print("      └── --full: Força sincronização completa")
    print()
    print("   python main.py etl-new INÍCIO FIM [--full-ref]")
    print("      └── ETL completo normalizado (referências + incidentes)")
    print("      └── --full-ref: Força sync completa das referências")
    print("      └── Exemplo: python main.py etl-new 2025-09-01 2025-09-15")
    print()
    print("   python main.py quick-sync [DIAS]")
    print("      └── Sincronização rápida dos últimos N dias (padrão: 1)")
    print("      └── Exemplo: python main.py quick-sync 3")
    print()
    print("   python main.py sync-companies [TIPO]")
    print(
        "      └── Sincroniza empresas por tipo (customer, vendor, manufacturer)"
    )
    print("      └── Exemplo: python main.py sync-companies customer")
    print()

    # Comandos principais originais
    print("🔧 CONFIGURAÇÃO (LEGADO):")
    print("   python main.py config [--json]")
    print("      └── Extrai apenas dados de configuração (contratos, grupos)")
    print()

    print("📅 EXTRAÇÃO POR DATA (LEGADO):")
    print("   python main.py today [--json]")
    print("      └── Extrai dados de hoje")
    print()
    print("   python main.py yesterday [--json]")
    print("      └── Extrai dados de ontem")
    print()
    print("   python main.py range INÍCIO FIM [--json]")
    print("      └── Extrai dados do período especificado")
    print("      └── Exemplo: python main.py range 2025-09-01 2025-09-15")
    print()

    print("📊 EXTRAÇÃO AUTOMÁTICA (LEGADO):")
    print("   python main.py daily [DIAS] [--json]")
    print("      └── Extrai dados dos últimos N dias (padrão: 3)")
    print("      └── Exemplo: python main.py daily 7")
    print()
    print("   python main.py backlog [--json]")
    print("      └── Extrai todos os dados incluindo incidentes abertos")
    print()

    print("📊 ANÁLISE E LOGS:")
    print("   python main.py analyze")
    print("      └── Análise comparativa de uso de espaço")
    print()
    print("   python main.py logs")
    print("      └── Mostra últimas execuções registradas")
    print()

    print("🔧 OPÇÕES ADICIONAIS:")
    print("   --json, -j")
    print(
        "      └── Habilita armazenamento JSON comprimido paralelo (comandos legado)"
    )
    print()
    print("   --full")
    print("      └── Força sincronização completa (comandos sync-ref)")
    print()
    print("   --full-ref")
    print("      └── Força sincronização completa das referências (etl-new)")
    print()
    print("   help, --help, -h")
    print("      └── Mostra esta mensagem de ajuda")
    print()

    print("📝 EXEMPLOS PRÁTICOS:")
    print()
    print("   🆕 NOVA ARQUITETURA RECOMENDADA:")
    print("   # Sincronização incremental de referências")
    print("   python main.py sync-ref")
    print()
    print("   # ETL completo normalizado")
    print("   python main.py etl-new 2025-09-01 2025-09-15")
    print()
    print("   # Sincronização rápida diária")
    print("   python main.py quick-sync")
    print()

    print("   📊 COMANDOS LEGADO:")
    print("   # Extração simples de hoje")
    print("   python main.py today")
    print()
    print("   # Extração de período com JSON")
    print("   python main.py range 2025-09-01 2025-09-15 --json")
    print()
    print("   # Ver execuções anteriores")
    print("   python main.py logs")
    print()

    print("💡 FORMATO DE DATAS: YYYY-MM-DD (ex: 2025-09-15)")
    print()
    print("🔄 MIGRAÇÃO: Use os comandos 'etl-new' e 'sync-ref' para a nova")
    print("   arquitetura normalizada com melhor performance!")
    print("=" * 60)


if __name__ == "__main__":
    main()
