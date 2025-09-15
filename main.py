"""
Script principal para execução da extração e carga de dados do ServiceNow
"""

import datetime
import sys
import time
from typing import Optional

from database_manager import DatabaseManager
from extractors.contract_group_extractor import (ContractSLAExtractor,
                                                 GroupExtractor)
from extractors.incident_extractor import IncidentExtractor
from extractors.sla_extractor import SLAExtractor
from extractors.task_extractor import TaskExtractor
from extractors.time_worked_extractor import TimeWorkedExtractor
from json_data_manager import JSONDataManager


class ServiceNowETL:
    """Classe principal para orquestrar o processo ETL do ServiceNow"""
    
    def __init__(self, enable_json_storage: bool = False):
        self.db_manager = DatabaseManager()
        self.json_manager = JSONDataManager() if enable_json_storage else None
        self.enable_json_storage = enable_json_storage
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
        config_dataframes = {
            "contract_sla": contract_df,
            "groups": groups_df
        }
        
        db_start_time = time.time()
        success = self.db_manager.save_dataframes_to_database(config_dataframes)
        db_end_time = time.time()
        
        total_time = time.time() - start_time
        db_time = db_end_time - db_start_time
        
        print(f"\n⏱️  Tempo configuração - Total: {total_time:.2f}s | BD: {db_time:.2f}s")
        
        if success:
            print("✅ Dados de configuração salvos com sucesso")
        else:
            print("❌ Erro ao salvar dados de configuração")
        
        return success
    
    def extract_incident_data(
        self, 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None,
        include_backlog: bool = False
    ):
        """Extrai dados de incidentes e dados relacionados"""
        print("📊 Iniciando extração de dados de incidentes...")
        start_time = time.time()
        
        # 1. Extrai incidentes
        incidents_df = self.incident_extractor.extract_data(start_date, end_date, include_backlog)
        
        if incidents_df.is_empty():
            print("⚠️  Nenhum incidente encontrado para o período especificado")
            return True
        
        # 2. Obtém IDs dos incidentes para buscar dados relacionados
        incident_ids = incidents_df.select("sys_id").to_series().to_list()
        
        # 3. Extrai dados relacionados
        print("🔗 Extraindo dados relacionados aos incidentes...")
        
        tasks_df = self.task_extractor.extract_data(incident_ids, start_date, end_date)
        slas_df = self.sla_extractor.extract_data(incident_ids, start_date, end_date)
        time_worked_df = self.time_worked_extractor.extract_data(incident_ids, start_date, end_date)
        
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
            "task_time_worked": time_worked_df
        }
        
        # 5. Salva no banco
        db_start_time = time.time()
        success = self.db_manager.save_dataframes_to_database(
            incident_dataframes, start_date, end_date
        )
        
        # 6. Salva em JSON comprimido se habilitado
        if self.enable_json_storage and success:
            extraction_metrics = {
                'total_requests': (
                    self.incident_extractor.get_api_metrics()['total_requests'] +
                    self.task_extractor.get_api_metrics()['total_requests'] +
                    self.sla_extractor.get_api_metrics()['total_requests'] +
                    self.time_worked_extractor.get_api_metrics()['total_requests']
                )
            }
            
            json_success = self.json_manager.save_json_data_to_db(
                incident_dataframes,
                extraction_type='daily' if start_date == end_date else 'range',
                start_date=start_date,
                end_date=end_date,
                extraction_metrics=extraction_metrics
            )
            
            if json_success:
                print("✅ Dados também salvos em formato JSON comprimido")
        
        db_end_time = time.time()
        
        total_time = time.time() - start_time
        db_time = db_end_time - db_start_time
        api_time = extraction_time
        
        print(f"\n⏱️  Tempo incidentes - Total: {total_time:.2f}s | API: {api_time:.2f}s | BD: {db_time:.2f}s")
        
        if success:
            print("✅ Dados de incidentes salvos com sucesso")
        else:
            print("❌ Erro ao salvar dados de incidentes")
        
        return success
    
    def run_full_etl(
        self, 
        start_date: Optional[str] = None, 
        end_date: Optional[str] = None,
        include_backlog: bool = False
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
            if not self.extract_incident_data(start_date, end_date, include_backlog):
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
        print("\n" + "="*60)
        print("📊 RESUMO FINAL DE PERFORMANCE")
        print("="*60)
        
        # Métricas gerais
        print(f"⏱️  Tempo total do ETL: {total_etl_time:.2f}s ({total_etl_time/60:.1f} minutos)")
        
        # Coleta métricas de todos os extractors
        all_extractors = [
            ("Incidentes", self.incident_extractor),
            ("Tarefas", self.task_extractor),
            ("SLAs", self.sla_extractor),
            ("Tempo Trabalhado", self.time_worked_extractor),
            ("Contratos SLA", self.contract_extractor),
            ("Grupos", self.group_extractor)
        ]
        
        total_api_requests = 0
        total_api_time = 0.0
        total_failed_requests = 0
        
        for name, extractor in all_extractors:
            metrics = extractor.get_api_metrics()
            total_api_requests += metrics['total_requests']
            total_api_time += metrics['total_api_time']
            total_failed_requests += metrics['failed_requests']
        
        # Métricas consolidadas da API
        if total_api_requests > 0:
            avg_api_time = total_api_time / total_api_requests
            api_success_rate = ((total_api_requests - total_failed_requests) / total_api_requests) * 100
            
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
        
        # Cálculo de eficiência
        if total_etl_time > 0:
            api_percentage = (total_api_time / total_etl_time) * 100
            print("\n📈 Análise de Performance:")
            print(f"   ├── Tempo gasto com API: {api_percentage:.1f}% do total")
            print(f"   └── Throughput: {total_api_requests/total_etl_time:.1f} requisições/segundo")
        
        print("="*60)


    def run_daily_etl(self, days_back: int = 3):
        """Executa ETL para os últimos N dias"""
        today = datetime.date.today()
        
        print(f"📅 Executando ETL diário para os últimos {days_back} dias")
        
        for i in range(days_back, -1, -1):
            day = today - datetime.timedelta(days=i)
            date_str = day.strftime("%Y-%m-%d")
            
            print(f"\n📅 Processando dia: {date_str}")
            
            success = self.extract_incident_data(
                start_date=date_str,
                end_date=date_str,
                include_backlog=False
            )
            
            if not success:
                print(f"❌ Falha ao processar dia {date_str}")
                return False
        
        return True


def main():
    """Função principal"""
    
    # Verifica se deve habilitar armazenamento JSON
    enable_json = "--json" in sys.argv or "-j" in sys.argv
    
    etl = ServiceNowETL(enable_json_storage=enable_json)
    
    if enable_json:
        print("📄 Armazenamento JSON comprimido HABILITADO")
    
    # Verifica argumentos da linha de comando
    if len(sys.argv) > 1:
        command = sys.argv[1].lower()
        
        if command == "config":
            # Executa apenas extração de configuração
            etl.extract_configuration_data()
            
        elif command == "daily":
            # Executa ETL diário (últimos 3 dias)
            etl.run_daily_etl(days_back=3)
            
        elif command == "backlog":
            # Executa ETL com backlog
            etl.run_full_etl(include_backlog=True)
            
        elif command == "custom" and len(sys.argv) >= 4:
            # Executa ETL para datas específicas
            start_date = sys.argv[2]
            end_date = sys.argv[3]
            etl.run_full_etl(start_date=start_date, end_date=end_date)
            
        elif command == "analyze":
            # Executa análise de espaço
            from storage_analyzer import StorageAnalyzer
            analyzer = StorageAnalyzer()
            analyzer.print_detailed_analysis()
            
        else:
            print("Uso:")
            print("  python main.py config          - Extrai apenas dados de configuração")
            print("  python main.py daily           - Extrai dados dos últimos 3 dias")
            print("  python main.py backlog         - Extrai dados incluindo backlog")
            print("  python main.py custom YYYY-MM-DD YYYY-MM-DD - Extrai dados do período específico")
            print("  python main.py analyze         - Análise de uso de espaço")
            print("  ")
            print("Opções adicionais:")
            print("  --json, -j                     - Habilita armazenamento JSON comprimido")
    else:
        # Execução padrão - ETL completo para os últimos 2 dias
        etl.run_full_etl()


if __name__ == "__main__":
    main()if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()