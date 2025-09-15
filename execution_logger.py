"""
Gerenciador de logs de execução do ServiceNow ETL
"""

import json
import uuid
from datetime import datetime
from typing import Dict, List, Optional, Any
import socket
import getpass
from config import get_db_connection


class ExecutionLogger:
    """Gerenciador de logs de execução do ETL ServiceNow"""
    
    def __init__(self):
        self.execution_id = str(uuid.uuid4())
        self.started_at = datetime.now()
        self.execution_data = {
            'execution_id': self.execution_id,
            'execution_type': None,
            'start_date': None,
            'end_date': None,
            'started_at': self.started_at,
            'ended_at': None,
            'duration_seconds': None,
            'status': 'running',
            'json_storage_enabled': False,
            
            # Métricas API
            'total_api_requests': 0,
            'failed_api_requests': 0,
            'total_api_time_seconds': 0.0,
            'api_success_rate': 0.0,
            
            # Métricas BD
            'total_db_transactions': 0,
            'total_records_processed': 0,
            'db_time_seconds': 0.0,
            
            # Métricas JSON
            'json_size_kb': None,
            'compressed_size_kb': None,
            'compression_ratio': None,
            
            # Detalhes
            'tables_processed': [],
            'error_message': None,
            'records_by_table': {},
            
            # Metadados
            'hostname': socket.gethostname(),
            'username': getpass.getuser()
        }
        
        print(f"📊 Log de execução iniciado - ID: {self.execution_id[:8]}...")
    
    def set_execution_params(self, execution_type: str, start_date: Optional[str] = None, 
                           end_date: Optional[str] = None, json_enabled: bool = False):
        """Define parâmetros da execução"""
        self.execution_data.update({
            'execution_type': execution_type,
            'start_date': start_date,
            'end_date': end_date,
            'json_storage_enabled': json_enabled
        })
        
        period_str = ""
        if start_date and end_date:
            if start_date == end_date:
                period_str = f" ({start_date})"
            else:
                period_str = f" ({start_date} até {end_date})"
        
        json_str = " + JSON" if json_enabled else ""
        print(f"🎯 Execução: {execution_type.upper()}{period_str}{json_str}")
    
    def update_api_metrics(self, extractors: List[Any]):
        """Atualiza métricas da API baseado nos extractors"""
        total_requests = 0
        failed_requests = 0
        total_api_time = 0.0
        
        for extractor in extractors:
            if hasattr(extractor, 'get_api_metrics'):
                metrics = extractor.get_api_metrics()
                total_requests += metrics.get('total_requests', 0)
                failed_requests += metrics.get('failed_requests', 0)
                total_api_time += metrics.get('total_api_time', 0.0)
        
        success_rate = 0.0
        if total_requests > 0:
            success_rate = ((total_requests - failed_requests) / total_requests) * 100
        
        self.execution_data.update({
            'total_api_requests': total_requests,
            'failed_api_requests': failed_requests,
            'total_api_time_seconds': round(total_api_time, 2),
            'api_success_rate': round(success_rate, 2)
        })
    
    def update_db_metrics(self, db_manager: Any):
        """Atualiza métricas do banco de dados"""
        if hasattr(db_manager, 'get_db_metrics_data'):
            db_metrics = db_manager.get_db_metrics_data()
            self.execution_data.update({
                'total_db_transactions': db_metrics.get('total_transactions', 0),
                'total_records_processed': db_metrics.get('total_records', 0),
                'db_time_seconds': round(db_metrics.get('total_time', 0.0), 2)
            })
    
    def update_json_metrics(self, json_manager: Any):
        """Atualiza métricas JSON"""
        if hasattr(json_manager, 'get_json_metrics'):
            json_metrics = json_manager.get_json_metrics()
            
            if json_metrics['total_saves'] > 0:
                self.execution_data.update({
                    'json_size_kb': round(json_metrics.get('total_json_size_kb', 0), 2),
                    'compressed_size_kb': round(json_metrics.get('total_compressed_size_kb', 0), 2),
                    'compression_ratio': round(json_metrics.get('avg_compression_ratio', 0), 2)
                })
    
    def add_processed_table(self, table_name: str, record_count: int):
        """Adiciona tabela processada ao log"""
        if table_name not in self.execution_data['tables_processed']:
            self.execution_data['tables_processed'].append(table_name)
        
        self.execution_data['records_by_table'][table_name] = record_count
    
    def set_error(self, error_message: str):
        """Define erro na execução"""
        self.execution_data['status'] = 'failed'
        self.execution_data['error_message'] = str(error_message)
        print(f"❌ Erro registrado no log: {error_message}")
    
    def set_success(self):
        """Marca execução como bem-sucedida"""
        self.execution_data['status'] = 'success'
        print("✅ Execução marcada como bem-sucedida")
    
    def set_partial_success(self):
        """Marca execução como parcialmente bem-sucedida"""
        self.execution_data['status'] = 'partial'
        print("⚠️ Execução marcada como parcial")
    
    def finish_execution(self):
        """Finaliza a execução e calcula duração"""
        self.execution_data['ended_at'] = datetime.now()
        
        duration = self.execution_data['ended_at'] - self.execution_data['started_at']
        self.execution_data['duration_seconds'] = round(duration.total_seconds(), 2)
        
        print(f"⏱️ Execução finalizada em {self.execution_data['duration_seconds']}s")
    
    def save_to_database(self) -> bool:
        """Salva log no banco de dados"""
        try:
            with get_db_connection() as conn:
                cursor = conn.cursor()
                
                # Prepara dados para inserção
                tables_processed_str = ','.join(self.execution_data['tables_processed'])
                records_by_table_json = json.dumps(self.execution_data['records_by_table'])
                
                insert_query = """
                INSERT INTO servicenow_execution_log (
                    execution_id, execution_type, start_date, end_date,
                    started_at, ended_at, duration_seconds, status, json_storage_enabled,
                    total_api_requests, failed_api_requests, total_api_time_seconds, api_success_rate,
                    total_db_transactions, total_records_processed, db_time_seconds,
                    json_size_kb, compressed_size_kb, compression_ratio,
                    tables_processed, error_message, records_by_table,
                    hostname, username
                ) VALUES (
                    ?, ?, ?, ?, ?, ?, ?, ?, ?,
                    ?, ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?, ?,
                    ?, ?
                )
                """
                
                cursor.execute(insert_query,
                    self.execution_data['execution_id'],
                    self.execution_data['execution_type'],
                    self.execution_data['start_date'],
                    self.execution_data['end_date'],
                    self.execution_data['started_at'],
                    self.execution_data['ended_at'],
                    self.execution_data['duration_seconds'],
                    self.execution_data['status'],
                    self.execution_data['json_storage_enabled'],
                    self.execution_data['total_api_requests'],
                    self.execution_data['failed_api_requests'],
                    self.execution_data['total_api_time_seconds'],
                    self.execution_data['api_success_rate'],
                    self.execution_data['total_db_transactions'],
                    self.execution_data['total_records_processed'],
                    self.execution_data['db_time_seconds'],
                    self.execution_data['json_size_kb'],
                    self.execution_data['compressed_size_kb'],
                    self.execution_data['compression_ratio'],
                    tables_processed_str,
                    self.execution_data['error_message'],
                    records_by_table_json,
                    self.execution_data['hostname'],
                    self.execution_data['username']
                )
                
                conn.commit()
                print(f"💾 Log salvo no banco - ID: {self.execution_id[:8]}...")
                return True
                
        except Exception as e:
            print(f"❌ Erro ao salvar log no banco: {e}")
            return False
    
    def print_execution_summary(self):
        """Imprime resumo da execução"""
        print("\n" + "="*60)
        print("📊 RESUMO DA EXECUÇÃO")
        print("="*60)
        
        data = self.execution_data
        
        # Informações gerais
        print(f"🆔 ID da Execução: {data['execution_id']}")
        print(f"📅 Tipo: {data['execution_type'].upper()}")
        
        if data['start_date'] and data['end_date']:
            if data['start_date'] == data['end_date']:
                print(f"📆 Data: {data['start_date']}")
            else:
                print(f"📆 Período: {data['start_date']} até {data['end_date']}")
        
        print(f"🎯 Status: {data['status'].upper()}")
        print(f"⏱️ Duração: {data['duration_seconds']}s")
        print(f"💻 Host: {data['hostname']} ({data['username']})")
        
        # Métricas API
        if data['total_api_requests'] > 0:
            print("\n🌐 Métricas API:")
            print(f"   ├── Requisições: {data['total_api_requests']:,}")
            print(f"   ├── Taxa sucesso: {data['api_success_rate']:.1f}%")
            print(f"   └── Tempo API: {data['total_api_time_seconds']}s")
        
        # Métricas BD
        if data['total_records_processed'] > 0:
            print("\n💾 Métricas Banco:")
            print(f"   ├── Registros: {data['total_records_processed']:,}")
            print(f"   ├── Transações: {data['total_db_transactions']}")
            print(f"   └── Tempo BD: {data['db_time_seconds']}s")
        
        # Métricas JSON
        if data['json_storage_enabled'] and data['compression_ratio']:
            print("\n📦 Métricas JSON:")
            print(f"   ├── Tamanho original: {data['json_size_kb']:.1f} KB")
            print(f"   ├── Comprimido: {data['compressed_size_kb']:.1f} KB")
            print(f"   └── Economia: {data['compression_ratio']:.1f}%")
        
        # Tabelas processadas
        if data['tables_processed']:
            print("\n📋 Tabelas processadas:")
            for table in data['tables_processed']:
                count = data['records_by_table'].get(table, 0)
                print(f"   ├── {table}: {count:,} registros")
        
        # Erro se houver
        if data['error_message']:
            print(f"\n❌ Erro: {data['error_message']}")
        
        print("="*60)
    
    def get_execution_data(self) -> Dict:
        """Retorna dados da execução"""
        return self.execution_data.copy()


def get_recent_executions(limit: int = 10) -> List[Dict]:
    """Busca execuções recentes do banco"""
    try:
        with get_db_connection() as conn:
            cursor = conn.cursor()
            
            query = """
            SELECT TOP (?) 
                execution_id, execution_type, start_date, end_date,
                started_at, duration_seconds, status, total_records_processed,
                json_storage_enabled, error_message
            FROM servicenow_execution_log 
            ORDER BY started_at DESC
            """
            
            cursor.execute(query, limit)
            results = cursor.fetchall()
            
            executions = []
            for row in results:
                executions.append({
                    'execution_id': row[0],
                    'execution_type': row[1],
                    'start_date': row[2],
                    'end_date': row[3],
                    'started_at': row[4],
                    'duration_seconds': row[5],
                    'status': row[6],
                    'total_records_processed': row[7],
                    'json_storage_enabled': row[8],
                    'error_message': row[9]
                })
            
            return executions
            
    except Exception as e:
        print(f"❌ Erro ao buscar execuções recentes: {e}")
        return []


def print_recent_executions(limit: int = 5):
    """Imprime execuções recentes"""
    executions = get_recent_executions(limit)
    
    if not executions:
        print("ℹ️ Nenhuma execução encontrada no log")
        return
    
    print(f"\n📋 ÚLTIMAS {len(executions)} EXECUÇÕES:")
    print("-" * 80)
    
    for exec_data in executions:
        status_icon = {
            'success': '✅',
            'failed': '❌', 
            'partial': '⚠️',
            'running': '🔄'
        }.get(exec_data['status'], '❓')
        
        period = ""
        if exec_data['start_date'] and exec_data['end_date']:
            if exec_data['start_date'] == exec_data['end_date']:
                period = f" ({exec_data['start_date']})"
            else:
                period = f" ({exec_data['start_date']} até {exec_data['end_date']})"
        
        json_indicator = " 📦" if exec_data['json_storage_enabled'] else ""
        records = f"{exec_data['total_records_processed']:,}" if exec_data['total_records_processed'] else "0"
        
        print(f"{status_icon} {exec_data['execution_type'].upper()}{period}{json_indicator}")
        print(f"   ├── ID: {exec_data['execution_id'][:8]}... | {exec_data['started_at'].strftime('%d/%m/%Y %H:%M')}")
        print(f"   ├── Duração: {exec_data['duration_seconds']}s | Registros: {records}")
        
        if exec_data['error_message']:
            error_preview = exec_data['error_message'][:50] + "..." if len(exec_data['error_message']) > 50 else exec_data['error_message']
            print(f"   └── Erro: {error_preview}")
        else:
            print(f"   └── Status: {exec_data['status']}")
        
        print()