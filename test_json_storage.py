"""
Script de exemplo para testar armazenamento JSON comprimido
"""

import polars as pl

from json_data_manager import JSONDataManager


def create_sample_data():
    """Cria dados de exemplo para teste"""
    
    # Dados de incidentes
    incidents_data = [
        {
            "sys_id": "INC001",
            "number": "INC0000001",
            "short_description": "Sistema lento",
            "description": "O sistema está muito lento para todos os usuários",
            "state": "6",
            "priority": "3",
            "urgency": "3",
            "impact": "3",
            "opened_at": "2024-01-15 09:30:00",
            "closed_at": None,
            "caller_id": "USER001",
            "assignment_group": "GRP001"
        },
        {
            "sys_id": "INC002", 
            "number": "INC0000002",
            "short_description": "Email não funciona",
            "description": "Não consigo enviar emails",
            "state": "7",
            "priority": "4",
            "urgency": "2", 
            "impact": "2",
            "opened_at": "2024-01-15 10:15:00",
            "closed_at": "2024-01-15 14:30:00",
            "caller_id": "USER002",
            "assignment_group": "GRP002"
        }
    ]
    
    # Dados de tarefas
    tasks_data = [
        {
            "sys_id": "TASK001",
            "number": "RITM0001001", 
            "parent": "INC001",
            "short_description": "Investigar lentidão",
            "state": "2",
            "assigned_to": "TECH001",
            "opened_at": "2024-01-15 09:45:00",
            "closed_at": None
        },
        {
            "sys_id": "TASK002",
            "number": "RITM0001002",
            "parent": "INC002", 
            "short_description": "Reiniciar servidor email",
            "state": "3",
            "assigned_to": "TECH002",
            "opened_at": "2024-01-15 10:20:00",
            "closed_at": "2024-01-15 14:00:00"
        }
    ]
    
    # Dados de SLA
    sla_data = [
        {
            "sys_id": "SLA001",
            "task": "INC001",
            "sla_definition": "Incident SLA",
            "stage": "In Progress",
            "has_breached": False,
            "percentage": 45.5,
            "duration": "PT4H30M"
        },
        {
            "sys_id": "SLA002", 
            "task": "INC002",
            "sla_definition": "Incident SLA",
            "stage": "Completed",
            "has_breached": False,
            "percentage": 100.0,
            "duration": "PT4H15M"
        }
    ]
    
    # Dados de tempo trabalhado
    time_worked_data = [
        {
            "sys_id": "TW001",
            "task": "TASK001",
            "user": "TECH001", 
            "work_start": "2024-01-15 09:45:00",
            "work_end": "2024-01-15 11:45:00",
            "work_notes": "Analisando logs do sistema",
            "duration": "PT2H00M"
        },
        {
            "sys_id": "TW002",
            "task": "TASK002", 
            "user": "TECH002",
            "work_start": "2024-01-15 10:20:00", 
            "work_end": "2024-01-15 14:00:00",
            "work_notes": "Reiniciado servidor e testado envio de emails",
            "duration": "PT3H40M"
        }
    ]
    
    # Converte para DataFrames Polars
    dataframes = {
        "incident": pl.DataFrame(incidents_data),
        "incident_task": pl.DataFrame(tasks_data),
        "incident_sla": pl.DataFrame(sla_data), 
        "task_time_worked": pl.DataFrame(time_worked_data)
    }
    
    return dataframes


def test_json_compression():
    """Testa o sistema de compressão JSON"""
    
    print("🧪 Testando sistema de armazenamento JSON comprimido")
    print("=" * 60)
    
    # 1. Cria dados de exemplo
    sample_dataframes = create_sample_data()
    
    print("📊 Dados de exemplo criados:")
    for table_name, df in sample_dataframes.items():
        print(f"   ├── {table_name}: {len(df)} registros")
    
    # 2. Inicializa gerenciador JSON
    json_manager = JSONDataManager()
    
    # 3. Testa salvamento
    print("\n💾 Testando salvamento JSON...")
    
    extraction_metrics = {
        'total_requests': 8  # Simulado
    }
    
    success = json_manager.save_json_data_to_db(
        sample_dataframes,
        extraction_type='test',
        start_date='2024-01-15',
        end_date='2024-01-15',
        extraction_metrics=extraction_metrics
    )
    
    if success:
        print("✅ Salvamento JSON realizado com sucesso!")
    else:
        print("❌ Falha no salvamento JSON")
        return
    
    # 4. Testa carregamento
    print("\n📖 Testando carregamento JSON...")
    
    loaded_data = json_manager.load_json_data_from_db('2024-01-15', 'test')
    
    if loaded_data:
        print("✅ Carregamento JSON realizado com sucesso!")
        print(f"   ├── Tabelas carregadas: {len(loaded_data['data'])}")
        print(f"   ├── Total de registros: {loaded_data['metadata']['total_records']}")
        print(f"   └── Timestamp: {loaded_data['metadata']['extraction_timestamp']}")
        
        # Mostra uma amostra dos dados
        print("\n📋 Amostra dos dados carregados:")
        for table_name, table_data in loaded_data['data'].items():
            print(f"   ├── {table_name}: {table_data['count']} registros")
            if table_data['records']:
                first_record = table_data['records'][0]
                print(f"   │   └── Exemplo: {list(first_record.keys())[:3]}...")
    else:
        print("❌ Falha no carregamento JSON")
    
    # 5. Exibe métricas
    print("\n📊 Métricas do teste:")
    json_manager.print_json_metrics()


def test_compression_comparison():
    """Testa diferentes cenários de compressão"""
    
    print("\n🔄 Testando diferentes cenários de compressão")
    print("=" * 60)
    
    json_manager = JSONDataManager()
    
    # Cenário 1: Dados pequenos
    small_data = {"incident": pl.DataFrame([{"sys_id": "test", "description": "Small test"}])}
    
    # Cenário 2: Dados médios (repetindo dados de exemplo)
    medium_data = create_sample_data()
    
    # Cenário 3: Dados grandes (simulando com repetição)
    large_incidents = []
    for i in range(100):  
        large_incidents.append({
            "sys_id": f"INC{i:03d}",
            "number": f"INC000{i:04d}",
            "short_description": f"Incident {i} - Sistema com problema crítico número {i}",
            "description": f"Descrição detalhada do incidente {i} com muitas informações específicas sobre o problema encontrado no sistema. Este é um texto longo para simular dados reais que podem ser comprimidos eficientemente.",
            "state": str((i % 7) + 1),
            "priority": str((i % 5) + 1),
            "opened_at": f"2024-01-15 {(i % 24):02d}:30:00"
        })
    
    large_data = {"incident": pl.DataFrame(large_incidents)}
    
    scenarios = [
        ("Pequeno", small_data),
        ("Médio", medium_data), 
        ("Grande", large_data)
    ]
    
    print("📊 Comparação de compressão:")
    print("-" * 50)
    
    for scenario_name, data in scenarios:
        # Prepara dados
        prepared_data = json_manager.prepare_servicenow_data(data)
        json_string = json_manager.compact_json_data(prepared_data)
        compressed_data = json_manager.compress_data(json_string)
        
        # Calcula métricas
        metrics = json_manager.calculate_sizes(json_string, compressed_data)
        
        total_records = sum(len(df) for df in data.values())
        
        print(f"\n{scenario_name}:")
        print(f"   ├── Registros: {total_records}")
        print(f"   ├── JSON: {metrics['json_size_kb']:.2f} KB")
        print(f"   ├── Comprimido: {metrics['compressed_size_kb']:.2f} KB")
        print(f"   ├── Economia: {metrics['compression_ratio']:.1f}%")
        print(f"   └── KB por registro: {metrics['compressed_size_kb']/total_records:.3f}")


def main():
    """Função principal do teste"""
    try:
        # Testa funcionalidade básica
        test_json_compression()
        
        # Testa diferentes cenários
        test_compression_comparison()
        
        print("\n🎉 Todos os testes concluídos!")
        
    except Exception as e:
        print(f"❌ Erro durante os testes: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()    main()    main()