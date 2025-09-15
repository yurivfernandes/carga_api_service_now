# ServiceNow ETL - Sistema Modular de Extração

Sistema completo e modular para extração, transformação e carregamento (ETL) de dados do ServiceNow, com suporte a armazenamento JSON comprimido para otimização de espaço.

## � Funcionalidades

- **Extração Modular**: Classes especializadas para diferentes tipos de dados
- **Armazenamento Duplo**: Tabelas normalizadas + JSON comprimido
- **Monitoramento de Performance**: Métricas detalhadas de API e banco de dados
- **Análise de Espaço**: Comparação entre modelos de armazenamento
- **Configuração SSL**: Bypass automático para certificados auto-assinados
- **Logs Detalhados**: Rastreamento completo de tempos e operações

## 🏗️ Arquitetura

### Estrutura de Arquivos

```
carga_api_service_now/
├── 📄 main.py                     # Orquestrador principal
├── ⚙️ config.py                   # Configurações e conexões
├── 🔐 ssl_config.py               # Configurações SSL
├── 🗄️ database_manager.py         # Operações de banco normalizado
├── 📦 json_data_manager.py        # Gerenciador JSON comprimido
├── 📊 storage_analyzer.py         # Análise comparativa de espaço
├── 🧪 test_json_storage.py        # Testes do sistema JSON
├── 📜 create_json_table.sql       # Script de criação da tabela JSON
├── extractors/
│   ├── 🛠️ base_extractor.py       # Classe base para extractors
│   ├── 🎫 incident_extractor.py   # Extração de incidentes
│   ├── ✅ task_extractor.py       # Extração de tarefas
│   ├── ⏰ sla_extractor.py        # Extração de SLAs
│   ├── 🕐 time_worked_extractor.py # Extração de tempo trabalhado
│   └── 👥 contract_group_extractor.py # Contratos e grupos
└── 📋 app_old.py                  # Script original (backup)
```

### Componentes Principais

#### 1. **Extractors (extractors/)**
- `BaseExtractor`: Classe base com SSL, métricas e funcionalidades comuns
- `IncidentExtractor`: Extração de incidentes com enriquecimento
- `TaskExtractor`: Tarefas relacionadas aos incidentes
- `SLAExtractor`: Dados de SLA por incidente
- `TimeWorkedExtractor`: Registro de tempo trabalhado
- `ContractSLAExtractor` & `GroupExtractor`: Dados de configuração

#### 2. **Gerenciadores de Dados**
- `DatabaseManager`: Operações CRUD em tabelas normalizadas
- `JSONDataManager`: Compressão e armazenamento em formato JSON

#### 3. **Análise e Monitoramento**
- `StorageAnalyzer`: Análise comparativa de uso de espaço
- Sistema de métricas integrado em todos os componentes