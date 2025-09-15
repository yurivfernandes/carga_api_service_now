# ServiceNow ETL System - Versão Normalizada

Sistema ETL para extração e carga de dados do ServiceNow com arquitetura normalizada e alta performance.

## 🆕 Nova Arquitetura Normalizada

O sistema foi completamente reestruturado para separar dados transacionais de dados de referência, resultando em:

- **Melhor Performance**: Sincronização incremental inteligente
- **Dados Normalizados**: Referências separadas em tabelas dedicadas
- **Menos Chamadas API**: Cache e detecção de mudanças por hash
- **Logging Completo**: Rastreamento detalhado de execuções
- **Manutenção Simplificada**: Estrutura mais limpa e organizada

## 📋 Estrutura do Banco de Dados

### Tabelas de Referência
- **`sys_user`**: Usuários do sistema (sincronização incremental)
- **`sys_company`**: Empresas/organizações (sincronização incremental)
- **`cmn_department`**: Departamentos organizacionais

### Tabelas Transacionais
- **`incident`**: Incidentes (apenas IDs de referência, sem enriquecimento)
- **`servicenow_execution_log`**: Log de execuções do ETL

### Views Enriquecidas
- **`incident_enriched`**: Incidentes com nomes das referências
- **`incident_with_names`**: View alternativa para consultas

## 🚀 Comandos Disponíveis

### Nova Arquitetura (Recomendada)

```bash
# Sincronização incremental de dados de referência
python main.py sync-ref

# Sincronização completa de dados de referência
python main.py sync-ref --full

# ETL completo normalizado
python main.py etl-new 2025-01-01 2025-01-31

# ETL com sincronização completa das referências
python main.py etl-new 2025-01-01 2025-01-31 --full-ref

# Sincronização rápida diária (1 dia)
python main.py quick-sync

# Sincronização rápida (últimos 3 dias)
python main.py quick-sync 3

# Sincronizar empresas por tipo
python main.py sync-companies customer
python main.py sync-companies vendor
python main.py sync-companies manufacturer
```

### Comandos Legado (Compatibilidade)

```bash
# Extração de hoje
python main.py today

# Extração de ontem
python main.py yesterday

# Extração por período
python main.py range 2025-01-01 2025-01-31

# Extração dos últimos N dias
python main.py daily 7

# Configuração apenas
python main.py config
```

### Análise e Logs

```bash
# Ver execuções recentes
python main.py logs

# Análise de armazenamento
python main.py analyze

# Ajuda completa
python main.py help
```

## 🔄 Fluxo de Trabalho Recomendado

### Configuração Inicial

1. **Execute as migrações SQL**:
   ```bash
   # Aplicar no SQL Server
   sql/create_execution_log_table.sql
   sql/create_reference_tables.sql
   sql/cleanup_incident_table.sql
   ```

2. **Primeira sincronização completa**:
   ```bash
   python main.py sync-ref --full
   ```

3. **ETL inicial de incidentes**:
   ```bash
   python main.py etl-new 2025-01-01 2025-01-31 --full-ref
   ```

### Execução Diária

```bash
# Sincronização rápida diária (automatizar via cron/scheduler)
python main.py quick-sync
```

### Sincronização Semanal

```bash
# Sincronização incremental de referências
python main.py sync-ref

# ETL de período específico
python main.py etl-new 2025-01-15 2025-01-21
```

### Sincronização Mensal

```bash
# Sincronização completa de referências (1x por mês)
python main.py sync-ref --full
```

## ⚡ Características da Sincronização Incremental

### Usuários (`sys_user`)
- **Detecção de mudanças**: Hash MD5 dos dados principais
- **Sincronização inteligente**: Apenas usuários modificados
- **Referências em falta**: Identifica usuários referenciados em incidentes
- **Batch processing**: Processa grandes volumes em lotes

### Empresas (`sys_company`)
- **Tipos suportados**: Customer, Vendor, Manufacturer
- **Cache de referências**: Evita chamadas desnecessárias
- **Empresas ativas/inativas**: Lógica otimizada de busca
- **Hash de mudanças**: Controle preciso de alterações

### Incidentes (`incident`)
- **Sem enriquecimento**: Apenas IDs de referência armazenados
- **Performance otimizada**: Queries mais rápidas
- **Views para consulta**: Dados enriquecidos via views SQL
- **Timestamps ETL**: Controle de quando dados foram processados

## 🧪 Testes

Execute o script de testes para validar o sistema:

```bash
# Todos os testes
python test_etl.py all

# Testes específicos
python test_etl.py ref          # Sincronização de referências
python test_etl.py incidents    # Extração de incidentes
python test_etl.py full         # Fluxo completo
python test_etl.py quick        # Sincronização rápida
python test_etl.py companies    # Sincronização de empresas
```

## 📊 Monitoramento

### Logs de Execução

O sistema mantém log detalhado de todas as execuções:

```bash
# Ver últimas execuções
python main.py logs
```

### Métricas Disponíveis

- **Tempo de execução**: Total, API, Banco de dados
- **Número de registros**: Por tabela processada
- **Taxa de sucesso**: API e banco de dados
- **Detecção de erros**: Logs detalhados de falhas

### Views de Análise

```sql
-- Estatísticas de execução
SELECT * FROM v_execution_summary;

-- Análise de performance
SELECT * FROM v_execution_performance;

-- Execuções com erro
SELECT * FROM v_execution_errors;
```

## 🔧 Configuração

### Variáveis de Ambiente

```bash
# ServiceNow
SERVICENOW_INSTANCE=your-instance
SERVICENOW_USERNAME=your-username
SERVICENOW_PASSWORD=your-password

# Banco de Dados
DB_SERVER=your-server
DB_DATABASE=your-database
DB_USERNAME=your-db-username
DB_PASSWORD=your-db-password
```

### Configuração de Performance

- **Batch Size**: 50-100 registros por lote (configurável)
- **API Timeout**: 30 segundos (configurável)
- **Max Retries**: 3 tentativas (configurável)
- **Hash Algorithm**: MD5 para detecção de mudanças

## 📈 Benefícios da Nova Arquitetura

### Performance
- **90% menos chamadas API** para dados de referência
- **Sincronização incremental** baseada em timestamps e hash
- **Queries otimizadas** com índices apropriados

### Manutenibilidade
- **Código modular** com extractors especializados
- **Logs estruturados** para troubleshooting
- **Testes automatizados** para validação

### Escalabilidade
- **Processamento em lotes** para grandes volumes
- **Controle de memória** eficiente
- **Recuperação de erros** robusta

## 🔄 Migração do Sistema Antigo

### Passo 1: Backup
```bash
# Backup das tabelas atuais
# (usar ferramentas do SQL Server)
```

### Passo 2: Aplicar Migrações
```bash
# Executar scripts SQL na ordem:
# 1. create_reference_tables.sql
# 2. create_execution_log_table.sql  
# 3. cleanup_incident_table.sql
```

### Passo 3: Primeira Sincronização
```bash
# Sincronização completa inicial
python main.py sync-ref --full
python main.py etl-new 2025-01-01 2025-01-31 --full-ref
```

### Passo 4: Validação
```bash
# Executar testes
python test_etl.py all

# Verificar dados
python main.py analyze
```

## 🆘 Troubleshooting

### Problemas Comuns

1. **Erro de conexão API**:
   - Verificar credenciais ServiceNow
   - Validar instância ativa
   - Verificar permissões do usuário

2. **Erro de banco de dados**:
   - Validar string de conexão
   - Verificar permissões SQL Server
   - Confirmar tabelas criadas

3. **Sincronização lenta**:
   - Usar sincronização incremental
   - Verificar índices nas tabelas
   - Ajustar batch size

### Logs Detalhados

```bash
# Verificar últimas execuções
python main.py logs

# Análise de performance
python main.py analyze
```

## 📞 Suporte

Para problemas ou dúvidas:

1. Verificar logs de execução
2. Executar testes diagnósticos
3. Consultar documentação da API ServiceNow
4. Verificar configurações do banco de dados

---

**Versão**: 2.0 - Arquitetura Normalizada  
**Última atualização**: Janeiro 2025