# 🚀 GUIA RÁPIDO - NOVA INTERFACE DO SERVICENOW ETL

## ✅ O QUE MUDOU

### 1. **Interface Mais Intuitiva**
Antes: `python main.py custom 2025-09-01 2025-09-15`
Agora: `python main.py range 2025-09-01 2025-09-15`

### 2. **Comandos Mais Claros**
- ✅ `python main.py today` - Dados de hoje
- ✅ `python main.py yesterday` - Dados de ontem  
- ✅ `python main.py range INÍCIO FIM` - Período específico
- ✅ `python main.py daily 7` - Últimos 7 dias
- ✅ `python main.py logs` - Ver execuções anteriores

### 3. **Sistema de Log Completo**
- 📊 Log detalhado de cada execução
- ⏱️ Métricas de performance (API, banco, JSON)
- 🗂️ Histórico de execuções no banco
- ❌ Registro de erros e falhas

## 📋 COMANDOS PRINCIPAIS

### Extração por Data (mais comum)
```bash
# Extrair dados de hoje
python main.py today

# Extrair dados de ontem  
python main.py yesterday

# Extrair período específico (MAIS USADO)
python main.py range 2025-09-01 2025-09-15

# Com armazenamento JSON comprimido
python main.py range 2025-09-01 2025-09-15 --json
```

### Extração Automática
```bash
# Últimos 3 dias (padrão)
python main.py daily

# Últimos 7 dias
python main.py daily 7

# Todos os dados incluindo backlog
python main.py backlog
```

### Análise e Logs
```bash
# Ver últimas execuções
python main.py logs

# Análise de espaço
python main.py analyze

# Ajuda completa
python main.py help
```

## 🎯 EXEMPLOS PRÁTICOS

### Cenário 1: Extração Diária
```bash
# Todo dia executar:
python main.py today --json
```

### Cenário 2: Extração Semanal  
```bash
# Toda segunda executar:
python main.py daily 7 --json
```

### Cenário 3: Extração Histórica
```bash
# Para recuperar dados de janeiro:
python main.py range 2025-01-01 2025-01-31 --json
```

### Cenário 4: Monitoramento
```bash
# Verificar execuções recentes:
python main.py logs

# Analisar eficiência:
python main.py analyze
```

## 📊 NOVO SISTEMA DE LOG

### Tabela de Log
- 🗃️ Tabela: `servicenow_execution_log`
- 📊 Script: `create_execution_log_table.sql`
- 🔍 Views: `vw_execution_summary`, `vw_recent_executions`

### Métricas Registradas
- ⏱️ **Performance**: Tempo total, tempo de API, tempo de BD
- 📊 **API**: Total de requisições, taxa de sucesso, falhas
- 💾 **Banco**: Transações, registros processados
- 📦 **JSON**: Tamanhos, compressão, economia de espaço
- ❌ **Erros**: Mensagens de erro completas

### Consultas Úteis
```sql
-- Últimas execuções
SELECT * FROM vw_recent_executions;

-- Resumo por tipo
SELECT * FROM vw_execution_summary;

-- Execuções que falharam hoje
SELECT * FROM servicenow_execution_log 
WHERE status = 'failed' 
AND started_at >= CAST(GETDATE() AS DATE);
```

## 🔧 MIGRAÇÃO

### Se você usava antes:
```bash
# ANTES
python main.py custom 2025-09-01 2025-09-15

# AGORA  
python main.py range 2025-09-01 2025-09-15
```

### Comandos que continuam iguais:
- ✅ `python main.py config`
- ✅ `python main.py daily` 
- ✅ `python main.py backlog`
- ✅ `python main.py analyze`

## 💡 DICAS

1. **Use `--json`** para armazenamento paralelo e análise de eficiência
2. **Use `logs`** para monitorar execuções e identificar problemas  
3. **Use `range`** em vez de `custom` (mais intuitivo)
4. **Use `today/yesterday`** para execuções diárias
5. **Use `daily N`** para especificar quantos dias voltar

## ⚠️ IMPORTANTE

- **Formato de datas**: YYYY-MM-DD (ex: 2025-09-15)
- **Período**: Data INÍCIO e FIM sempre obrigatórias no `range`
- **Log**: Cada execução é registrada automaticamente
- **Erros**: Todas as falhas são logadas para análise

---

**Sistema mais intuitivo, completo e profissional! 🎉**