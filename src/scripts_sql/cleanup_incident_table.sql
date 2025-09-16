-- ================================================================
-- SCRIPT PARA LIMPAR TABELA DE INCIDENTES
-- Remove campos de dados enriquecidos e mantém apenas IDs de referência
-- ================================================================

-- ================================================================
-- 1. BACKUP DA ESTRUTURA ATUAL (OPCIONAL)
-- ================================================================

-- Criar tabela de backup antes das mudanças
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='incident_backup_before_cleanup' AND xtype='U')
BEGIN
    SELECT * INTO incident_backup_before_cleanup FROM incident WHERE 1=0;
    PRINT '💾 Tabela de backup incident_backup_before_cleanup criada';
    
    -- Copia alguns registros para backup (opcional)
    -- INSERT INTO incident_backup_before_cleanup SELECT TOP 100 * FROM incident;
END

-- ================================================================
-- 2. IDENTIFICAR COLUNAS A SEREM REMOVIDAS
-- ================================================================

PRINT '🔍 Identificando colunas de dados enriquecidos a serem removidas...';

-- Lista colunas que começam com dv_ (dados de valor/derived values)
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'incident' 
AND COLUMN_NAME LIKE 'dv_%'
ORDER BY ORDINAL_POSITION;

-- ================================================================
-- 3. REMOVER COLUNAS DE DADOS ENRIQUECIDOS
-- ================================================================

PRINT '🧹 Removendo colunas de dados enriquecidos da tabela incident...';

-- Remove coluna dv_company se existir
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'incident' AND COLUMN_NAME = 'dv_company')
BEGIN
    ALTER TABLE incident DROP COLUMN dv_company;
    PRINT '✅ Coluna dv_company removida';
END
ELSE
BEGIN
    PRINT 'ℹ️ Coluna dv_company não existe';
END

-- Remove coluna dv_resolved_by se existir
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'incident' AND COLUMN_NAME = 'dv_resolved_by')
BEGIN
    ALTER TABLE incident DROP COLUMN dv_resolved_by;
    PRINT '✅ Coluna dv_resolved_by removida';
END
ELSE
BEGIN
    PRINT 'ℹ️ Coluna dv_resolved_by não existe';
END

-- Remove coluna dv_opened_by se existir  
IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'incident' AND COLUMN_NAME = 'dv_opened_by')
BEGIN
    ALTER TABLE incident DROP COLUMN dv_opened_by;
    PRINT '✅ Coluna dv_opened_by removida';
END
ELSE
BEGIN
    PRINT 'ℹ️ Coluna dv_opened_by não existe';
END

-- Remove outras colunas derivadas se existirem
DECLARE @sql NVARCHAR(MAX) = '';
DECLARE @column_name NVARCHAR(128);

-- Cursor para encontrar todas as colunas dv_
DECLARE column_cursor CURSOR FOR
SELECT COLUMN_NAME
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'incident' 
AND COLUMN_NAME LIKE 'dv_%'
AND COLUMN_NAME NOT IN ('dv_company', 'dv_resolved_by', 'dv_opened_by'); -- Já tratadas acima

OPEN column_cursor;
FETCH NEXT FROM column_cursor INTO @column_name;

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @sql = 'ALTER TABLE incident DROP COLUMN ' + @column_name;
    EXEC sp_executesql @sql;
    PRINT '✅ Coluna ' + @column_name + ' removida';
    
    FETCH NEXT FROM column_cursor INTO @column_name;
END;

CLOSE column_cursor;
DEALLOCATE column_cursor;

-- ================================================================
-- 4. GARANTIR QUE COLUNAS DE ID DE REFERÊNCIA EXISTAM
-- ================================================================

PRINT '🔧 Verificando colunas de ID de referência...';

-- Garantir que company existe como NVARCHAR(32)
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'incident' AND COLUMN_NAME = 'company')
BEGIN
    ALTER TABLE incident ADD company NVARCHAR(32) NULL;
    PRINT '✅ Coluna company (ID) adicionada';
END
ELSE
BEGIN
    PRINT 'ℹ️ Coluna company (ID) já existe';
END

-- Garantir que resolved_by existe como NVARCHAR(32)
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'incident' AND COLUMN_NAME = 'resolved_by')
BEGIN
    ALTER TABLE incident ADD resolved_by NVARCHAR(32) NULL;
    PRINT '✅ Coluna resolved_by (ID) adicionada';
END
ELSE
BEGIN
    PRINT 'ℹ️ Coluna resolved_by (ID) já existe';
END

-- Garantir que opened_by existe como NVARCHAR(32)  
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'incident' AND COLUMN_NAME = 'opened_by')
BEGIN
    ALTER TABLE incident ADD opened_by NVARCHAR(32) NULL;
    PRINT '✅ Coluna opened_by (ID) adicionada';
END
ELSE
BEGIN
    PRINT 'ℹ️ Coluna opened_by (ID) já existe';
END

-- Garantir que caller_id existe como NVARCHAR(32)
IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'incident' AND COLUMN_NAME = 'caller_id')
BEGIN
    ALTER TABLE incident ADD caller_id NVARCHAR(32) NULL;
    PRINT '✅ Coluna caller_id (ID) adicionada';
END
ELSE
BEGIN
    PRINT 'ℹ️ Coluna caller_id (ID) já existe';
END

-- ================================================================
-- 5. CRIAR ÍNDICES PARA PERFORMANCE DAS REFERÊNCIAS
-- ================================================================

PRINT '📊 Criando índices para campos de referência...';

-- Índice para company
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_incident_company')
BEGIN
    CREATE INDEX IX_incident_company ON incident (company);
    PRINT '📊 Índice IX_incident_company criado';
END

-- Índice para resolved_by
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_incident_resolved_by')
BEGIN
    CREATE INDEX IX_incident_resolved_by ON incident (resolved_by);
    PRINT '📊 Índice IX_incident_resolved_by criado';
END

-- Índice para opened_by
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_incident_opened_by')
BEGIN
    CREATE INDEX IX_incident_opened_by ON incident (opened_by);
    PRINT '📊 Índice IX_incident_opened_by criado';
END

-- Índice para caller_id
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_incident_caller_id')
BEGIN
    CREATE INDEX IX_incident_caller_id ON incident (caller_id);
    PRINT '📊 Índice IX_incident_caller_id criado';
END

-- ================================================================
-- 6. CRIAR VIEW PARA DADOS ENRIQUECIDOS (SUBSTITUI OS CAMPOS REMOVIDOS)
-- ================================================================

PRINT '📊 Criando view para dados enriquecidos...';

-- View principal com todos os dados enriquecidos
IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_incident_enriched')
    DROP VIEW vw_incident_enriched;

CREATE VIEW vw_incident_enriched AS
SELECT 
    i.*,
    
    -- Dados da empresa
    c.name as company_name,
    c.phone as company_phone,
    c.city as company_city,
    c.state as company_state,
    c.country as company_country,
    
    -- Dados do usuário que resolveu
    u_resolved.user_name as resolved_by_username,
    u_resolved.name as resolved_by_name,
    u_resolved.email as resolved_by_email,
    
    -- Dados do usuário que abriu
    u_opened.user_name as opened_by_username,
    u_opened.name as opened_by_name,
    u_opened.email as opened_by_email,
    
    -- Dados do solicitante
    u_caller.user_name as caller_username,
    u_caller.name as caller_name,
    u_caller.email as caller_email
    
FROM incident i
LEFT JOIN sys_company c ON i.company = c.sys_id
LEFT JOIN sys_user u_resolved ON i.resolved_by = u_resolved.sys_id
LEFT JOIN sys_user u_opened ON i.opened_by = u_opened.sys_id
LEFT JOIN sys_user u_caller ON i.caller_id = u_caller.sys_id;

PRINT '📊 View vw_incident_enriched criada com sucesso';

-- View resumida para relatórios rápidos
IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_incident_summary')
    DROP VIEW vw_incident_summary;

CREATE VIEW vw_incident_summary AS
SELECT 
    i.sys_id,
    i.number,
    i.short_description,
    i.state,
    i.priority,
    i.urgency,
    i.impact,
    i.opened_at,
    i.closed_at,
    
    -- Apenas nomes para relatórios
    c.name as company_name,
    u_resolved.name as resolved_by_name,
    u_opened.name as opened_by_name,
    u_caller.name as caller_name
    
FROM incident i
LEFT JOIN sys_company c ON i.company = c.sys_id
LEFT JOIN sys_user u_resolved ON i.resolved_by = u_resolved.sys_id
LEFT JOIN sys_user u_opened ON i.opened_by = u_opened.sys_id  
LEFT JOIN sys_user u_caller ON i.caller_id = u_caller.sys_id;

PRINT '📊 View vw_incident_summary criada com sucesso';

-- ================================================================
-- 7. VERIFICAÇÕES FINAIS
-- ================================================================

PRINT '';
PRINT '🔍 VERIFICAÇÕES FINAIS:';
PRINT '';

-- Contar registros
DECLARE @record_count INT;
SELECT @record_count = COUNT(*) FROM incident;
PRINT 'Total de incidentes na tabela: ' + CAST(@record_count AS VARCHAR);

-- Verificar estrutura final
PRINT '';
PRINT 'Estrutura final da tabela incident (campos de referência):';
SELECT 
    COLUMN_NAME,
    DATA_TYPE,
    CHARACTER_MAXIMUM_LENGTH,
    IS_NULLABLE
FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'incident' 
AND COLUMN_NAME IN ('company', 'resolved_by', 'opened_by', 'caller_id')
ORDER BY ORDINAL_POSITION;

-- Verificar se há dados órfãos
PRINT '';
PRINT 'Verificando referências órfãs...';

SELECT 'Incidentes com company_id não encontrada:' as issue;
SELECT COUNT(*) as count
FROM incident i
LEFT JOIN sys_company c ON i.company = c.sys_id
WHERE i.company IS NOT NULL AND c.sys_id IS NULL;

-- ================================================================
-- CONSULTAS DE EXEMPLO
-- ================================================================

PRINT '';
PRINT '📋 CONSULTAS DE EXEMPLO PARA A NOVA ESTRUTURA:';
PRINT '';
PRINT '-- Incidentes com dados completos (usando view):';
PRINT 'SELECT TOP 10 * FROM vw_incident_enriched;';
PRINT '';
PRINT '-- Resumo para relatórios:';
PRINT 'SELECT * FROM vw_incident_summary WHERE state = ''6'';';
PRINT '';
PRINT '-- Incidentes por empresa (usando ID):';
PRINT 'SELECT company, COUNT(*) as incident_count FROM incident GROUP BY company;';
PRINT '';
PRINT '-- Performance melhorada com índices:';
PRINT 'SELECT * FROM incident WHERE company = ''12345'' AND resolved_by IS NOT NULL;';

PRINT '';
PRINT '🎉 Limpeza da tabela incident concluída com sucesso!';
PRINT 'ℹ️ Estrutura otimizada: IDs de referência + Views para dados enriquecidos';
PRINT 'ℹ️ Próximo passo: Criar extractors específicos para sys_user e sys_company';