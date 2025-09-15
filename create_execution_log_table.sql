-- ================================================================
-- TABELA DE LOG DE EXECUÇÕES ETL SERVICENOW
-- ================================================================

-- Criação da tabela principal de log
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='servicenow_execution_log' AND xtype='U')
BEGIN
    CREATE TABLE servicenow_execution_log (
        id INT IDENTITY(1,1) PRIMARY KEY,
        execution_id UNIQUEIDENTIFIER DEFAULT NEWID(),
        execution_type VARCHAR(20) NOT NULL, -- 'config', 'daily', 'custom', 'backlog', 'analyze'
        start_date DATE NULL,
        end_date DATE NULL,
        started_at DATETIME2 DEFAULT GETDATE(),
        ended_at DATETIME2 NULL,
        duration_seconds DECIMAL(10,2) NULL,
        status VARCHAR(20) NOT NULL, -- 'running', 'success', 'failed', 'partial'
        json_storage_enabled BIT DEFAULT 0,
        
        -- Métricas de API
        total_api_requests INT DEFAULT 0,
        failed_api_requests INT DEFAULT 0,
        total_api_time_seconds DECIMAL(10,2) DEFAULT 0,
        api_success_rate DECIMAL(5,2) DEFAULT 0,
        
        -- Métricas de Banco
        total_db_transactions INT DEFAULT 0,
        total_records_processed INT DEFAULT 0,
        db_time_seconds DECIMAL(10,2) DEFAULT 0,
        
        -- Métricas JSON (se habilitado)
        json_size_kb DECIMAL(12,2) NULL,
        compressed_size_kb DECIMAL(12,2) NULL,
        compression_ratio DECIMAL(5,2) NULL,
        
        -- Detalhes da execução
        tables_processed VARCHAR(500) NULL, -- Lista de tabelas processadas
        error_message NVARCHAR(MAX) NULL,
        records_by_table NVARCHAR(MAX) NULL, -- JSON com contadores por tabela
        
        -- Metadados
        hostname VARCHAR(100) DEFAULT HOST_NAME(),
        username VARCHAR(100) DEFAULT SYSTEM_USER,
        created_at DATETIME2 DEFAULT GETDATE()
    );
    
    PRINT '✅ Tabela servicenow_execution_log criada com sucesso!';
END
ELSE
BEGIN
    PRINT 'ℹ️ Tabela servicenow_execution_log já existe';
END

-- Índices para otimização
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_execution_log_date_type')
BEGIN
    CREATE INDEX IX_execution_log_date_type 
    ON servicenow_execution_log (execution_type, start_date, end_date);
    PRINT '📊 Índice IX_execution_log_date_type criado';
END

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_execution_log_status')
BEGIN
    CREATE INDEX IX_execution_log_status 
    ON servicenow_execution_log (status, started_at DESC);
    PRINT '📊 Índice IX_execution_log_status criado';
END

-- ================================================================
-- VIEWS AUXILIARES PARA ANÁLISE
-- ================================================================

-- View de resumo de execuções
IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_execution_summary')
    DROP VIEW vw_execution_summary;

CREATE VIEW vw_execution_summary AS
SELECT 
    execution_type,
    COUNT(*) as total_executions,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_executions,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_executions,
    AVG(duration_seconds) as avg_duration_seconds,
    AVG(total_records_processed) as avg_records_processed,
    MAX(started_at) as last_execution,
    AVG(CASE WHEN json_storage_enabled = 1 THEN compression_ratio ELSE NULL END) as avg_compression_ratio
FROM servicenow_execution_log
GROUP BY execution_type;

PRINT '📊 View vw_execution_summary criada';

-- View de execuções recentes
IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_recent_executions')
    DROP VIEW vw_recent_executions;

CREATE VIEW vw_recent_executions AS
SELECT TOP 50
    execution_id,
    execution_type,
    CASE 
        WHEN start_date = end_date THEN CONVERT(VARCHAR, start_date, 23)
        WHEN start_date IS NOT NULL AND end_date IS NOT NULL THEN 
            CONVERT(VARCHAR, start_date, 23) + ' até ' + CONVERT(VARCHAR, end_date, 23)
        ELSE 'N/A'
    END as period,
    status,
    duration_seconds,
    total_records_processed,
    CASE WHEN json_storage_enabled = 1 THEN 'Sim' ELSE 'Não' END as json_enabled,
    started_at,
    error_message
FROM servicenow_execution_log
ORDER BY started_at DESC;

PRINT '📊 View vw_recent_executions criada';

-- ================================================================
-- PROCEDURES AUXILIARES
-- ================================================================

-- Procedure para limpar logs antigos (manter últimos 90 dias)
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'sp_cleanup_old_execution_logs')
    DROP PROCEDURE sp_cleanup_old_execution_logs;

CREATE PROCEDURE sp_cleanup_old_execution_logs
    @days_to_keep INT = 90
AS
BEGIN
    SET NOCOUNT ON;
    
    DECLARE @cutoff_date DATETIME2 = DATEADD(DAY, -@days_to_keep, GETDATE());
    DECLARE @deleted_rows INT;
    
    DELETE FROM servicenow_execution_log 
    WHERE started_at < @cutoff_date;
    
    SET @deleted_rows = @@ROWCOUNT;
    
    PRINT CONCAT('🧹 Removidos ', @deleted_rows, ' registros de log antigos (mais de ', @days_to_keep, ' dias)');
END;

PRINT '🧹 Procedure sp_cleanup_old_execution_logs criada';

-- ================================================================
-- CONSULTAS DE EXEMPLO
-- ================================================================

PRINT '';
PRINT '📋 CONSULTAS DE EXEMPLO:';
PRINT '';
PRINT '-- Últimas 10 execuções:';
PRINT 'SELECT * FROM vw_recent_executions;';
PRINT '';
PRINT '-- Resumo por tipo de execução:';
PRINT 'SELECT * FROM vw_execution_summary;';
PRINT '';
PRINT '-- Execuções que falharam hoje:';
PRINT 'SELECT * FROM servicenow_execution_log WHERE status = ''failed'' AND started_at >= CAST(GETDATE() AS DATE);';
PRINT '';
PRINT '-- Performance das últimas execuções:';
PRINT 'SELECT execution_type, AVG(duration_seconds) as avg_duration, AVG(total_api_requests) as avg_requests FROM servicenow_execution_log WHERE started_at >= DATEADD(DAY, -7, GETDATE()) GROUP BY execution_type;';
PRINT '';
PRINT '-- Limpar logs antigos (manter últimos 90 dias):';
PRINT 'EXEC sp_cleanup_old_execution_logs @days_to_keep = 90;';

PRINT '';
PRINT '🎉 Sistema de log de execuções configurado com sucesso!';