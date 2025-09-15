-- Script SQL para criar tabela de dados JSON compactados do ServiceNow
-- Tabela única que armazena todos os dados de um período em formato JSON compactado

IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='servicenow_data_json' AND xtype='U')
BEGIN
    CREATE TABLE servicenow_data_json (
        id BIGINT IDENTITY(1,1) PRIMARY KEY,
        data_extraction DATE NOT NULL,
        extraction_type NVARCHAR(50) NOT NULL, -- 'daily', 'backlog', 'custom'
        start_date DATE NULL, -- Para filtros de período
        end_date DATE NULL,   -- Para filtros de período
        data_json NVARCHAR(MAX) NOT NULL, -- Dados JSON compactados
        data_compressed VARBINARY(MAX) NULL, -- Dados comprimidos com gzip (opcional)
        record_count INT NOT NULL DEFAULT 0, -- Total de registros no JSON
        json_size_kb DECIMAL(10,2) NOT NULL DEFAULT 0, -- Tamanho do JSON em KB
        compressed_size_kb DECIMAL(10,2) NULL, -- Tamanho comprimido em KB
        compression_ratio DECIMAL(5,2) NULL, -- Razão de compressão (se comprimido)
        extraction_duration_seconds DECIMAL(8,2) NULL, -- Tempo de extração
        api_requests_count INT NULL, -- Número de requisições à API
        created_at DATETIME2 NOT NULL DEFAULT GETDATE(),
        updated_at DATETIME2 NULL,
        
        -- Índices para performance
        INDEX IX_servicenow_data_json_date (data_extraction),
        INDEX IX_servicenow_data_json_period (start_date, end_date),
        INDEX IX_servicenow_data_json_type (extraction_type),
        INDEX IX_servicenow_data_json_created (created_at)
    );
    
    PRINT 'Tabela servicenow_data_json criada com sucesso!';
END
ELSE
BEGIN
    PRINT 'Tabela servicenow_data_json já existe.';
END;

-- Comentários da tabela
EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'Tabela para armazenar dados do ServiceNow em formato JSON compactado para análise de eficiência de armazenamento', 
    @level0type = N'SCHEMA', @level0name = N'dbo', 
    @level1type = N'TABLE', @level1name = N'servicenow_data_json';

-- Comentários das colunas principais
EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'Data da extração dos dados', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'servicenow_data_json', @level2type = N'COLUMN', @level2name = N'data_extraction';
EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'Dados JSON compactados contendo incidentes, tarefas, SLAs, etc.', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'servicenow_data_json', @level2type = N'COLUMN', @level2name = N'data_json';
EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'Dados comprimidos com gzip para máxima economia de espaço', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'servicenow_data_json', @level2type = N'COLUMN', @level2name = N'data_compressed';
EXEC sp_addextendedproperty @name = N'MS_Description', @value = N'Total de registros armazenados no JSON', @level0type = N'SCHEMA', @level0name = N'dbo', @level1type = N'TABLE', @level1name = N'servicenow_data_json', @level2type = N'COLUMN', @level2name = N'record_count';

-- View para facilitar consultas
CREATE OR ALTER VIEW vw_servicenow_data_summary AS
SELECT 
    id,
    data_extraction,
    extraction_type,
    start_date,
    end_date,
    record_count,
    json_size_kb,
    compressed_size_kb,
    compression_ratio,
    extraction_duration_seconds,
    api_requests_count,
    CASE 
        WHEN compressed_size_kb IS NOT NULL THEN compressed_size_kb 
        ELSE json_size_kb 
    END as effective_size_kb,
    created_at,
    DATEDIFF(day, created_at, GETDATE()) as days_old
FROM servicenow_data_json;

PRINT 'View vw_servicenow_data_summary criada com sucesso!';

-- Exemplo de query para análise
/*
-- Total de espaço usado por data
SELECT 
    data_extraction,
    SUM(record_count) as total_records,
    SUM(json_size_kb) as total_json_kb,
    SUM(ISNULL(compressed_size_kb, json_size_kb)) as total_effective_kb,
    AVG(compression_ratio) as avg_compression_ratio
FROM servicenow_data_json 
GROUP BY data_extraction
ORDER BY data_extraction DESC;

-- Comparação de eficiência por tipo de extração
SELECT 
    extraction_type,
    COUNT(*) as extractions_count,
    AVG(json_size_kb) as avg_json_size_kb,
    AVG(compressed_size_kb) as avg_compressed_size_kb,
    AVG(compression_ratio) as avg_compression_ratio,
    AVG(record_count) as avg_records_per_extraction
FROM servicenow_data_json 
GROUP BY extraction_type;
*/