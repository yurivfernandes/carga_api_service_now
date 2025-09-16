-- ================================================================
-- CRIAÇÃO DAS TABELAS DE DADOS DE REFERÊNCIA (MASTER DATA)
-- ================================================================

-- ================================================================
-- 1. TABELA DE EMPRESAS (COMPANIES)
-- ================================================================

IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='sys_company' AND xtype='U')
BEGIN
    CREATE TABLE sys_company (
        sys_id NVARCHAR(32) PRIMARY KEY,
        name NVARCHAR(255) NOT NULL,
        
        -- Campos básicos da empresa
        parent NVARCHAR(32) NULL,
        customer BIT DEFAULT 0,
        vendor BIT DEFAULT 0,
        manufacturer BIT DEFAULT 0,
        
        -- Informações de contato
        phone NVARCHAR(40) NULL,
        fax NVARCHAR(40) NULL,
        website NVARCHAR(1024) NULL,
        
        -- Endereço
        street NVARCHAR(255) NULL,
        city NVARCHAR(40) NULL,
        state NVARCHAR(40) NULL,
        zip NVARCHAR(40) NULL,
        country NVARCHAR(40) NULL,
        
        -- Informações fiscais
        federal_tax_id NVARCHAR(40) NULL,
        
        -- Status
        active BIT DEFAULT 1,
        
        -- Auditoria ServiceNow
        sys_created_on DATETIME2 NULL,
        sys_created_by NVARCHAR(40) NULL,
        sys_updated_on DATETIME2 NULL,
        sys_updated_by NVARCHAR(40) NULL,
        
        -- Controle ETL
        etl_created_at DATETIME2 DEFAULT GETDATE(),
        etl_updated_at DATETIME2 DEFAULT GETDATE(),
        etl_hash NVARCHAR(64) NULL -- Para controle de mudanças
    );
    
    PRINT '✅ Tabela sys_company criada com sucesso!';
END
ELSE
BEGIN
    PRINT 'ℹ️ Tabela sys_company já existe';
END

-- ================================================================
-- 2. TABELA DE USUÁRIOS (USERS)
-- ================================================================

IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='sys_user' AND xtype='U')
BEGIN
    CREATE TABLE sys_user (
        sys_id NVARCHAR(32) PRIMARY KEY,
        user_name NVARCHAR(40) NOT NULL,
        
        -- Nome completo e informações pessoais
        name NVARCHAR(100) NULL,
        first_name NVARCHAR(40) NULL,
        last_name NVARCHAR(40) NULL,
        middle_name NVARCHAR(40) NULL,
        
        -- Contato
        email NVARCHAR(100) NULL,
        phone NVARCHAR(40) NULL,
        mobile_phone NVARCHAR(40) NULL,
        
        -- Informações organizacionais
        company NVARCHAR(32) NULL, -- FK para sys_company
        department NVARCHAR(32) NULL,
        location NVARCHAR(32) NULL,
        manager NVARCHAR(32) NULL,
        title NVARCHAR(40) NULL,
        
        -- Status e configurações
        active BIT DEFAULT 1,
        locked_out BIT DEFAULT 0,
        web_service_access_only BIT DEFAULT 0,
        
        -- Informações de login
        last_login DATETIME2 NULL,
        last_login_time DATETIME2 NULL,
        failed_attempts INT DEFAULT 0,
        
        -- Timezone e localização
        time_zone NVARCHAR(40) NULL,
        date_format NVARCHAR(40) NULL,
        time_format NVARCHAR(40) NULL,
        
        -- Auditoria ServiceNow
        sys_created_on DATETIME2 NULL,
        sys_created_by NVARCHAR(40) NULL,
        sys_updated_on DATETIME2 NULL,
        sys_updated_by NVARCHAR(40) NULL,
        
        -- Controle ETL
        etl_created_at DATETIME2 DEFAULT GETDATE(),
        etl_updated_at DATETIME2 DEFAULT GETDATE(),
        etl_hash NVARCHAR(64) NULL -- Para controle de mudanças
    );
    
    PRINT '✅ Tabela sys_user criada com sucesso!';
END
ELSE
BEGIN
    PRINT 'ℹ️ Tabela sys_user já existe';
END

-- ================================================================
-- 3. TABELA DE DEPARTAMENTOS (OPCIONAL - SE PRECISAR)
-- ================================================================

IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='cmn_department' AND xtype='U')
BEGIN
    CREATE TABLE cmn_department (
        sys_id NVARCHAR(32) PRIMARY KEY,
        name NVARCHAR(40) NOT NULL,
        
        -- Hierarquia
        parent NVARCHAR(32) NULL,
        
        -- Informações organizacionais
        company NVARCHAR(32) NULL, -- FK para sys_company
        dept_head NVARCHAR(32) NULL, -- FK para sys_user
        
        -- Descrição
        description NVARCHAR(1000) NULL,
        
        -- Status
        active BIT DEFAULT 1,
        
        -- Auditoria ServiceNow
        sys_created_on DATETIME2 NULL,
        sys_created_by NVARCHAR(40) NULL,
        sys_updated_on DATETIME2 NULL,
        sys_updated_by NVARCHAR(40) NULL,
        
        -- Controle ETL
        etl_created_at DATETIME2 DEFAULT GETDATE(),
        etl_updated_at DATETIME2 DEFAULT GETDATE(),
        etl_hash NVARCHAR(64) NULL
    );
    
    PRINT '✅ Tabela cmn_department criada com sucesso!';
END
ELSE
BEGIN
    PRINT 'ℹ️ Tabela cmn_department já existe';
END

-- ================================================================
-- 4. ÍNDICES PARA PERFORMANCE
-- ================================================================

-- Índices para sys_company
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_sys_company_name')
BEGIN
    CREATE INDEX IX_sys_company_name ON sys_company (name);
    PRINT '📊 Índice IX_sys_company_name criado';
END

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_sys_company_active')
BEGIN
    CREATE INDEX IX_sys_company_active ON sys_company (active);
    PRINT '📊 Índice IX_sys_company_active criado';
END

-- Índices para sys_user
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_sys_user_username')
BEGIN
    CREATE INDEX IX_sys_user_username ON sys_user (user_name);
    PRINT '📊 Índice IX_sys_user_username criado';
END

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_sys_user_email')
BEGIN
    CREATE INDEX IX_sys_user_email ON sys_user (email);
    PRINT '📊 Índice IX_sys_user_email criado';
END

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_sys_user_company')
BEGIN
    CREATE INDEX IX_sys_user_company ON sys_user (company);
    PRINT '📊 Índice IX_sys_user_company criado';
END

IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_sys_user_active')
BEGIN
    CREATE INDEX IX_sys_user_active ON sys_user (active);
    PRINT '📊 Índice IX_sys_user_active criado';
END

-- Índices para cmn_department
IF NOT EXISTS (SELECT * FROM sys.indexes WHERE name = 'IX_cmn_department_company')
BEGIN
    CREATE INDEX IX_cmn_department_company ON cmn_department (company);
    PRINT '📊 Índice IX_cmn_department_company criado';
END

-- ================================================================
-- 5. FOREIGN KEYS (OPCIONAL - PARA INTEGRIDADE)
-- ================================================================

-- FK sys_user -> sys_company
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_sys_user_company')
BEGIN
    ALTER TABLE sys_user 
    ADD CONSTRAINT FK_sys_user_company 
    FOREIGN KEY (company) REFERENCES sys_company(sys_id);
    PRINT '🔗 FK sys_user -> sys_company criada';
END

-- FK cmn_department -> sys_company  
IF NOT EXISTS (SELECT * FROM sys.foreign_keys WHERE name = 'FK_cmn_department_company')
BEGIN
    ALTER TABLE cmn_department 
    ADD CONSTRAINT FK_cmn_department_company 
    FOREIGN KEY (company) REFERENCES sys_company(sys_id);
    PRINT '🔗 FK cmn_department -> sys_company criada';
END

-- ================================================================
-- 6. VIEWS AUXILIARES
-- ================================================================

-- View de usuários com empresa
IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_users_with_company')
    DROP VIEW vw_users_with_company;

CREATE VIEW vw_users_with_company AS
SELECT 
    u.sys_id,
    u.user_name,
    u.name,
    u.first_name,
    u.last_name,
    u.email,
    u.phone,
    u.active,
    u.company as company_id,
    c.name as company_name,
    u.department,
    u.title,
    u.last_login,
    u.sys_created_on,
    u.etl_updated_at
FROM sys_user u
LEFT JOIN sys_company c ON u.company = c.sys_id;

PRINT '📊 View vw_users_with_company criada';

-- View de empresas com contadores
IF EXISTS (SELECT * FROM sys.views WHERE name = 'vw_companies_with_stats')
    DROP VIEW vw_companies_with_stats;

CREATE VIEW vw_companies_with_stats AS
SELECT 
    c.sys_id,
    c.name,
    c.active,
    c.customer,
    c.vendor,
    c.phone,
    c.website,
    c.city,
    c.state,
    c.country,
    COUNT(u.sys_id) as user_count,
    c.sys_created_on,
    c.etl_updated_at
FROM sys_company c
LEFT JOIN sys_user u ON c.sys_id = u.company
GROUP BY c.sys_id, c.name, c.active, c.customer, c.vendor, 
         c.phone, c.website, c.city, c.state, c.country, 
         c.sys_created_on, c.etl_updated_at;

PRINT '📊 View vw_companies_with_stats criada';

-- ================================================================
-- 7. PROCEDURES PARA MANUTENÇÃO
-- ================================================================

-- Procedure para identificar registros órfãos
IF EXISTS (SELECT * FROM sys.procedures WHERE name = 'sp_check_reference_integrity')
    DROP PROCEDURE sp_check_reference_integrity;

CREATE PROCEDURE sp_check_reference_integrity
AS
BEGIN
    SET NOCOUNT ON;
    
    PRINT '🔍 Verificando integridade referencial...';
    
    -- Usuários sem empresa válida
    SELECT 'Usuários sem empresa válida:' as issue;
    SELECT u.sys_id, u.user_name, u.name, u.company
    FROM sys_user u
    LEFT JOIN sys_company c ON u.company = c.sys_id
    WHERE u.company IS NOT NULL AND c.sys_id IS NULL;
    
    -- Empresas sem usuários
    SELECT 'Empresas sem usuários:' as issue;
    SELECT c.sys_id, c.name, COUNT(u.sys_id) as user_count
    FROM sys_company c
    LEFT JOIN sys_user u ON c.sys_id = u.company
    GROUP BY c.sys_id, c.name
    HAVING COUNT(u.sys_id) = 0;
END;

PRINT '🔍 Procedure sp_check_reference_integrity criada';

-- ================================================================
-- CONSULTAS DE EXEMPLO
-- ================================================================

PRINT '';
PRINT '📋 CONSULTAS DE EXEMPLO:';
PRINT '';
PRINT '-- Usuários ativos com empresas:';
PRINT 'SELECT * FROM vw_users_with_company WHERE active = 1;';
PRINT '';
PRINT '-- Empresas clientes com estatísticas:';
PRINT 'SELECT * FROM vw_companies_with_stats WHERE customer = 1;';
PRINT '';
PRINT '-- Verificar integridade referencial:';
PRINT 'EXEC sp_check_reference_integrity;';
PRINT '';
PRINT '-- Top 10 empresas por número de usuários:';
PRINT 'SELECT TOP 10 * FROM vw_companies_with_stats ORDER BY user_count DESC;';

PRINT '';
PRINT '🎉 Tabelas de dados de referência criadas com sucesso!';
PRINT 'ℹ️ Próximo passo: Executar script para limpar tabela incident e criar extractors específicos.';

MERGE INTO contract_sla AS target USING (SELECT ? AS [schedule_source], ? AS [relative_duration_works_on], ? AS [retroactive_pause], ? AS [set_start_to], ? AS [timezone], ? AS [when_to_cancel], ? AS [sys_updated_on], ? AS [type], ? AS [pause_condition], ? AS [sys_class_name], ? AS [duration], ? AS [sys_id], ? AS [sys_updated_by], ? AS [cancel_condition], ? AS [sys_created_on], ? AS [vendor], ? AS [sys_domain], ? AS [reset_condition], ? AS [resume_condition], ? AS [sys_name], ? AS [reset_action], ? AS [flow], ? AS [sys_created_by], ? AS [stop_condition], ? AS [start_condition], ? AS [schedule_source_field], ? AS [workflow], ? AS [service_commitment], ? AS [sys_mod_count], ? AS [active], ? AS [sys_overrides], ? AS [adv_condition_type], ? AS [collection], ? AS [sys_domain_path], ? AS [sys_tags], ? AS [target], ? AS [schedule], ? AS [sys_update_name], ? AS [timezone_source], ? AS [enable_logging], ? AS [name], ? AS [retroactive], ? AS [when_to_resume], ? AS [sys_policy], ? AS [dv_sys_domain], ? AS [dv_workflow], ? AS [dv_schedule], ? AS [dv_flow]) AS source ON target.[schedule_source] = source.[schedule_source] WHEN MATCHED THEN UPDATE SET target.[relative_duration_works_on] = source.[relative_duration_works_on], target.[retroactive_pause] = source.[retroactive_pause], target.[set_start_to] = source.[set_start_to], target.[timezone] = source.[timezone], target.[when_to_cancel] = source.[when_to_cancel], target.[sys_updated_on] = source.[sys_updated_on], target.[type] = source.[type], target.[pause_condition] = source.[pause_condition], target.[sys_class_name] = source.[sys_class_name], target.[duration] = source.[duration], target.[sys_id] = source.[sys_id], target.[sys_updated_by] = source.[sys_updated_by], target.[cancel_condition] = source.[cancel_condition], target.[sys_created_on] = source.[sys_created_on], target.[vendor] = source.[vendor], target.[sys_domain] = source.[sys_domain], target.[reset_condition] = source.[reset_condition], target.[resume_condition] = source.[resume_condition], target.[sys_name] = source.[sys_name], target.[reset_action] = source.[reset_action], target.[flow] = source.[flow], target.[sys_created_by] = source.[sys_created_by], target.[stop_condition] = source.[stop_condition], target.[start_condition] = source.[start_condition], target.[schedule_source_field] = source.[schedule_source_field], target.[workflow] = source.[workflow], target.[service_commitment] = source.[service_commitment], target.[sys_mod_count] = source.[sys_mod_count], target.[active] = source.[active], target.[sys_overrides] = source.[sys_overrides], target.[adv_condition_type] = source.[adv_condition_type], target.[collection] = source.[collection], target.[sys_domain_path] = source.[sys_domain_path], target.[sys_tags] = source.[sys_tags], target.[target] = source.[target], target.[schedule] = source.[schedule], target.[sys_update_name] = source.[sys_update_name], target.[timezone_source] = source.[timezone_source], target.[enable_logging] = source.[enable_logging], target.[name] = source.[name], target.[retroactive] = source.[retroactive], target.[when_to_resume] = source.[when_to_resume], target.[sys_policy] = source.[sys_policy], target.[dv_sys_domain] = source.[dv_sys_domain], target.[dv_workflow] = source.[dv_workflow], target.[dv_schedule] = source.[dv_schedule], target.[dv_flow] = source.[dv_flow] WHEN NOT MATCHED THEN INSERT ([schedule_source], [relative_duration_works_on], [retroactive_pause], [set_start_to], [timezone], [when_to_cancel], [sys_updated_on], [type], [pause_condition], [sys_class_name], [duration], [sys_id], [sys_updated_by], [cancel_condition], [sys_created_on], [vendor], [sys_domain], [reset_condition], [resume_condition], [sys_name], [reset_action], [flow], [sys_created_by], [stop_condition], [start_condition], [schedule_source_field], [workflow], [service_commitment], [sys_mod_count], [active], [sys_overrides], [adv_condition_type], [collection], [sys_domain_path], [sys_tags], [target], [schedule], [sys_update_name], [timezone_source], [enable_logging], [name], [retroactive], [when_to_resume], [sys_policy], [dv_sys_domain], [dv_workflow], [dv_schedule], [dv_flow]) VALUES (source.[schedule_source], source.[relative_duration_works_on], source.[retroactive_pause], source.[set_start_to], source.[timezone], source.[when_to_cancel], source.[sys_updated_on], source.[type], source.[pause_condition], source.[sys_class_name], source.[duration], source.[sys_id], source.[sys_updated_by], source.[cancel_condition], source.[sys_created_on], source.[vendor], source.[sys_domain], source.[reset_condition], source.[resume_condition], source.[sys_name], source.[reset_action], source.[flow], source.[sys_created_by], source.[stop_condition], source.[start_condition], source.[schedule_source_field], source.[workflow], source.[service_commitment], source.[sys_mod_count], source.[active], source.[sys_overrides], source.[adv_condition_type], source.[collection], source.[sys_domain_path], source.[sys_tags], source.[target], source.[schedule], source.[sys_update_name], source.[timezone_source], source.[enable_logging], source.[name], source.[retroactive], source.[when_to_resume], source.[sys_policy], source.[dv_sys_domain], source.[dv_workflow], source.[dv_schedule], source.[dv_flow]);