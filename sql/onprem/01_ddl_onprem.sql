-- ============================================================
-- P&C Insurance Pipeline — On-Prem SQL DDL
-- Database: PNCINSURANCE
-- Server: Suma_Laptop\SQLEXPRESS
-- ============================================================

USE PNCINSURANCE;
GO

-- ── coverage ─────────────────────────────────────────────
IF OBJECT_ID('dbo.coverage', 'U') IS NOT NULL DROP TABLE dbo.coverage;
GO
CREATE TABLE dbo.coverage (
    coverage_id              VARCHAR(10)    PRIMARY KEY,
    coverage_name            VARCHAR(100)   NOT NULL,
    line_of_business         VARCHAR(20)    NOT NULL,
    coverage_type            VARCHAR(30),
    min_limit                DECIMAL(15,2),
    max_limit                DECIMAL(15,2),
    min_deductible           DECIMAL(15,2),
    max_deductible           DECIMAL(15,2),
    effective_date           DATE,
    is_active                BIT            DEFAULT 1,
    duck_creek_coverage_code VARCHAR(20),
    created_date             DATE           DEFAULT GETDATE(),
    modified_date            DATE           DEFAULT GETDATE()
);
GO

-- ── agent ─────────────────────────────────────────────────
IF OBJECT_ID('dbo.agent', 'U') IS NOT NULL DROP TABLE dbo.agent;
GO
CREATE TABLE dbo.agent (
    agent_id               VARCHAR(10)    PRIMARY KEY,
    agent_first_name       VARCHAR(50)    NOT NULL,
    agent_last_name        VARCHAR(50)    NOT NULL,
    agent_email            VARCHAR(100),
    agent_phone            VARCHAR(30),
    agency_name            VARCHAR(100),
    agency_city            VARCHAR(50),
    agency_state           VARCHAR(2),
    agency_zip             VARCHAR(10),
    license_number         VARCHAR(20),
    licensed_states        VARCHAR(200),
    line_of_business       VARCHAR(50),
    hire_date              DATE,
    termination_date       DATE,
    is_active              BIT            DEFAULT 1,
    ytd_policies_written   INT            DEFAULT 0,
    ytd_premium_written    DECIMAL(15,2)  DEFAULT 0,
    created_date           DATE           DEFAULT GETDATE(),
    modified_date          DATE           DEFAULT GETDATE()
);
GO

-- ── policy ────────────────────────────────────────────────
IF OBJECT_ID('dbo.policy', 'U') IS NOT NULL DROP TABLE dbo.policy;
GO
CREATE TABLE dbo.policy (
    policy_id            VARCHAR(15)    PRIMARY KEY,
    policy_number        VARCHAR(30)    NOT NULL,
    customer_id          VARCHAR(15)    NOT NULL,
    agent_id             VARCHAR(10),
    coverage_id          VARCHAR(10),
    line_of_business     VARCHAR(20),
    policy_status        VARCHAR(20),
    effective_date       DATE,
    expiration_date      DATE,
    policy_term_months   INT,
    annual_premium       DECIMAL(15,2),
    monthly_premium      DECIMAL(15,2),
    deductible           DECIMAL(15,2),
    coverage_limit       DECIMAL(15,2),
    payment_plan         VARCHAR(20),
    cancellation_date    DATE,
    cancellation_reason  VARCHAR(50),
    underwriting_score   VARCHAR(10),
    risk_state           VARCHAR(2),
    created_date         DATE           DEFAULT GETDATE(),
    modified_date        DATE           DEFAULT GETDATE()
);
GO

-- ── claims ────────────────────────────────────────────────
IF OBJECT_ID('dbo.claims', 'U') IS NOT NULL DROP TABLE dbo.claims;
GO
CREATE TABLE dbo.claims (
    claim_id           VARCHAR(15)    PRIMARY KEY,
    claim_number       VARCHAR(25)    NOT NULL,
    policy_id          VARCHAR(15)    NOT NULL,
    loss_date          DATE,
    fnol_date          DATE,
    reported_date      DATE,
    peril_type         VARCHAR(50),
    loss_description   VARCHAR(200),
    claim_status       VARCHAR(20),
    reserve_amount     VARCHAR(20),
    settlement_amount  VARCHAR(20),
    close_date         DATE,
    adjuster_id        VARCHAR(10),
    adjuster_notes     VARCHAR(300),
    subrogation_flag   VARCHAR(1),
    fraud_flag         VARCHAR(1),
    cat_event_name     VARCHAR(50),
    litigation_flag    VARCHAR(1),
    created_date       DATE           DEFAULT GETDATE(),
    modified_date      DATE           DEFAULT GETDATE()
);
GO

-- ── watermark ─────────────────────────────────────────────
IF OBJECT_ID('dbo.pipeline_watermark', 'U') IS NOT NULL DROP TABLE dbo.pipeline_watermark;
GO
CREATE TABLE dbo.pipeline_watermark (
    table_name      VARCHAR(100)   PRIMARY KEY,
    last_load_date  DATETIME       NOT NULL
);
GO

-- ── validation log ────────────────────────────────────────
IF OBJECT_ID('dbo.pipeline_validation_log', 'U') IS NOT NULL DROP TABLE dbo.pipeline_validation_log;
GO
CREATE TABLE dbo.pipeline_validation_log (
    log_id          INT IDENTITY(1,1) PRIMARY KEY,
    pipeline_run_id VARCHAR(100),
    layer           VARCHAR(10),
    table_name      VARCHAR(100),
    check_name      VARCHAR(100),
    check_status    VARCHAR(10),
    expected_value  VARCHAR(100),
    actual_value    VARCHAR(100),
    error_message   VARCHAR(500),
    logged_at       DATETIME DEFAULT GETDATE()
);
GO

PRINT 'On-prem DDL completed successfully';
GO
