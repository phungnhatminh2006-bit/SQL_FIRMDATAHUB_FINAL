-- =============================
-- 1. CREATE DATABASE
-- =============================
DROP DATABASE vn_firm_panel;
CREATE DATABASE vn_firm_panel;
USE vn_firm_panel;

-- =============================
-- 2. DIM TABLES
-- =============================
-- Exchange
CREATE TABLE dim_exchange (
    exchange_id TINYINT AUTO_INCREMENT PRIMARY KEY,
    exchange_code VARCHAR(10) UNIQUE,
    exchange_name VARCHAR(100)
);

INSERT INTO dim_exchange (exchange_code, exchange_name) VALUES
('HOSE', 'Ho Chi Minh Stock Exchange'),
('HNX', 'Hanoi Stock Exchange');


-- Industry
CREATE TABLE dim_industry_l2 (
    industry_l2_id SMALLINT AUTO_INCREMENT PRIMARY KEY,
    industry_l2_name VARCHAR(150) UNIQUE
);

INSERT INTO dim_industry_l2 (industry_l2_name) VALUES
('Basic Materials'),
('Food & Beverage'),
('Chemicals'),
('Oil & Gas'),
('Industrial Goods & Services'),
('Personal & Household Goods'),
('Construction & Materials'),
('Automobiles & Parts'),
('Healthcare');


-- Data Source
CREATE TABLE dim_data_source (
    source_id SMALLINT AUTO_INCREMENT PRIMARY KEY,
    source_name VARCHAR(100) UNIQUE,
    source_type ENUM('market','financial_statement','ownership','text_report','manual'),
    provider VARCHAR(150),
    note VARCHAR(255)
);

INSERT INTO dim_data_source (source_name, source_type, provider, note) VALUES
('FiinPro','ownership','FiinGroup','Ownership data'),
('BCTC_Audited','financial_statement','Company','Financial statements'),
('Vietstock','market','Vietstock','Market data'),
('AnnualReport','text_report','Company','Innovation & metadata');

-- Firm
CREATE TABLE dim_firm (
    firm_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    ticker VARCHAR(20) UNIQUE NOT NULL,
    company_name VARCHAR(255) NOT NULL,
    exchange_id TINYINT,
    industry_l2_id SMALLINT,
    FOREIGN KEY (exchange_id) REFERENCES dim_exchange(exchange_id),
    FOREIGN KEY (industry_l2_id) REFERENCES dim_industry_l2(industry_l2_id)
);

-- Thay tên 20 firm của nhóm
INSERT INTO dim_firm (ticker, company_name, exchange_id, industry_l2_id) VALUES
-- HOSE = 1, HNX = 2
('ABS','Binh Thuan Argiculture',1,3),
('ADP','A Dong Paint',1,3),
('ADS','Damsan',1,6),
('AAT','Tien Son Thanh Hoa',1,6),
('BAX','Thong Nhat',2,7),
('BRC','Ben Thanh Rubber',1,8),
('BMC','Binh Dinh Minerals',1,7),
('DQC','Dien Quang Group',1,5),
('DHP','Hai Phong Electrical',2,6),
('HVT','Viet Tri Chemical',2,3),
('BCF','Bich Chi Food',2,2),
('GTA','Thuan An Wood',1,7),
('GMX','My Xuan Brick Tile Pottery',2,7),
('HCD','HCD Investment Producing and Trading',1,3),
('LAF','Long An Food Processing Export',1,2),
('LDP','Lam Dong Pharmaceutical',2,9),
('MEL','Me Lin Steel',2,1),
('PLP','Pha Le Plastics Manufacturing and Technology',1,3),
('TCR','Taicera Enterprise Company',1,7),
('DTT','Do Thanh Technology',1,5);


-- =============================
-- 3. SNAPSHOT TABLE
-- =============================
CREATE TABLE fact_data_snapshot (
    snapshot_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    snapshot_date DATE NOT NULL,
    fiscal_year SMALLINT NOT NULL,
    source_id SMALLINT NOT NULL,
    version_tag VARCHAR(50),

    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (source_id) REFERENCES dim_data_source(source_id)
);

INSERT INTO fact_data_snapshot 
(snapshot_date, fiscal_year, source_id, version_tag)
VALUES 
('2020-12-31', 2020, 2, 'v1'),
('2021-12-31', 2021, 2, 'v1'),
('2022-12-31', 2022, 2, 'v1'),
('2023-12-31', 2023, 2, 'v1'),
('2024-12-31', 2024, 2, 'v1');
-- =============================
-- 4. FACT TABLES
-- =============================
-- Ownership
CREATE TABLE fact_ownership_year (
    firm_id BIGINT NOT NULL,
	fiscal_year SMALLINT NOT NULL,
	snapshot_id BIGINT NOT NULL,

	managerial_inside_own DECIMAL(10,6) CHECK (managerial_inside_own BETWEEN 0 AND 1),
	state_own DECIMAL(10,6) CHECK (state_own BETWEEN 0 AND 1),
	institutional_own DECIMAL(10,6) CHECK (institutional_own BETWEEN 0 AND 1),
	foreign_own DECIMAL(10,6) CHECK (foreign_own BETWEEN 0 AND 1),

    PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
    FOREIGN KEY (firm_id) REFERENCES dim_firm(firm_id),
    FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id)
);


-- Financial
DROP TABLE IF EXISTS fact_financial_year;

CREATE TABLE fact_financial_year (
    firm_id BIGINT NOT NULL,
    fiscal_year SMALLINT NOT NULL,
    snapshot_id BIGINT NOT NULL,

    unit_scale VARCHAR(20),
    currency_code VARCHAR(10),
    net_sales DECIMAL(20,2),
    total_assets DECIMAL(20,2),
    selling_expenses DECIMAL(20,2),
    general_admin_expenses DECIMAL(20,2),
    manufacturing_overhead DECIMAL(20,2),
    raw_material_consumption DECIMAL(20,2),
    merchandise_purchase_year DECIMAL(20,2),
    wip_goods_purchase DECIMAL(20,2),
    outside_manufacturing_expenses DECIMAL(20,2),
    production_cost DECIMAL(20,2),
    net_operating_income DECIMAL(20,2),
    rnd_expenses DECIMAL(20,2),
    intangible_assets_net DECIMAL(20,2),
    net_ppe DECIMAL(20,2),
    inventory DECIMAL(20,2),
    total_equity DECIMAL(20,2),
    total_liabilities DECIMAL(20,2),
    cash_and_equivalents DECIMAL(20,2),
    long_term_debt DECIMAL(20,2),
    current_assets DECIMAL(20,2),
    current_liabilities DECIMAL(20,2),
    net_income DECIMAL(20,2),
    growth_ratio DECIMAL(10,6),

    PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
    FOREIGN KEY (firm_id) REFERENCES dim_firm(firm_id),
    FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id)
);

-- Cashflow
CREATE TABLE fact_cashflow_year (
    firm_id BIGINT NOT NULL,
	fiscal_year SMALLINT NOT NULL,
	snapshot_id BIGINT NOT NULL,

    net_cfo DECIMAL(20,2),
    capex DECIMAL(20,2),
    net_cfi DECIMAL(20,2),

    PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
    FOREIGN KEY (firm_id) REFERENCES dim_firm(firm_id),
    FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id)
);


-- Market
CREATE TABLE fact_market_year (
    firm_id BIGINT NOT NULL,
	fiscal_year SMALLINT NOT NULL,
	snapshot_id BIGINT NOT NULL,

    shares_outstanding BIGINT,
    share_price DECIMAL(20,4),
    market_value_equity DECIMAL(20,2),
    dividend_cash_paid DECIMAL(20,2),
    eps_basic DECIMAL(20,6),

    PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
    FOREIGN KEY (firm_id) REFERENCES dim_firm(firm_id),
    FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id)
);


-- Innovation
CREATE TABLE fact_innovation_year (
    firm_id BIGINT NOT NULL,
	fiscal_year SMALLINT NOT NULL,
	snapshot_id BIGINT NOT NULL,

	product_innovation TINYINT CHECK (product_innovation IN (0,1)),
	process_innovation TINYINT CHECK (process_innovation IN (0,1)),
    evidence_source_id SMALLINT NULL,
    evidence_note VARCHAR(500),

    PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
    FOREIGN KEY (firm_id) REFERENCES dim_firm(firm_id),
    FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id),
    FOREIGN KEY (evidence_source_id) REFERENCES dim_data_source(source_id)
);

-- Firm meta
CREATE TABLE fact_firm_year_meta (
    firm_id BIGINT NOT NULL,
	fiscal_year SMALLINT NOT NULL,
	snapshot_id BIGINT NOT NULL,

    employees_count INT,
    firm_age SMALLINT,

    PRIMARY KEY (firm_id, fiscal_year, snapshot_id),
    FOREIGN KEY (firm_id) REFERENCES dim_firm(firm_id),
    FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id)
);

-- =============================
-- 5. AUDIT TABLE
-- =============================
CREATE TABLE fact_value_override_log (
    override_id BIGINT AUTO_INCREMENT PRIMARY KEY,
    firm_id BIGINT,
    fiscal_year SMALLINT,
    snapshot_id BIGINT,
    column_name VARCHAR(80),
    old_value VARCHAR(255),
    new_value VARCHAR(255),
    changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (firm_id) REFERENCES dim_firm(firm_id),
    FOREIGN KEY (snapshot_id) REFERENCES fact_data_snapshot(snapshot_id)
);


-- =============================
-- 6. VIEW
-- =============================
CREATE OR REPLACE VIEW vw_firm_panel_latest AS
SELECT 
    fin.firm_id,
    fin.fiscal_year,
    fin.snapshot_id,
    fin.net_sales,
    fin.total_assets,
    fin.net_income,
    mkt.market_value_equity,
    own.foreign_own

FROM fact_financial_year fin

JOIN (
    SELECT fiscal_year, MAX(snapshot_id) AS max_snap
    FROM fact_data_snapshot
    GROUP BY fiscal_year
) latest
ON fin.fiscal_year = latest.fiscal_year
AND fin.snapshot_id = latest.max_snap

JOIN fact_market_year mkt 
  ON fin.firm_id = mkt.firm_id 
 AND fin.fiscal_year = mkt.fiscal_year 
 AND fin.snapshot_id = mkt.snapshot_id

JOIN fact_ownership_year own
  ON fin.firm_id = own.firm_id 
 AND fin.fiscal_year = own.fiscal_year 
 AND fin.snapshot_id = own.snapshot_id;