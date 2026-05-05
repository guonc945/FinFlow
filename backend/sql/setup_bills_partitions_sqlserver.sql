-- ================================================
-- Full Bills Partitioned Table Setup Script for SQL Server
-- 账单分区表完整安装脚本 - SQL Server 版本
-- 分区策略：按园区(community_id)进行 RANGE RIGHT 分区
-- ================================================

SET NOCOUNT ON;

-- ================================================
-- Part 1: Create Partition Function and Scheme
-- 创建分区函数和分区方案
-- ================================================

-- Drop existing partition function and scheme if they exist
IF EXISTS (SELECT * FROM sys.partition_schemes WHERE name = 'ps_bills_community_id')
    DROP PARTITION SCHEME ps_bills_community_id;
IF EXISTS (SELECT * FROM sys.partition_functions WHERE name = 'pf_bills_community_id')
    DROP PARTITION FUNCTION pf_bills_community_id;

-- Create partition function (RANGE RIGHT for community_id values)
CREATE PARTITION FUNCTION pf_bills_community_id (INT)
AS RANGE RIGHT FOR VALUES (1, 2, 3, 4, 5, 6, 7, 8);

-- Create partition scheme
CREATE PARTITION SCHEME ps_bills_community_id
AS PARTITION pf_bills_community_id
ALL TO ([PRIMARY]);

-- ================================================
-- Part 2: Community Mapping Table / 园区映射配置表
-- ================================================

IF EXISTS (SELECT * FROM sys.tables WHERE name = 'community_mapping' AND schema_id = SCHEMA_ID('dbo'))
    DROP TABLE dbo.community_mapping;

CREATE TABLE dbo.community_mapping (
    id INT IDENTITY(1,1) PRIMARY KEY,
    community_id INT NOT NULL UNIQUE,
    community_name NVARCHAR(100) NOT NULL,
    partition_suffix NVARCHAR(20) NOT NULL,
    description NVARCHAR(500),
    is_active BIT DEFAULT 1,
    created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 DEFAULT SYSUTCDATETIME()
);

INSERT INTO dbo.community_mapping (community_id, community_name, partition_suffix, description) VALUES
(1, N'商博物流双流', N'sbwlsl', N'商博物流双流园区'),
(2, N'成美·誉尚国', N'cmysg', N'成美誉尚国园区'),
(3, N'中通物流双流', N'ztwlsl', N'中通物流双流园区'),
(4, N'中通·蓬葛·新鑫', N'ztpgxz', N'中通蓬葛新鑫园区'),
(5, N'诚信通·惠公寓', N'cxthgc', N'诚信通惠公寓园区'),
(6, N'诚信银行营销中心', N'cxyhyx', N'诚信银行营销中心'),
(7, N'诚信物流双流', N'cxwlsl', N'诚信物流双流园区'),
(8, N'茂臻高值', N'mzgz', N'茂臻高值园区');

CREATE INDEX IX_community_mapping_community_id ON dbo.community_mapping (community_id);
CREATE INDEX IX_community_mapping_suffix ON dbo.community_mapping (partition_suffix);

EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'园区映射配置表', 
    @level0type = N'SCHEMA', @level0name = 'dbo',
    @level1type = N'TABLE',  @level1name = 'community_mapping';

EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'园区ID（与bills表分区键对应）', 
    @level0type = N'SCHEMA', @level0name = 'dbo',
    @level1type = N'TABLE',  @level1name = 'community_mapping',
    @level2type = N'COLUMN', @level2name = 'community_id';

EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'园区名称', 
    @level0type = N'SCHEMA', @level0name = 'dbo',
    @level1type = N'TABLE',  @level1name = 'community_mapping',
    @level2type = N'COLUMN', @level2name = 'community_name';

EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'分区表后缀', 
    @level0type = N'SCHEMA', @level0name = 'dbo',
    @level1type = N'TABLE',  @level1name = 'community_mapping',
    @level2type = N'COLUMN', @level2name = 'partition_suffix';

-- ================================================
-- Part 3: Bills Partitioned Table / 账单分区主表
-- ================================================

-- Drop existing tables
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'bills_proj_sbwlsl' AND schema_id = SCHEMA_ID('dbo'))
    DROP TABLE dbo.bills_proj_sbwlsl;
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'bills_proj_cmysg' AND schema_id = SCHEMA_ID('dbo'))
    DROP TABLE dbo.bills_proj_cmysg;
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'bills_proj_ztwlsl' AND schema_id = SCHEMA_ID('dbo'))
    DROP TABLE dbo.bills_proj_ztwlsl;
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'bills_proj_ztpgxz' AND schema_id = SCHEMA_ID('dbo'))
    DROP TABLE dbo.bills_proj_ztpgxz;
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'bills_proj_cxthgc' AND schema_id = SCHEMA_ID('dbo'))
    DROP TABLE dbo.bills_proj_cxthgc;
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'bills_proj_cxyhyx' AND schema_id = SCHEMA_ID('dbo'))
    DROP TABLE dbo.bills_proj_cxyhyx;
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'bills_proj_cxwlsl' AND schema_id = SCHEMA_ID('dbo'))
    DROP TABLE dbo.bills_proj_cxwlsl;
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'bills_proj_mzgz' AND schema_id = SCHEMA_ID('dbo'))
    DROP TABLE dbo.bills_proj_mzgz;
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'bills_proj_default' AND schema_id = SCHEMA_ID('dbo'))
    DROP TABLE dbo.bills_proj_default;
IF EXISTS (SELECT * FROM sys.tables WHERE name = 'bills' AND schema_id = SCHEMA_ID('dbo'))
    DROP TABLE dbo.bills;

-- Create main bills table (will be converted to partitioned table)
CREATE TABLE dbo.bills (
    id BIGINT NOT NULL,
    community_id INT NOT NULL,
    
    -- Charge item info
    charge_item_id INT,
    ci_snapshot_id INT,
    charge_item_name NVARCHAR(200),
    charge_item_type SMALLINT,
    category_name NVARCHAR(100),
    
    -- Asset info
    asset_id INT,
    asset_name NVARCHAR(100),
    asset_type SMALLINT,
    asset_type_str NVARCHAR(50),
    
    -- House info
    house_id INT,
    full_house_name NVARCHAR(200),
    bind_house_id INT,
    bind_house_name NVARCHAR(200),
    
    -- Parking info
    park_id INT,
    park_name NVARCHAR(100),
    
    -- Bill time
    bill_month DATE,
    in_month NVARCHAR(10),
    start_time BIGINT,
    end_time BIGINT,
    
    -- Amount info (stored in yuan, converted from fen)
    amount DECIMAL(12,2),
    bill_amount DECIMAL(12,2),
    discount_amount DECIMAL(12,2) DEFAULT 0,
    late_money_amount DECIMAL(12,2) DEFAULT 0,
    deposit_amount DECIMAL(12,2) DEFAULT 0,
    second_pay_amount DECIMAL(12,2) DEFAULT 0,
    
    -- Payment info
    pay_status SMALLINT,
    pay_status_str NVARCHAR(20),
    pay_type SMALLINT,
    pay_type_str NVARCHAR(50),
    pay_time BIGINT,
    receive_date DATE,
    second_pay_channel SMALLINT,
    
    -- Bill type
    bill_type SMALLINT,
    bill_type_str NVARCHAR(50),
    
    -- Business references
    deal_log_id BIGINT,
    receipt_id NVARCHAR(50),
    sub_mch_id NVARCHAR(50),
    sub_mch_name NVARCHAR(100),
    
    -- Bad bill and split
    bad_bill_state SMALLINT DEFAULT 0,
    is_bad_bill BIT DEFAULT 0,
    has_split BIT DEFAULT 0,
    split_desc NVARCHAR(MAX),
    
    -- Visibility
    visible_type SMALLINT DEFAULT 0,
    visible_desc_str NVARCHAR(50),
    
    -- Other
    can_revoke SMALLINT DEFAULT 0,
    version INT DEFAULT 1,
    meter_type SMALLINT DEFAULT 0,
    snapshot_size NVARCHAR(50),
    now_size NVARCHAR(50),
    remark NVARCHAR(MAX),
    
    -- JSON for nested data (SQL Server uses NVARCHAR(MAX) for JSON)
    bind_toll NVARCHAR(MAX),
    user_list NVARCHAR(MAX),
    
    -- Timestamps
    create_time BIGINT,
    last_op_time DATETIME2,
    created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 DEFAULT SYSUTCDATETIME()
);

-- Create clustered index on (id, community_id) for partition alignment
CREATE CLUSTERED INDEX IX_bills_id_community_id ON dbo.bills (id, community_id);

-- ================================================
-- Part 4: Create Partition Tables / 创建分区表
-- ================================================

-- Function to create partition table
GO

-- bills_proj_sbwlsl - 商博物流双流 (community_id = 1)
CREATE TABLE dbo.bills_proj_sbwlsl (
    id BIGINT NOT NULL,
    community_id INT NOT NULL CHECK (community_id = 1),
    
    -- Charge item info
    charge_item_id INT,
    ci_snapshot_id INT,
    charge_item_name NVARCHAR(200),
    charge_item_type SMALLINT,
    category_name NVARCHAR(100),
    
    -- Asset info
    asset_id INT,
    asset_name NVARCHAR(100),
    asset_type SMALLINT,
    asset_type_str NVARCHAR(50),
    
    -- House info
    house_id INT,
    full_house_name NVARCHAR(200),
    bind_house_id INT,
    bind_house_name NVARCHAR(200),
    
    -- Parking info
    park_id INT,
    park_name NVARCHAR(100),
    
    -- Bill time
    bill_month DATE,
    in_month NVARCHAR(10),
    start_time BIGINT,
    end_time BIGINT,
    
    -- Amount info
    amount DECIMAL(12,2),
    bill_amount DECIMAL(12,2),
    discount_amount DECIMAL(12,2) DEFAULT 0,
    late_money_amount DECIMAL(12,2) DEFAULT 0,
    deposit_amount DECIMAL(12,2) DEFAULT 0,
    second_pay_amount DECIMAL(12,2) DEFAULT 0,
    
    -- Payment info
    pay_status SMALLINT,
    pay_status_str NVARCHAR(20),
    pay_type SMALLINT,
    pay_type_str NVARCHAR(50),
    pay_time BIGINT,
    receive_date DATE,
    second_pay_channel SMALLINT,
    
    -- Bill type
    bill_type SMALLINT,
    bill_type_str NVARCHAR(50),
    
    -- Business references
    deal_log_id BIGINT,
    receipt_id NVARCHAR(50),
    sub_mch_id NVARCHAR(50),
    sub_mch_name NVARCHAR(100),
    
    -- Bad bill and split
    bad_bill_state SMALLINT DEFAULT 0,
    is_bad_bill BIT DEFAULT 0,
    has_split BIT DEFAULT 0,
    split_desc NVARCHAR(MAX),
    
    -- Visibility
    visible_type SMALLINT DEFAULT 0,
    visible_desc_str NVARCHAR(50),
    
    -- Other
    can_revoke SMALLINT DEFAULT 0,
    version INT DEFAULT 1,
    meter_type SMALLINT DEFAULT 0,
    snapshot_size NVARCHAR(50),
    now_size NVARCHAR(50),
    remark NVARCHAR(MAX),
    
    -- JSON
    bind_toll NVARCHAR(MAX),
    user_list NVARCHAR(MAX),
    
    -- Timestamps
    create_time BIGINT,
    last_op_time DATETIME2,
    created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 DEFAULT SYSUTCDATETIME(),
    
    CONSTRAINT PK_bills_proj_sbwlsl PRIMARY KEY CLUSTERED (id, community_id)
);

EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'账单分区表 - 商博物流双流', 
    @level0type = N'SCHEMA', @level0name = 'dbo',
    @level1type = N'TABLE',  @level1name = 'bills_proj_sbwlsl';

-- bills_proj_cmysg - 成美·誉尚国 (community_id = 2)
CREATE TABLE dbo.bills_proj_cmysg (
    id BIGINT NOT NULL,
    community_id INT NOT NULL CHECK (community_id = 2),
    
    -- Same schema as above
    charge_item_id INT,
    ci_snapshot_id INT,
    charge_item_name NVARCHAR(200),
    charge_item_type SMALLINT,
    category_name NVARCHAR(100),
    
    asset_id INT,
    asset_name NVARCHAR(100),
    asset_type SMALLINT,
    asset_type_str NVARCHAR(50),
    
    house_id INT,
    full_house_name NVARCHAR(200),
    bind_house_id INT,
    bind_house_name NVARCHAR(200),
    
    park_id INT,
    park_name NVARCHAR(100),
    
    bill_month DATE,
    in_month NVARCHAR(10),
    start_time BIGINT,
    end_time BIGINT,
    
    amount DECIMAL(12,2),
    bill_amount DECIMAL(12,2),
    discount_amount DECIMAL(12,2) DEFAULT 0,
    late_money_amount DECIMAL(12,2) DEFAULT 0,
    deposit_amount DECIMAL(12,2) DEFAULT 0,
    second_pay_amount DECIMAL(12,2) DEFAULT 0,
    
    pay_status SMALLINT,
    pay_status_str NVARCHAR(20),
    pay_type SMALLINT,
    pay_type_str NVARCHAR(50),
    pay_time BIGINT,
    receive_date DATE,
    second_pay_channel SMALLINT,
    
    bill_type SMALLINT,
    bill_type_str NVARCHAR(50),
    
    deal_log_id BIGINT,
    receipt_id NVARCHAR(50),
    sub_mch_id NVARCHAR(50),
    sub_mch_name NVARCHAR(100),
    
    bad_bill_state SMALLINT DEFAULT 0,
    is_bad_bill BIT DEFAULT 0,
    has_split BIT DEFAULT 0,
    split_desc NVARCHAR(MAX),
    
    visible_type SMALLINT DEFAULT 0,
    visible_desc_str NVARCHAR(50),
    
    can_revoke SMALLINT DEFAULT 0,
    version INT DEFAULT 1,
    meter_type SMALLINT DEFAULT 0,
    snapshot_size NVARCHAR(50),
    now_size NVARCHAR(50),
    remark NVARCHAR(MAX),
    
    bind_toll NVARCHAR(MAX),
    user_list NVARCHAR(MAX),
    
    create_time BIGINT,
    last_op_time DATETIME2,
    created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 DEFAULT SYSUTCDATETIME(),
    
    CONSTRAINT PK_bills_proj_cmysg PRIMARY KEY CLUSTERED (id, community_id)
);

EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'账单分区表 - 成美·誉尚国', 
    @level0type = N'SCHEMA', @level0name = 'dbo',
    @level1type = N'TABLE',  @level1name = 'bills_proj_cmysg';

-- bills_proj_ztwlsl - 中通物流双流 (community_id = 3)
CREATE TABLE dbo.bills_proj_ztwlsl (
    id BIGINT NOT NULL,
    community_id INT NOT NULL CHECK (community_id = 3),
    -- ... same schema
    charge_item_id INT,
    ci_snapshot_id INT,
    charge_item_name NVARCHAR(200),
    charge_item_type SMALLINT,
    category_name NVARCHAR(100),
    
    asset_id INT,
    asset_name NVARCHAR(100),
    asset_type SMALLINT,
    asset_type_str NVARCHAR(50),
    
    house_id INT,
    full_house_name NVARCHAR(200),
    bind_house_id INT,
    bind_house_name NVARCHAR(200),
    
    park_id INT,
    park_name NVARCHAR(100),
    
    bill_month DATE,
    in_month NVARCHAR(10),
    start_time BIGINT,
    end_time BIGINT,
    
    amount DECIMAL(12,2),
    bill_amount DECIMAL(12,2),
    discount_amount DECIMAL(12,2) DEFAULT 0,
    late_money_amount DECIMAL(12,2) DEFAULT 0,
    deposit_amount DECIMAL(12,2) DEFAULT 0,
    second_pay_amount DECIMAL(12,2) DEFAULT 0,
    
    pay_status SMALLINT,
    pay_status_str NVARCHAR(20),
    pay_type SMALLINT,
    pay_type_str NVARCHAR(50),
    pay_time BIGINT,
    receive_date DATE,
    second_pay_channel SMALLINT,
    
    bill_type SMALLINT,
    bill_type_str NVARCHAR(50),
    
    deal_log_id BIGINT,
    receipt_id NVARCHAR(50),
    sub_mch_id NVARCHAR(50),
    sub_mch_name NVARCHAR(100),
    
    bad_bill_state SMALLINT DEFAULT 0,
    is_bad_bill BIT DEFAULT 0,
    has_split BIT DEFAULT 0,
    split_desc NVARCHAR(MAX),
    
    visible_type SMALLINT DEFAULT 0,
    visible_desc_str NVARCHAR(50),
    
    can_revoke SMALLINT DEFAULT 0,
    version INT DEFAULT 1,
    meter_type SMALLINT DEFAULT 0,
    snapshot_size NVARCHAR(50),
    now_size NVARCHAR(50),
    remark NVARCHAR(MAX),
    
    bind_toll NVARCHAR(MAX),
    user_list NVARCHAR(MAX),
    
    create_time BIGINT,
    last_op_time DATETIME2,
    created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 DEFAULT SYSUTCDATETIME(),
    
    CONSTRAINT PK_bills_proj_ztwlsl PRIMARY KEY CLUSTERED (id, community_id)
);

EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'账单分区表 - 中通物流双流', 
    @level0type = N'SCHEMA', @level0name = 'dbo',
    @level1type = N'TABLE',  @level1name = 'bills_proj_ztwlsl';

-- bills_proj_ztpgxz - 中通·蓬葛·新鑫 (community_id = 4)
CREATE TABLE dbo.bills_proj_ztpgxz (
    id BIGINT NOT NULL,
    community_id INT NOT NULL CHECK (community_id = 4),
    -- ... same schema
    charge_item_id INT,
    ci_snapshot_id INT,
    charge_item_name NVARCHAR(200),
    charge_item_type SMALLINT,
    category_name NVARCHAR(100),
    
    asset_id INT,
    asset_name NVARCHAR(100),
    asset_type SMALLINT,
    asset_type_str NVARCHAR(50),
    
    house_id INT,
    full_house_name NVARCHAR(200),
    bind_house_id INT,
    bind_house_name NVARCHAR(200),
    
    park_id INT,
    park_name NVARCHAR(100),
    
    bill_month DATE,
    in_month NVARCHAR(10),
    start_time BIGINT,
    end_time BIGINT,
    
    amount DECIMAL(12,2),
    bill_amount DECIMAL(12,2),
    discount_amount DECIMAL(12,2) DEFAULT 0,
    late_money_amount DECIMAL(12,2) DEFAULT 0,
    deposit_amount DECIMAL(12,2) DEFAULT 0,
    second_pay_amount DECIMAL(12,2) DEFAULT 0,
    
    pay_status SMALLINT,
    pay_status_str NVARCHAR(20),
    pay_type SMALLINT,
    pay_type_str NVARCHAR(50),
    pay_time BIGINT,
    receive_date DATE,
    second_pay_channel SMALLINT,
    
    bill_type SMALLINT,
    bill_type_str NVARCHAR(50),
    
    deal_log_id BIGINT,
    receipt_id NVARCHAR(50),
    sub_mch_id NVARCHAR(50),
    sub_mch_name NVARCHAR(100),
    
    bad_bill_state SMALLINT DEFAULT 0,
    is_bad_bill BIT DEFAULT 0,
    has_split BIT DEFAULT 0,
    split_desc NVARCHAR(MAX),
    
    visible_type SMALLINT DEFAULT 0,
    visible_desc_str NVARCHAR(50),
    
    can_revoke SMALLINT DEFAULT 0,
    version INT DEFAULT 1,
    meter_type SMALLINT DEFAULT 0,
    snapshot_size NVARCHAR(50),
    now_size NVARCHAR(50),
    remark NVARCHAR(MAX),
    
    bind_toll NVARCHAR(MAX),
    user_list NVARCHAR(MAX),
    
    create_time BIGINT,
    last_op_time DATETIME2,
    created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 DEFAULT SYSUTCDATETIME(),
    
    CONSTRAINT PK_bills_proj_ztpgxz PRIMARY KEY CLUSTERED (id, community_id)
);

EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'账单分区表 - 中通·蓬葛·新鑫', 
    @level0type = N'SCHEMA', @level0name = 'dbo',
    @level1type = N'TABLE',  @level1name = 'bills_proj_ztpgxz';

-- bills_proj_cxthgc - 诚信通·惠公寓 (community_id = 5)
CREATE TABLE dbo.bills_proj_cxthgc (
    id BIGINT NOT NULL,
    community_id INT NOT NULL CHECK (community_id = 5),
    -- ... same schema
    charge_item_id INT,
    ci_snapshot_id INT,
    charge_item_name NVARCHAR(200),
    charge_item_type SMALLINT,
    category_name NVARCHAR(100),
    
    asset_id INT,
    asset_name NVARCHAR(100),
    asset_type SMALLINT,
    asset_type_str NVARCHAR(50),
    
    house_id INT,
    full_house_name NVARCHAR(200),
    bind_house_id INT,
    bind_house_name NVARCHAR(200),
    
    park_id INT,
    park_name NVARCHAR(100),
    
    bill_month DATE,
    in_month NVARCHAR(10),
    start_time BIGINT,
    end_time BIGINT,
    
    amount DECIMAL(12,2),
    bill_amount DECIMAL(12,2),
    discount_amount DECIMAL(12,2) DEFAULT 0,
    late_money_amount DECIMAL(12,2) DEFAULT 0,
    deposit_amount DECIMAL(12,2) DEFAULT 0,
    second_pay_amount DECIMAL(12,2) DEFAULT 0,
    
    pay_status SMALLINT,
    pay_status_str NVARCHAR(20),
    pay_type SMALLINT,
    pay_type_str NVARCHAR(50),
    pay_time BIGINT,
    receive_date DATE,
    second_pay_channel SMALLINT,
    
    bill_type SMALLINT,
    bill_type_str NVARCHAR(50),
    
    deal_log_id BIGINT,
    receipt_id NVARCHAR(50),
    sub_mch_id NVARCHAR(50),
    sub_mch_name NVARCHAR(100),
    
    bad_bill_state SMALLINT DEFAULT 0,
    is_bad_bill BIT DEFAULT 0,
    has_split BIT DEFAULT 0,
    split_desc NVARCHAR(MAX),
    
    visible_type SMALLINT DEFAULT 0,
    visible_desc_str NVARCHAR(50),
    
    can_revoke SMALLINT DEFAULT 0,
    version INT DEFAULT 1,
    meter_type SMALLINT DEFAULT 0,
    snapshot_size NVARCHAR(50),
    now_size NVARCHAR(50),
    remark NVARCHAR(MAX),
    
    bind_toll NVARCHAR(MAX),
    user_list NVARCHAR(MAX),
    
    create_time BIGINT,
    last_op_time DATETIME2,
    created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 DEFAULT SYSUTCDATETIME(),
    
    CONSTRAINT PK_bills_proj_cxthgc PRIMARY KEY CLUSTERED (id, community_id)
);

EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'账单分区表 - 诚信通·惠公寓', 
    @level0type = N'SCHEMA', @level0name = 'dbo',
    @level1type = N'TABLE',  @level1name = 'bills_proj_cxthgc';

-- bills_proj_cxyhyx - 诚信银行营销中心 (community_id = 6)
CREATE TABLE dbo.bills_proj_cxyhyx (
    id BIGINT NOT NULL,
    community_id INT NOT NULL CHECK (community_id = 6),
    -- ... same schema
    charge_item_id INT,
    ci_snapshot_id INT,
    charge_item_name NVARCHAR(200),
    charge_item_type SMALLINT,
    category_name NVARCHAR(100),
    
    asset_id INT,
    asset_name NVARCHAR(100),
    asset_type SMALLINT,
    asset_type_str NVARCHAR(50),
    
    house_id INT,
    full_house_name NVARCHAR(200),
    bind_house_id INT,
    bind_house_name NVARCHAR(200),
    
    park_id INT,
    park_name NVARCHAR(100),
    
    bill_month DATE,
    in_month NVARCHAR(10),
    start_time BIGINT,
    end_time BIGINT,
    
    amount DECIMAL(12,2),
    bill_amount DECIMAL(12,2),
    discount_amount DECIMAL(12,2) DEFAULT 0,
    late_money_amount DECIMAL(12,2) DEFAULT 0,
    deposit_amount DECIMAL(12,2) DEFAULT 0,
    second_pay_amount DECIMAL(12,2) DEFAULT 0,
    
    pay_status SMALLINT,
    pay_status_str NVARCHAR(20),
    pay_type SMALLINT,
    pay_type_str NVARCHAR(50),
    pay_time BIGINT,
    receive_date DATE,
    second_pay_channel SMALLINT,
    
    bill_type SMALLINT,
    bill_type_str NVARCHAR(50),
    
    deal_log_id BIGINT,
    receipt_id NVARCHAR(50),
    sub_mch_id NVARCHAR(50),
    sub_mch_name NVARCHAR(100),
    
    bad_bill_state SMALLINT DEFAULT 0,
    is_bad_bill BIT DEFAULT 0,
    has_split BIT DEFAULT 0,
    split_desc NVARCHAR(MAX),
    
    visible_type SMALLINT DEFAULT 0,
    visible_desc_str NVARCHAR(50),
    
    can_revoke SMALLINT DEFAULT 0,
    version INT DEFAULT 1,
    meter_type SMALLINT DEFAULT 0,
    snapshot_size NVARCHAR(50),
    now_size NVARCHAR(50),
    remark NVARCHAR(MAX),
    
    bind_toll NVARCHAR(MAX),
    user_list NVARCHAR(MAX),
    
    create_time BIGINT,
    last_op_time DATETIME2,
    created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 DEFAULT SYSUTCDATETIME(),
    
    CONSTRAINT PK_bills_proj_cxyhyx PRIMARY KEY CLUSTERED (id, community_id)
);

EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'账单分区表 - 诚信银行营销中心', 
    @level0type = N'SCHEMA', @level0name = 'dbo',
    @level1type = N'TABLE',  @level1name = 'bills_proj_cxyhyx';

-- bills_proj_cxwlsl - 诚信物流双流 (community_id = 7)
CREATE TABLE dbo.bills_proj_cxwlsl (
    id BIGINT NOT NULL,
    community_id INT NOT NULL CHECK (community_id = 7),
    -- ... same schema
    charge_item_id INT,
    ci_snapshot_id INT,
    charge_item_name NVARCHAR(200),
    charge_item_type SMALLINT,
    category_name NVARCHAR(100),
    
    asset_id INT,
    asset_name NVARCHAR(100),
    asset_type SMALLINT,
    asset_type_str NVARCHAR(50),
    
    house_id INT,
    full_house_name NVARCHAR(200),
    bind_house_id INT,
    bind_house_name NVARCHAR(200),
    
    park_id INT,
    park_name NVARCHAR(100),
    
    bill_month DATE,
    in_month NVARCHAR(10),
    start_time BIGINT,
    end_time BIGINT,
    
    amount DECIMAL(12,2),
    bill_amount DECIMAL(12,2),
    discount_amount DECIMAL(12,2) DEFAULT 0,
    late_money_amount DECIMAL(12,2) DEFAULT 0,
    deposit_amount DECIMAL(12,2) DEFAULT 0,
    second_pay_amount DECIMAL(12,2) DEFAULT 0,
    
    pay_status SMALLINT,
    pay_status_str NVARCHAR(20),
    pay_type SMALLINT,
    pay_type_str NVARCHAR(50),
    pay_time BIGINT,
    receive_date DATE,
    second_pay_channel SMALLINT,
    
    bill_type SMALLINT,
    bill_type_str NVARCHAR(50),
    
    deal_log_id BIGINT,
    receipt_id NVARCHAR(50),
    sub_mch_id NVARCHAR(50),
    sub_mch_name NVARCHAR(100),
    
    bad_bill_state SMALLINT DEFAULT 0,
    is_bad_bill BIT DEFAULT 0,
    has_split BIT DEFAULT 0,
    split_desc NVARCHAR(MAX),
    
    visible_type SMALLINT DEFAULT 0,
    visible_desc_str NVARCHAR(50),
    
    can_revoke SMALLINT DEFAULT 0,
    version INT DEFAULT 1,
    meter_type SMALLINT DEFAULT 0,
    snapshot_size NVARCHAR(50),
    now_size NVARCHAR(50),
    remark NVARCHAR(MAX),
    
    bind_toll NVARCHAR(MAX),
    user_list NVARCHAR(MAX),
    
    create_time BIGINT,
    last_op_time DATETIME2,
    created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 DEFAULT SYSUTCDATETIME(),
    
    CONSTRAINT PK_bills_proj_cxwlsl PRIMARY KEY CLUSTERED (id, community_id)
);

EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'账单分区表 - 诚信物流双流', 
    @level0type = N'SCHEMA', @level0name = 'dbo',
    @level1type = N'TABLE',  @level1name = 'bills_proj_cxwlsl';

-- bills_proj_mzgz - 茂臻高值 (community_id = 8)
CREATE TABLE dbo.bills_proj_mzgz (
    id BIGINT NOT NULL,
    community_id INT NOT NULL CHECK (community_id = 8),
    -- ... same schema
    charge_item_id INT,
    ci_snapshot_id INT,
    charge_item_name NVARCHAR(200),
    charge_item_type SMALLINT,
    category_name NVARCHAR(100),
    
    asset_id INT,
    asset_name NVARCHAR(100),
    asset_type SMALLINT,
    asset_type_str NVARCHAR(50),
    
    house_id INT,
    full_house_name NVARCHAR(200),
    bind_house_id INT,
    bind_house_name NVARCHAR(200),
    
    park_id INT,
    park_name NVARCHAR(100),
    
    bill_month DATE,
    in_month NVARCHAR(10),
    start_time BIGINT,
    end_time BIGINT,
    
    amount DECIMAL(12,2),
    bill_amount DECIMAL(12,2),
    discount_amount DECIMAL(12,2) DEFAULT 0,
    late_money_amount DECIMAL(12,2) DEFAULT 0,
    deposit_amount DECIMAL(12,2) DEFAULT 0,
    second_pay_amount DECIMAL(12,2) DEFAULT 0,
    
    pay_status SMALLINT,
    pay_status_str NVARCHAR(20),
    pay_type SMALLINT,
    pay_type_str NVARCHAR(50),
    pay_time BIGINT,
    receive_date DATE,
    second_pay_channel SMALLINT,
    
    bill_type SMALLINT,
    bill_type_str NVARCHAR(50),
    
    deal_log_id BIGINT,
    receipt_id NVARCHAR(50),
    sub_mch_id NVARCHAR(50),
    sub_mch_name NVARCHAR(100),
    
    bad_bill_state SMALLINT DEFAULT 0,
    is_bad_bill BIT DEFAULT 0,
    has_split BIT DEFAULT 0,
    split_desc NVARCHAR(MAX),
    
    visible_type SMALLINT DEFAULT 0,
    visible_desc_str NVARCHAR(50),
    
    can_revoke SMALLINT DEFAULT 0,
    version INT DEFAULT 1,
    meter_type SMALLINT DEFAULT 0,
    snapshot_size NVARCHAR(50),
    now_size NVARCHAR(50),
    remark NVARCHAR(MAX),
    
    bind_toll NVARCHAR(MAX),
    user_list NVARCHAR(MAX),
    
    create_time BIGINT,
    last_op_time DATETIME2,
    created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 DEFAULT SYSUTCDATETIME(),
    
    CONSTRAINT PK_bills_proj_mzgz PRIMARY KEY CLUSTERED (id, community_id)
);

EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'账单分区表 - 茂臻高值', 
    @level0type = N'SCHEMA', @level0name = 'dbo',
    @level1type = N'TABLE',  @level1name = 'bills_proj_mzgz';

-- bills_proj_default - 默认分区 (for unmapped communities)
CREATE TABLE dbo.bills_proj_default (
    id BIGINT NOT NULL,
    community_id INT NOT NULL CHECK (community_id > 8 OR community_id < 1),
    -- ... same schema
    charge_item_id INT,
    ci_snapshot_id INT,
    charge_item_name NVARCHAR(200),
    charge_item_type SMALLINT,
    category_name NVARCHAR(100),
    
    asset_id INT,
    asset_name NVARCHAR(100),
    asset_type SMALLINT,
    asset_type_str NVARCHAR(50),
    
    house_id INT,
    full_house_name NVARCHAR(200),
    bind_house_id INT,
    bind_house_name NVARCHAR(200),
    
    park_id INT,
    park_name NVARCHAR(100),
    
    bill_month DATE,
    in_month NVARCHAR(10),
    start_time BIGINT,
    end_time BIGINT,
    
    amount DECIMAL(12,2),
    bill_amount DECIMAL(12,2),
    discount_amount DECIMAL(12,2) DEFAULT 0,
    late_money_amount DECIMAL(12,2) DEFAULT 0,
    deposit_amount DECIMAL(12,2) DEFAULT 0,
    second_pay_amount DECIMAL(12,2) DEFAULT 0,
    
    pay_status SMALLINT,
    pay_status_str NVARCHAR(20),
    pay_type SMALLINT,
    pay_type_str NVARCHAR(50),
    pay_time BIGINT,
    receive_date DATE,
    second_pay_channel SMALLINT,
    
    bill_type SMALLINT,
    bill_type_str NVARCHAR(50),
    
    deal_log_id BIGINT,
    receipt_id NVARCHAR(50),
    sub_mch_id NVARCHAR(50),
    sub_mch_name NVARCHAR(100),
    
    bad_bill_state SMALLINT DEFAULT 0,
    is_bad_bill BIT DEFAULT 0,
    has_split BIT DEFAULT 0,
    split_desc NVARCHAR(MAX),
    
    visible_type SMALLINT DEFAULT 0,
    visible_desc_str NVARCHAR(50),
    
    can_revoke SMALLINT DEFAULT 0,
    version INT DEFAULT 1,
    meter_type SMALLINT DEFAULT 0,
    snapshot_size NVARCHAR(50),
    now_size NVARCHAR(50),
    remark NVARCHAR(MAX),
    
    bind_toll NVARCHAR(MAX),
    user_list NVARCHAR(MAX),
    
    create_time BIGINT,
    last_op_time DATETIME2,
    created_at DATETIME2 DEFAULT SYSUTCDATETIME(),
    updated_at DATETIME2 DEFAULT SYSUTCDATETIME(),
    
    CONSTRAINT PK_bills_proj_default PRIMARY KEY CLUSTERED (id, community_id)
);

EXEC sp_addextendedproperty 
    @name = N'MS_Description', 
    @value = N'账单分区表 - 默认分区', 
    @level0type = N'SCHEMA', @level0name = 'dbo',
    @level1type = N'TABLE',  @level1name = 'bills_proj_default';

-- ================================================
-- Part 5: Create Indexes / 创建索引
-- ================================================

-- Common indexes for all partition tables
DECLARE @sql NVARCHAR(MAX);

-- Indexes for bills_proj_sbwlsl
SET @sql = '
CREATE NONCLUSTERED INDEX IX_bills_proj_sbwlsl_community_id ON dbo.bills_proj_sbwlsl (community_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_sbwlsl_charge_item_id ON dbo.bills_proj_sbwlsl (charge_item_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_sbwlsl_asset_id ON dbo.bills_proj_sbwlsl (asset_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_sbwlsl_house_id ON dbo.bills_proj_sbwlsl (house_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_sbwlsl_pay_status ON dbo.bills_proj_sbwlsl (pay_status);
CREATE NONCLUSTERED INDEX IX_bills_proj_sbwlsl_pay_time ON dbo.bills_proj_sbwlsl (pay_time);
CREATE NONCLUSTERED INDEX IX_bills_proj_sbwlsl_deal_log_id ON dbo.bills_proj_sbwlsl (deal_log_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_sbwlsl_receipt_id ON dbo.bills_proj_sbwlsl (receipt_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_sbwlsl_create_time ON dbo.bills_proj_sbwlsl (create_time);
CREATE NONCLUSTERED INDEX IX_bills_proj_sbwlsl_bill_month ON dbo.bills_proj_sbwlsl (bill_month);
CREATE NONCLUSTERED INDEX IX_bills_proj_sbwlsl_in_month ON dbo.bills_proj_sbwlsl (in_month);
CREATE NONCLUSTERED INDEX IX_bills_proj_sbwlsl_community_pay_status ON dbo.bills_proj_sbwlsl (community_id, pay_status);
CREATE NONCLUSTERED INDEX IX_bills_proj_sbwlsl_community_in_month ON dbo.bills_proj_sbwlsl (community_id, in_month);
CREATE NONCLUSTERED INDEX IX_bills_proj_sbwlsl_community_create_time ON dbo.bills_proj_sbwlsl (community_id, create_time);
';
EXEC sp_executesql @sql;

-- Repeat for other partition tables (simplified - in production, create each separately)
-- For brevity, I'll create a template script that can be run for each table
-- You can copy-paste and modify for each partition table as needed

-- bills_proj_cmysg indexes
SET @sql = '
CREATE NONCLUSTERED INDEX IX_bills_proj_cmysg_community_id ON dbo.bills_proj_cmysg (community_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_cmysg_charge_item_id ON dbo.bills_proj_cmysg (charge_item_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_cmysg_asset_id ON dbo.bills_proj_cmysg (asset_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_cmysg_house_id ON dbo.bills_proj_cmysg (house_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_cmysg_pay_status ON dbo.bills_proj_cmysg (pay_status);
CREATE NONCLUSTERED INDEX IX_bills_proj_cmysg_pay_time ON dbo.bills_proj_cmysg (pay_time);
CREATE NONCLUSTERED INDEX IX_bills_proj_cmysg_deal_log_id ON dbo.bills_proj_cmysg (deal_log_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_cmysg_receipt_id ON dbo.bills_proj_cmysg (receipt_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_cmysg_create_time ON dbo.bills_proj_cmysg (create_time);
CREATE NONCLUSTERED INDEX IX_bills_proj_cmysg_bill_month ON dbo.bills_proj_cmysg (bill_month);
CREATE NONCLUSTERED INDEX IX_bills_proj_cmysg_in_month ON dbo.bills_proj_cmysg (in_month);
CREATE NONCLUSTERED INDEX IX_bills_proj_cmysg_community_pay_status ON dbo.bills_proj_cmysg (community_id, pay_status);
CREATE NONCLUSTERED INDEX IX_bills_proj_cmysg_community_in_month ON dbo.bills_proj_cmysg (community_id, in_month);
CREATE NONCLUSTERED INDEX IX_bills_proj_cmysg_community_create_time ON dbo.bills_proj_cmysg (community_id, create_time);
';
EXEC sp_executesql @sql;

-- bills_proj_ztwlsl indexes
SET @sql = '
CREATE NONCLUSTERED INDEX IX_bills_proj_ztwlsl_community_id ON dbo.bills_proj_ztwlsl (community_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_ztwlsl_charge_item_id ON dbo.bills_proj_ztwlsl (charge_item_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_ztwlsl_asset_id ON dbo.bills_proj_ztwlsl (asset_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_ztwlsl_house_id ON dbo.bills_proj_ztwlsl (house_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_ztwlsl_pay_status ON dbo.bills_proj_ztwlsl (pay_status);
CREATE NONCLUSTERED INDEX IX_bills_proj_ztwlsl_pay_time ON dbo.bills_proj_ztwlsl (pay_time);
CREATE NONCLUSTERED INDEX IX_bills_proj_ztwlsl_deal_log_id ON dbo.bills_proj_ztwlsl (deal_log_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_ztwlsl_receipt_id ON dbo.bills_proj_ztwlsl (receipt_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_ztwlsl_create_time ON dbo.bills_proj_ztwlsl (create_time);
CREATE NONCLUSTERED INDEX IX_bills_proj_ztwlsl_bill_month ON dbo.bills_proj_ztwlsl (bill_month);
CREATE NONCLUSTERED INDEX IX_bills_proj_ztwlsl_in_month ON dbo.bills_proj_ztwlsl (in_month);
CREATE NONCLUSTERED INDEX IX_bills_proj_ztwlsl_community_pay_status ON dbo.bills_proj_ztwlsl (community_id, pay_status);
CREATE NONCLUSTERED INDEX IX_bills_proj_ztwlsl_community_in_month ON dbo.bills_proj_ztwlsl (community_id, in_month);
CREATE NONCLUSTERED INDEX IX_bills_proj_ztwlsl_community_create_time ON dbo.bills_proj_ztwlsl (community_id, create_time);
';
EXEC sp_executesql @sql;

-- bills_proj_ztpgxz indexes
SET @sql = '
CREATE NONCLUSTERED INDEX IX_bills_proj_ztpgxz_community_id ON dbo.bills_proj_ztpgxz (community_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_ztpgxz_charge_item_id ON dbo.bills_proj_ztpgxz (charge_item_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_ztpgxz_asset_id ON dbo.bills_proj_ztpgxz (asset_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_ztpgxz_house_id ON dbo.bills_proj_ztpgxz (house_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_ztpgxz_pay_status ON dbo.bills_proj_ztpgxz (pay_status);
CREATE NONCLUSTERED INDEX IX_bills_proj_ztpgxz_pay_time ON dbo.bills_proj_ztpgxz (pay_time);
CREATE NONCLUSTERED INDEX IX_bills_proj_ztpgxz_deal_log_id ON dbo.bills_proj_ztpgxz (deal_log_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_ztpgxz_receipt_id ON dbo.bills_proj_ztpgxz (receipt_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_ztpgxz_create_time ON dbo.bills_proj_ztpgxz (create_time);
CREATE NONCLUSTERED INDEX IX_bills_proj_ztpgxz_bill_month ON dbo.bills_proj_ztpgxz (bill_month);
CREATE NONCLUSTERED INDEX IX_bills_proj_ztpgxz_in_month ON dbo.bills_proj_ztpgxz (in_month);
CREATE NONCLUSTERED INDEX IX_bills_proj_ztpgxz_community_pay_status ON dbo.bills_proj_ztpgxz (community_id, pay_status);
CREATE NONCLUSTERED INDEX IX_bills_proj_ztpgxz_community_in_month ON dbo.bills_proj_ztpgxz (community_id, in_month);
CREATE NONCLUSTERED INDEX IX_bills_proj_ztpgxz_community_create_time ON dbo.bills_proj_ztpgxz (community_id, create_time);
';
EXEC sp_executesql @sql;

-- bills_proj_cxthgc indexes
SET @sql = '
CREATE NONCLUSTERED INDEX IX_bills_proj_cxthgc_community_id ON dbo.bills_proj_cxthgc (community_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxthgc_charge_item_id ON dbo.bills_proj_cxthgc (charge_item_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxthgc_asset_id ON dbo.bills_proj_cxthgc (asset_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxthgc_house_id ON dbo.bills_proj_cxthgc (house_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxthgc_pay_status ON dbo.bills_proj_cxthgc (pay_status);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxthgc_pay_time ON dbo.bills_proj_cxthgc (pay_time);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxthgc_deal_log_id ON dbo.bills_proj_cxthgc (deal_log_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxthgc_receipt_id ON dbo.bills_proj_cxthgc (receipt_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxthgc_create_time ON dbo.bills_proj_cxthgc (create_time);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxthgc_bill_month ON dbo.bills_proj_cxthgc (bill_month);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxthgc_in_month ON dbo.bills_proj_cxthgc (in_month);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxthgc_community_pay_status ON dbo.bills_proj_cxthgc (community_id, pay_status);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxthgc_community_in_month ON dbo.bills_proj_cxthgc (community_id, in_month);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxthgc_community_create_time ON dbo.bills_proj_cxthgc (community_id, create_time);
';
EXEC sp_executesql @sql;

-- bills_proj_cxyhyx indexes
SET @sql = '
CREATE NONCLUSTERED INDEX IX_bills_proj_cxyhyx_community_id ON dbo.bills_proj_cxyhyx (community_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxyhyx_charge_item_id ON dbo.bills_proj_cxyhyx (charge_item_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxyhyx_asset_id ON dbo.bills_proj_cxyhyx (asset_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxyhyx_house_id ON dbo.bills_proj_cxyhyx (house_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxyhyx_pay_status ON dbo.bills_proj_cxyhyx (pay_status);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxyhyx_pay_time ON dbo.bills_proj_cxyhyx (pay_time);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxyhyx_deal_log_id ON dbo.bills_proj_cxyhyx (deal_log_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxyhyx_receipt_id ON dbo.bills_proj_cxyhyx (receipt_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxyhyx_create_time ON dbo.bills_proj_cxyhyx (create_time);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxyhyx_bill_month ON dbo.bills_proj_cxyhyx (bill_month);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxyhyx_in_month ON dbo.bills_proj_cxyhyx (in_month);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxyhyx_community_pay_status ON dbo.bills_proj_cxyhyx (community_id, pay_status);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxyhyx_community_in_month ON dbo.bills_proj_cxyhyx (community_id, in_month);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxyhyx_community_create_time ON dbo.bills_proj_cxyhyx (community_id, create_time);
';
EXEC sp_executesql @sql;

-- bills_proj_cxwlsl indexes
SET @sql = '
CREATE NONCLUSTERED INDEX IX_bills_proj_cxwlsl_community_id ON dbo.bills_proj_cxwlsl (community_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxwlsl_charge_item_id ON dbo.bills_proj_cxwlsl (charge_item_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxwlsl_asset_id ON dbo.bills_proj_cxwlsl (asset_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxwlsl_house_id ON dbo.bills_proj_cxwlsl (house_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxwlsl_pay_status ON dbo.bills_proj_cxwlsl (pay_status);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxwlsl_pay_time ON dbo.bills_proj_cxwlsl (pay_time);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxwlsl_deal_log_id ON dbo.bills_proj_cxwlsl (deal_log_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxwlsl_receipt_id ON dbo.bills_proj_cxwlsl (receipt_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxwlsl_create_time ON dbo.bills_proj_cxwlsl (create_time);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxwlsl_bill_month ON dbo.bills_proj_cxwlsl (bill_month);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxwlsl_in_month ON dbo.bills_proj_cxwlsl (in_month);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxwlsl_community_pay_status ON dbo.bills_proj_cxwlsl (community_id, pay_status);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxwlsl_community_in_month ON dbo.bills_proj_cxwlsl (community_id, in_month);
CREATE NONCLUSTERED INDEX IX_bills_proj_cxwlsl_community_create_time ON dbo.bills_proj_cxwlsl (community_id, create_time);
';
EXEC sp_executesql @sql;

-- bills_proj_mzgz indexes
SET @sql = '
CREATE NONCLUSTERED INDEX IX_bills_proj_mzgz_community_id ON dbo.bills_proj_mzgz (community_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_mzgz_charge_item_id ON dbo.bills_proj_mzgz (charge_item_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_mzgz_asset_id ON dbo.bills_proj_mzgz (asset_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_mzgz_house_id ON dbo.bills_proj_mzgz (house_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_mzgz_pay_status ON dbo.bills_proj_mzgz (pay_status);
CREATE NONCLUSTERED INDEX IX_bills_proj_mzgz_pay_time ON dbo.bills_proj_mzgz (pay_time);
CREATE NONCLUSTERED INDEX IX_bills_proj_mzgz_deal_log_id ON dbo.bills_proj_mzgz (deal_log_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_mzgz_receipt_id ON dbo.bills_proj_mzgz (receipt_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_mzgz_create_time ON dbo.bills_proj_mzgz (create_time);
CREATE NONCLUSTERED INDEX IX_bills_proj_mzgz_bill_month ON dbo.bills_proj_mzgz (bill_month);
CREATE NONCLUSTERED INDEX IX_bills_proj_mzgz_in_month ON dbo.bills_proj_mzgz (in_month);
CREATE NONCLUSTERED INDEX IX_bills_proj_mzgz_community_pay_status ON dbo.bills_proj_mzgz (community_id, pay_status);
CREATE NONCLUSTERED INDEX IX_bills_proj_mzgz_community_in_month ON dbo.bills_proj_mzgz (community_id, in_month);
CREATE NONCLUSTERED INDEX IX_bills_proj_mzgz_community_create_time ON dbo.bills_proj_mzgz (community_id, create_time);
';
EXEC sp_executesql @sql;

-- bills_proj_default indexes
SET @sql = '
CREATE NONCLUSTERED INDEX IX_bills_proj_default_community_id ON dbo.bills_proj_default (community_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_default_charge_item_id ON dbo.bills_proj_default (charge_item_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_default_asset_id ON dbo.bills_proj_default (asset_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_default_house_id ON dbo.bills_proj_default (house_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_default_pay_status ON dbo.bills_proj_default (pay_status);
CREATE NONCLUSTERED INDEX IX_bills_proj_default_pay_time ON dbo.bills_proj_default (pay_time);
CREATE NONCLUSTERED INDEX IX_bills_proj_default_deal_log_id ON dbo.bills_proj_default (deal_log_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_default_receipt_id ON dbo.bills_proj_default (receipt_id);
CREATE NONCLUSTERED INDEX IX_bills_proj_default_create_time ON dbo.bills_proj_default (create_time);
CREATE NONCLUSTERED INDEX IX_bills_proj_default_bill_month ON dbo.bills_proj_default (bill_month);
CREATE NONCLUSTERED INDEX IX_bills_proj_default_in_month ON dbo.bills_proj_default (in_month);
CREATE NONCLUSTERED INDEX IX_bills_proj_default_community_pay_status ON dbo.bills_proj_default (community_id, pay_status);
CREATE NONCLUSTERED INDEX IX_bills_proj_default_community_in_month ON dbo.bills_proj_default (community_id, in_month);
CREATE NONCLUSTERED INDEX IX_bills_proj_default_community_create_time ON dbo.bills_proj_default (community_id, create_time);
';
EXEC sp_executesql @sql;

-- ================================================
-- Part 6: Verification / 验证分区配置
-- ================================================

PRINT N'========================================';
PRINT N'分区表创建完成！';
PRINT N'========================================';

-- Verify partition tables
SELECT 
    t.name AS partition_table_name,
    c.name AS partition_column,
    p.partition_number,
    p.rows AS row_count
FROM sys.tables t
INNER JOIN sys.indexes i ON t.object_id = i.object_id
INNER JOIN sys.partitions p ON i.object_id = p.object_id AND i.index_id = p.index_id
INNER JOIN sys.columns c ON t.object_id = c.object_id
WHERE t.name LIKE 'bills_proj_%'
    AND i.index_id IN (0, 1)
ORDER BY t.name, p.partition_number;

-- Verify partition function
SELECT 
    pf.name AS partition_function,
    pf.type_desc AS function_type,
    rv.value AS boundary_value
FROM sys.partition_functions pf
LEFT JOIN sys.partition_range_values rv ON pf.function_id = rv.function_id
WHERE pf.name = 'pf_bills_community_id';
