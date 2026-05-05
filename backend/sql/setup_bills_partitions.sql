-- ================================================
-- Full Bills Partitioned Table Setup Script
-- 账单分区表完整安装脚本
-- 执行顺序：1. 园区映射表 2. 主表及分区表
-- ================================================

-- ================================================
-- Part 1: Community Mapping Table / 园区映射配置表
-- ================================================

DROP TABLE IF EXISTS community_mapping CASCADE;

CREATE TABLE community_mapping (
    id SERIAL PRIMARY KEY,
    community_id INTEGER NOT NULL UNIQUE,
    community_name VARCHAR(100) NOT NULL,
    partition_suffix VARCHAR(20) NOT NULL,
    description VARCHAR(500),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO community_mapping (community_id, community_name, partition_suffix, description) VALUES
(1, '商博物流双流', 'sbwlsl', '商博物流双流园区'),
(2, '成美·誉尚国', 'cmysg', '成美誉尚国园区'),
(3, '中通物流双流', 'ztwlsl', '中通物流双流园区'),
(4, '中通·蓬葛·新鑫', 'ztpgxz', '中通蓬葛新鑫园区'),
(5, '诚信通·惠公寓', 'cxthgc', '诚信通惠公寓园区'),
(6, '诚信银行营销中心', 'cxyhyx', '诚信银行营销中心'),
(7, '诚信物流双流', 'cxwlsl', '诚信物流双流园区'),
(8, '茂臻高值', 'mzgz', '茂臻高值园区');

CREATE INDEX idx_community_mapping_community_id ON community_mapping (community_id);
CREATE INDEX idx_community_mapping_suffix ON community_mapping (partition_suffix);

COMMENT ON TABLE community_mapping IS '园区映射配置表';
COMMENT ON COLUMN community_mapping.community_id IS '园区ID（与bills表分区键对应）';
COMMENT ON COLUMN community_mapping.community_name IS '园区名称';
COMMENT ON COLUMN community_mapping.partition_suffix IS '分区表后缀';

-- ================================================
-- Part 2: Bills Partitioned Table / 账单分区主表
-- ================================================

DROP TABLE IF EXISTS bills_proj_sbwlsl CASCADE;
DROP TABLE IF EXISTS bills_proj_cmysg CASCADE;
DROP TABLE IF EXISTS bills_proj_ztwlsl CASCADE;
DROP TABLE IF EXISTS bills_proj_ztpgxz CASCADE;
DROP TABLE IF EXISTS bills_proj_cxthgc CASCADE;
DROP TABLE IF EXISTS bills_proj_cxyhyx CASCADE;
DROP TABLE IF EXISTS bills_proj_cxwlsl CASCADE;
DROP TABLE IF EXISTS bills_proj_mzgz CASCADE;
DROP TABLE IF EXISTS bills_proj_default CASCADE;
DROP TABLE IF EXISTS bills CASCADE;

CREATE TABLE bills (
    -- Primary key and identifiers
    id BIGINT NOT NULL,
    community_id INTEGER NOT NULL,
    
    -- Charge item info
    charge_item_id INTEGER,
    ci_snapshot_id INTEGER,
    charge_item_name VARCHAR(200),
    charge_item_type SMALLINT,
    category_name VARCHAR(100),
    
    -- Asset info
    asset_id INTEGER,
    asset_name VARCHAR(100),
    asset_type SMALLINT,
    asset_type_str VARCHAR(50),
    
    -- House info
    house_id INTEGER,
    full_house_name VARCHAR(200),
    bind_house_id INTEGER,
    bind_house_name VARCHAR(200),
    
    -- Parking info
    park_id INTEGER,
    park_name VARCHAR(100),
    
    -- Bill time
    bill_month DATE,
    in_month VARCHAR(10),
    start_time BIGINT,
    end_time BIGINT,
    
    -- Amount info (yuan)
    amount NUMERIC(12,2),
    bill_amount NUMERIC(12,2),
    discount_amount NUMERIC(12,2) DEFAULT 0,
    late_money_amount NUMERIC(12,2) DEFAULT 0,
    deposit_amount NUMERIC(12,2) DEFAULT 0,
    second_pay_amount NUMERIC(12,2) DEFAULT 0,
    
    -- Payment info
    pay_status SMALLINT,
    pay_status_str VARCHAR(20),
    pay_type SMALLINT,
    pay_type_str VARCHAR(50),
    pay_time BIGINT,
    second_pay_channel SMALLINT,
    
    -- Bill type
    bill_type SMALLINT,
    bill_type_str VARCHAR(50),
    
    -- Business references
    deal_log_id BIGINT,
    receipt_id VARCHAR(50),
    sub_mch_id VARCHAR(50),
    sub_mch_name VARCHAR(100),
    
    -- Bad bill and split
    bad_bill_state SMALLINT DEFAULT 0,
    is_bad_bill BOOLEAN DEFAULT FALSE,
    has_split BOOLEAN DEFAULT FALSE,
    split_desc TEXT,
    
    -- Visibility
    visible_type SMALLINT DEFAULT 0,
    visible_desc_str VARCHAR(50),
    
    -- Other
    can_revoke SMALLINT DEFAULT 0,
    version INTEGER DEFAULT 1,
    meter_type SMALLINT DEFAULT 0,
    snapshot_size VARCHAR(50),
    now_size VARCHAR(50),
    remark TEXT,
    
    -- JSONB for nested data
    bind_toll JSONB,
    user_list JSONB,
    
    -- Timestamps
    create_time BIGINT,
    last_op_time TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    PRIMARY KEY (id, community_id)
) PARTITION BY LIST (community_id);

-- ================================================
-- Part 3: Create Partition Tables / 创建分区表
-- ================================================

-- bills_proj_sbwlsl - 商博物流双流
CREATE TABLE bills_proj_sbwlsl PARTITION OF bills FOR VALUES IN (1);

-- bills_proj_cmysg - 成美·誉尚国
CREATE TABLE bills_proj_cmysg PARTITION OF bills FOR VALUES IN (2);

-- bills_proj_ztwlsl - 中通物流双流
CREATE TABLE bills_proj_ztwlsl PARTITION OF bills FOR VALUES IN (3);

-- bills_proj_ztpgxz - 中通·蓬葛·新鑫
CREATE TABLE bills_proj_ztpgxz PARTITION OF bills FOR VALUES IN (4);

-- bills_proj_cxthgc - 诚信通·惠公寓
CREATE TABLE bills_proj_cxthgc PARTITION OF bills FOR VALUES IN (5);

-- bills_proj_cxyhyx - 诚信银行营销中心
CREATE TABLE bills_proj_cxyhyx PARTITION OF bills FOR VALUES IN (6);

-- bills_proj_cxwlsl - 诚信物流双流
CREATE TABLE bills_proj_cxwlsl PARTITION OF bills FOR VALUES IN (7);

-- bills_proj_mzgz - 茂臻高值
CREATE TABLE bills_proj_mzgz PARTITION OF bills FOR VALUES IN (8);

-- Default partition for unmapped communities
CREATE TABLE bills_proj_default PARTITION OF bills DEFAULT;

-- ================================================
-- Part 4: Create Indexes / 创建索引
-- ================================================

CREATE INDEX idx_bills_community_id ON bills (community_id);
CREATE INDEX idx_bills_charge_item_id ON bills (charge_item_id);
CREATE INDEX idx_bills_asset_id ON bills (asset_id);
CREATE INDEX idx_bills_house_id ON bills (house_id);
CREATE INDEX idx_bills_pay_status ON bills (pay_status);
CREATE INDEX idx_bills_pay_time ON bills (pay_time);
CREATE INDEX idx_bills_deal_log_id ON bills (deal_log_id);
CREATE INDEX idx_bills_receipt_id ON bills (receipt_id);
CREATE INDEX idx_bills_create_time ON bills (create_time);
CREATE INDEX idx_bills_bill_month ON bills (bill_month);
CREATE INDEX idx_bills_in_month ON bills (in_month);

-- Composite indexes for common queries
CREATE INDEX idx_bills_community_pay_status ON bills (community_id, pay_status);
CREATE INDEX idx_bills_community_in_month ON bills (community_id, in_month);
CREATE INDEX idx_bills_community_create_time ON bills (community_id, create_time);

-- ================================================
-- Part 5: Add Comments / 添加注释
-- ================================================

COMMENT ON TABLE bills IS '账单主表（按园区分区）';
COMMENT ON COLUMN bills.id IS '账单唯一ID';
COMMENT ON COLUMN bills.community_id IS '园区ID（分区键）';
COMMENT ON COLUMN bills.amount IS '原始金额（元）';
COMMENT ON COLUMN bills.bill_amount IS '账单金额（元）';
COMMENT ON COLUMN bills.pay_status IS '支付状态：1=待缴，3=已缴';
COMMENT ON COLUMN bills.bind_toll IS '绑定收费规则（JSONB数组）';
COMMENT ON COLUMN bills.user_list IS '关联用户列表（JSONB数组）';

COMMENT ON TABLE bills_proj_sbwlsl IS '账单分区表 - 商博物流双流';
COMMENT ON TABLE bills_proj_cmysg IS '账单分区表 - 成美·誉尚国';
COMMENT ON TABLE bills_proj_ztwlsl IS '账单分区表 - 中通物流双流';
COMMENT ON TABLE bills_proj_ztpgxz IS '账单分区表 - 中通·蓬葛·新鑫';
COMMENT ON TABLE bills_proj_cxthgc IS '账单分区表 - 诚信通·惠公寓';
COMMENT ON TABLE bills_proj_cxyhyx IS '账单分区表 - 诚信银行营销中心';
COMMENT ON TABLE bills_proj_cxwlsl IS '账单分区表 - 诚信物流双流';
COMMENT ON TABLE bills_proj_mzgz IS '账单分区表 - 茂臻高值';
COMMENT ON TABLE bills_proj_default IS '账单分区表 - 默认分区';

-- ================================================
-- Part 6: Analyze Tables / 分析表
-- ================================================

ANALYZE community_mapping;
ANALYZE bills;

-- ================================================
-- Verification: View partition info
-- 验证：查看分区信息
-- ================================================

SELECT 
    parent.relname AS parent_table,
    child.relname AS partition_name,
    pg_get_expr(child.relpartbound, child.oid) AS partition_expression
FROM pg_inherits
JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
JOIN pg_class child ON pg_inherits.inhrelid = child.oid
WHERE parent.relname = 'bills'
ORDER BY child.relname;
