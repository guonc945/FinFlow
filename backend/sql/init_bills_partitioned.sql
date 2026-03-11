-- ================================================
-- Bills Partitioned Table Initialization Script
-- 账单分区表初始化脚本
-- 分区策略：按园区(community_id)进行LIST分区
-- ================================================

-- Drop existing tables
DROP TABLE IF EXISTS bills_proj_sbwlsl CASCADE;
DROP TABLE IF EXISTS bills_proj_cmysg CASCADE;
DROP TABLE IF EXISTS bills_proj_ztwlsl CASCADE;
DROP TABLE IF EXISTS bills_proj_ztpgxz CASCADE;
DROP TABLE IF EXISTS bills_proj_cxthgc CASCADE;
DROP TABLE IF EXISTS bills_proj_cxyhyx CASCADE;
DROP TABLE IF EXISTS bills_proj_cxwlsl CASCADE;
DROP TABLE IF EXISTS bills_proj_mzgz CASCADE;
DROP TABLE IF EXISTS bills CASCADE;

-- ================================================
-- 1. Main Table: bills (Partitioned by community_id)
-- 主表：按园区ID进行LIST分区
-- ================================================
CREATE TABLE bills (
    -- Primary key and identifiers / 主键和标识符
    id BIGINT NOT NULL,
    community_id INTEGER NOT NULL,  -- 园区ID，分区键
    
    -- Charge item info / 收费项目信息
    charge_item_id INTEGER,
    ci_snapshot_id INTEGER,
    charge_item_name VARCHAR(200),
    charge_item_type SMALLINT,
    category_name VARCHAR(100),
    
    -- Asset info / 资产信息
    asset_id INTEGER,
    asset_name VARCHAR(100),
    asset_type SMALLINT,
    asset_type_str VARCHAR(50),
    
    -- House info / 房屋信息
    house_id INTEGER,
    full_house_name VARCHAR(200),
    bind_house_id INTEGER,
    bind_house_name VARCHAR(200),
    
    -- Parking info / 车位信息
    park_id INTEGER,
    park_name VARCHAR(100),
    
    -- Bill time / 账单时间
    bill_month DATE,
    in_month VARCHAR(10),
    start_time BIGINT,
    end_time BIGINT,
    
    -- Amount info (stored in yuan, converted from fen) / 金额信息（单位：元）
    amount NUMERIC(12,2),
    bill_amount NUMERIC(12,2),
    discount_amount NUMERIC(12,2) DEFAULT 0,
    late_money_amount NUMERIC(12,2) DEFAULT 0,
    deposit_amount NUMERIC(12,2) DEFAULT 0,
    second_pay_amount NUMERIC(12,2) DEFAULT 0,
    
    -- Payment info / 支付信息
    pay_status SMALLINT,
    pay_status_str VARCHAR(20),
    pay_type SMALLINT,
    pay_type_str VARCHAR(50),
    pay_time BIGINT,
    second_pay_channel SMALLINT,
    
    -- Bill type / 账单类型
    bill_type SMALLINT,
    bill_type_str VARCHAR(50),
    
    -- Business references / 业务引用
    deal_log_id BIGINT,
    receipt_id VARCHAR(50),
    sub_mch_id VARCHAR(50),
    sub_mch_name VARCHAR(100),
    
    -- Bad bill and split / 坏账和拆分
    bad_bill_state SMALLINT DEFAULT 0,
    is_bad_bill BOOLEAN DEFAULT FALSE,
    has_split BOOLEAN DEFAULT FALSE,
    split_desc TEXT,
    
    -- Visibility / 可见性
    visible_type SMALLINT DEFAULT 0,
    visible_desc_str VARCHAR(50),
    
    -- Other / 其他
    can_revoke SMALLINT DEFAULT 0,
    version INTEGER DEFAULT 1,
    meter_type SMALLINT DEFAULT 0,
    snapshot_size VARCHAR(50),
    now_size VARCHAR(50),
    remark TEXT,
    
    -- JSONB for nested data / 嵌套数据（JSONB格式）
    bind_toll JSONB,
    user_list JSONB,
    
    -- Timestamps / 时间戳
    create_time BIGINT,
    last_op_time TIMESTAMPTZ,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    
    -- Primary key must include partition key
    -- 主键必须包含分区键
    PRIMARY KEY (id, community_id)
) PARTITION BY LIST (community_id);

-- ================================================
-- 2. Partition Tables by Community / 按园区创建分区表
-- ================================================

-- 园区分区表映射说明：
-- bills_proj_sbwlsl - 商博物流双流
-- bills_proj_cmysg  - 成美·誉尚国
-- bills_proj_ztwlsl - 中通物流双流
-- bills_proj_ztpgxz - 中通·蓬葛·新鑫
-- bills_proj_cxthgc - 诚信通·惠公寓
-- bills_proj_cxyhyx - 诚信银行营销中心
-- bills_proj_cxwlsl - 诚信物流双流
-- bills_proj_mzgz   - 茂臻高值

-- 定义园区ID常量（需根据实际community_id值替换）
-- Community ID mapping (replace with actual IDs):
-- 1 = 商博物流双流 (sbwlsl)
-- 2 = 成美·誉尚国 (cmysg)
-- 3 = 中通物流双流 (ztwlsl)
-- 4 = 中通·蓬葛·新鑫 (ztpgxz)
-- 5 = 诚信通·惠公寓 (cxthgc)
-- 6 = 诚信银行营销中心 (cxyhyx)
-- 7 = 诚信物流双流 (cxwlsl)
-- 8 = 茂臻高值 (mzgz)

-- 分区表1：商博物流双流
CREATE TABLE bills_proj_sbwlsl PARTITION OF bills
    FOR VALUES IN (1);

-- 分区表2：成美·誉尚国
CREATE TABLE bills_proj_cmysg PARTITION OF bills
    FOR VALUES IN (2);

-- 分区表3：中通物流双流
CREATE TABLE bills_proj_ztwlsl PARTITION OF bills
    FOR VALUES IN (3);

-- 分区表4：中通·蓬葛·新鑫
CREATE TABLE bills_proj_ztpgxz PARTITION OF bills
    FOR VALUES IN (4);

-- 分区表5：诚信通·惠公寓
CREATE TABLE bills_proj_cxthgc PARTITION OF bills
    FOR VALUES IN (5);

-- 分区表6：诚信银行营销中心
CREATE TABLE bills_proj_cxyhyx PARTITION OF bills
    FOR VALUES IN (6);

-- 分区表7：诚信物流双流
CREATE TABLE bills_proj_cxwlsl PARTITION OF bills
    FOR VALUES IN (7);

-- 分区表8：茂臻高值
CREATE TABLE bills_proj_mzgz PARTITION OF bills
    FOR VALUES IN (8);

-- ================================================
-- 3. Default Partition for Unmapped Communities
-- 默认分区（用于存放未映射园区的数据）
-- ================================================
CREATE TABLE bills_proj_default PARTITION OF bills
    DEFAULT;

-- ================================================
-- 4. Create Indexes on Main Table
-- 在主表上创建索引（会自动应用到所有分区）
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

-- Composite indexes for common queries / 复合索引（常用查询）
CREATE INDEX idx_bills_community_pay_status ON bills (community_id, pay_status);
CREATE INDEX idx_bills_community_in_month ON bills (community_id, in_month);
CREATE INDEX idx_bills_community_create_time ON bills (community_id, create_time);

-- ================================================
-- 5. Add Comments / 添加注释
-- ================================================

COMMENT ON TABLE bills IS '账单主表（按园区分区）';
COMMENT ON COLUMN bills.id IS '账单唯一ID（来自Mark系统）';
COMMENT ON COLUMN bills.community_id IS '园区ID（分区键）';
COMMENT ON COLUMN bills.charge_item_id IS '收费项目ID';
COMMENT ON COLUMN bills.ci_snapshot_id IS '收费项目快照ID';
COMMENT ON COLUMN bills.charge_item_name IS '收费项目名称';
COMMENT ON COLUMN bills.charge_item_type IS '收费项目类型';
COMMENT ON COLUMN bills.category_name IS '费用类别名称';
COMMENT ON COLUMN bills.asset_id IS '资产ID';
COMMENT ON COLUMN bills.asset_name IS '资产名称';
COMMENT ON COLUMN bills.asset_type IS '资产类型';
COMMENT ON COLUMN bills.asset_type_str IS '资产类型描述';
COMMENT ON COLUMN bills.house_id IS '房屋ID';
COMMENT ON COLUMN bills.full_house_name IS '完整房屋名称';
COMMENT ON COLUMN bills.bind_house_id IS '绑定房屋ID';
COMMENT ON COLUMN bills.bind_house_name IS '绑定房屋名称';
COMMENT ON COLUMN bills.park_id IS '车位ID';
COMMENT ON COLUMN bills.park_name IS '车位名称';
COMMENT ON COLUMN bills.bill_month IS '账单月份（DATE类型）';
COMMENT ON COLUMN bills.in_month IS '账单月份（YYYY-MM格式）';
COMMENT ON COLUMN bills.start_time IS '计费开始时间（时间戳）';
COMMENT ON COLUMN bills.end_time IS '计费结束时间（时间戳）';
COMMENT ON COLUMN bills.amount IS '原始金额（元）';
COMMENT ON COLUMN bills.bill_amount IS '账单金额（元）';
COMMENT ON COLUMN bills.discount_amount IS '优惠金额（元）';
COMMENT ON COLUMN bills.late_money_amount IS '滞纳金（元）';
COMMENT ON COLUMN bills.deposit_amount IS '押金金额（元）';
COMMENT ON COLUMN bills.second_pay_amount IS '二次支付金额（元）';
COMMENT ON COLUMN bills.pay_status IS '支付状态：1=待缴，3=已缴';
COMMENT ON COLUMN bills.pay_status_str IS '支付状态描述';
COMMENT ON COLUMN bills.pay_type IS '支付方式';
COMMENT ON COLUMN bills.pay_type_str IS '支付方式描述';
COMMENT ON COLUMN bills.pay_time IS '支付时间（时间戳）';
COMMENT ON COLUMN bills.second_pay_channel IS '二次支付渠道';
COMMENT ON COLUMN bills.bill_type IS '账单类型';
COMMENT ON COLUMN bills.bill_type_str IS '账单类型描述';
COMMENT ON COLUMN bills.deal_log_id IS '交易日志ID';
COMMENT ON COLUMN bills.receipt_id IS '收据ID';
COMMENT ON COLUMN bills.sub_mch_id IS '子商户ID';
COMMENT ON COLUMN bills.sub_mch_name IS '子商户名称';
COMMENT ON COLUMN bills.bad_bill_state IS '坏账状态';
COMMENT ON COLUMN bills.is_bad_bill IS '是否为坏账';
COMMENT ON COLUMN bills.has_split IS '是否已拆分';
COMMENT ON COLUMN bills.split_desc IS '拆分描述';
COMMENT ON COLUMN bills.visible_type IS '可见类型';
COMMENT ON COLUMN bills.visible_desc_str IS '可见性描述';
COMMENT ON COLUMN bills.can_revoke IS '是否可撤销';
COMMENT ON COLUMN bills.version IS '版本号';
COMMENT ON COLUMN bills.meter_type IS '表计类型';
COMMENT ON COLUMN bills.snapshot_size IS '快照面积';
COMMENT ON COLUMN bills.now_size IS '当前面积';
COMMENT ON COLUMN bills.remark IS '备注';
COMMENT ON COLUMN bills.bind_toll IS '绑定收费规则（JSONB数组）';
COMMENT ON COLUMN bills.user_list IS '关联用户列表（JSONB数组）';
COMMENT ON COLUMN bills.create_time IS '创建时间（时间戳）';
COMMENT ON COLUMN bills.last_op_time IS '最后操作时间';
COMMENT ON COLUMN bills.created_at IS '记录创建时间';
COMMENT ON COLUMN bills.updated_at IS '记录更新时间';

-- 分区表注释
COMMENT ON TABLE bills_proj_sbwlsl IS '账单分区表 - 商博物流双流';
COMMENT ON TABLE bills_proj_cmysg IS '账单分区表 - 成美·誉尚国';
COMMENT ON TABLE bills_proj_ztwlsl IS '账单分区表 - 中通物流双流';
COMMENT ON TABLE bills_proj_ztpgxz IS '账单分区表 - 中通·蓬葛·新鑫';
COMMENT ON TABLE bills_proj_cxthgc IS '账单分区表 - 诚信通·惠公寓';
COMMENT ON TABLE bills_proj_cxyhyx IS '账单分区表 - 诚信银行营销中心';
COMMENT ON TABLE bills_proj_cxwlsl IS '账单分区表 - 诚信物流双流';
COMMENT ON TABLE bills_proj_mzgz IS '账单分区表 - 茂臻高值';
COMMENT ON TABLE bills_proj_default IS '账单分区表 - 默认分区（未映射园区）';

-- ================================================
-- 6. Analyze Tables / 分析表
-- ================================================

ANALYZE bills;

-- ================================================
-- 7. Query Examples / 查询示例
-- ================================================

-- 查询特定园区的所有账单
-- SELECT * FROM bills WHERE community_id = 1;
-- 等同于: SELECT * FROM bills_proj_sbwlsl;

-- 查询多个园区的账单
-- SELECT * FROM bills WHERE community_id IN (1, 2, 3);

-- 跨所有分区查询
-- SELECT * FROM bills WHERE pay_status = 1;

-- 查看分区信息
-- SELECT 
--     parent.relname AS parent_table,
--     child.relname AS partition_name,
--     pg_get_expr(child.relpartbound, child.oid) AS partition_expression
-- FROM pg_inherits
-- JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
-- JOIN pg_class child ON pg_inherits.inhrelid = child.oid
-- WHERE parent.relname = 'bills';
