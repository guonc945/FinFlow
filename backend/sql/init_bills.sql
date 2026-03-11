-- ================================================
-- Bills Table Initialization Script
-- ================================================

-- Drop existing table
DROP TABLE IF EXISTS bills CASCADE;

-- ================================================
-- 1. Main Table: bills
-- ================================================
CREATE TABLE bills (
    -- Primary key and identifiers
    id BIGINT NOT NULL PRIMARY KEY,
    community_id INTEGER,
    
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
    
    -- Amount info (stored in yuan, converted from fen)
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
    receive_date DATE,
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
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- ================================================
-- 2. Create Indexes
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

-- ================================================
-- 3. Add Comments
-- ================================================

COMMENT ON TABLE bills IS 'Bills main table';
COMMENT ON COLUMN bills.id IS 'Bill unique ID (from Mark system)';
COMMENT ON COLUMN bills.community_id IS 'Community identifier';
COMMENT ON COLUMN bills.bill_month IS 'Bill month as DATE';
COMMENT ON COLUMN bills.in_month IS 'Bill month string (YYYY-MM format)';
COMMENT ON COLUMN bills.amount IS 'Original amount (yuan)';
COMMENT ON COLUMN bills.bill_amount IS 'Bill amount (yuan)';
COMMENT ON COLUMN bills.pay_status IS 'Payment status: 1=pending, 3=paid';
COMMENT ON COLUMN bills.bind_toll IS 'Bound charge rules (JSONB array)';
COMMENT ON COLUMN bills.user_list IS 'Related user list (JSONB array)';

-- ================================================
-- 4. Analyze Table
-- ================================================

ANALYZE bills;
