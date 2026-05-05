-- ================================================
-- Community Mapping Table
-- 园区映射配置表
-- ================================================

DROP TABLE IF EXISTS community_mapping CASCADE;

CREATE TABLE community_mapping (
    id SERIAL PRIMARY KEY,
    community_id INTEGER NOT NULL UNIQUE,
    community_name VARCHAR(100) NOT NULL,
    partition_suffix VARCHAR(20) NOT NULL,  -- 分区表后缀
    description VARCHAR(500),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Insert community mappings / 插入园区映射数据
INSERT INTO community_mapping (community_id, community_name, partition_suffix, description) VALUES
(1, '商博物流双流', 'sbwlsl', '商博物流双流园区'),
(2, '成美·誉尚国', 'cmysg', '成美誉尚国园区'),
(3, '中通物流双流', 'ztwlsl', '中通物流双流园区'),
(4, '中通·蓬葛·新鑫', 'ztpgxz', '中通蓬葛新鑫园区'),
(5, '诚信通·惠公寓', 'cxthgc', '诚信通惠公寓园区'),
(6, '诚信银行营销中心', 'cxyhyx', '诚信银行营销中心'),
(7, '诚信物流双流', 'cxwlsl', '诚信物流双流园区'),
(8, '茂臻高值', 'mzgz', '茂臻高值园区');

-- Create index
CREATE INDEX idx_community_mapping_community_id ON community_mapping (community_id);
CREATE INDEX idx_community_mapping_suffix ON community_mapping (partition_suffix);

-- Add comments
COMMENT ON TABLE community_mapping IS '园区映射配置表';
COMMENT ON COLUMN community_mapping.community_id IS '园区ID（与bills表分区键对应）';
COMMENT ON COLUMN community_mapping.community_name IS '园区名称';
COMMENT ON COLUMN community_mapping.partition_suffix IS '分区表后缀（如sbwlsl对应bills_proj_sbwlsl）';
COMMENT ON COLUMN community_mapping.description IS '园区描述';
COMMENT ON COLUMN community_mapping.is_active IS '是否启用';

ANALYZE community_mapping;
