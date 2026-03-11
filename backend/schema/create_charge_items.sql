CREATE TABLE charge_items (
    item_id SERIAL PRIMARY KEY,
    communityID VARCHAR(20) NOT NULL,
    item_name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_charge_items_community ON charge_items(communityID);
CREATE INDEX idx_charge_items_name ON charge_items(item_name);
