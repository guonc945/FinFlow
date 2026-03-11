-- Create organizations table
CREATE TABLE IF NOT EXISTS organizations (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    code VARCHAR(50) UNIQUE,
    parent_id INTEGER REFERENCES organizations(id),
    level INTEGER DEFAULT 1,
    sort_order INTEGER DEFAULT 0,
    status INTEGER DEFAULT 1,
    description VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_organizations_code ON organizations(code);
CREATE INDEX IF NOT EXISTS idx_organizations_parent ON organizations(parent_id);

-- Create users table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE,
    phone VARCHAR(20),
    real_name VARCHAR(50),
    password_hash VARCHAR(255) NOT NULL,
    avatar VARCHAR(500),
    org_id INTEGER REFERENCES organizations(id),
    status INTEGER DEFAULT 1,
    last_login TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_users_username ON users(username);
CREATE INDEX IF NOT EXISTS idx_users_email ON users(email);
CREATE INDEX IF NOT EXISTS idx_users_org ON users(org_id);

-- Insert sample data
INSERT INTO organizations (name, code, parent_id, level, sort_order, description) VALUES
    ('总公司', 'ORG001', NULL, 1, 1, '集团总部'),
    ('技术部', 'ORG002', 1, 2, 1, '技术研发部门'),
    ('财务部', 'ORG003', 1, 2, 2, '财务管理部门'),
    ('人事部', 'ORG004', 1, 2, 3, '人力资源部门'),
    ('前端组', 'ORG005', 2, 3, 1, '前端开发小组'),
    ('后端组', 'ORG006', 2, 3, 2, '后端开发小组')
ON CONFLICT DO NOTHING;

-- Insert default admin user (password: admin123)
INSERT INTO users (username, email, real_name, password_hash, org_id, status) VALUES
    ('admin', 'admin@finflow.com', '系统管理员', '240be518fabd2724ddb6f04eeb9d5b32586c86cbc7be44e2a5f7e6bc51b3d94e', 1, 1)
ON CONFLICT DO NOTHING;
