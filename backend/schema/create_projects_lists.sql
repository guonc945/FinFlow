CREATE TABLE projects_lists (
    proj_id SERIAL PRIMARY KEY,
    proj_name VARCHAR(100) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_projects_lists_name ON projects_lists(proj_name);
