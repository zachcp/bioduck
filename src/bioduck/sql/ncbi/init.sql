-- Initialize DuckDB extensions for direct data loading
INSTALL httpfs;
LOAD httpfs;

-- Create a settings table for the database
CREATE TABLE IF NOT EXISTS ncbi_settings (
    setting_name VARCHAR PRIMARY KEY,
    setting_value VARCHAR,
    setting_type VARCHAR, -- 'string', 'integer', 'boolean', etc.
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Set basic database configuration
INSERT OR IGNORE INTO ncbi_settings VALUES 
    ('database_version', '1', 'integer', CURRENT_TIMESTAMP);