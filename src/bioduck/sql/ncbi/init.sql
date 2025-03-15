-- Initialize DuckDB extensions and setup functions for NCBI database
INSTALL httpfs;
LOAD httpfs;

-- Setup utility functions and macros
CREATE OR REPLACE MACRO setup_download_dir() AS
BEGIN
    -- Expand user home directory
    DECLARE home_dir VARCHAR;
    SELECT CASE 
        WHEN CURRENT_SETTING('platform') = 'windows' THEN '%USERPROFILE%/.bioduck/tmp'
        ELSE '~/.bioduck/tmp'
    END INTO home_dir;
    
    -- Create the directory if it doesn't exist
    CALL system('mkdir -p ' || home_dir);
    
    -- Return the directory path
    SELECT home_dir;
END;

-- Create a table to track downloads
CREATE TABLE IF NOT EXISTS download_status (
    file_name VARCHAR,
    url VARCHAR,
    status VARCHAR,
    download_time TIMESTAMP,
    file_size BIGINT
);

-- Function to check if a file needs downloading or updating
CREATE OR REPLACE FUNCTION needs_download(file_name VARCHAR, max_age_days INTEGER DEFAULT 1) AS
BEGIN
    DECLARE needs_dl BOOLEAN;
    SELECT COUNT(*) = 0 FROM download_status 
    WHERE download_status.file_name = file_name
      AND status = 'complete'
      AND download_time > CURRENT_TIMESTAMP - INTERVAL max_age_days DAY
    INTO needs_dl;
    RETURN needs_dl;
END;

-- Create a settings table for the database
CREATE TABLE IF NOT EXISTS ncbi_settings (
    setting_name VARCHAR PRIMARY KEY,
    setting_value VARCHAR,
    setting_type VARCHAR, -- 'string', 'integer', 'boolean', etc.
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Set a useful default for cache TTL
INSERT OR IGNORE INTO ncbi_settings VALUES 
    ('download_cache_days', '1', 'integer', CURRENT_TIMESTAMP),
    ('default_download_dir', '~/.bioduck/tmp', 'string', CURRENT_TIMESTAMP),
    ('database_version', '1', 'integer', CURRENT_TIMESTAMP);