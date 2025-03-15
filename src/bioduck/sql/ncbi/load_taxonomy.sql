-- Install and load HTTP extension if not already installed
INSTALL httpfs;
LOAD httpfs;

-- Set up temporary downloads directory 
CREATE OR REPLACE MACRO setup_download_dir() AS
BEGIN
    -- Create a temporary directory for downloads if it doesn't exist
    SELECT '~/.bioduck/tmp' AS tmp_dir;
END;

-- Download taxonomy data
CREATE OR REPLACE PROCEDURE download_taxonomy_data() AS
BEGIN
    -- Get temporary directory
    DECLARE tmp_dir VARCHAR;
    SELECT setup_download_dir() INTO tmp_dir;
    
    -- Define taxonomy file URLs
    DECLARE taxonomy_url VARCHAR := 'https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/new_taxdump/new_taxdump.tar.gz';
    DECLARE output_file VARCHAR := tmp_dir || '/new_taxdump.tar.gz';
    
    -- Show status
    SELECT 'Downloading taxonomy data from ' || taxonomy_url AS status;
    
    -- Create a system table to track downloads
    CREATE TABLE IF NOT EXISTS download_status (
        file_name VARCHAR,
        url VARCHAR,
        status VARCHAR,
        download_time TIMESTAMP
    );
    
    -- Check if we already downloaded and extracted this recently (within 1 day)
    DECLARE recently_downloaded BOOLEAN;
    SELECT COUNT(*) > 0 FROM download_status 
    WHERE file_name = 'rankedlineage.dmp' 
      AND status = 'complete'
      AND download_time > CURRENT_TIMESTAMP - INTERVAL 1 DAY
    INTO recently_downloaded;
    
    -- If not recently downloaded, get the file
    IF NOT recently_downloaded THEN
        -- Download the file using HTTP extension
        COPY (SELECT * FROM 'https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/new_taxdump/new_taxdump.tar.gz' LIMIT 0)
        TO output_file
        (FORMAT 'binary');
        
        -- Extract rankedlineage.dmp from the tarball
        CALL system('mkdir -p ' || tmp_dir || ' && cd ' || tmp_dir || 
                   ' && tar -xzf new_taxdump.tar.gz rankedlineage.dmp');
        
        -- Record the download
        INSERT INTO download_status VALUES 
            ('rankedlineage.dmp', taxonomy_url, 'complete', CURRENT_TIMESTAMP);
    ELSE
        SELECT 'Using cached taxonomy data (downloaded within the last day)' AS status;
    END IF;
END;

-- Execute the download procedure
CALL download_taxonomy_data();

-- Process taxonomy data
CREATE OR REPLACE TABLE raw_taxonomy (line VARCHAR);

-- Get temporary directory path
DECLARE tmp_dir VARCHAR;
SELECT setup_download_dir() INTO tmp_dir;

-- Load the entire file as single lines
COPY raw_taxonomy
FROM
    tmp_dir || '/rankedlineage.dmp' (
        DELIMITER '', -- Read entire lines
        AUTO_DETECT false
    );

DROP TABLE IF EXISTS ncbi_taxonomy;

CREATE TABLE ncbi_taxonomy AS
SELECT
    CAST(SPLIT_PART (line, E'\t|\t', 1) AS INTEGER) as tax_id,
    SPLIT_PART (line, E'\t|\t', 2) as tax_name,
    SPLIT_PART (line, E'\t|\t', 3) as species,
    SPLIT_PART (line, E'\t|\t', 4) as genus,
    SPLIT_PART (line, E'\t|\t', 5) as family,
    SPLIT_PART (line, E'\t|\t', 6) as "order",
    SPLIT_PART (line, E'\t|\t', 7) as class,
    SPLIT_PART (line, E'\t|\t', 8) as phylum,
    SPLIT_PART (line, E'\t|\t', 9) as kingdom,
    REGEXP_REPLACE (SPLIT_PART (line, E'\t|\t', 10), '\t\|$', '') as superkingdom
FROM
    raw_taxonomy;

DROP TABLE IF EXISTS raw_taxonomy;
