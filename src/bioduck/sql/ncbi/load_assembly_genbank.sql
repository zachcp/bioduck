-- Make sure HTTP extension is loaded
INSTALL httpfs;
LOAD httpfs;

-- Download GenBank assembly summary
CREATE OR REPLACE PROCEDURE download_genbank_assembly() AS
BEGIN
    -- Get temporary directory
    DECLARE tmp_dir VARCHAR;
    SELECT setup_download_dir() INTO tmp_dir;
    
    -- Define file URL
    DECLARE genbank_url VARCHAR := 'https://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/assembly_summary_genbank.txt';
    DECLARE output_file VARCHAR := tmp_dir || '/assembly_summary_genbank.txt';
    
    -- Show status
    SELECT 'Downloading GenBank assembly data from ' || genbank_url AS status;
    
    -- Create a system table to track downloads if it doesn't exist
    CREATE TABLE IF NOT EXISTS download_status (
        file_name VARCHAR,
        url VARCHAR,
        status VARCHAR,
        download_time TIMESTAMP
    );
    
    -- Check if we already downloaded this recently (within 1 day)
    DECLARE recently_downloaded BOOLEAN;
    SELECT COUNT(*) > 0 FROM download_status 
    WHERE file_name = 'assembly_summary_genbank.txt' 
      AND status = 'complete'
      AND download_time > CURRENT_TIMESTAMP - INTERVAL 1 DAY
    INTO recently_downloaded;
    
    -- If not recently downloaded, get the file
    IF NOT recently_downloaded THEN
        -- Create directory if it doesn't exist
        CALL system('mkdir -p ' || tmp_dir);
        
        -- Download the file using HTTP extension
        COPY (SELECT * FROM genbank_url)
        TO output_file;
        
        -- Record the download
        DELETE FROM download_status WHERE file_name = 'assembly_summary_genbank.txt';
        INSERT INTO download_status VALUES 
            ('assembly_summary_genbank.txt', genbank_url, 'complete', CURRENT_TIMESTAMP);
    ELSE
        SELECT 'Using cached GenBank assembly data (downloaded within the last day)' AS status;
    END IF;
END;

-- Execute the download procedure
CALL download_genbank_assembly();

-- Process assembly data
DROP TABLE IF EXISTS assembly_summary_genbank;

-- Get temporary directory path
DECLARE tmp_dir VARCHAR;
SELECT setup_download_dir() INTO tmp_dir;

CREATE TABLE assembly_summary_genbank AS
SELECT
    -- Convert this into 3 columns: GCA_000001515.5 => GCA; 1515; 5
    REGEXP_EXTRACT ("column00", '^(GCA|GCF)', 0) as assembly_accession_loc,
    CAST(
        REGEXP_EXTRACT ("column00", '_(\d+)\.', 1) AS INTEGER
    ) as assembly_accession_int,
    CAST(
        REGEXP_EXTRACT ("column00", '\.(\d+)$', 1) AS INTEGER
    ) as assembly_accession_version,
    -- Convert this into 1 column: PRJNA13184 => 13184
    CASE
        WHEN "column01" = '' THEN NULL
        ELSE CAST(
            NULLIF(REGEXP_EXTRACT ("column01", 'PRJNA(\d+)', 1), '') AS INTEGER
        )
    END as bioproject,
    -- Convert this into 1 column: SAMN02803731 => 2803731
    -- "column02" as biosample,
    CASE
        WHEN "column01" = '' THEN NULL
        ELSE CAST(
            NULLIF(REGEXP_EXTRACT ("column02", 'SAMN(\d+)', 1), '') AS INTEGER
        )
    END as biosample,
    "column03" as wgs_master,
    "column04" as refseq_category,
    CAST("column05" AS INTEGER) as tax_id, -- int
    CAST("column06" AS INTEGER) as species_taxid, -- int
    "column07" as organism_name,
    "column08" as infraspecific_name,
    "column09" as isolate,
    CAST("column10" as version_status_enum) as version_status,
    CAST("column11" as assembly_level_enum) as assembly_level,
    CAST("column12" as release_type_enum) as release_type,
    CAST("column13" as genome_rep_enum) as genome_rep,
    CAST("column14" AS DATE) as seq_rel_date,
    EXTRACT(
        YEAR
        FROM
            CAST("column14" AS DATE)
    ) as year,
    EXTRACT(
        MONTH
        FROM
            CAST("column14" AS DATE)
    ) as month,
    "column15" as asm_name,
    "column16" as submitter,
    "column17" as gbrs_paired_asm,
    "column18" as paired_asm_comp,
    REPLACE (
        "column19",
        'https://ftp.ncbi.nlm.nih.gov/genomes/all/',
        ''
    ) as ftp_path,
    "column20" as excluded_from_refseq,
    CAST("column21" AS relation_to_type_material_enum) as relation_to_type_material, -- enum
    "column22" as asm_not_live_date,
    CAST("column23" AS assembly_type_enum) as assembly_type, -- enum
    "column24" as group, -- very useful for sorting.
    CAST("column25" AS BIGINT) as genome_size, -- int
    "column26" as genome_size_ungapped, -- int
    "column27" as gc_percent, --f32
    "column28" as replicon_count, -- smallint
    "column29" as scaffold_count, -- small int
    "column30" as contig_count, -- small int
    "column31" as annotation_provider, -- str
    "column32" as annotation_name,
    "column33" as annotation_date,
    TRY_CAST (NULLIF("column34", 'na') AS BIGINT) as total_gene_count,
    "column35" as protein_coding_gene_count, --int
    "column36" as non_coding_gene_count, --int
    "column37" as pubmed_id -- comma separated IDs
FROM
    read_csv_auto (
        tmp_dir || '/assembly_summary_genbank.txt',
        sep = '\t',
        header = false,
        skip = 2
    );
