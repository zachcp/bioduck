import os
import sys
import click
import duckdb
import importlib.resources
import requests
import shutil
import tempfile
import tarfile
from pathlib import Path


@click.group()
def cli():
    """BioDuck CLI tool for managing and running SQL queries against DuckDB databases."""
    pass


@cli.command()
@click.argument('sql_file', type=click.Path(exists=True, dir_okay=False))
@click.option('--db', '-d', default=':memory:', help='Path to DuckDB database file. Uses in-memory DB by default.')
@click.option('--output', '-o', help='Save results to this file instead of displaying them.')
def run(sql_file, db, output):
    """Run a SQL file against a DuckDB database."""
    with open(sql_file, 'r') as f:
        query = f.read()

    conn = duckdb.connect(db)
    result = conn.execute(query)

    if output:
        result.df().to_csv(output, index=False)
        click.echo(f"Results saved to {output}")
    else:
        click.echo(result.fetchdf())


@cli.command()
@click.option('--db', '-d', default=':memory:', help='Path to DuckDB database file. Uses in-memory DB by default.')
@click.option('--sql-dir', '-s', default='./sql', help='Directory containing SQL files to initialize the database.')
def ui(db, sql_dir):
    """Run DuckDB UI after optionally initializing with SQL files."""
    if os.path.exists(sql_dir):
        conn = duckdb.connect(db)

        # Run all SQL files in the specified directory
        sql_files = [f for f in os.listdir(sql_dir) if f.endswith('.sql')]
        for sql_file in sorted(sql_files):
            click.echo(f"Running {sql_file}...")
            with open(os.path.join(sql_dir, sql_file), 'r') as f:
                query = f.read()
            conn.execute(query)
        conn.close()

    # Launch DuckDB UI
    os.system(f"duckdb -ui {db}")


@cli.command()
@click.argument('name')
@click.option('--dir', '-d', default='./sql', help='Directory to create the SQL file in.')
def create(name, dir):
    """Create a new SQL file with the given name."""
    if not os.path.exists(dir):
        os.makedirs(dir)

    file_path = os.path.join(dir, f"{name}.sql")
    if os.path.exists(file_path):
        click.echo(f"File {file_path} already exists.")
        return

    with open(file_path, 'w') as f:
        f.write(f"-- {name}.sql\n\n")

    click.echo(f"Created SQL file: {file_path}")


def download_file(url, destination):
    """Download a file from a URL to a destination path with progress indication."""
    click.echo(f"Downloading {url}...")
    
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        total_size = int(response.headers.get('content-length', 0))
        
        with open(destination, 'wb') as f:
            with click.progressbar(length=total_size, label=f"Downloading {os.path.basename(url)}") as bar:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        bar.update(len(chunk))
    
    click.echo(f"Downloaded to {destination}")
    return destination


def extract_from_tarfile(tar_path, file_to_extract, destination_dir):
    """Extract a specific file from a tar archive."""
    click.echo(f"Extracting {file_to_extract} from {tar_path}...")
    
    with tarfile.open(tar_path, 'r:gz') as tar:
        # Find the file in the archive
        try:
            member = tar.getmember(file_to_extract)
            tar.extract(member, path=destination_dir)
            click.echo(f"Extracted to {os.path.join(destination_dir, file_to_extract)}")
            return os.path.join(destination_dir, file_to_extract)
        except KeyError:
            click.echo(f"File {file_to_extract} not found in archive", err=True)
            return None


@cli.command()
@click.option('--db-path', '-d', default='~/.bioduck/ncbi.db', help='Path to save/load the NCBI database.')
@click.option('--force', '-f', is_flag=True, help='Force recreation of database even if it exists.')
@click.option('--launch-ui', '-u', is_flag=True, help='Launch DuckDB UI after setup.')
@click.option('--data-dir', default='~/.bioduck/data', help='Directory to store downloaded data files.')
def ncbi(db_path, force, launch_ui, data_dir):
    """Set up an NCBI database using the included SQL scripts."""
    # Resolve paths and create directories
    db_path = os.path.expanduser(db_path)
    data_dir = os.path.expanduser(data_dir)
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    os.makedirs(data_dir, exist_ok=True)

    # Check if database already exists
    db_exists = os.path.exists(db_path) and os.path.getsize(db_path) > 0

    if db_exists:
        if force:
            click.echo(f"Removing existing database at {db_path}")
            os.remove(db_path)
        else:
            click.echo(f"Database already exists at {db_path}")
            if launch_ui:
                click.echo("Launching DuckDB UI...")
                os.system(f"duckdb -ui {db_path}")
            return

    # Download necessary files
    click.echo("Downloading required NCBI data files...")
    
    # Download taxonomy data
    taxonomy_url = "https://ftp.ncbi.nlm.nih.gov/pub/taxonomy/new_taxdump/new_taxdump.tar.gz"
    taxonomy_tar = os.path.join(data_dir, "new_taxdump.tar.gz")
    download_file(taxonomy_url, taxonomy_tar)
    
    # Extract needed taxonomy file
    taxonomy_file = extract_from_tarfile(taxonomy_tar, "rankedlineage.dmp", data_dir)
    
    # Download assembly summaries
    genbank_url = "https://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/assembly_summary_genbank.txt"
    refseq_url = "https://ftp.ncbi.nlm.nih.gov/genomes/ASSEMBLY_REPORTS/assembly_summary_refseq.txt"
    
    genbank_file = os.path.join(data_dir, "assembly_summary_genbank.txt")
    refseq_file = os.path.join(data_dir, "assembly_summary_refseq.txt")
    
    download_file(genbank_url, genbank_file)
    download_file(refseq_url, refseq_file)
    
    # Access SQL files from package resources
    try:
        # For Python 3.9+
        files_resource = importlib.resources.files('bioduck.sql.ncbi')
        sql_dir_exists = files_resource.is_dir()
    except (AttributeError, ImportError):
        # Fallback for older Python versions
        try:
            sql_dir_exists = importlib.resources.is_resource('bioduck.sql.ncbi', '__init__.py')
        except (ImportError, FileNotFoundError):
            sql_dir_exists = False

    if not sql_dir_exists:
        click.echo("Error: SQL directory not found in package", err=True)
        sys.exit(1)

    # Create/connect to the database
    click.echo(f"Setting up NCBI database at {db_path}...")
    conn = duckdb.connect(db_path)
    
    # Load httpfs extension
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    
    # Set up the working directory for SQL files
    conn.execute(f"SET temp_directory = '{data_dir}'")
    
    # Define the order of SQL files to run
    sql_files = ['init.sql', 'enums.sql', 'load_taxonomy.sql', 'load_assembly_genbank.sql', 'load_assembly_refseq.sql']

    # Execute each SQL file in order
    for sql_file in sql_files:
        try:
            # Try to read SQL file from package resources using Python 3.9+ API
            try:
                sql_path = files_resource.joinpath(sql_file)
                query = sql_path.read_text()
            except (AttributeError, NameError):
                # Fallback for older Python versions
                query = importlib.resources.read_text('bioduck.sql.ncbi', sql_file)

            click.echo(f"Running {sql_file}...")
            try:
                # For init.sql, we just need to initialize the database setup
                if sql_file == 'init.sql':
                    conn.execute(query)
                else:
                    # For data loading files, run them with the current working directory set to data_dir
                    conn.execute(f"SET working_directory = '{data_dir}'")
                    conn.execute(query)
            except Exception as e:
                click.echo(f"Error executing {sql_file}: {e}", err=True)
        except FileNotFoundError:
            click.echo(f"Warning: SQL file {sql_file} not found in package", err=True)

    conn.close()
    click.echo(f"NCBI database setup complete at {db_path}")

    # Launch UI if requested
    if launch_ui:
        click.echo("Launching DuckDB UI...")
        os.system(f"duckdb -ui {db_path}")


if __name__ == "__main__":
    cli()
