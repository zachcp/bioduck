import os
import sys
import click
import duckdb


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


if __name__ == "__main__":
    cli()
