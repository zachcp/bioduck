"""BioDuck - A CLI tool for running SQL files against DuckDB and exploring data interactively."""
import importlib.resources

__version__ = "0.1.0"

def get_sql_path(package_path):
    """Get the path to a SQL file in the package.
    
    Args:
        package_path (str): Path to the SQL file, e.g. 'ncbi/enums.sql'
    
    Returns:
        The contents of the SQL file as a string.
    """
    parts = package_path.split('/')
    if len(parts) == 1:
        return importlib.resources.read_text('bioduck.sql', parts[0])
    else:
        return importlib.resources.read_text(f'bioduck.sql.{parts[0]}', parts[1])