# BioDuck

A CLI tool for running SQL files against DuckDB and exploring data interactively.

## Installation

```bash
pip install .
```

This will install the BioDuck CLI tool along with all included SQL files and resources.

## Usage

### Create a SQL File

```bash
bioduck create my_query
```

This creates a file `./sql/my_query.sql` that you can edit with your SQL query.

### Run a SQL File

```bash
bioduck run ./sql/my_query.sql
```

You can specify a database file:

```bash
bioduck run ./sql/my_query.sql --db my_database.duckdb
```

Save results to a CSV file:

```bash
bioduck run ./sql/my_query.sql --output results.csv
```

### Launch the DuckDB UI

Run the DuckDB UI after initializing with SQL files:

```bash
bioduck ui
```

By default, it will run all SQL files in the `./sql` directory in alphabetical order, then launch the DuckDB UI.

You can specify a different SQL directory:

```bash
bioduck ui --sql-dir ./my_queries
```

And a specific database file:

```bash
bioduck ui --db my_database.duckdb
```

## Project Structure

- Place your SQL files in the `./sql` directory
- SQL files are run in alphabetical order when using the `ui` command
- You can use the `create` command to create new SQL files

## NCBI Database

BioDuck includes built-in support for creating and managing NCBI biological data databases.

### Creating an NCBI Database

```bash
bioduck ncbi
```

This will:
1. Create a database at `~/.bioduck/ncbi.db` (if it doesn't already exist)
2. Initialize it with NCBI database schema and loaders for:
   - Taxonomy data
   - GenBank assembly data 
   - RefSeq assembly data
   
The database will automatically download the required files from NCBI FTP servers:
- Downloads are cached for 1 day by default to avoid repeated downloads
- All data fetching and processing happens directly in DuckDB SQL

### Accessing an Existing NCBI Database

If the database already exists, the command will simply report its location:

```bash
bioduck ncbi
# Database already exists at /home/user/.bioduck/ncbi.db
```

### Launch UI for an NCBI Database

To open the DuckDB UI with your NCBI database:

```bash
bioduck ncbi --launch-ui
# or shorter form:
bioduck ncbi -u
```

### Recreate Database

To force recreation of the database even if it exists:

```bash
bioduck ncbi --force
```

### Specify Custom Database Location

```bash
bioduck ncbi --db-path /path/to/custom/ncbi.db
```