# BioDuck

A CLI tool for running SQL files against DuckDB and exploring data interactively.

## Installation

```bash
pip install .
```

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