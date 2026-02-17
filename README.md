# DuckDB Polyglot Extension

A DuckDB extension that provides SQL transpilation and analysis capabilities, powered by the [polyglot-sql](https://crates.io/crates/polyglot-sql) crate. It can transpile SQL between 34 different dialects, validate SQL, parse SQL into AST (JSON), optimize queries, trace column lineage, and diff SQL statements.

## Installation
You can install this extension from the community extension repo like this:

```sql
INSTALL polyglot FROM community;
LOAD polyglot;
```

## Functions

### `polyglot_transpile(sql VARCHAR, from_dialect VARCHAR [, to_dialect VARCHAR]) -> VARCHAR`

Transpiles a SQL query from the given dialect to DuckDB SQL (default), or to any other supported dialect when `to_dialect` is specified.

```sql
-- Transpile to DuckDB (default)
SELECT polyglot_transpile('SELECT NOW()', 'postgresql');

-- Transpile between arbitrary dialects
SELECT polyglot_transpile('SELECT DATEDIFF(''day'', ''2020-01-01'', ''2025-10-12'')', 'snowflake', 'postgresql');
```

If the input contains multiple statements, they are joined with `;\n` in the result.

### `polyglot_dialects() -> TABLE(dialect_name VARCHAR)`

Returns a table of all 34 supported dialect names.

```sql
SELECT * FROM polyglot_dialects();
```

```
┌──────────────┐
│ dialect_name │
│   varchar    │
├──────────────┤
│ athena       │
│ bigquery     │
│ clickhouse   │
│ cockroachdb  │
│ databricks   │
│ datafusion   │
│ doris        │
│ dremio       │
│ drill        │
│ druid        │
│ duckdb       │
│ dune         │
│ exasol       │
│ fabric       │
│ generic      │
│ hive         │
│ materialize  │
│ mysql        │
│ oracle       │
│ postgresql   │
│ presto       │
│ redshift     │
│ risingwave   │
│ singlestore  │
│ snowflake    │
│ solr         │
│ spark        │
│ sqlite       │
│ starrocks    │
│ tableau      │
│ teradata     │
│ tidb         │
│ trino        │
│ tsql         │
├──────────────┤
│   34 rows    │
└──────────────┘
```

Some dialects also accept aliases (e.g. `postgres` for `postgresql`, `mssql` or `sqlserver` for `tsql`, `memsql` for `singlestore`).

### `polyglot_validate(sql VARCHAR, dialect VARCHAR) -> STRUCT(valid BOOLEAN, message VARCHAR, line INTEGER, col INTEGER)`

Validates a SQL statement against the given dialect's grammar. Returns a struct indicating whether the SQL is valid, and if not, the first error's details.

```sql
SELECT polyglot_validate('SELECT 1', 'generic');
-- {'valid': true, 'message': '', 'line': 0, 'col': 0}

SELECT polyglot_validate('SLECT 1', 'generic');
-- {'valid': false, 'message': Invalid expression / Unexpected token, 'line': 0, 'col': 0}
```

### `polyglot_parse(sql VARCHAR, dialect VARCHAR) -> JSON`

Parses a SQL statement into an AST and returns it as a JSON string.

```sql
SELECT polyglot_parse('SELECT 1', 'generic');
-- Returns a JSON array of parsed AST expressions
```

### `polyglot_optimize(sql VARCHAR, dialect VARCHAR) -> VARCHAR`

Parses a SQL statement, applies simplification and canonicalization optimization passes, and returns the optimized SQL string.

```sql
SELECT polyglot_optimize('SELECT a FROM t WHERE b = 1', 'generic');
-- SELECT a FROM t WHERE b = 1
```

### `polyglot_query(sql VARCHAR, from_dialect VARCHAR) -> TABLE`

Transpiles a SQL query from the given dialect to DuckDB SQL, then executes it and returns the results. The output schema is dynamic, matching the columns of the executed query.

```sql
SELECT * FROM polyglot_query('SELECT 1 AS a, 2 AS b', 'postgresql');
```

```
┌───┬───┐
│ a │ b │
├───┼───┤
│ 1 │ 2 │
└───┴───┘
```

### `polyglot_lineage(sql VARCHAR, dialect VARCHAR, column_name VARCHAR) -> TABLE(node_name VARCHAR, source_table VARCHAR)`

Traces the lineage of a column through a SQL statement, returning the dependency graph as a table of nodes.

```sql
SELECT * FROM polyglot_lineage('SELECT a FROM t', 'generic', 'a');
```

```
┌───────────┬──────────────┐
│ node_name │ source_table │
├───────────┼──────────────┤
│ a         │              │
│ t.a       │              │
└───────────┴──────────────┘
```

### `polyglot_diff(source_sql VARCHAR, target_sql VARCHAR, dialect VARCHAR) -> TABLE(edit_type VARCHAR, expression VARCHAR)`

Computes the diff between two SQL statements, returning each edit (insert, remove, move, update) as a row.

```sql
SELECT * FROM polyglot_diff('SELECT a FROM t', 'SELECT b FROM t', 'generic');
```

```
┌───────────┬────────────┐
│ edit_type │ expression │
├───────────┼────────────┤
│ remove    │ a          │
│ insert    │ b          │
└───────────┴────────────┘
```

## Building

### Dependencies

- [Rust toolchain](https://rustup.rs/)
- Python3 + Python3-venv
- [Make](https://www.gnu.org/software/make)
- Git

### Build steps

Clone the repo with submodules:

```shell
git clone --recurse-submodules <repo>
```

Configure (sets up Python venv with DuckDB test runner):

```shell
make configure
```

Build debug:

```shell
make debug
```

Build release:

```shell
make release
```

## Running

Start DuckDB with the `-unsigned` flag (required for locally built extensions):

```shell
duckdb -unsigned
```

Then load the extension:

```sql
LOAD 'build/debug/polyglot.duckdb_extension';

SELECT polyglot_transpile('SELECT NOW()', 'postgresql');
SELECT * FROM polyglot_dialects();
SELECT * FROM polyglot_query('SELECT 1 AS a, 2 AS b', 'generic');
SELECT polyglot_validate('SELECT 1', 'generic');
SELECT polyglot_parse('SELECT 1', 'generic');
SELECT polyglot_optimize('SELECT a FROM t WHERE b = 1', 'generic');
SELECT * FROM polyglot_lineage('SELECT a FROM t', 'generic', 'a');
SELECT * FROM polyglot_diff('SELECT a FROM t', 'SELECT b FROM t', 'generic');
```

## Testing

Tests are written in SQLLogicTest format in `test/sql/polyglot.test`.

```shell
make test_debug
```

Or for release builds:

```shell
make test_release
```
