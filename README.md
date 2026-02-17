# DuckDB Polyglot Extension

A DuckDB extension that provides SQL transpilation capabilities, powered by the [polyglot-sql](https://crates.io/crates/polyglot-sql) crate. It can transpile SQL from 33 different dialects into DuckDB SQL, and optionally execute the transpiled query directly.

## Installation
You can install this extension from the community extension repo like this:

```sql
INSTALL polyglot FROM community;
LOAD polyglot;
```

## Functions

### `polyglot_transpile(sql VARCHAR, from_dialect VARCHAR) -> VARCHAR`

Transpiles a SQL query from the given dialect to DuckDB SQL.

```sql
SELECT polyglot_transpile('SELECT NOW()', 'postgresql');
-- Returns the DuckDB-compatible equivalent
```

If the input contains multiple statements, they are joined with `;\n` in the result.

### `polyglot_parse(sql VARCHAR, from_dialect VARCHAR) -> VARCHAR`

Parses SQL and returns the AST serialized as a JSON string.

```sql
SELECT polyglot_parse(
  'SELECT a, b FROM t WHERE x > 1',
  'generic'
);
-- Returns JSON array of parsed statements
```

You can use DuckDB JSON functions to inspect the AST:

```sql
SELECT json_extract_string(
  polyglot_parse('SELECT a FROM t', 'generic'),
  '$[0].select.from.expressions[0].table.name.name'
);
-- t
```

### `polyglot_validate(sql VARCHAR, from_dialect VARCHAR) -> VARCHAR`

Validates SQL and returns a JSON string with fields:

- `valid` (boolean)
- `errors` (array of validation issues with message/code/severity and optional line/column)

```sql
SELECT polyglot_validate('SELECT * FORM users', 'generic');
-- Returns JSON like:
-- {"valid":false,"errors":[{"message":"...","line":1,"column":10,"severity":"error","code":"E001"}]}
```

### `polyglot_dialects() -> TABLE(dialect_name VARCHAR)`

Returns a table of all 33 supported dialect names.

```sql
SELECT * FROM polyglot_dialects();
```

```
┌──────────────┐
│ dialect_name │
│   varchar    │
├──────────────┤
│ generic      │
│ postgresql   │
│ mysql        │
│ bigquery     │
│ snowflake    │
│ duckdb       │
│ sqlite       │
│ hive         │
│ spark        │
│ trino        │
│ presto       │
│ redshift     │
│ tsql         │
│ oracle       │
│ clickhouse   │
│ databricks   │
│ athena       │
│ teradata     │
│ doris        │
│ starrocks    │
│ materialize  │
│ risingwave   │
│ singlestore  │
│ cockroachdb  │
│ tidb         │
│ druid        │
│ solr         │
│ tableau      │
│ dune         │
│ fabric       │
│ drill        │
│ dremio       │
│ exasol       │
├──────────────┤
│   33 rows    │
└──────────────┘
```

Some dialects also accept aliases (e.g. `postgres` for `postgresql`, `mssql` or `sqlserver` for `tsql`, `memsql` for `singlestore`).

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
