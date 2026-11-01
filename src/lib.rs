use duckdb_loadable_macros::{AsDuckDB, duckdb_entrypoint_c_api_v2};
use duckdb_neo::{
    DuckDBType, Error, Result,
    bind_arguments::BindArgument,
    connection::Context,
    data_chunk::{DataChunkRef, VectorCollection},
    extension::Extension,
    macro_function::{MacroBody, MacroFunctionBuilder},
    scalar::{ScalarCallbacks, ScalarFunctionBuilder},
    signature::{FunctionParameter, SignatureBuilder},
    table_function::{
        BindFunctionHandle, ExecColumnInfo, InitColumnData, TableFunctionBuilder,
        TableFunctionCallbacks,
    },
    vector::{Unknown, Vector},
};
use polyglot_sql::DialectType;

fn invalid_input(error: impl std::fmt::Display) -> Error {
    Error::invalid_input(error.to_string())
}

fn sql_signature() -> SignatureBuilder {
    SignatureBuilder::new()
        .param(FunctionParameter::new::<String>("sql"))
        .param(FunctionParameter::new::<String>("dialect"))
}

// ============== String scalar functions ==============

#[derive(Clone, Copy)]
enum SqlScalar {
    Transpile,
    Parse,
    Optimize,
}

impl ScalarCallbacks for SqlScalar {
    type BindData = ();
    type InitData = ();

    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        _init_data: Option<&mut Self::InitData>,
        _context: &Context,
        input: &mut VectorCollection,
        output: Vector<'_, Unknown>,
    ) -> Result<()> {
        let len = input.row_count()?;
        let sqls = input.get_vector_at::<String>(0)?.reader()?;
        let dialects = input.get_vector_at::<String>(1)?.reader()?;
        let targets = if matches!(self, Self::Transpile) {
            Some(input.get_vector_at::<String>(2)?.reader()?)
        } else {
            None
        };
        let mut output = output.cast::<String>()?;
        output.set_size(len)?;
        let mut writer = output.writer()?;

        for i in 0..len {
            let (Some(sql), Some(dialect)) = (sqls.get(i)?, dialects.get(i)?) else {
                writer.set(i, None)?;
                continue;
            };
            let dialect: DialectType = dialect.parse().map_err(invalid_input)?;
            let result = match self {
                Self::Transpile => {
                    let Some(target) = targets.as_ref().unwrap().get(i)? else {
                        writer.set(i, None)?;
                        continue;
                    };
                    let target = target.parse().map_err(invalid_input)?;
                    polyglot_sql::transpile(sql, dialect, target)
                        .map_err(invalid_input)?
                        .join(";\n")
                }
                Self::Parse => {
                    let expressions = polyglot_sql::parse(sql, dialect).map_err(invalid_input)?;
                    serde_json::to_string(&expressions).map_err(invalid_input)?
                }
                Self::Optimize => {
                    let expr = polyglot_sql::parse_one(sql, dialect).map_err(invalid_input)?;
                    let optimized = polyglot_sql::optimizer::quick_optimize(expr, Some(dialect));
                    polyglot_sql::generate(&optimized, dialect).map_err(invalid_input)?
                }
            };
            writer.set(i, Some(result.as_str()))?;
        }
        Ok(())
    }
}

// ============== ValidateScalar ==============

struct ValidateScalar;

#[derive(AsDuckDB)]
struct ValidationResult {
    valid: bool,
    message: String,
    line: i32,
    col: i32,
}

impl ScalarCallbacks for ValidateScalar {
    type BindData = ();
    type InitData = ();

    fn exec(
        &self,
        _bind_data: Option<&Self::BindData>,
        _init_data: Option<&mut Self::InitData>,
        _context: &Context,
        input: &mut VectorCollection,
        output: Vector<'_, Unknown>,
    ) -> Result<()> {
        let len = input.row_count()?;
        let sqls = input.get_vector_at::<String>(0)?.reader()?;
        let dialects = input.get_vector_at::<String>(1)?.reader()?;
        let mut output = output.cast::<ValidationResult>()?;
        output.set_size(len)?;
        let mut writer = output.writer()?;

        for i in 0..len {
            let (Some(sql), Some(dialect)) = (sqls.get(i)?, dialects.get(i)?) else {
                writer.set(i, None)?;
                continue;
            };
            let dialect = dialect.parse().map_err(invalid_input)?;
            let result = polyglot_sql::validate(sql, dialect);
            let (message, line, col) = if result.valid {
                ("", 0, 0)
            } else if let Some(error) = result.errors.first() {
                (
                    error.message.as_str(),
                    error.line.unwrap_or(0) as i32,
                    error.column.unwrap_or(0) as i32,
                )
            } else {
                ("Unknown validation error", 0, 0)
            };
            writer.set(
                i,
                Some(ValidationResult {
                    valid: result.valid,
                    message: message.to_owned(),
                    line,
                    col,
                }),
            )?;
        }
        Ok(())
    }
}

// ============== Text table functions ==============

const DIALECT_TYPES: &[DialectType] = &[
    DialectType::Athena,
    DialectType::BigQuery,
    DialectType::ClickHouse,
    DialectType::CockroachDB,
    DialectType::Databricks,
    DialectType::DataFusion,
    DialectType::Doris,
    DialectType::Dremio,
    DialectType::Drill,
    DialectType::Druid,
    DialectType::DuckDB,
    DialectType::Dune,
    DialectType::Exasol,
    DialectType::Fabric,
    DialectType::Generic,
    DialectType::Hive,
    DialectType::Materialize,
    DialectType::MySQL,
    DialectType::Oracle,
    DialectType::PostgreSQL,
    DialectType::Presto,
    DialectType::Redshift,
    DialectType::RisingWave,
    DialectType::SingleStore,
    DialectType::Snowflake,
    DialectType::Solr,
    DialectType::Spark,
    DialectType::SQLite,
    DialectType::StarRocks,
    DialectType::Tableau,
    DialectType::Teradata,
    DialectType::TiDB,
    DialectType::Trino,
    DialectType::TSQL,
];

#[derive(Clone, Copy)]
enum TextTable {
    Dialects,
    Lineage,
    Diff,
}

fn string_argument(arguments: &[BindArgument], index: usize) -> Result<String> {
    arguments
        .get(index)
        .and_then(|argument| argument.value.as_ref())
        .ok_or_else(|| Error::invalid_input("Arguments must be constant strings"))?
        .get::<String>()?
        .ok_or_else(|| Error::invalid_input("Arguments must not be NULL"))
}

impl TableFunctionCallbacks for TextTable {
    type BindData = Vec<Vec<String>>;
    type GlobalState = ();
    type LocalState = usize;

    fn bind(
        &self,
        context: &Context,
        arguments: Vec<BindArgument>,
        bind: BindFunctionHandle<'_>,
    ) -> Result<Self::BindData> {
        let (columns, rows): (&[&str], Vec<Vec<String>>) = match self {
            Self::Dialects => (
                &["dialect_name"],
                DIALECT_TYPES
                    .iter()
                    .map(|dialect| vec![dialect.to_string()])
                    .collect(),
            ),
            Self::Lineage => {
                let sql = string_argument(&arguments, 0)?;
                let dialect = string_argument(&arguments, 1)?
                    .parse()
                    .map_err(invalid_input)?;
                let column = string_argument(&arguments, 2)?;
                let expr = polyglot_sql::parse_one(&sql, dialect).map_err(invalid_input)?;
                let root = polyglot_sql::lineage::lineage(&column, &expr, Some(dialect), false)
                    .map_err(invalid_input)?;
                (
                    &["node_name", "source_table"],
                    root.walk()
                        .map(|node| vec![node.name.clone(), node.source_name.clone()])
                        .collect(),
                )
            }
            Self::Diff => {
                let source = string_argument(&arguments, 0)?;
                let target = string_argument(&arguments, 1)?;
                let dialect = string_argument(&arguments, 2)?
                    .parse()
                    .map_err(invalid_input)?;
                let source = polyglot_sql::parse_one(&source, dialect).map_err(invalid_input)?;
                let target = polyglot_sql::parse_one(&target, dialect).map_err(invalid_input)?;
                let rows = polyglot_sql::diff::diff(&source, &target, true)
                    .into_iter()
                    .map(|edit| {
                        use polyglot_sql::diff::Edit;
                        let (kind, expression) = match edit {
                            Edit::Insert { expression } => ("insert", expression),
                            Edit::Remove { expression } => ("remove", expression),
                            Edit::Move { target, .. } => ("move", target),
                            Edit::Update { target, .. } => ("update", target),
                            Edit::Keep { target, .. } => ("keep", target),
                        };
                        vec![
                            kind.to_string(),
                            polyglot_sql::generate(&expression, dialect).unwrap_or_default(),
                        ]
                    })
                    .collect();
                (&["edit_type", "expression"], rows)
            }
        };
        let varchar = String::logical_type(context)?;
        for column in columns {
            bind.add_result_column(column, &varchar)?;
        }
        bind.set_cardinality(rows.len(), true)?;
        Ok(rows)
    }

    fn init_global_state(
        &self,
        _bind_data: &Self::BindData,
        _context: &Context,
        _column_data: InitColumnData<'_>,
    ) -> Result<(Option<Self::GlobalState>, Option<usize>)> {
        // One cursor scans the materialized rows in their original order.
        Ok((None, Some(1)))
    }

    fn init_local_state(
        &self,
        _bind_data: &Self::BindData,
        _global_state: Option<&Self::GlobalState>,
        _context: &Context,
        _column_data: InitColumnData<'_>,
    ) -> Result<Option<Self::LocalState>> {
        Ok(Some(0))
    }

    fn exec(
        &self,
        rows: &Self::BindData,
        _global_state: Option<&Self::GlobalState>,
        local_state: Option<&mut Self::LocalState>,
        _context: &Context,
        mut output: DataChunkRef<'_>,
        _column_info: ExecColumnInfo<'_>,
    ) -> Result<()> {
        let current = local_state.ok_or_else(|| Error::api_error("Missing table scan state"))?;
        let len = (rows.len() - *current).min(output.capacity()?);
        for column in 0..output.col_count() {
            let mut vector = output.get_vector_at_mut::<String>(column)?;
            vector.set_size(len)?;
            let mut writer = vector.writer()?;
            for (i, row) in rows[*current..*current + len].iter().enumerate() {
                writer.set(i, Some(row[column].as_str()))?;
            }
        }
        *current += len;
        Ok(())
    }
}

// ============== Entrypoint ==============

#[duckdb_entrypoint_c_api_v2(extension_name = "polyglot", api_version = "v2.0.0")]
fn extension_entrypoint(extension: &Extension) -> Result<()> {
    ScalarFunctionBuilder::new(
        "polyglot_transpile",
        sql_signature()
            .param(FunctionParameter::with_default("to_dialect", "duckdb"))
            .return_type(String::logical_type(extension)?),
        SqlScalar::Transpile,
    )
    .register(extension)?;
    ScalarFunctionBuilder::new(
        "polyglot_validate",
        sql_signature().return_type(ValidationResult::logical_type(extension)?),
        ValidateScalar,
    )
    .register(extension)?;
    for (name, callback) in [
        ("polyglot_parse", SqlScalar::Parse),
        ("polyglot_optimize", SqlScalar::Optimize),
    ] {
        ScalarFunctionBuilder::new(
            name,
            sql_signature().return_type(String::logical_type(extension)?),
            callback,
        )
        .register(extension)?;
    }
    TableFunctionBuilder::new(
        "polyglot_dialects",
        SignatureBuilder::new(),
        TextTable::Dialects,
    )
    .register(extension)?;
    TableFunctionBuilder::new(
        "polyglot_lineage",
        sql_signature().param(FunctionParameter::new::<String>("column_name")),
        TextTable::Lineage,
    )
    .register(extension)?;
    TableFunctionBuilder::new(
        "polyglot_diff",
        SignatureBuilder::new()
            .param(FunctionParameter::new::<String>("source_sql"))
            .param(FunctionParameter::new::<String>("target_sql"))
            .param(FunctionParameter::new::<String>("dialect")),
        TextTable::Diff,
    )
    .register(extension)?;

    // Let DuckDB bind and execute the transpiled query in the caller's session,
    // preserving its native column types without an Arrow conversion or a second connection.
    MacroFunctionBuilder::new(
        "polyglot_query",
        sql_signature(),
        MacroBody::Table("SELECT * FROM query(polyglot_transpile(sql, dialect))".to_string()),
    )
    .register(extension)?;
    Ok(())
}
