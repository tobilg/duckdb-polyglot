use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vtab::{
        record_batch_to_duckdb_data_chunk, to_duckdb_logical_type, BindInfo, InitInfo,
        TableFunctionInfo, VTab,
    },
    vscalar::{ArrowFunctionSignature, VArrowScalar},
    Connection,
};

use arrow::{
    array::{Array, BooleanArray, Int32Array, StringArray, StructArray},
    datatypes::{DataType, Field, Fields},
    record_batch::RecordBatch,
};

use polyglot_sql::DialectType;

use std::{
    error::Error,
    ffi::CString,
    sync::{Arc, Mutex},
};

// ============== SharedConn ==============

/// Wraps a Connection to make it Send+Sync via unsafe impl.
/// Safety: Access is always serialized through a Mutex.
struct SendableConn(Connection);
unsafe impl Send for SendableConn {}
unsafe impl Sync for SendableConn {}

/// Shared connection handle passed as extra_info to QueryVTab.
#[derive(Clone)]
struct SharedConn(Arc<Mutex<SendableConn>>);

// ============== TranspileScalar ==============

struct TranspileScalar;

impl VArrowScalar for TranspileScalar {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: RecordBatch,
    ) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let sql_col = input
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let dialect_col = input
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let has_to_dialect = input.num_columns() >= 3;
        let to_dialect_col = if has_to_dialect {
            Some(
                input
                    .column(2)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap(),
            )
        } else {
            None
        };

        let results: Vec<Option<String>> = (0..sql_col.len())
            .map(
                |i| -> Result<Option<String>, Box<dyn Error>> {
                    let sql = sql_col.value(i);
                    let dialect = dialect_col.value(i);

                    if sql_col.is_null(i) || dialect_col.is_null(i) {
                        return Ok(None);
                    }
                    if let Some(ref col) = to_dialect_col {
                        if col.is_null(i) {
                            return Ok(None);
                        }
                    }

                    let from_dialect: DialectType = dialect.parse()?;
                    let to_dialect: DialectType = if let Some(ref col) = to_dialect_col {
                        col.value(i).parse()?
                    } else {
                        DialectType::DuckDB
                    };

                    let result = polyglot_sql::transpile(
                        sql,
                        from_dialect,
                        to_dialect,
                    )?;
                    Ok(Some(result.join(";\n")))
                },
            )
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Arc::new(StringArray::from(results)))
    }

    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![
            ArrowFunctionSignature::exact(
                vec![DataType::Utf8, DataType::Utf8],
                DataType::Utf8,
            ),
            ArrowFunctionSignature::exact(
                vec![DataType::Utf8, DataType::Utf8, DataType::Utf8],
                DataType::Utf8,
            ),
        ]
    }
}

// ============== ValidateScalar ==============

struct ValidateScalar;

impl VArrowScalar for ValidateScalar {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: RecordBatch,
    ) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let sql_col = input
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let dialect_col = input
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let mut valids = Vec::with_capacity(sql_col.len());
        let mut messages = Vec::with_capacity(sql_col.len());
        let mut lines = Vec::with_capacity(sql_col.len());
        let mut cols = Vec::with_capacity(sql_col.len());

        for i in 0..sql_col.len() {
            if sql_col.is_null(i) || dialect_col.is_null(i) {
                valids.push(true);
                messages.push(String::new());
                lines.push(0i32);
                cols.push(0i32);
                continue;
            }

            let sql = sql_col.value(i);
            let dialect_str = dialect_col.value(i);
            let dialect: DialectType = dialect_str.parse()?;

            let result = polyglot_sql::validate(sql, dialect);

            if result.valid {
                valids.push(true);
                messages.push(String::new());
                lines.push(0);
                cols.push(0);
            } else if let Some(err) = result.errors.first() {
                valids.push(false);
                messages.push(err.message.clone());
                lines.push(err.line.unwrap_or(0) as i32);
                cols.push(err.column.unwrap_or(0) as i32);
            } else {
                valids.push(false);
                messages.push("Unknown validation error".to_string());
                lines.push(0);
                cols.push(0);
            }
        }

        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("valid", DataType::Boolean, false)),
                Arc::new(BooleanArray::from(valids)) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("message", DataType::Utf8, false)),
                Arc::new(StringArray::from(messages)) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("line", DataType::Int32, false)),
                Arc::new(Int32Array::from(lines)) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("col", DataType::Int32, false)),
                Arc::new(Int32Array::from(cols)) as Arc<dyn Array>,
            ),
        ]);

        Ok(Arc::new(struct_array))
    }

    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Utf8, DataType::Utf8],
            DataType::Struct(Fields::from(vec![
                Field::new("valid", DataType::Boolean, false),
                Field::new("message", DataType::Utf8, false),
                Field::new("line", DataType::Int32, false),
                Field::new("col", DataType::Int32, false),
            ])),
        )]
    }
}

// ============== ParseScalar ==============

struct ParseScalar;

impl VArrowScalar for ParseScalar {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: RecordBatch,
    ) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let sql_col = input
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let dialect_col = input
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let results: Vec<Option<String>> = (0..sql_col.len())
            .map(|i| -> Result<Option<String>, Box<dyn Error>> {
                if sql_col.is_null(i) || dialect_col.is_null(i) {
                    return Ok(None);
                }

                let sql = sql_col.value(i);
                let dialect_str = dialect_col.value(i);
                let dialect: DialectType = dialect_str.parse()?;

                let expressions = polyglot_sql::parse(sql, dialect)?;
                let json = serde_json::to_string(&expressions)?;
                Ok(Some(json))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Arc::new(StringArray::from(results)))
    }

    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Utf8, DataType::Utf8],
            DataType::Utf8,
        )]
    }
}

// ============== OptimizeScalar ==============

struct OptimizeScalar;

impl VArrowScalar for OptimizeScalar {
    type State = ();

    fn invoke(
        _: &Self::State,
        input: RecordBatch,
    ) -> Result<Arc<dyn Array>, Box<dyn Error>> {
        let sql_col = input
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let dialect_col = input
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let results: Vec<Option<String>> = (0..sql_col.len())
            .map(|i| -> Result<Option<String>, Box<dyn Error>> {
                if sql_col.is_null(i) || dialect_col.is_null(i) {
                    return Ok(None);
                }

                let sql = sql_col.value(i);
                let dialect_str = dialect_col.value(i);
                let dialect: DialectType = dialect_str.parse()?;

                let expr = polyglot_sql::parse_one(sql, dialect)?;
                let optimized = polyglot_sql::optimizer::quick_optimize(expr, Some(dialect));
                let result = polyglot_sql::generate(&optimized, dialect)?;
                Ok(Some(result))
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Arc::new(StringArray::from(results)))
    }

    fn signatures() -> Vec<ArrowFunctionSignature> {
        vec![ArrowFunctionSignature::exact(
            vec![DataType::Utf8, DataType::Utf8],
            DataType::Utf8,
        )]
    }
}

// ============== DialectsVTab ==============

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

#[repr(C)]
struct DialectsBindData;

#[repr(C)]
struct DialectsInitData {
    current: Mutex<usize>,
}

struct DialectsVTab;

impl VTab for DialectsVTab {
    type BindData = DialectsBindData;
    type InitData = DialectsInitData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        bind.add_result_column(
            "dialect_name",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        Ok(DialectsBindData)
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(DialectsInitData {
            current: Mutex::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let init_data = func.get_init_data();
        let mut current = init_data.current.lock().unwrap();

        if *current >= DIALECT_TYPES.len() {
            output.set_len(0);
        } else {
            let remaining = DIALECT_TYPES.len() - *current;
            let batch_size = remaining.min(2048);
            let vector = output.flat_vector(0);
            for i in 0..batch_size {
                let name = CString::new(DIALECT_TYPES[*current + i].to_string())?;
                vector.insert(i, name);
            }
            output.set_len(batch_size);
            *current += batch_size;
        }

        Ok(())
    }
}

// ============== QueryVTab ==============

#[repr(C)]
struct QueryBindData {
    batches: Vec<RecordBatch>,
}

#[repr(C)]
struct QueryInitData {
    current_batch: Mutex<usize>,
}

struct QueryVTab;

impl VTab for QueryVTab {
    type BindData = QueryBindData;
    type InitData = QueryInitData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let shared = unsafe { &*bind.get_extra_info::<SharedConn>() };

        let sql = bind.get_parameter(0).to_string();
        let dialect_str = bind.get_parameter(1).to_string();

        let from_dialect: DialectType = dialect_str.parse()?;
        let transpiled =
            polyglot_sql::transpile(&sql, from_dialect, DialectType::DuckDB)?;
        let duckdb_sql = transpiled.join("; ");

        let conn_guard = shared.0.lock().unwrap();
        let conn = &conn_guard.0;

        let mut stmt = conn.prepare(&duckdb_sql)?;
        let arrow_result = stmt.query_arrow([])?;
        let schema = arrow_result.get_schema();

        for field in schema.fields() {
            let logical_type = to_duckdb_logical_type(field.data_type())?;
            bind.add_result_column(field.name(), logical_type);
        }

        let batches: Vec<RecordBatch> = arrow_result.collect();

        Ok(QueryBindData { batches })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(QueryInitData {
            current_batch: Mutex::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let bind_data = func.get_bind_data();
        let init_data = func.get_init_data();

        let mut idx = init_data.current_batch.lock().unwrap();
        if *idx >= bind_data.batches.len() {
            output.set_len(0);
        } else {
            let batch = &bind_data.batches[*idx];
            record_batch_to_duckdb_data_chunk(batch, output)?;
            *idx += 1;
        }

        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ])
    }
}

// ============== LineageVTab ==============

#[repr(C)]
struct LineageBindData {
    rows: Vec<(String, String)>,
}

#[repr(C)]
struct LineageInitData {
    current: Mutex<usize>,
}

struct LineageVTab;

impl VTab for LineageVTab {
    type BindData = LineageBindData;
    type InitData = LineageInitData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let sql = bind.get_parameter(0).to_string();
        let dialect_str = bind.get_parameter(1).to_string();
        let column_name = bind.get_parameter(2).to_string();

        let dialect: DialectType = dialect_str.parse()?;
        let expr = polyglot_sql::parse_one(&sql, dialect)?;
        let root = polyglot_sql::lineage::lineage(&column_name, &expr, Some(dialect), false)?;

        let rows: Vec<(String, String)> = root
            .walk()
            .map(|n| (n.name.clone(), n.source_name.clone()))
            .collect();

        bind.add_result_column(
            "node_name",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column(
            "source_table",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );

        Ok(LineageBindData { rows })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(LineageInitData {
            current: Mutex::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let bind_data = func.get_bind_data();
        let init_data = func.get_init_data();
        let mut current = init_data.current.lock().unwrap();

        if *current >= bind_data.rows.len() {
            output.set_len(0);
        } else {
            let remaining = bind_data.rows.len() - *current;
            let batch_size = remaining.min(2048);
            let name_vec = output.flat_vector(0);
            for i in 0..batch_size {
                let (ref name, _) = bind_data.rows[*current + i];
                let c_name = CString::new(name.as_str())?;
                name_vec.insert(i, c_name);
            }
            let source_vec = output.flat_vector(1);
            for i in 0..batch_size {
                let (_, ref source) = bind_data.rows[*current + i];
                let c_source = CString::new(source.as_str())?;
                source_vec.insert(i, c_source);
            }
            output.set_len(batch_size);
            *current += batch_size;
        }

        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ])
    }
}

// ============== DiffVTab ==============

#[repr(C)]
struct DiffBindData {
    rows: Vec<(String, String)>,
}

#[repr(C)]
struct DiffInitData {
    current: Mutex<usize>,
}

struct DiffVTab;

impl VTab for DiffVTab {
    type BindData = DiffBindData;
    type InitData = DiffInitData;

    fn bind(bind: &BindInfo) -> Result<Self::BindData, Box<dyn Error>> {
        let source_sql = bind.get_parameter(0).to_string();
        let target_sql = bind.get_parameter(1).to_string();
        let dialect_str = bind.get_parameter(2).to_string();

        let dialect: DialectType = dialect_str.parse()?;
        let source_expr = polyglot_sql::parse_one(&source_sql, dialect)?;
        let target_expr = polyglot_sql::parse_one(&target_sql, dialect)?;

        let edits = polyglot_sql::diff::diff(&source_expr, &target_expr, true);

        let rows: Vec<(String, String)> = edits
            .into_iter()
            .map(|edit| {
                use polyglot_sql::diff::Edit;
                match edit {
                    Edit::Insert { expression } => {
                        let sql = polyglot_sql::generate(&expression, dialect)
                            .unwrap_or_default();
                        ("insert".to_string(), sql)
                    }
                    Edit::Remove { expression } => {
                        let sql = polyglot_sql::generate(&expression, dialect)
                            .unwrap_or_default();
                        ("remove".to_string(), sql)
                    }
                    Edit::Move { target, .. } => {
                        let sql = polyglot_sql::generate(&target, dialect)
                            .unwrap_or_default();
                        ("move".to_string(), sql)
                    }
                    Edit::Update { target, .. } => {
                        let sql = polyglot_sql::generate(&target, dialect)
                            .unwrap_or_default();
                        ("update".to_string(), sql)
                    }
                    Edit::Keep { target, .. } => {
                        let sql = polyglot_sql::generate(&target, dialect)
                            .unwrap_or_default();
                        ("keep".to_string(), sql)
                    }
                }
            })
            .collect();

        bind.add_result_column(
            "edit_type",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );
        bind.add_result_column(
            "expression",
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        );

        Ok(DiffBindData { rows })
    }

    fn init(_: &InitInfo) -> Result<Self::InitData, Box<dyn Error>> {
        Ok(DiffInitData {
            current: Mutex::new(0),
        })
    }

    fn func(
        func: &TableFunctionInfo<Self>,
        output: &mut DataChunkHandle,
    ) -> Result<(), Box<dyn Error>> {
        let bind_data = func.get_bind_data();
        let init_data = func.get_init_data();
        let mut current = init_data.current.lock().unwrap();

        if *current >= bind_data.rows.len() {
            output.set_len(0);
        } else {
            let remaining = bind_data.rows.len() - *current;
            let batch_size = remaining.min(2048);
            let type_vec = output.flat_vector(0);
            for i in 0..batch_size {
                let (ref edit_type, _) = bind_data.rows[*current + i];
                let c_type = CString::new(edit_type.as_str())?;
                type_vec.insert(i, c_type);
            }
            let expr_vec = output.flat_vector(1);
            for i in 0..batch_size {
                let (_, ref expression) = bind_data.rows[*current + i];
                let c_expr = CString::new(expression.as_str())?;
                expr_vec.insert(i, c_expr);
            }
            output.set_len(batch_size);
            *current += batch_size;
        }

        Ok(())
    }

    fn parameters() -> Option<Vec<LogicalTypeHandle>> {
        Some(vec![
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
            LogicalTypeHandle::from(LogicalTypeId::Varchar),
        ])
    }
}

// ============== Entrypoint ==============

fn extension_entrypoint(
    con: Connection,
    shared_conn: SharedConn,
) -> Result<(), Box<dyn Error>> {
    con.register_scalar_function::<TranspileScalar>("polyglot_transpile")?;
    con.register_scalar_function::<ValidateScalar>("polyglot_validate")?;
    con.register_scalar_function::<ParseScalar>("polyglot_parse")?;
    con.register_scalar_function::<OptimizeScalar>("polyglot_optimize")?;
    con.register_table_function::<DialectsVTab>("polyglot_dialects")?;
    con.register_table_function::<LineageVTab>("polyglot_lineage")?;
    con.register_table_function::<DiffVTab>("polyglot_diff")?;
    con.register_table_function_with_extra_info::<QueryVTab, SharedConn>(
        "polyglot_query",
        &shared_conn,
    )?;
    Ok(())
}

/// # Safety
///
/// Entrypoint called by DuckDB
#[no_mangle]
pub unsafe extern "C" fn polyglot_init_c_api(
    info: libduckdb_sys::duckdb_extension_info,
    access: *const libduckdb_sys::duckdb_extension_access,
) -> bool {
    let init_result = polyglot_init_c_api_internal(info, access);

    if let Err(x) = init_result {
        let error_c_string = CString::new(x.to_string());
        match error_c_string {
            Ok(e) => {
                (*access).set_error.unwrap()(info, e.as_ptr());
            }
            Err(_e) => {
                let error_alloc_failure = c"An error occurred but the extension failed to allocate memory for an error string";
                (*access).set_error.unwrap()(info, error_alloc_failure.as_ptr());
            }
        }
        return false;
    }

    init_result.unwrap()
}

/// # Safety
///
/// Internal entrypoint for error handling
unsafe fn polyglot_init_c_api_internal(
    info: libduckdb_sys::duckdb_extension_info,
    access: *const libduckdb_sys::duckdb_extension_access,
) -> std::result::Result<bool, Box<dyn std::error::Error>> {
    let have_api_struct =
        libduckdb_sys::duckdb_rs_extension_api_init(info, access, "v1.4.4")
            .unwrap();

    if !have_api_struct {
        return Ok(false);
    }

    let db: libduckdb_sys::duckdb_database =
        *(*access).get_database.unwrap()(info);

    // Create the main connection for registering functions
    let connection = Connection::open_from_raw(db.cast())?;

    // Create a second connection for QueryVTab to execute queries
    let query_conn = Connection::open_from_raw(db.cast())?;
    let shared_conn = SharedConn(Arc::new(Mutex::new(SendableConn(query_conn))));

    extension_entrypoint(connection, shared_conn)?;

    Ok(true)
}
