use duckdb::{
    core::{DataChunkHandle, Inserter, LogicalTypeHandle, LogicalTypeId},
    vscalar::{ArrowFunctionSignature, VArrowScalar},
    vtab::{
        record_batch_to_duckdb_data_chunk, to_duckdb_logical_type, BindInfo, InitInfo,
        TableFunctionInfo, VTab,
    },
    Connection,
};

use arrow::{
    array::{Array, StringArray},
    datatypes::DataType,
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

    fn invoke(_: &Self::State, input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
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

        let results: Vec<Option<String>> = sql_col
            .iter()
            .zip(dialect_col.iter())
            .map(|(sql, dialect)| -> Result<Option<String>, Box<dyn Error>> {
                match (sql, dialect) {
                    (Some(s), Some(d)) => {
                        let from_dialect: DialectType = d.parse()?;
                        let result = polyglot_sql::transpile(s, from_dialect, DialectType::DuckDB)?;
                        Ok(Some(result.join(";\n")))
                    }
                    _ => Ok(None),
                }
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

// ============== ParseScalar ==============

struct ParseScalar;

impl VArrowScalar for ParseScalar {
    type State = ();

    fn invoke(_: &Self::State, input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
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

        let results: Vec<Option<String>> = sql_col
            .iter()
            .zip(dialect_col.iter())
            .map(|(sql, dialect)| -> Result<Option<String>, Box<dyn Error>> {
                match (sql, dialect) {
                    (Some(s), Some(d)) => {
                        let parsed_dialect: DialectType = d.parse()?;
                        let ast = polyglot_sql::parse(s, parsed_dialect)?;
                        let ast_json = serde_json::to_string(&ast)?;
                        Ok(Some(ast_json))
                    }
                    _ => Ok(None),
                }
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

// ============== ValidateScalar ==============

struct ValidateScalar;

impl VArrowScalar for ValidateScalar {
    type State = ();

    fn invoke(_: &Self::State, input: RecordBatch) -> Result<Arc<dyn Array>, Box<dyn Error>> {
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

        let results: Vec<Option<String>> = sql_col
            .iter()
            .zip(dialect_col.iter())
            .map(|(sql, dialect)| -> Result<Option<String>, Box<dyn Error>> {
                match (sql, dialect) {
                    (Some(s), Some(d)) => {
                        let parsed_dialect: DialectType = d.parse()?;
                        let validation = polyglot_sql::validate(s, parsed_dialect);
                        let validation_json = serde_json::to_string(&validation)?;
                        Ok(Some(validation_json))
                    }
                    _ => Ok(None),
                }
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

const DIALECT_NAMES: &[&str] = &[
    "generic",
    "postgresql",
    "mysql",
    "bigquery",
    "snowflake",
    "duckdb",
    "sqlite",
    "hive",
    "spark",
    "trino",
    "presto",
    "redshift",
    "tsql",
    "oracle",
    "clickhouse",
    "databricks",
    "athena",
    "teradata",
    "doris",
    "starrocks",
    "materialize",
    "risingwave",
    "singlestore",
    "cockroachdb",
    "tidb",
    "druid",
    "solr",
    "tableau",
    "dune",
    "fabric",
    "drill",
    "dremio",
    "exasol",
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

        if *current >= DIALECT_NAMES.len() {
            output.set_len(0);
        } else {
            let remaining = DIALECT_NAMES.len() - *current;
            let batch_size = remaining.min(2048);
            let vector = output.flat_vector(0);
            for i in 0..batch_size {
                let name = CString::new(DIALECT_NAMES[*current + i])?;
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
        let transpiled = polyglot_sql::transpile(&sql, from_dialect, DialectType::DuckDB)?;
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

// ============== Entrypoint ==============

fn extension_entrypoint(con: Connection, shared_conn: SharedConn) -> Result<(), Box<dyn Error>> {
    con.register_scalar_function::<TranspileScalar>("polyglot_transpile")?;
    con.register_scalar_function::<ParseScalar>("polyglot_parse")?;
    con.register_scalar_function::<ValidateScalar>("polyglot_validate")?;
    con.register_table_function::<DialectsVTab>("polyglot_dialects")?;
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
        libduckdb_sys::duckdb_rs_extension_api_init(info, access, "v1.4.4").unwrap();

    if !have_api_struct {
        return Ok(false);
    }

    let db: libduckdb_sys::duckdb_database = *(*access).get_database.unwrap()(info);

    // Create the main connection for registering functions
    let connection = Connection::open_from_raw(db.cast())?;

    // Create a second connection for QueryVTab to execute queries
    let query_conn = Connection::open_from_raw(db.cast())?;
    let shared_conn = SharedConn(Arc::new(Mutex::new(SendableConn(query_conn))));

    extension_entrypoint(connection, shared_conn)?;

    Ok(true)
}
