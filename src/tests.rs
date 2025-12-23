use pgrx::prelude::*;

#[pg_schema]
mod tests {
    use super::*;
    use arrow::array::{
        builder::StringDictionaryBuilder, Array, BooleanArray, Decimal128Array, Float32Array,
        Int32Array, ListBuilder, StringArray, StructArray, UInt16Array, UInt32Array,
    };
    use arrow::datatypes::{DataType, Field, Int32Type, Schema};
    use arrow::record_batch::RecordBatch;
    use lance_rs::Dataset;
    use sqllogictest::{DBOutput, DefaultColumnType, Runner};
    use std::ffi::{CStr, OsStr};
    use std::fs;
    use std::path::{Path, PathBuf};
    use std::sync::Arc;
    use tempfile::TempDir;

    struct SpiSltDb;

    impl sqllogictest::DB for SpiSltDb {
        type Error = pgrx::spi::SpiError;
        type ColumnType = DefaultColumnType;

        fn run(&mut self, sql: &str) -> Result<DBOutput<Self::ColumnType>, Self::Error> {
            let sql = sql.trim();
            if sql.is_empty() {
                return Ok(DBOutput::StatementComplete(0));
            }

            Spi::connect_mut(|client| {
                let mut tuptable = client.update(sql, None, &[])?;

                let columns = match tuptable.columns() {
                    Ok(columns) => columns,
                    Err(pgrx::spi::SpiError::NoTupleTable) => {
                        return Ok(DBOutput::StatementComplete(tuptable.len() as u64));
                    }
                    Err(e) => return Err(e),
                };

                let mut types = Vec::with_capacity(columns);
                let mut type_oids = Vec::with_capacity(columns);
                for i in 1..=columns {
                    let oid = tuptable.column_type_oid(i)?.value();
                    types.push(map_pg_oid_to_slt(oid));
                    type_oids.push(oid);
                }

                let mut rows = Vec::new();
                while tuptable.next().is_some() {
                    let mut row = Vec::with_capacity(columns);
                    for (idx, oid) in type_oids.iter().enumerate() {
                        let datum = tuptable.get_datum_by_ordinal(idx + 1)?;
                        row.push(format_pg_datum(datum, *oid));
                    }
                    rows.push(row);
                }

                Ok(DBOutput::Rows { types, rows })
            })
        }

        fn engine_name(&self) -> &str {
            "postgres"
        }
    }

    fn map_pg_oid_to_slt(oid: pg_sys::Oid) -> DefaultColumnType {
        match PgOid::from_untagged(oid) {
            PgOid::BuiltIn(builtin) => match builtin {
                pg_sys::BuiltinOid::INT2OID
                | pg_sys::BuiltinOid::INT4OID
                | pg_sys::BuiltinOid::INT8OID
                | pg_sys::BuiltinOid::OIDOID => DefaultColumnType::Integer,
                pg_sys::BuiltinOid::FLOAT4OID
                | pg_sys::BuiltinOid::FLOAT8OID
                | pg_sys::BuiltinOid::NUMERICOID => DefaultColumnType::FloatingPoint,
                _ => DefaultColumnType::Text,
            },
            _ => DefaultColumnType::Text,
        }
    }

    fn format_pg_datum(datum: Option<pg_sys::Datum>, type_oid: pg_sys::Oid) -> String {
        match datum {
            None => "NULL".to_string(),
            Some(datum) => unsafe {
                let mut out_func = pg_sys::Oid::from(0u32);
                let mut is_varlena = false;
                pg_sys::getTypeOutputInfo(type_oid, &mut out_func, &mut is_varlena);

                let ptr = pg_sys::OidOutputFunctionCall(out_func, datum);
                let s = CStr::from_ptr(ptr)
                    .to_str()
                    .unwrap_or("<invalid utf8>")
                    .to_string();
                pg_sys::pfree(ptr as *mut _);
                s
            },
        }
    }

    fn slt_identifier(input: &str) -> String {
        let mut out = String::with_capacity(input.len());
        for ch in input.chars() {
            if ch.is_ascii_alphanumeric() {
                out.push(ch.to_ascii_lowercase());
            } else {
                out.push('_');
            }
        }
        if out.is_empty() {
            out.push('_');
        }
        let first = out.as_bytes()[0];
        if !first.is_ascii_alphabetic() && first != b'_' {
            out.insert(0, '_');
        }
        if out.len() > 50 {
            out.truncate(50);
        }
        out
    }

    fn list_slt_files(dir: &Path) -> Vec<PathBuf> {
        let mut files: Vec<PathBuf> = fs::read_dir(dir)
            .expect("read_dir tests/sql")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| path.extension().is_some_and(|ext| ext == OsStr::new("slt")))
            .collect();
        files.sort();
        files
    }

    struct LanceTestDataGenerator {
        temp_dir: TempDir,
    }

    impl LanceTestDataGenerator {
        fn new() -> Result<Self, Box<dyn std::error::Error>> {
            Ok(Self {
                temp_dir: TempDir::new()?,
            })
        }

        fn create_table_with_decimal_and_dictionary(
            &self,
        ) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
            let table_path = self.temp_dir.path().join("fdw_misc");

            let u16_array = UInt16Array::from(vec![1, u16::MAX, 2]);
            let u32_array = UInt32Array::from(vec![1, u32::MAX, 42]);

            let dec_array = Decimal128Array::from(vec![Some(12345i128), Some(-10i128), None])
                .with_precision_and_scale(10, 2)?;

            let mut dict_builder = StringDictionaryBuilder::<Int32Type>::new();
            dict_builder.append("foo")?;
            dict_builder.append("bar")?;
            dict_builder.append_null();
            let dict_array = dict_builder.finish();

            let schema = Arc::new(Schema::new(vec![
                Field::new("u16", DataType::UInt16, false),
                Field::new("u32", DataType::UInt32, false),
                Field::new("dec", dec_array.data_type().clone(), true),
                Field::new("dict", dict_array.data_type().clone(), true),
            ]));

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(u16_array),
                    Arc::new(u32_array),
                    Arc::new(dec_array),
                    Arc::new(dict_array),
                ],
            )?;

            let reader = arrow::record_batch::RecordBatchIterator::new(vec![Ok(batch)], schema);
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                Dataset::write(reader, table_path.to_str().unwrap(), None).await
            })?;

            Ok(table_path)
        }

        fn create_table_with_struct_and_list(
            &self,
        ) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
            let table_path = self.temp_dir.path().join("fdw_table");

            let id_array = Int32Array::from(vec![1, 2, 3]);
            let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
            let active_array = BooleanArray::from(vec![true, false, true]);

            let mut emb_builder = ListBuilder::new(arrow::array::Float32Builder::new());
            for embedding in [
                vec![0.1, 0.2, 0.3],
                vec![0.4, 0.5, 0.6],
                vec![0.7, 0.8, 0.9],
            ] {
                for v in embedding {
                    emb_builder.values().append_value(v);
                }
                emb_builder.append(true);
            }
            let emb_array = emb_builder.finish();

            let meta_score = Float32Array::from(vec![1.0, 2.0, 3.0]);
            let meta_tag = StringArray::from(vec!["a", "b", "c"]);
            let meta_struct = StructArray::from(vec![
                (
                    Arc::new(Field::new("score", DataType::Float32, false)),
                    Arc::new(meta_score) as _,
                ),
                (
                    Arc::new(Field::new("tag", DataType::Utf8, false)),
                    Arc::new(meta_tag) as _,
                ),
            ]);

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("active", DataType::Boolean, false),
                Field::new(
                    "embedding",
                    DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
                    false,
                ),
                Field::new("meta", meta_struct.data_type().clone(), false),
            ]));

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(id_array),
                    Arc::new(name_array),
                    Arc::new(active_array),
                    Arc::new(emb_array),
                    Arc::new(meta_struct),
                ],
            )?;

            let reader = arrow::record_batch::RecordBatchIterator::new(vec![Ok(batch)], schema);
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                Dataset::write(reader, table_path.to_str().unwrap(), None).await
            })?;

            Ok(table_path)
        }
    }

    #[pg_test]
    fn test_fdw_import_and_scan() {
        Spi::run("SELECT pg_advisory_lock(424242)").expect("advisory lock");

        let gen = LanceTestDataGenerator::new().expect("generator");
        let path = gen
            .create_table_with_struct_and_list()
            .expect("create table");
        let uri = path.to_str().unwrap();

        Spi::run("DROP SCHEMA IF EXISTS slt_unit CASCADE").expect("drop schema");
        Spi::run("CREATE SCHEMA slt_unit").expect("create schema");
        Spi::run("SET search_path TO slt_unit, public").expect("set search_path");
        Spi::run("DROP SERVER IF EXISTS lance_srv_unit CASCADE").expect("drop server");
        Spi::run("CREATE SERVER lance_srv_unit FOREIGN DATA WRAPPER lance_fdw")
            .expect("create server");

        let import_sql = format!(
            "SELECT lance_import('lance_srv_unit', 'slt_unit', 't_fdw', '{}', NULL)",
            uri.replace('\'', "''")
        );
        Spi::run(&import_sql).expect("lance_import");

        let cnt = Spi::get_one::<i64>("SELECT count(*) FROM slt_unit.t_fdw")
            .expect("count")
            .expect("count value");
        assert_eq!(cnt, 3);

        let v = Spi::get_one::<String>("SELECT name FROM slt_unit.t_fdw WHERE id = 2")
            .expect("select")
            .expect("value");
        assert_eq!(v, "Bob");

        Spi::run("SELECT pg_advisory_unlock(424242)").expect("advisory unlock");
    }

    #[pg_test]
    fn test_sqllogictest() {
        Spi::run("SELECT pg_advisory_lock(424242)").expect("advisory lock");

        let gen = LanceTestDataGenerator::new().expect("generator");
        let path = gen
            .create_table_with_struct_and_list()
            .expect("create table");
        let uri = path.to_str().expect("uri").replace('\'', "''");

        let scripts_dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/sql");
        let slt_files = list_slt_files(&scripts_dir);
        assert!(
            !slt_files.is_empty(),
            "no .slt files found under {}",
            scripts_dir.display()
        );

        for (idx, file) in slt_files.iter().enumerate() {
            let stem = file
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("unknown");
            let schema = format!("slt_{}_{}", idx, slt_identifier(stem));
            let server = format!("{}_srv", schema);

            let mut script = fs::read_to_string(file).expect("read .slt file");
            script = script.replace("${LANCE_URI}", &uri);
            script = script.replace("${SCHEMA}", &schema);
            script = script.replace("${SERVER}", &server);

            let prefix = format!(
                "statement ok\n\
DROP SCHEMA IF EXISTS {schema} CASCADE;\n\n\
statement ok\n\
CREATE SCHEMA {schema};\n\n\
statement ok\n\
SET search_path TO {schema}, public;\n\n\
statement ok\n\
DROP SERVER IF EXISTS {server} CASCADE;\n\n\
statement ok\n\
CREATE SERVER {server} FOREIGN DATA WRAPPER lance_fdw;\n\n"
            );
            let full_script = format!("{prefix}\n{script}\n");

            let mut runner = Runner::new(|| async { Ok::<_, pgrx::spi::SpiError>(SpiSltDb) });
            if let Err(e) = runner.run_script_with_name(&full_script, file.display().to_string()) {
                panic!("{}", e.display(false));
            }
        }

        Spi::run("SELECT pg_advisory_unlock(424242)").expect("advisory unlock");
    }

    #[pg_test]
    fn test_fdw_mapping_matches_conversion() {
        Spi::run("SELECT pg_advisory_lock(424242)").expect("advisory lock");

        let gen = LanceTestDataGenerator::new().expect("generator");
        let path = gen
            .create_table_with_decimal_and_dictionary()
            .expect("create table");
        let uri = path.to_str().unwrap();

        Spi::run("DROP SCHEMA IF EXISTS slt_unit_map CASCADE").expect("drop schema");
        Spi::run("CREATE SCHEMA slt_unit_map").expect("create schema");
        Spi::run("SET search_path TO slt_unit_map, public").expect("set search_path");
        Spi::run("DROP SERVER IF EXISTS lance_srv_unit_map CASCADE").expect("drop server");
        Spi::run("CREATE SERVER lance_srv_unit_map FOREIGN DATA WRAPPER lance_fdw")
            .expect("create server");

        let import_sql = format!(
            "SELECT lance_import('lance_srv_unit_map', 'slt_unit_map', 't_misc', '{}', NULL)",
            uri.replace('\'', "''")
        );
        Spi::run(&import_sql).expect("lance_import");

        let u16_ty = Spi::get_one::<String>(
            "SELECT atttypid::regtype::text FROM pg_attribute \
             WHERE attrelid = 'slt_unit_map.t_misc'::regclass AND attname = 'u16'",
        )
        .expect("u16 type")
        .expect("u16 type value");
        assert_eq!(u16_ty, "integer");

        let u32_ty = Spi::get_one::<String>(
            "SELECT atttypid::regtype::text FROM pg_attribute \
             WHERE attrelid = 'slt_unit_map.t_misc'::regclass AND attname = 'u32'",
        )
        .expect("u32 type")
        .expect("u32 type value");
        assert_eq!(u32_ty, "bigint");

        let dec_ty = Spi::get_one::<String>(
            "SELECT atttypid::regtype::text FROM pg_attribute \
             WHERE attrelid = 'slt_unit_map.t_misc'::regclass AND attname = 'dec'",
        )
        .expect("dec type")
        .expect("dec type value");
        assert_eq!(dec_ty, "numeric");

        let dict_ty = Spi::get_one::<String>(
            "SELECT atttypid::regtype::text FROM pg_attribute \
             WHERE attrelid = 'slt_unit_map.t_misc'::regclass AND attname = 'dict'",
        )
        .expect("dict type")
        .expect("dict type value");
        assert_eq!(dict_ty, "text");

        let u32_max = Spi::get_one::<i64>("SELECT u32 FROM slt_unit_map.t_misc WHERE u16 = 65535")
            .expect("u32")
            .expect("u32 value");
        assert_eq!(u32_max, u32::MAX as i64);

        let dec_ok = Spi::get_one::<bool>(
            "SELECT dec = 123.45::numeric FROM slt_unit_map.t_misc WHERE u16 = 1",
        )
        .expect("dec compare")
        .expect("dec compare value");
        assert!(dec_ok);

        let dict = Spi::get_one::<String>("SELECT dict FROM slt_unit_map.t_misc WHERE u16 = 1")
            .expect("dict")
            .expect("dict value");
        assert_eq!(dict, "foo");

        let nulls_ok = Spi::get_one::<bool>(
            "SELECT dec IS NULL AND dict IS NULL FROM slt_unit_map.t_misc WHERE u16 = 2",
        )
        .expect("null check")
        .expect("null check value");
        assert!(nulls_ok);

        Spi::run("SELECT pg_advisory_unlock(424242)").expect("advisory unlock");
    }
}
