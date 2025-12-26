use pgrx::prelude::*;

#[pg_schema]
mod tests {
    use super::*;
    use arrow::array::{
        builder::StringDictionaryBuilder, Array, BooleanArray, Decimal128Array, Float32Array,
        Int32Array, ListBuilder, StringArray, StructArray, UInt16Array, UInt32Array, UInt64Array,
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

        let mut last_was_underscore = false;
        for ch in input.chars() {
            let lower = ch.to_ascii_lowercase();
            let ok = matches!(lower, 'a'..='z' | '0'..='9' | '_');
            let mapped = if ok { lower } else { '_' };

            if mapped == '_' {
                if last_was_underscore {
                    continue;
                }
                last_was_underscore = true;
            } else {
                last_was_underscore = false;
            }

            out.push(mapped);
        }

        while out.starts_with('_') {
            out.remove(0);
        }
        while out.ends_with('_') {
            out.pop();
        }

        if out.is_empty() {
            out.push('_');
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

        fn create_dir_namespace_simple_table(
            &self,
            root: &Path,
            table_name: &str,
        ) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
            let table_path = root.join(format!("{}.lance", table_name));

            let id_array = Int32Array::from(vec![1, 2, 3]);
            let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
            let active_array = BooleanArray::from(vec![true, false, true]);

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("active", DataType::Boolean, false),
            ]));

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(id_array),
                    Arc::new(name_array),
                    Arc::new(active_array),
                ],
            )?;

            let reader = arrow::record_batch::RecordBatchIterator::new(vec![Ok(batch)], schema);
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                Dataset::write(reader, table_path.to_str().unwrap(), None).await
            })?;

            Ok(table_path)
        }

        fn create_dir_namespace_nested_table(
            &self,
            root: &Path,
            namespace: &[&str],
            table_name: &str,
        ) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
            let mut dir = root.to_path_buf();
            for seg in namespace {
                dir = dir.join(seg);
            }
            fs::create_dir_all(&dir)?;

            let table_path = dir.join(format!("{}.lance", table_name));

            let id_array = Int32Array::from(vec![1, 2, 3]);
            let name_array = StringArray::from(vec!["Alice", "Bob", "Charlie"]);
            let active_array = BooleanArray::from(vec![true, false, true]);

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("name", DataType::Utf8, false),
                Field::new("active", DataType::Boolean, false),
            ]));

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(id_array),
                    Arc::new(name_array),
                    Arc::new(active_array),
                ],
            )?;

            let reader = arrow::record_batch::RecordBatchIterator::new(vec![Ok(batch)], schema);
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(async {
                Dataset::write(reader, table_path.to_str().unwrap(), None).await
            })?;

            Ok(table_path)
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

        fn create_table_with_u64_overflow(
            &self,
        ) -> Result<std::path::PathBuf, Box<dyn std::error::Error>> {
            let table_path = self.temp_dir.path().join("fdw_u64_overflow");

            let id_array = Int32Array::from(vec![1, 2, 3]);
            let u64_array = UInt64Array::from(vec![u64::MAX, u64::MAX, u64::MAX]);

            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("u64", DataType::UInt64, false),
            ]));

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(id_array), Arc::new(u64_array)],
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
    fn test_sqllogictest() {
        Spi::run("SELECT pg_advisory_lock(424242)").expect("advisory lock");

        let gen = LanceTestDataGenerator::new().expect("generator");
        let struct_list_path = gen
            .create_table_with_struct_and_list()
            .expect("create table");
        let struct_list_uri = struct_list_path.to_str().expect("uri").replace('\'', "''");

        let misc_path = gen
            .create_table_with_decimal_and_dictionary()
            .expect("create table");
        let misc_uri = misc_path.to_str().expect("uri").replace('\'', "''");

        let overflow_path = gen.create_table_with_u64_overflow().expect("create table");
        let overflow_uri = overflow_path.to_str().expect("uri").replace('\'', "''");

        let bad_uri = gen
            .temp_dir
            .path()
            .join("does_not_exist")
            .to_str()
            .expect("uri")
            .replace('\'', "''");

        let ns_root = gen.temp_dir.path().join("ns_root");
        fs::create_dir_all(&ns_root).expect("create ns_root");
        gen.create_dir_namespace_simple_table(&ns_root, "t_root")
            .expect("create root table");
        gen.create_dir_namespace_nested_table(&ns_root, &["TeamA", "images"], "train")
            .expect("create nested table");

        let ns_root_uri = ns_root.to_str().expect("uri").replace('\'', "''");
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            use lance_namespace::models::{CreateNamespaceRequest, RegisterTableRequest};
            use lance_namespace::LanceNamespace;
            use lance_namespace_impls::DirectoryNamespaceBuilder;

            let ns = DirectoryNamespaceBuilder::new(ns_root.to_str().unwrap())
                .manifest_enabled(true)
                .dir_listing_enabled(true)
                .build()
                .await
                .expect("build namespace");

            let mut req = CreateNamespaceRequest::new();
            req.id = Some(vec!["TeamA".to_string()]);
            ns.create_namespace(req).await.expect("create TeamA");

            let mut req = CreateNamespaceRequest::new();
            req.id = Some(vec!["TeamA".to_string(), "images".to_string()]);
            ns.create_namespace(req).await.expect("create TeamA/images");

            let mut reg = RegisterTableRequest::new("TeamA/images/train.lance".to_string());
            reg.id = Some(vec![
                "TeamA".to_string(),
                "images".to_string(),
                "train".to_string(),
            ]);
            ns.register_table(reg).await.expect("register table");
        });

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
            script = script.replace("${LANCE_URI}", &struct_list_uri);
            script = script.replace("${LANCE_URI_STRUCT_LIST}", &struct_list_uri);
            script = script.replace("${LANCE_URI_MISC}", &misc_uri);
            script = script.replace("${LANCE_URI_OVERFLOW}", &overflow_uri);
            script = script.replace("${LANCE_BAD_URI}", &bad_uri);
            script = script.replace("${LANCE_NS_ROOT}", &ns_root_uri);
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
}
