use pgrx::prelude::*;

mod fdw;

pgrx::pg_module_magic!();

pgrx::extension_sql!(
    r#"
CREATE FOREIGN DATA WRAPPER lance_fdw
HANDLER lance_fdw_handler
VALIDATOR lance_fdw_validator;
"#,
    name = "lance_fdw",
    requires = [lance_fdw_handler, lance_fdw_validator]
);

#[pg_extern]
unsafe fn lance_fdw_handler() -> PgBox<pg_sys::FdwRoutine> {
    fdw::handler::build_fdw_routine()
}

#[pg_extern]
fn lance_fdw_validator(options: Vec<String>, _catalog: pg_sys::Oid) {
    for opt in options {
        let (k, _v) = opt
            .split_once('=')
            .ok_or_else(|| format!("invalid option format: {}", opt))
            .unwrap_or_else(|e| pgrx::error!("{}", e));

        if k == "uri" || k == "batch_size" || k.starts_with("aws_") || k.starts_with("s3_") {
            continue;
        }

        pgrx::error!("unsupported option: {}", k);
    }
}

#[pg_extern]
fn lance_import(
    server_name: &str,
    local_schema: &str,
    table_name: &str,
    uri: &str,
    batch_size: default!(Option<i64>, "NULL"),
) {
    fdw::import::import_lance_table(server_name, local_schema, table_name, uri, batch_size)
        .unwrap_or_else(|e| pgrx::error!("lance_import failed: {}", e));
}

#[cfg(any(test, feature = "pg_test"))]
#[pg_schema]
mod tests {
    use arrow::array::{
        Array, BooleanArray, Float32Array, Int32Array, ListBuilder, StringArray, StructArray,
    };
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use lance_rs::Dataset;
    use pgrx::prelude::*;
    use std::sync::Arc;
    use tempfile::TempDir;

    struct LanceTestDataGenerator {
        temp_dir: TempDir,
    }

    impl LanceTestDataGenerator {
        fn new() -> Result<Self, Box<dyn std::error::Error>> {
            Ok(Self {
                temp_dir: TempDir::new()?,
            })
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
        let gen = LanceTestDataGenerator::new().expect("generator");
        let path = gen
            .create_table_with_struct_and_list()
            .expect("create table");
        let uri = path.to_str().unwrap();

        Spi::run("CREATE SERVER lance_srv FOREIGN DATA WRAPPER lance_fdw").expect("create server");

        let import_sql = format!(
            "SELECT lance_import('lance_srv', 'public', 't_fdw', '{}', NULL)",
            uri.replace('\'', "''")
        );
        Spi::run(&import_sql).expect("lance_import");

        let cnt = Spi::get_one::<i64>("SELECT count(*) FROM public.t_fdw")
            .expect("count")
            .expect("count value");
        assert_eq!(cnt, 3);

        let v = Spi::get_one::<String>("SELECT name FROM public.t_fdw WHERE id = 2")
            .expect("select")
            .expect("value");
        assert_eq!(v, "Bob");
    }
}

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
