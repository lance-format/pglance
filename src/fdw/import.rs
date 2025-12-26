use crate::fdw::ddl::{create_composite_type_sql, create_foreign_table_sql, order_composite_types};
use crate::fdw::type_mapping::build_schema_mapping;
use lance_rs::Dataset;
use pgrx::spi::Spi;

pub fn import_lance_table(
    server_name: &str,
    local_schema: &str,
    table_name: &str,
    uri: &str,
    batch_size: Option<i64>,
) -> Result<(), String> {
    let rt = tokio::runtime::Runtime::new().map_err(|e| e.to_string())?;
    let dataset = rt
        .block_on(async { Dataset::open(uri).await })
        .map_err(|e| e.to_string())?;

    let lance_schema = dataset.schema();
    let arrow_fields: Vec<arrow::datatypes::Field> = lance_schema
        .fields
        .iter()
        .map(|f| arrow::datatypes::Field::new(f.name.clone(), f.data_type().clone(), f.nullable))
        .collect();

    let mapping = build_schema_mapping(table_name, &arrow_fields).map_err(|e| e.to_string())?;

    for ty in order_composite_types(&mapping.composite_types) {
        Spi::run(&create_composite_type_sql(local_schema, &ty)).map_err(|e| e.to_string())?;
    }

    let mut options = vec![("uri".to_string(), uri.to_string())];
    if let Some(bs) = batch_size {
        options.push(("batch_size".to_string(), bs.to_string()));
    }
    Spi::run(&create_foreign_table_sql(
        server_name,
        local_schema,
        table_name,
        options,
        mapping.column_types,
    ))
    .map_err(|e| e.to_string())?;
    Ok(())
}
