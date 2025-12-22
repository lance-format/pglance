use crate::fdw::type_mapping::{build_schema_mapping, CompositeTypeDef};
use lance_rs::Dataset;
use pgrx::spi::Spi;
use std::collections::BTreeMap;

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

    let ordered = order_composite_types(&mapping.composite_types);

    for ty in ordered {
        let sql = create_composite_type_sql(local_schema, &ty);
        Spi::run(&sql).map_err(|e| e.to_string())?;
    }

    let table_sql = create_foreign_table_sql(
        server_name,
        local_schema,
        table_name,
        uri,
        batch_size,
        mapping.column_types,
    );
    Spi::run(&table_sql).map_err(|e| e.to_string())?;
    Ok(())
}

fn order_composite_types(types: &[CompositeTypeDef]) -> Vec<CompositeTypeDef> {
    let by_name: BTreeMap<String, CompositeTypeDef> = types
        .iter()
        .cloned()
        .map(|t| (t.type_name.clone(), t))
        .collect();
    let mut emitted = BTreeMap::<String, bool>::new();

    let mut out = Vec::new();
    loop {
        let mut progressed = false;

        for (name, def) in by_name.iter() {
            if emitted.get(name).copied().unwrap_or(false) {
                continue;
            }

            let deps = def
                .fields
                .iter()
                .filter_map(|(_, ty)| base_type_name(ty))
                .filter(|t| t.starts_with("lance_"))
                .collect::<Vec<_>>();

            if deps
                .iter()
                .all(|d| emitted.get(d).copied().unwrap_or(false))
            {
                out.push(def.clone());
                emitted.insert(name.clone(), true);
                progressed = true;
            }
        }

        if !progressed {
            break;
        }
        if out.len() == by_name.len() {
            break;
        }
    }

    for (name, def) in by_name.into_iter() {
        if !emitted.get(&name).copied().unwrap_or(false) {
            out.push(def);
        }
    }

    out
}

fn base_type_name(ty: &str) -> Option<String> {
    let mut t = ty.trim();
    while let Some(stripped) = t.strip_suffix("[]") {
        t = stripped.trim_end();
    }
    Some(t.to_string())
}

fn create_composite_type_sql(schema: &str, ty: &CompositeTypeDef) -> String {
    let qtype = qualify_composite_type(&ty.type_name, schema);
    let columns = ty
        .fields
        .iter()
        .map(|(n, t)| format!("{} {}", quote_ident(n), qualify_type_string(t, schema)))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "DO $$ BEGIN CREATE TYPE {} AS ({}); EXCEPTION WHEN duplicate_object THEN NULL; END $$;",
        qtype, columns
    )
}

fn create_foreign_table_sql(
    server: &str,
    schema: &str,
    table: &str,
    uri: &str,
    batch_size: Option<i64>,
    columns: Vec<(String, String)>,
) -> String {
    let cols = columns
        .into_iter()
        .map(|(n, t)| format!("{} {}", quote_ident(&n), qualify_type_string(&t, schema)))
        .collect::<Vec<_>>()
        .join(", ");

    let mut opts = vec![format!("uri {}", quote_literal(uri))];
    if let Some(bs) = batch_size {
        opts.push(format!("batch_size {}", quote_literal(&bs.to_string())));
    }

    format!(
        "DO $$ BEGIN CREATE FOREIGN TABLE {}.{} ({}) SERVER {} OPTIONS ({}); EXCEPTION WHEN duplicate_table THEN NULL; END $$;",
        quote_ident(schema),
        quote_ident(table),
        cols,
        quote_ident(server),
        opts.join(", ")
    )
}

fn quote_ident(ident: &str) -> String {
    let escaped = ident.replace('"', "\"\"");
    format!("\"{}\"", escaped)
}

fn quote_literal(value: &str) -> String {
    let escaped = value.replace('\'', "''");
    format!("'{}'", escaped)
}

fn qualify_composite_type(type_name: &str, schema: &str) -> String {
    format!("{}.{}", quote_ident(schema), quote_ident(type_name))
}

fn qualify_type_string(ty: &str, schema: &str) -> String {
    let mut t = ty.trim().to_string();
    let mut dims = 0usize;
    while let Some(stripped) = t.trim_end().strip_suffix("[]") {
        dims += 1;
        t = stripped.trim_end().to_string();
    }

    let base = if t.starts_with("lance_") {
        qualify_composite_type(&t, schema)
    } else {
        t
    };

    let mut out = base;
    for _ in 0..dims {
        out.push_str("[]");
    }
    out
}
