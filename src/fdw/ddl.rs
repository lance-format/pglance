use crate::fdw::type_mapping::CompositeTypeDef;
use std::collections::BTreeMap;

pub(crate) fn create_schema_sql(schema: &str) -> String {
    format!(
        "DO $$ BEGIN CREATE SCHEMA {}; EXCEPTION WHEN duplicate_schema THEN NULL; END $$;",
        quote_ident(schema)
    )
}

pub(crate) fn order_composite_types(types: &[CompositeTypeDef]) -> Vec<CompositeTypeDef> {
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

pub(crate) fn create_composite_type_sql(schema: &str, ty: &CompositeTypeDef) -> String {
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

pub(crate) fn create_foreign_table_sql(
    server: &str,
    schema: &str,
    table: &str,
    options: Vec<(String, String)>,
    columns: Vec<(String, String)>,
) -> String {
    let cols = columns
        .into_iter()
        .map(|(n, t)| format!("{} {}", quote_ident(&n), qualify_type_string(&t, schema)))
        .collect::<Vec<_>>()
        .join(", ");

    let opts = options
        .into_iter()
        .map(|(k, v)| format!("{} {}", quote_ident(&k), quote_literal(&v)))
        .collect::<Vec<_>>()
        .join(", ");

    format!(
        "DO $$ BEGIN CREATE FOREIGN TABLE {}.{} ({}) SERVER {} OPTIONS ({}); EXCEPTION WHEN duplicate_table THEN NULL; END $$;",
        quote_ident(schema),
        quote_ident(table),
        cols,
        quote_ident(server),
        opts
    )
}

pub(crate) fn quote_ident(ident: &str) -> String {
    let escaped = ident.replace('\"', "\"\"");
    format!("\"{}\"", escaped)
}

pub(crate) fn quote_literal(value: &str) -> String {
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
