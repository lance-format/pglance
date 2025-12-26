use crate::fdw::attach_namespace::{
    attach_one, build_mapping, foreign_server_oid_by_name, list_table_ids, AttachContext,
    AttachNamespaceRow, TableId, OPT_NS_TABLE_ID,
};
use crate::fdw::ddl::{quote_ident, quote_literal};
use crate::fdw::namespace::connect_namespace;
use crate::fdw::type_mapping::build_schema_mapping;
use lance_namespace::LanceNamespace;
use lance_rs::dataset::builder::DatasetBuilder;
use pgrx::pg_sys;
use pgrx::spi::Spi;
use pgrx::JsonB;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use tokio::runtime::Runtime;

const DEFAULT_LIMIT_PER_LIST_CALL: i32 = 1000;
const OPT_BATCH_SIZE: &str = "batch_size";

#[derive(Debug, Clone)]
struct LocalNamespaceTable {
    local_schema: String,
    local_table: String,
    relid: pg_sys::Oid,
    batch_size: Option<i64>,
}

pub fn sync_namespace(
    server_name: &str,
    root_namespace_id: TableId,
    schema_prefix: &str,
    drop_missing: bool,
    recreate_changed: bool,
    dry_run: bool,
) -> Result<Vec<AttachNamespaceRow>, String> {
    let runtime = Runtime::new().map_err(|e| e.to_string())?;
    let server_oid = foreign_server_oid_by_name(server_name)?;
    let namespace = connect_namespace(&runtime, server_oid)?;

    let remote_table_ids = list_table_ids(
        &runtime,
        namespace.as_ref(),
        root_namespace_id.clone(),
        DEFAULT_LIMIT_PER_LIST_CALL,
    )?;

    let (conflicts, mapping) = build_mapping(schema_prefix, &remote_table_ids)?;
    let expected_set: BTreeSet<TableId> = remote_table_ids.into_iter().collect();

    let mut out = Vec::new();
    for table_id in conflicts {
        out.push(row_error(
            &table_id,
            schema_prefix,
            "mapping conflict: multiple table_id map to the same (schema, table)",
        ));
    }

    let (local_by_id, local_errors) =
        introspect_local_namespace_tables(server_name, &root_namespace_id)?;
    out.extend(local_errors);

    for (table_id, (expected_schema, expected_table)) in mapping {
        let Some(local) = local_by_id.get(&table_id) else {
            let ctx = AttachContext::new(&runtime, namespace.clone(), server_name, None, dry_run);
            match attach_one(&ctx, &table_id, &expected_schema, &expected_table) {
                Ok(row) => out.push(row),
                Err(e) => out.push(row_failed(
                    &table_id,
                    &expected_schema,
                    &expected_table,
                    "create_table",
                    &e,
                )),
            }
            continue;
        };

        if local.local_schema != expected_schema || local.local_table != expected_table {
            out.push(row_ok(
                &table_id,
                &expected_schema,
                &expected_table,
                "drift",
                &format!(
                    "local mapping differs: actual={}.{} expected={}.{}",
                    local.local_schema, local.local_table, expected_schema, expected_table
                ),
            ));
            continue;
        }

        let drift_detail = detect_schema_drift(
            &runtime,
            namespace.clone(),
            &table_id,
            &expected_schema,
            &expected_table,
            local.relid,
        )?;
        let Some(drift_detail) = drift_detail else {
            out.push(row_ok(
                &table_id,
                &expected_schema,
                &expected_table,
                "in_sync",
                "up to date",
            ));
            continue;
        };

        if !recreate_changed {
            out.push(row_ok(
                &table_id,
                &expected_schema,
                &expected_table,
                "drift",
                &drift_detail,
            ));
            continue;
        }

        if dry_run {
            out.push(row_skipped(
                &table_id,
                &expected_schema,
                &expected_table,
                "recreate_table",
                &format!("dry_run: {}", drift_detail),
            ));
            continue;
        }

        match drop_foreign_table(&expected_schema, &expected_table) {
            Ok(()) => {}
            Err(e) => {
                out.push(row_failed(
                    &table_id,
                    &expected_schema,
                    &expected_table,
                    "recreate_table",
                    &format!("failed to drop existing foreign table: {}", e),
                ));
                continue;
            }
        }

        let ctx = AttachContext::new(
            &runtime,
            namespace.clone(),
            server_name,
            local.batch_size,
            false,
        );
        match attach_one(&ctx, &table_id, &expected_schema, &expected_table) {
            Ok(mut row) => {
                row.3 = "recreate_table".to_string();
                row.5 = format!("recreated: {}", drift_detail);
                out.push(row);
            }
            Err(e) => out.push(row_failed(
                &table_id,
                &expected_schema,
                &expected_table,
                "recreate_table",
                &format!("failed to recreate foreign table: {}", e),
            )),
        }
    }

    let mut local_only = Vec::new();
    for (table_id, local) in local_by_id.into_iter() {
        if expected_set.contains(&table_id) {
            continue;
        }
        local_only.push((table_id, local));
    }
    local_only.sort_by(|a, b| {
        (a.1.local_schema.as_str(), a.1.local_table.as_str())
            .cmp(&(b.1.local_schema.as_str(), b.1.local_table.as_str()))
    });

    for (table_id, local) in local_only {
        if !drop_missing {
            out.push(row_ok(
                &table_id,
                &local.local_schema,
                &local.local_table,
                "drift",
                "local foreign table not found in remote namespace listing",
            ));
            continue;
        }

        if dry_run {
            out.push(row_skipped(
                &table_id,
                &local.local_schema,
                &local.local_table,
                "drop_table",
                "dry_run",
            ));
            continue;
        }

        match drop_foreign_table(&local.local_schema, &local.local_table) {
            Ok(()) => out.push(row_ok(
                &table_id,
                &local.local_schema,
                &local.local_table,
                "drop_table",
                "dropped",
            )),
            Err(e) => out.push(row_failed(
                &table_id,
                &local.local_schema,
                &local.local_table,
                "drop_table",
                &e,
            )),
        }
    }

    Ok(out)
}

fn row_ok(
    table_id: &[String],
    local_schema: &str,
    local_table: &str,
    action: &str,
    detail: &str,
) -> AttachNamespaceRow {
    (
        JsonB(serde_json::to_value(table_id).unwrap_or(serde_json::Value::Null)),
        local_schema.to_string(),
        local_table.to_string(),
        action.to_string(),
        "ok".to_string(),
        detail.to_string(),
    )
}

fn row_skipped(
    table_id: &[String],
    local_schema: &str,
    local_table: &str,
    action: &str,
    detail: &str,
) -> AttachNamespaceRow {
    (
        JsonB(serde_json::to_value(table_id).unwrap_or(serde_json::Value::Null)),
        local_schema.to_string(),
        local_table.to_string(),
        action.to_string(),
        "skipped".to_string(),
        detail.to_string(),
    )
}

fn row_failed(
    table_id: &[String],
    local_schema: &str,
    local_table: &str,
    action: &str,
    detail: &str,
) -> AttachNamespaceRow {
    (
        JsonB(serde_json::to_value(table_id).unwrap_or(serde_json::Value::Null)),
        local_schema.to_string(),
        local_table.to_string(),
        action.to_string(),
        "failed".to_string(),
        detail.to_string(),
    )
}

fn row_error(table_id: &[String], schema_prefix: &str, detail: &str) -> AttachNamespaceRow {
    let (schema, table) =
        crate::fdw::naming::schema_and_table_for_table_id(schema_prefix, table_id)
            .unwrap_or_else(|_| ("_".to_string(), "_".to_string()));
    row_failed(table_id, &schema, &table, "error", detail)
}

fn introspect_local_namespace_tables(
    server_name: &str,
    root_namespace_id: &[String],
) -> Result<
    (
        BTreeMap<TableId, LocalNamespaceTable>,
        Vec<AttachNamespaceRow>,
    ),
    String,
> {
    let server_lit = quote_literal(server_name);
    let ns_table_id_lit = quote_literal(OPT_NS_TABLE_ID);
    let batch_size_lit = quote_literal(OPT_BATCH_SIZE);

    let sql = format!(
        "SELECT n.nspname::text AS local_schema, \
                c.relname::text AS local_table, \
                c.oid::bigint AS relid, \
                opt.option_value AS table_id_json, \
                opt_bs.option_value AS batch_size \
           FROM pg_class c \
           JOIN pg_namespace n ON n.oid = c.relnamespace \
           JOIN pg_foreign_table ft ON ft.ftrelid = c.oid \
           JOIN pg_foreign_server fs ON fs.oid = ft.ftserver \
           JOIN LATERAL pg_options_to_table(ft.ftoptions) opt ON opt.option_name = {ns_table_id_lit} \
      LEFT JOIN LATERAL pg_options_to_table(ft.ftoptions) opt_bs ON opt_bs.option_name = {batch_size_lit} \
          WHERE fs.srvname = {server_lit} \
       ORDER BY n.nspname, c.relname"
    );

    Spi::connect(|client| {
        let mut local = BTreeMap::<TableId, LocalNamespaceTable>::new();
        let mut errors = Vec::<AttachNamespaceRow>::new();

        let tuptable = client.select(&sql, None, &[]).map_err(|e| e.to_string())?;
        for row in tuptable {
            let local_schema = row
                .get_by_name::<String, _>("local_schema")
                .map_err(|e| e.to_string())?
                .unwrap_or_default();
            let local_table = row
                .get_by_name::<String, _>("local_table")
                .map_err(|e| e.to_string())?
                .unwrap_or_default();
            let relid_i64 = row
                .get_by_name::<i64, _>("relid")
                .map_err(|e| e.to_string())?
                .unwrap_or(0);
            let relid = pg_sys::Oid::from(relid_i64 as u32);
            let table_id_json = row
                .get_by_name::<String, _>("table_id_json")
                .map_err(|e| e.to_string())?;
            let batch_size = row
                .get_by_name::<String, _>("batch_size")
                .map_err(|e| e.to_string())?
                .and_then(|v| v.parse::<i64>().ok());

            let Some(table_id_json) = table_id_json else {
                errors.push(row_failed(
                    &[],
                    &local_schema,
                    &local_table,
                    "error",
                    &format!("missing required option: {}", OPT_NS_TABLE_ID),
                ));
                continue;
            };

            let table_id = match serde_json::from_str::<Vec<String>>(&table_id_json) {
                Ok(v) if !v.is_empty() => v,
                Ok(_) => {
                    errors.push(row_failed(
                        &[],
                        &local_schema,
                        &local_table,
                        "error",
                        &format!("invalid option: {} must not be empty", OPT_NS_TABLE_ID),
                    ));
                    continue;
                }
                Err(e) => {
                    errors.push(row_failed(
                        &[],
                        &local_schema,
                        &local_table,
                        "error",
                        &format!(
                            "invalid option: {} must be a JSON array of strings, error={}",
                            OPT_NS_TABLE_ID, e
                        ),
                    ));
                    continue;
                }
            };

            if !has_prefix(&table_id, root_namespace_id) {
                continue;
            }

            if local
                .insert(
                    table_id.clone(),
                    LocalNamespaceTable {
                        local_schema: local_schema.clone(),
                        local_table: local_table.clone(),
                        relid,
                        batch_size,
                    },
                )
                .is_some()
            {
                errors.push(row_failed(
                    &table_id,
                    &local_schema,
                    &local_table,
                    "error",
                    "duplicate local ns.table_id (multiple foreign tables point to the same table_id)",
                ));
            }
        }

        Ok((local, errors))
    })
}

fn has_prefix(full: &[String], prefix: &[String]) -> bool {
    full.len() >= prefix.len() && full[..prefix.len()] == *prefix
}

fn drop_foreign_table(schema: &str, table: &str) -> Result<(), String> {
    let sql = format!(
        "DROP FOREIGN TABLE {}.{};",
        quote_ident(schema),
        quote_ident(table)
    );
    Spi::run(&sql).map_err(|e| e.to_string())
}

fn detect_schema_drift(
    runtime: &Runtime,
    namespace: Arc<dyn LanceNamespace>,
    table_id: &[String],
    local_schema: &str,
    local_table: &str,
    relid: pg_sys::Oid,
) -> Result<Option<String>, String> {
    let expected = expected_columns(runtime, namespace.clone(), table_id, local_table)?;
    let actual = local_columns(relid)?;

    let expected_norm: Vec<(String, String)> = expected
        .into_iter()
        .map(|(n, t)| (n, canonical_type_string(&t)))
        .collect();
    let actual_norm: Vec<(String, String)> = actual
        .into_iter()
        .map(|(n, t)| (n, canonical_type_string(&t)))
        .collect();

    if expected_norm == actual_norm {
        return Ok(None);
    }

    Ok(Some(format!(
        "schema drift: {}",
        summarize_column_diff(local_schema, local_table, &expected_norm, &actual_norm)
    )))
}

fn expected_columns(
    runtime: &Runtime,
    namespace: Arc<dyn LanceNamespace>,
    table_id: &[String],
    local_table: &str,
) -> Result<Vec<(String, String)>, String> {
    let dataset = runtime
        .block_on(async {
            DatasetBuilder::from_namespace(namespace, table_id.to_vec(), false)
                .await?
                .load()
                .await
        })
        .map_err(|e| e.to_string())?;

    let lance_schema = dataset.schema();
    let arrow_fields: Vec<arrow::datatypes::Field> = lance_schema
        .fields
        .iter()
        .map(|f| arrow::datatypes::Field::new(f.name.clone(), f.data_type().clone(), f.nullable))
        .collect();

    let mapping = build_schema_mapping(local_table, &arrow_fields).map_err(|e| e.to_string())?;
    Ok(mapping.column_types)
}

fn local_columns(relid: pg_sys::Oid) -> Result<Vec<(String, String)>, String> {
    let sql = format!(
        "SELECT a.attname::text AS name, \
                format_type(a.atttypid, a.atttypmod)::text AS ty \
           FROM pg_attribute a \
          WHERE a.attrelid = {} \
            AND a.attnum > 0 \
            AND NOT a.attisdropped \
       ORDER BY a.attnum",
        relid
    );

    Spi::connect(|client| {
        let mut out = Vec::new();
        let tuptable = client.select(&sql, None, &[]).map_err(|e| e.to_string())?;
        for row in tuptable {
            let name = row
                .get_by_name::<String, _>("name")
                .map_err(|e| e.to_string())?
                .unwrap_or_default();
            let ty = row
                .get_by_name::<String, _>("ty")
                .map_err(|e| e.to_string())?
                .unwrap_or_default();
            out.push((name, ty));
        }
        Ok(out)
    })
}

fn canonical_type_string(input: &str) -> String {
    let mut s = input.trim().to_ascii_lowercase();
    s = s.replace('\"', "");
    s = s.split_whitespace().collect::<Vec<_>>().join(" ");

    let mut dims = 0usize;
    while let Some(stripped) = s.strip_suffix("[]") {
        dims += 1;
        s = stripped.trim_end().to_string();
    }

    let base = s.rsplit('.').next().unwrap_or("").trim();
    let base = match base {
        "boolean" => "boolean",
        "int2" | "smallint" => "int2",
        "int4" | "integer" => "int4",
        "int8" | "bigint" => "int8",
        "float4" | "real" => "float4",
        "float8" | "double precision" => "float8",
        "text" => "text",
        "bytea" => "bytea",
        "date" => "date",
        "timestamp" | "timestamp without time zone" => "timestamp",
        "timestamptz" | "timestamp with time zone" => "timestamptz",
        "numeric" => "numeric",
        "jsonb" => "jsonb",
        other => other,
    }
    .to_string();

    let mut out = base;
    for _ in 0..dims {
        out.push_str("[]");
    }
    out
}

fn summarize_column_diff(
    local_schema: &str,
    local_table: &str,
    expected: &[(String, String)],
    actual: &[(String, String)],
) -> String {
    if expected.len() != actual.len() {
        return format!(
            "{}.{} column count mismatch: expected={} actual={}",
            local_schema,
            local_table,
            expected.len(),
            actual.len()
        );
    }

    for (idx, (exp, act)) in expected.iter().zip(actual.iter()).enumerate() {
        if exp.0 != act.0 {
            return format!(
                "{}.{} column name mismatch at position {}: expected={} actual={}",
                local_schema,
                local_table,
                idx + 1,
                exp.0,
                act.0
            );
        }
        if exp.1 != act.1 {
            return format!(
                "{}.{} column type mismatch for {}: expected={} actual={}",
                local_schema, local_table, exp.0, exp.1, act.1
            );
        }
    }

    format!("{}.{} column definitions differ", local_schema, local_table)
}
