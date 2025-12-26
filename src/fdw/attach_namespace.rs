use crate::fdw::ddl::{create_composite_type_sql, create_foreign_table_sql, create_schema_sql};
use crate::fdw::namespace::connect_namespace;
use crate::fdw::naming::schema_and_table_for_table_id;
use crate::fdw::type_mapping::build_schema_mapping;
use lance_namespace::models::{ListNamespacesRequest, ListTablesRequest};
use lance_namespace::LanceNamespace;
use lance_rs::dataset::builder::DatasetBuilder;
use pgrx::pg_sys;
use pgrx::spi::Spi;
use pgrx::JsonB;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ffi::CString;
use tokio::runtime::Runtime;

const OPT_NS_TABLE_ID: &str = "ns.table_id";

pub type TableId = Vec<String>;
pub type AttachNamespaceRow = (JsonB, String, String, String, String, String);
type LocalMapping = (String, String);
type MappingEntry = (TableId, LocalMapping);
type MappingResult = (Vec<TableId>, Vec<MappingEntry>);

pub fn attach_namespace(
    server_name: &str,
    root_namespace_id: TableId,
    schema_prefix: &str,
    batch_size: Option<i64>,
    limit_per_list_call: i32,
    dry_run: bool,
) -> Result<Vec<AttachNamespaceRow>, String> {
    let runtime = Runtime::new().map_err(|e| e.to_string())?;
    let server_oid = foreign_server_oid_by_name(server_name)?;
    let namespace = connect_namespace(&runtime, server_oid)?;

    let table_ids = list_table_ids(
        &runtime,
        namespace.as_ref(),
        root_namespace_id,
        limit_per_list_call,
    )?;

    let (conflicts, mapping) = build_mapping(schema_prefix, &table_ids)?;

    let mut out = Vec::with_capacity(table_ids.len());
    for table_id in conflicts {
        out.push(row_error(
            &table_id,
            schema_prefix,
            "mapping conflict: multiple table_id map to the same (schema, table)",
        ));
    }

    let ctx = AttachContext {
        runtime: &runtime,
        namespace,
        server_name,
        batch_size,
        dry_run,
    };

    for (table_id, (local_schema, local_table)) in mapping {
        match attach_one(&ctx, &table_id, &local_schema, &local_table) {
            Ok(row) => out.push(row),
            Err(e) => out.push((
                JsonB(serde_json::to_value(&table_id).unwrap_or(serde_json::Value::Null)),
                local_schema.clone(),
                local_table.clone(),
                "error".to_string(),
                "failed".to_string(),
                e,
            )),
        }
    }

    Ok(out)
}

fn foreign_server_oid_by_name(server_name: &str) -> Result<pg_sys::Oid, String> {
    let c = CString::new(server_name).map_err(|_| "server_name contains NUL".to_string())?;
    let oid = unsafe { pg_sys::get_foreign_server_oid(c.as_ptr(), true) };
    if oid == pg_sys::InvalidOid {
        return Err(format!("foreign server not found: {}", server_name));
    }
    Ok(oid)
}

fn list_table_ids(
    runtime: &Runtime,
    namespace: &dyn LanceNamespace,
    root_namespace_id: TableId,
    limit_per_list_call: i32,
) -> Result<Vec<TableId>, String> {
    let mut out = Vec::new();

    let mut queue = VecDeque::<TableId>::new();
    let mut visited = BTreeSet::<TableId>::new();
    queue.push_back(root_namespace_id.clone());
    visited.insert(root_namespace_id);

    while let Some(ns_id) = queue.pop_front() {
        let mut page_token: Option<String> = None;
        loop {
            let request = ListTablesRequest {
                id: Some(ns_id.clone()),
                page_token: page_token.clone(),
                limit: Some(limit_per_list_call),
            };

            let response = runtime
                .block_on(async { namespace.list_tables(request).await })
                .map_err(|e| e.to_string())?;
            for table_name in response.tables {
                let mut tid = ns_id.clone();
                tid.push(table_name);
                out.push(tid);
            }

            match response.page_token {
                None => break,
                Some(tok) if tok.is_empty() => break,
                Some(tok) => page_token = Some(tok),
            }
        }

        let mut page_token: Option<String> = None;
        loop {
            let request = ListNamespacesRequest {
                id: Some(ns_id.clone()),
                page_token: page_token.clone(),
                limit: Some(limit_per_list_call),
            };

            let response = runtime
                .block_on(async { namespace.list_namespaces(request).await })
                .map_err(|e| e.to_string())?;
            for child in response.namespaces {
                let mut child_id = ns_id.clone();
                child_id.push(child);
                if visited.insert(child_id.clone()) {
                    queue.push_back(child_id);
                }
            }

            match response.page_token {
                None => break,
                Some(tok) if tok.is_empty() => break,
                Some(tok) => page_token = Some(tok),
            }
        }
    }

    Ok(out)
}

fn build_mapping(schema_prefix: &str, table_ids: &[TableId]) -> Result<MappingResult, String> {
    let mut by_local = BTreeMap::<LocalMapping, Vec<TableId>>::new();
    for table_id in table_ids {
        let (schema, table) = schema_and_table_for_table_id(schema_prefix, table_id)?;
        by_local
            .entry((schema, table))
            .or_default()
            .push(table_id.clone());
    }

    let mut conflicts = Vec::<TableId>::new();
    let mut mapping = Vec::<MappingEntry>::new();
    for ((schema, table), ids) in by_local {
        if ids.len() > 1 {
            conflicts.extend(ids);
        } else if let Some(id) = ids.into_iter().next() {
            mapping.push((id, (schema, table)));
        }
    }

    Ok((conflicts, mapping))
}

fn row_error(table_id: &[String], schema_prefix: &str, detail: &str) -> AttachNamespaceRow {
    let (schema, table) = schema_and_table_for_table_id(schema_prefix, table_id)
        .unwrap_or_else(|_| ("_".to_string(), "_".to_string()));
    (
        JsonB(serde_json::to_value(table_id).unwrap_or(serde_json::Value::Null)),
        schema,
        table,
        "error".to_string(),
        "failed".to_string(),
        detail.to_string(),
    )
}

struct AttachContext<'a> {
    runtime: &'a Runtime,
    namespace: std::sync::Arc<dyn LanceNamespace>,
    server_name: &'a str,
    batch_size: Option<i64>,
    dry_run: bool,
}

fn attach_one(
    ctx: &AttachContext<'_>,
    table_id: &[String],
    local_schema: &str,
    local_table: &str,
) -> Result<AttachNamespaceRow, String> {
    let table_id_json = serde_json::to_string(table_id).map_err(|e| e.to_string())?;

    if let Some(exists) = lookup_existing_relation(local_schema, local_table)? {
        return match exists {
            ExistingRelation::ForeignTable {
                server,
                ns_table_id,
            } => {
                if server != ctx.server_name {
                    Err(format!(
                        "foreign table already exists with different server: existing={} expected={}",
                        server, ctx.server_name
                    ))
                } else if !table_id_matches(table_id, ns_table_id.as_deref())? {
                    Err(format!(
                        "foreign table already exists with different {}: existing={:?} expected={}",
                        OPT_NS_TABLE_ID, ns_table_id, table_id_json
                    ))
                } else {
                    Ok((
                        JsonB(serde_json::to_value(table_id).unwrap_or(serde_json::Value::Null)),
                        local_schema.to_string(),
                        local_table.to_string(),
                        "skip_existing".to_string(),
                        "skipped".to_string(),
                        "already attached".to_string(),
                    ))
                }
            }
            ExistingRelation::Other { relkind } => Err(format!(
                "relation already exists with relkind='{}' (expected foreign table)",
                relkind
            )),
        };
    }

    if ctx.dry_run {
        return Ok((
            JsonB(serde_json::to_value(table_id).unwrap_or(serde_json::Value::Null)),
            local_schema.to_string(),
            local_table.to_string(),
            "create_table".to_string(),
            "skipped".to_string(),
            "dry_run".to_string(),
        ));
    }

    let dataset = ctx
        .runtime
        .block_on(async {
            DatasetBuilder::from_namespace(ctx.namespace.clone(), table_id.to_vec(), false)
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

    Spi::run(&create_schema_sql(local_schema)).map_err(|e| e.to_string())?;

    let ordered = crate::fdw::ddl::order_composite_types(&mapping.composite_types);
    for ty in ordered {
        let sql = create_composite_type_sql(local_schema, &ty);
        Spi::run(&sql).map_err(|e| e.to_string())?;
    }

    let mut options = vec![(OPT_NS_TABLE_ID.to_string(), table_id_json)];
    if let Some(bs) = ctx.batch_size {
        options.push(("batch_size".to_string(), bs.to_string()));
    }
    let table_sql = create_foreign_table_sql(
        ctx.server_name,
        local_schema,
        local_table,
        options,
        mapping.column_types,
    );
    Spi::run(&table_sql).map_err(|e| e.to_string())?;

    Ok((
        JsonB(serde_json::to_value(table_id).unwrap_or(serde_json::Value::Null)),
        local_schema.to_string(),
        local_table.to_string(),
        "create_table".to_string(),
        "ok".to_string(),
        "attached".to_string(),
    ))
}

fn table_id_matches(expected: &[String], actual_json: Option<&str>) -> Result<bool, String> {
    let Some(actual_json) = actual_json else {
        return Ok(false);
    };
    let actual = serde_json::from_str::<Vec<String>>(actual_json).map_err(|e| {
        format!(
            "invalid existing option: {} must be a JSON array of strings, error={}",
            OPT_NS_TABLE_ID, e
        )
    })?;
    Ok(actual == expected)
}

enum ExistingRelation {
    ForeignTable {
        server: String,
        ns_table_id: Option<String>,
    },
    Other {
        relkind: char,
    },
}

fn lookup_existing_relation(schema: &str, table: &str) -> Result<Option<ExistingRelation>, String> {
    let schema_lit = crate::fdw::ddl::quote_literal(schema);
    let table_lit = crate::fdw::ddl::quote_literal(table);

    let relkind_sql = format!(
        "SELECT ( \
            SELECT c.relkind::text \
            FROM pg_class c \
            JOIN pg_namespace n ON n.oid = c.relnamespace \
            WHERE n.nspname = {} AND c.relname = {} \
         )",
        schema_lit, table_lit
    );
    let relkind = Spi::get_one::<String>(&relkind_sql)
        .map_err(|e| e.to_string())?
        .map(|s| s.chars().next().unwrap_or('?'));
    let Some(relkind) = relkind else {
        return Ok(None);
    };

    if relkind != 'f' {
        return Ok(Some(ExistingRelation::Other { relkind }));
    }

    let details_sql = format!(
        "SELECT ( \
            SELECT fs.srvname \
            FROM pg_class c \
            JOIN pg_namespace n ON n.oid = c.relnamespace \
            JOIN pg_foreign_table ft ON ft.ftrelid = c.oid \
            JOIN pg_foreign_server fs ON fs.oid = ft.ftserver \
            WHERE n.nspname = {} AND c.relname = {} \
            LIMIT 1 \
         ), ( \
            SELECT opt.option_value \
            FROM pg_class c \
            JOIN pg_namespace n ON n.oid = c.relnamespace \
            JOIN pg_foreign_table ft ON ft.ftrelid = c.oid \
            LEFT JOIN LATERAL pg_options_to_table(ft.ftoptions) opt ON opt.option_name = {} \
            WHERE n.nspname = {} AND c.relname = {} \
            LIMIT 1 \
         )",
        schema_lit,
        table_lit,
        crate::fdw::ddl::quote_literal(OPT_NS_TABLE_ID),
        schema_lit,
        table_lit
    );

    let (server, ns_table_id) =
        Spi::get_two::<String, String>(&details_sql).map_err(|e| e.to_string())?;
    let server = server.unwrap_or_default();

    Ok(Some(ExistingRelation::ForeignTable {
        server,
        ns_table_id,
    }))
}
