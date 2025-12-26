use pgrx::prelude::*;
use pgrx::JsonB;

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

        if k == "uri"
            || k == "batch_size"
            || k.starts_with("aws_")
            || k.starts_with("s3_")
            || k.starts_with("ns.")
        {
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

#[pg_extern]
fn lance_attach_namespace(
    server_name: &str,
    root_namespace_id: default!(Vec<String>, "ARRAY[]::text[]"),
    schema_prefix: default!(&str, "'lance'"),
    batch_size: default!(Option<i64>, "NULL"),
    limit_per_list_call: default!(i32, "1000"),
    dry_run: default!(bool, "false"),
) -> TableIterator<
    'static,
    (
        name!(table_id, JsonB),
        name!(local_schema, String),
        name!(local_table, String),
        name!(action, String),
        name!(status, String),
        name!(detail, String),
    ),
> {
    let rows = fdw::attach_namespace::attach_namespace(
        server_name,
        root_namespace_id,
        schema_prefix,
        batch_size,
        limit_per_list_call,
        dry_run,
    )
    .unwrap_or_else(|e| pgrx::error!("lance_attach_namespace failed: {}", e));

    TableIterator::new(rows)
}

#[pg_extern]
fn lance_sync_namespace(
    server_name: &str,
    root_namespace_id: default!(Vec<String>, "ARRAY[]::text[]"),
    schema_prefix: default!(&str, "'lance'"),
    drop_missing: default!(bool, "false"),
    recreate_changed: default!(bool, "false"),
    dry_run: default!(bool, "false"),
) -> TableIterator<
    'static,
    (
        name!(table_id, JsonB),
        name!(local_schema, String),
        name!(local_table, String),
        name!(action, String),
        name!(status, String),
        name!(detail, String),
    ),
> {
    let rows = fdw::sync_namespace::sync_namespace(
        server_name,
        root_namespace_id,
        schema_prefix,
        drop_missing,
        recreate_changed,
        dry_run,
    )
    .unwrap_or_else(|e| pgrx::error!("lance_sync_namespace failed: {}", e));

    TableIterator::new(rows)
}

#[cfg(any(test, feature = "pg_test"))]
mod tests;

#[cfg(test)]
pub mod pg_test {
    pub fn setup(_options: Vec<&str>) {}

    #[must_use]
    pub fn postgresql_conf_options() -> Vec<&'static str> {
        vec![]
    }
}
