use crate::fdw::options::parse_defelem_list;
use lance_rs::dataset::builder::DatasetBuilder;
use pgrx::pg_sys;
use std::collections::BTreeMap;

/// Resolve S3/storage options from a named foreign server.
///
/// Looks up the foreign server by name, extracts any `aws_*` and `s3_*` options,
/// and returns them as key-value pairs suitable for passing as Lance storage options.
pub fn resolve_storage_options(server_name: &str) -> Result<BTreeMap<String, String>, String> {
    unsafe {
        let server_name_c = std::ffi::CString::new(server_name)
            .map_err(|_| "invalid server name".to_string())?;

        let server = pg_sys::GetForeignServerByName(server_name_c.as_ptr(), true);
        if server.is_null() {
            return Err(format!("foreign server '{}' not found", server_name));
        }

        let mut raw_opts = BTreeMap::<String, String>::new();
        parse_defelem_list((*server).options, &mut raw_opts);

        let mut storage_opts = BTreeMap::<String, String>::new();
        for (k, v) in &raw_opts {
            if k.starts_with("aws_") || k.starts_with("s3_") {
                storage_opts.insert(k.clone(), v.clone());
            }
        }
        Ok(storage_opts)
    }
}

/// Open an existing Lance dataset at the given URI, optionally using storage options
/// from a foreign server.
pub async fn open_dataset(
    uri: &str,
    server_name: Option<&str>,
) -> Result<lance_rs::Dataset, String> {
    let storage_opts = match server_name {
        Some(name) => resolve_storage_options(name)?,
        None => BTreeMap::new(),
    };

    let mut builder = DatasetBuilder::from_uri(uri);
    for (k, v) in &storage_opts {
        builder = builder.with_storage_option(k, v);
    }
    builder.load().await.map_err(|e| format!("failed to open dataset '{}': {}", uri, e))
}

/// Build a DatasetBuilder for a given URI with storage options, but don't load it.
/// Useful for write operations that take a builder or URI + options.
pub fn storage_options_vec(server_name: Option<&str>) -> Result<Vec<(String, String)>, String> {
    match server_name {
        Some(name) => {
            let opts = resolve_storage_options(name)?;
            Ok(opts.into_iter().collect())
        }
        None => Ok(Vec::new()),
    }
}
