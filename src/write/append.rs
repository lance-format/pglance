use crate::write::pg_to_arrow::spi_to_arrow_batches;
use crate::write::storage::storage_options_vec;
use lance_rs::dataset::WriteMode;
use lance_rs::io::{ObjectStoreParams, StorageOptionsAccessor};
use lance_rs::Dataset;
use std::sync::Arc;
use std::time::Instant;
use tokio::runtime::Runtime;

/// Execute `lance_append` — write rows from a PostgreSQL query into a Lance dataset.
///
/// Returns `(rows_written, duration_ms)`.
pub fn lance_append_impl(
    uri: &str,
    source_query: &str,
    mode: &str,
    batch_size: usize,
    server_name: Option<&str>,
) -> Result<(i64, i64), String> {
    let start = Instant::now();

    let (schema, batches, total_rows) = spi_to_arrow_batches(source_query, batch_size)?;

    if batches.is_empty() {
        return Ok((0, start.elapsed().as_millis() as i64));
    }

    let rt = Runtime::new().map_err(|e| format!("failed to create tokio runtime: {}", e))?;

    let storage_opts = storage_options_vec(server_name)?;

    rt.block_on(async {
        let write_mode = match mode {
            "create" => WriteMode::Create,
            "append" => WriteMode::Append,
            "overwrite" => WriteMode::Overwrite,
            _ => return Err(format!("invalid mode '{}': must be 'create', 'append', or 'overwrite'", mode)),
        };

        let reader =
            arrow::record_batch::RecordBatchIterator::new(batches.into_iter().map(Ok), schema);

        let mut params = lance_rs::dataset::WriteParams::default();
        params.mode = write_mode;

        // Apply storage options
        if !storage_opts.is_empty() {
            let opts_map: std::collections::HashMap<String, String> =
                storage_opts.into_iter().collect();
            params.store_params = Some(ObjectStoreParams {
                storage_options_accessor: Some(Arc::new(
                    StorageOptionsAccessor::with_static_options(opts_map),
                )),
                ..Default::default()
            });
        }

        Dataset::write(reader, uri, Some(params))
            .await
            .map_err(|e| format!("lance write failed: {}", e))?;

        Ok(())
    })?;

    let duration_ms = start.elapsed().as_millis() as i64;
    Ok((total_rows as i64, duration_ms))
}
