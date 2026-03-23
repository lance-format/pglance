use crate::write::storage::open_dataset;
use std::time::Instant;
use tokio::runtime::Runtime;

/// Execute `lance_delete` — delete rows from a Lance dataset matching a predicate.
///
/// Returns `(fragments_removed, duration_ms)`.
pub fn lance_delete_impl(
    uri: &str,
    predicate: &str,
    server_name: Option<&str>,
) -> Result<(i64, i64), String> {
    let start = Instant::now();

    let rt = Runtime::new().map_err(|e| format!("failed to create tokio runtime: {}", e))?;

    let fragments_removed = rt.block_on(async {
        let mut dataset = open_dataset(uri, server_name).await?;

        let old_fragments = dataset.count_fragments();

        dataset
            .delete(predicate)
            .await
            .map_err(|e| format!("lance delete failed: {}", e))?;

        // Reload to count remaining fragments
        let new_fragments = dataset.count_fragments();

        let removed = (old_fragments as i64) - (new_fragments as i64);
        Ok::<i64, String>(removed.max(0))
    })?;

    let duration_ms = start.elapsed().as_millis() as i64;
    Ok((fragments_removed, duration_ms))
}
