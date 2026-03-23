use crate::write::pg_to_arrow::spi_to_arrow_batches;
use crate::write::storage::open_dataset;
use lance_rs::dataset::{MergeInsertBuilder, WhenMatched, WhenNotMatched};
use std::sync::Arc;
use std::time::Instant;
use tokio::runtime::Runtime;

/// Execute `lance_merge_insert` — upsert rows from a PostgreSQL query into a Lance dataset.
///
/// Returns `(rows_merged, rows_inserted, rows_updated, duration_ms)`.
/// Note: The Lance SDK may not return separate insert/update counts. If unavailable,
/// `rows_inserted` and `rows_updated` will be -1 (indicating unknown).
pub fn lance_merge_insert_impl(
    uri: &str,
    source_query: &str,
    on_columns: Vec<String>,
    when_matched: &str,
    when_not_matched: &str,
    batch_size: usize,
    server_name: Option<&str>,
) -> Result<(i64, i64, i64, i64), String> {
    let start = Instant::now();

    if on_columns.is_empty() {
        return Err("on_columns must not be empty".to_string());
    }

    let (schema, batches, total_rows) = spi_to_arrow_batches(source_query, batch_size)?;

    if batches.is_empty() {
        return Ok((0, 0, 0, start.elapsed().as_millis() as i64));
    }

    let rt = Runtime::new().map_err(|e| format!("failed to create tokio runtime: {}", e))?;

    rt.block_on(async {
        let dataset = open_dataset(uri, server_name).await?;

        // Validate that on_columns exist in both source schema and Lance schema
        let lance_schema = dataset.schema();
        let lance_field_names: Vec<String> =
            lance_schema.fields.iter().map(|f| f.name.clone()).collect();

        for col in &on_columns {
            if schema.column_with_name(col).is_none() {
                return Err(format!(
                    "on_column '{}' not found in source query result (columns: {})",
                    col,
                    schema
                        .fields()
                        .iter()
                        .map(|f| f.name().as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                ));
            }
            if !lance_field_names.contains(col) {
                return Err(format!(
                    "on_column '{}' not found in Lance schema (columns: {})",
                    col,
                    lance_field_names.join(", ")
                ));
            }
        }

        let reader = arrow::record_batch::RecordBatchIterator::new(
            batches.into_iter().map(Ok),
            schema.clone(),
        );

        let dataset_arc = Arc::new(dataset);
        let mut builder = MergeInsertBuilder::try_new(dataset_arc, on_columns.clone())
            .map_err(|e| format!("MergeInsertBuilder::try_new failed: {}", e))?;

        match when_matched {
            "update" => {
                builder.when_matched(WhenMatched::UpdateAll);
            }
            "nothing" => {
                builder.when_matched(WhenMatched::DoNothing);
            }
            _ => {
                return Err(format!(
                    "invalid when_matched value '{}': must be 'update' or 'nothing'",
                    when_matched
                ));
            }
        }

        match when_not_matched {
            "insert" => {
                builder.when_not_matched(WhenNotMatched::InsertAll);
            }
            "nothing" => {
                builder.when_not_matched(WhenNotMatched::DoNothing);
            }
            _ => {
                return Err(format!(
                    "invalid when_not_matched value '{}': must be 'insert' or 'nothing'",
                    when_not_matched
                ));
            }
        }

        let (_dataset, _stats) = builder.try_build()
            .map_err(|e| format!("merge_insert try_build failed: {}", e))?
            .execute_reader(reader)
            .await
            .map_err(|e| format!("lance merge_insert failed: {}", e))?;

        Ok(())
    })?;

    let duration_ms = start.elapsed().as_millis() as i64;
    // Lance merge_insert does not provide separate insert/update counts
    Ok((total_rows as i64, -1, -1, duration_ms))
}
