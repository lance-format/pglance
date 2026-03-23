use arrow::array::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Float32Builder, Float64Builder,
    Int16Builder, Int32Builder, Int64Builder, RecordBatch, StringBuilder,
    TimestampMicrosecondBuilder,
};
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use pgrx::datum::{Date, Timestamp, TimestampWithTimeZone};
use pgrx::pg_sys;
use std::sync::Arc;

/// Epoch difference: Unix epoch (1970-01-01) to PostgreSQL epoch (2000-01-01) in seconds.
const UNIX_TO_POSTGRES_EPOCH_SECS: i64 = 946_684_800;

/// Map a PostgreSQL type OID to the corresponding Arrow DataType.
pub fn pg_oid_to_arrow_type(oid: pg_sys::Oid) -> Result<DataType, String> {
    match oid {
        pg_sys::BOOLOID => Ok(DataType::Boolean),
        pg_sys::INT2OID => Ok(DataType::Int16),
        pg_sys::INT4OID => Ok(DataType::Int32),
        pg_sys::INT8OID => Ok(DataType::Int64),
        pg_sys::FLOAT4OID => Ok(DataType::Float32),
        pg_sys::FLOAT8OID => Ok(DataType::Float64),
        pg_sys::NUMERICOID => Ok(DataType::Utf8), // numeric → string representation for Lance
        pg_sys::TEXTOID | pg_sys::VARCHAROID => Ok(DataType::Utf8),
        pg_sys::BYTEAOID => Ok(DataType::Binary),
        pg_sys::DATEOID => Ok(DataType::Date32),
        pg_sys::TIMESTAMPOID => Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
        pg_sys::TIMESTAMPTZOID => Ok(DataType::Timestamp(
            TimeUnit::Microsecond,
            Some("UTC".into()),
        )),
        pg_sys::JSONBOID => Ok(DataType::Utf8),
        _ => {
            let type_name = unsafe {
                let ptr = pg_sys::format_type_be(oid);
                if ptr.is_null() {
                    format!("oid {}", oid)
                } else {
                    let s = std::ffi::CStr::from_ptr(ptr).to_string_lossy().to_string();
                    pg_sys::pfree(ptr.cast());
                    s
                }
            };
            Err(format!("unsupported PostgreSQL type for write: {}", type_name))
        }
    }
}

/// Column metadata extracted from an SPI result set.
pub struct SpiColumnInfo {
    pub name: String,
    pub pg_oid: pg_sys::Oid,
    pub arrow_type: DataType,
}

/// Build an Arrow Schema from SPI column metadata.
pub fn build_arrow_schema(columns: &[SpiColumnInfo]) -> Schema {
    let fields: Vec<Field> = columns
        .iter()
        .map(|c| Field::new(&c.name, c.arrow_type.clone(), true))
        .collect();
    Schema::new(fields)
}

/// Extract column info from an SPI TupleTable's tuple descriptor.
///
/// # Safety
/// Caller must ensure we are in a valid SPI context with a valid tuple table.
pub unsafe fn extract_spi_columns(tuptable: &pgrx::spi::SpiTupleTable) -> Result<Vec<SpiColumnInfo>, String> {
    let ncols = tuptable.columns().map_err(|e| format!("failed to get column count: {}", e))?;
    if ncols == 0 {
        return Err("source_query returned zero columns".to_string());
    }

    let mut columns = Vec::with_capacity(ncols);
    for i in 1..=ncols {
        let name = tuptable.column_name(i).map_err(|e| format!("failed to get column name {}: {}", i, e))?;
        let oid_val = tuptable.column_type_oid(i).map_err(|e| format!("failed to get column type {}: {}", i, e))?;
        let oid = pg_sys::Oid::from(oid_val.value());
        let arrow_type = pg_oid_to_arrow_type(oid)?;
        columns.push(SpiColumnInfo {
            name,
            pg_oid: oid,
            arrow_type,
        });
    }
    Ok(columns)
}

enum TypedBuilder {
    Boolean(BooleanBuilder),
    Int16(Int16Builder),
    Int32(Int32Builder),
    Int64(Int64Builder),
    Float32(Float32Builder),
    Float64(Float64Builder),
    Utf8(StringBuilder),
    Binary(BinaryBuilder),
    Date32(Date32Builder),
    TimestampMicro(TimestampMicrosecondBuilder),
    TimestampMicroUtc(TimestampMicrosecondBuilder),
}

impl TypedBuilder {
    fn new(dt: &DataType) -> Result<Self, String> {
        match dt {
            DataType::Boolean => Ok(Self::Boolean(BooleanBuilder::new())),
            DataType::Int16 => Ok(Self::Int16(Int16Builder::new())),
            DataType::Int32 => Ok(Self::Int32(Int32Builder::new())),
            DataType::Int64 => Ok(Self::Int64(Int64Builder::new())),
            DataType::Float32 => Ok(Self::Float32(Float32Builder::new())),
            DataType::Float64 => Ok(Self::Float64(Float64Builder::new())),
            DataType::Utf8 => Ok(Self::Utf8(StringBuilder::new())),
            DataType::Binary => Ok(Self::Binary(BinaryBuilder::new())),
            DataType::Date32 => Ok(Self::Date32(Date32Builder::new())),
            DataType::Timestamp(TimeUnit::Microsecond, None) => {
                Ok(Self::TimestampMicro(TimestampMicrosecondBuilder::new()))
            }
            DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => Ok(Self::TimestampMicroUtc(
                TimestampMicrosecondBuilder::new().with_timezone("UTC"),
            )),
            _ => Err(format!("unsupported Arrow type for builder: {:?}", dt)),
        }
    }

    fn finish(&mut self) -> Arc<dyn arrow::array::Array> {
        match self {
            Self::Boolean(b) => Arc::new(b.finish()),
            Self::Int16(b) => Arc::new(b.finish()),
            Self::Int32(b) => Arc::new(b.finish()),
            Self::Int64(b) => Arc::new(b.finish()),
            Self::Float32(b) => Arc::new(b.finish()),
            Self::Float64(b) => Arc::new(b.finish()),
            Self::Utf8(b) => Arc::new(b.finish()),
            Self::Binary(b) => Arc::new(b.finish()),
            Self::Date32(b) => Arc::new(b.finish()),
            Self::TimestampMicro(b) => Arc::new(b.finish()),
            Self::TimestampMicroUtc(b) => Arc::new(b.finish()),
        }
    }
}

/// Append a single datum (by column index) from an SPI tuple into the typed builder.
///
/// # Safety
/// Must be called in a valid SPI context.
unsafe fn append_datum(
    builder: &mut TypedBuilder,
    heap_tuple: &pgrx::spi::SpiHeapTupleData,
    col_index: usize,
    pg_oid: pg_sys::Oid,
) -> Result<(), String> {
    match builder {
        TypedBuilder::Boolean(b) => {
            match heap_tuple.get_datum_by_ordinal(col_index + 1) {
                Ok(val) => match val.value::<bool>() {
                    Ok(Some(v)) => b.append_value(v),
                    Ok(None) => b.append_null(),
                    Err(_) => b.append_null(),
                },
                Err(_) => b.append_null(),
            }
        }
        TypedBuilder::Int16(b) => {
            match heap_tuple.get_datum_by_ordinal(col_index + 1) {
                Ok(val) => match val.value::<i16>() {
                    Ok(Some(v)) => b.append_value(v),
                    Ok(None) => b.append_null(),
                    Err(_) => b.append_null(),
                },
                Err(_) => b.append_null(),
            }
        }
        TypedBuilder::Int32(b) => {
            match heap_tuple.get_datum_by_ordinal(col_index + 1) {
                Ok(val) => match val.value::<i32>() {
                    Ok(Some(v)) => b.append_value(v),
                    Ok(None) => b.append_null(),
                    Err(_) => b.append_null(),
                },
                Err(_) => b.append_null(),
            }
        }
        TypedBuilder::Int64(b) => {
            match heap_tuple.get_datum_by_ordinal(col_index + 1) {
                Ok(val) => match val.value::<i64>() {
                    Ok(Some(v)) => b.append_value(v),
                    Ok(None) => b.append_null(),
                    Err(_) => b.append_null(),
                },
                Err(_) => b.append_null(),
            }
        }
        TypedBuilder::Float32(b) => {
            match heap_tuple.get_datum_by_ordinal(col_index + 1) {
                Ok(val) => match val.value::<f32>() {
                    Ok(Some(v)) => b.append_value(v),
                    Ok(None) => b.append_null(),
                    Err(_) => b.append_null(),
                },
                Err(_) => b.append_null(),
            }
        }
        TypedBuilder::Float64(b) => {
            match heap_tuple.get_datum_by_ordinal(col_index + 1) {
                Ok(val) => match val.value::<f64>() {
                    Ok(Some(v)) => b.append_value(v),
                    Ok(None) => b.append_null(),
                    Err(_) => b.append_null(),
                },
                Err(_) => b.append_null(),
            }
        }
        TypedBuilder::Utf8(b) => {
            // Handle text, varchar, numeric (as string), and jsonb (as string)
            match pg_oid {
                pg_sys::NUMERICOID => {
                    match heap_tuple.get_datum_by_ordinal(col_index + 1) {
                        Ok(val) => match val.value::<pgrx::AnyNumeric>() {
                            Ok(Some(v)) => b.append_value(v.to_string()),
                            Ok(None) => b.append_null(),
                            Err(_) => b.append_null(),
                        },
                        Err(_) => b.append_null(),
                    }
                }
                pg_sys::JSONBOID => {
                    match heap_tuple.get_datum_by_ordinal(col_index + 1) {
                        Ok(val) => match val.value::<pgrx::JsonB>() {
                            Ok(Some(v)) => {
                                b.append_value(serde_json::to_string(&v.0).unwrap_or_default())
                            }
                            Ok(None) => b.append_null(),
                            Err(_) => b.append_null(),
                        },
                        Err(_) => b.append_null(),
                    }
                }
                _ => {
                    match heap_tuple.get_datum_by_ordinal(col_index + 1) {
                        Ok(val) => match val.value::<String>() {
                            Ok(Some(v)) => b.append_value(&v),
                            Ok(None) => b.append_null(),
                            Err(_) => b.append_null(),
                        },
                        Err(_) => b.append_null(),
                    }
                }
            }
        }
        TypedBuilder::Binary(b) => {
            match heap_tuple.get_datum_by_ordinal(col_index + 1) {
                Ok(val) => match val.value::<Vec<u8>>() {
                    Ok(Some(v)) => b.append_value(&v),
                    Ok(None) => b.append_null(),
                    Err(_) => b.append_null(),
                },
                Err(_) => b.append_null(),
            }
        }
        TypedBuilder::Date32(b) => {
            match heap_tuple.get_datum_by_ordinal(col_index + 1) {
                Ok(val) => match val.value::<Date>() {
                    Ok(Some(date)) => {
                        // to_unix_epoch_days returns days since 1970-01-01
                        let days = date.to_unix_epoch_days();
                        b.append_value(days);
                    }
                    Ok(None) => b.append_null(),
                    Err(_) => b.append_null(),
                },
                Err(_) => b.append_null(),
            }
        }
        TypedBuilder::TimestampMicro(b) => {
            match heap_tuple.get_datum_by_ordinal(col_index + 1) {
                Ok(val) => match val.value::<Timestamp>() {
                    Ok(Some(ts)) => {
                        // pgrx Timestamp is microseconds since PG epoch (2000-01-01)
                        // Arrow Timestamp(Microsecond, None) is microseconds since Unix epoch
                        let pg_micros: i64 = ts.into();
                        let unix_micros =
                            pg_micros + UNIX_TO_POSTGRES_EPOCH_SECS.saturating_mul(1_000_000);
                        b.append_value(unix_micros);
                    }
                    Ok(None) => b.append_null(),
                    Err(_) => b.append_null(),
                },
                Err(_) => b.append_null(),
            }
        }
        TypedBuilder::TimestampMicroUtc(b) => {
            match heap_tuple.get_datum_by_ordinal(col_index + 1) {
                Ok(val) => match val.value::<TimestampWithTimeZone>() {
                    Ok(Some(ts)) => {
                        let pg_micros: i64 = ts.into();
                        let unix_micros =
                            pg_micros + UNIX_TO_POSTGRES_EPOCH_SECS.saturating_mul(1_000_000);
                        b.append_value(unix_micros);
                    }
                    Ok(None) => b.append_null(),
                    Err(_) => b.append_null(),
                },
                Err(_) => b.append_null(),
            }
        }
    }
    Ok(())
}

/// Convert an SPI result set into Arrow RecordBatches.
///
/// Executes the given `source_query` via SPI and converts rows into RecordBatches,
/// yielding one batch per `batch_size` rows.
///
/// Returns `(schema, batches, total_row_count)`.
pub fn spi_to_arrow_batches(
    source_query: &str,
    batch_size: usize,
) -> Result<(Arc<Schema>, Vec<RecordBatch>, u64), String> {
    let batch_size = batch_size.max(1);
    let mut all_batches = Vec::new();
    let mut total_rows: u64 = 0;
    let mut schema: Option<Arc<Schema>> = None;
    let mut columns_info: Option<Vec<SpiColumnInfo>> = None;

    pgrx::spi::Spi::connect(|client| {
        let tuptable = client
            .select(source_query, None, &[])
            .map_err(|e| format!("SPI query failed: {}", e))?;

        // Extract column info from the first call
        let cols = unsafe { extract_spi_columns(&tuptable)? };
        let arrow_schema = Arc::new(build_arrow_schema(&cols));
        schema = Some(arrow_schema.clone());

        let mut builders: Vec<TypedBuilder> = cols
            .iter()
            .map(|c| TypedBuilder::new(&c.arrow_type))
            .collect::<Result<Vec<_>, _>>()?;

        let mut rows_in_batch: usize = 0;

        for row in tuptable {
            for (col_idx, col) in cols.iter().enumerate() {
                unsafe {
                    append_datum(&mut builders[col_idx], &row, col_idx, col.pg_oid)?;
                }
            }
            rows_in_batch += 1;
            total_rows += 1;

            if rows_in_batch >= batch_size {
                let arrays: Vec<Arc<dyn arrow::array::Array>> =
                    builders.iter_mut().map(|b| b.finish()).collect();
                let batch = RecordBatch::try_new(arrow_schema.clone(), arrays)
                    .map_err(|e| format!("failed to create RecordBatch: {}", e))?;
                all_batches.push(batch);

                // Reset builders
                builders = cols
                    .iter()
                    .map(|c| TypedBuilder::new(&c.arrow_type))
                    .collect::<Result<Vec<_>, _>>()?;
                rows_in_batch = 0;
            }
        }

        // Flush remaining rows
        if rows_in_batch > 0 {
            let arrays: Vec<Arc<dyn arrow::array::Array>> =
                builders.iter_mut().map(|b| b.finish()).collect();
            let batch = RecordBatch::try_new(arrow_schema.clone(), arrays)
                .map_err(|e| format!("failed to create RecordBatch: {}", e))?;
            all_batches.push(batch);
        }

        columns_info = Some(cols);
        Ok::<_, String>(())
    })
    .map_err(|e| format!("SPI connect failed: {}", e))?;

    let schema = schema.ok_or_else(|| "no schema produced".to_string())?;
    Ok((schema, all_batches, total_rows))
}
