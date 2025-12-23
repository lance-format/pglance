use arrow::array::cast::AsArray;
use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array, Decimal256Array,
    FixedSizeBinaryArray, FixedSizeListArray, Float16Array, Float32Array, Float64Array,
    GenericListArray, Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray,
    LargeStringArray, StringArray, StructArray, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray, UInt16Array,
    UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, TimeUnit as ArrowTimeUnit};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use chrono::Datelike;
use pgrx::datum::{Date, Timestamp, TimestampWithTimeZone};
use pgrx::pg_sys;
use pgrx::prelude::IntoDatum;
use pgrx::AnyNumeric;
use pgrx::JsonB;
use serde_json::{Map, Number, Value};

const UNIX_TO_POSTGRES_EPOCH_SECS: i64 = 946_684_800;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConvertErrorKind {
    TypeMismatch,
    UnsupportedType,
    ValueOutOfRange,
    Internal,
}

#[derive(Debug)]
pub struct ConvertError {
    pub kind: ConvertErrorKind,
    pub message: String,
}

impl ConvertError {
    fn type_mismatch(message: impl Into<String>) -> Self {
        Self {
            kind: ConvertErrorKind::TypeMismatch,
            message: message.into(),
        }
    }

    fn unsupported_type(message: impl Into<String>) -> Self {
        Self {
            kind: ConvertErrorKind::UnsupportedType,
            message: message.into(),
        }
    }

    fn value_out_of_range(message: impl Into<String>) -> Self {
        Self {
            kind: ConvertErrorKind::ValueOutOfRange,
            message: message.into(),
        }
    }

    fn internal(message: impl Into<String>) -> Self {
        Self {
            kind: ConvertErrorKind::Internal,
            message: message.into(),
        }
    }
}

impl std::fmt::Display for ConvertError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

fn require_target_oid(
    target_type_oid: pg_sys::Oid,
    expected_oid: pg_sys::Oid,
    message: &'static str,
) -> Result<(), ConvertError> {
    if target_type_oid != expected_oid {
        return Err(ConvertError::type_mismatch(message));
    }
    Ok(())
}

pub fn validate_arrow_type_for_pg_oid(
    arrow_type: &DataType,
    target_type_oid: pg_sys::Oid,
) -> Result<(), ConvertError> {
    match arrow_type {
        DataType::Boolean => require_target_oid(
            target_type_oid,
            pg_sys::BOOLOID,
            "arrow boolean requires postgres boolean",
        ),
        DataType::Int8 | DataType::UInt8 | DataType::Int16 => require_target_oid(
            target_type_oid,
            pg_sys::INT2OID,
            "arrow int8/uint8/int16 requires postgres int2",
        ),
        DataType::UInt16 | DataType::Int32 => require_target_oid(
            target_type_oid,
            pg_sys::INT4OID,
            "arrow uint16/int32 requires postgres int4",
        ),
        DataType::UInt32 | DataType::Int64 | DataType::UInt64 => require_target_oid(
            target_type_oid,
            pg_sys::INT8OID,
            "arrow uint32/int64/uint64 requires postgres int8",
        ),
        DataType::Float16 | DataType::Float32 => require_target_oid(
            target_type_oid,
            pg_sys::FLOAT4OID,
            "arrow float16/float32 requires postgres float4",
        ),
        DataType::Float64 => require_target_oid(
            target_type_oid,
            pg_sys::FLOAT8OID,
            "arrow float64 requires postgres float8",
        ),
        DataType::Utf8 | DataType::LargeUtf8 => require_target_oid(
            target_type_oid,
            pg_sys::TEXTOID,
            "arrow utf8 requires postgres text",
        ),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => require_target_oid(
            target_type_oid,
            pg_sys::BYTEAOID,
            "arrow binary requires postgres bytea",
        ),
        DataType::Date32 | DataType::Date64 => require_target_oid(
            target_type_oid,
            pg_sys::DATEOID,
            "arrow date requires postgres date",
        ),
        DataType::Timestamp(_, tz) => {
            if tz.is_some() {
                require_target_oid(
                    target_type_oid,
                    pg_sys::TIMESTAMPTZOID,
                    "arrow timestamp with timezone requires postgres timestamptz",
                )
            } else {
                require_target_oid(
                    target_type_oid,
                    pg_sys::TIMESTAMPOID,
                    "arrow timestamp without timezone requires postgres timestamp",
                )
            }
        }
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => require_target_oid(
            target_type_oid,
            pg_sys::NUMERICOID,
            "arrow decimal requires postgres numeric",
        ),
        DataType::Dictionary(_, value) => validate_arrow_type_for_pg_oid(value.as_ref(), target_type_oid),
        DataType::List(elem) | DataType::LargeList(elem) | DataType::FixedSizeList(elem, _) => {
            if target_type_oid == pg_sys::JSONBOID {
                return Ok(());
            }
            let elem_oid = unsafe { pg_sys::get_element_type(target_type_oid) };
            if elem_oid == pg_sys::InvalidOid {
                return Err(ConvertError::type_mismatch(
                    "arrow list requires postgres array (or jsonb)",
                ));
            }
            validate_arrow_type_for_pg_oid(elem.data_type(), elem_oid)
        }
        DataType::Struct(fields) => unsafe {
            let tupdesc =
                pg_sys::lookup_type_cache(target_type_oid, pg_sys::TYPECACHE_TUPDESC as i32);
            if tupdesc.is_null() || (*tupdesc).tupDesc.is_null() {
                return Err(ConvertError::type_mismatch(
                    "arrow struct requires postgres composite type",
                ));
            }
            let tupdesc = (*tupdesc).tupDesc;
            let natts = (*tupdesc).natts as usize;
            for i in 0..natts {
                let attr = *(*tupdesc).attrs.as_ptr().add(i);
                if attr.attisdropped {
                    continue;
                }
                let name = std::ffi::CStr::from_ptr(attr.attname.data.as_ptr())
                    .to_string_lossy()
                    .to_string();
                let field = fields.iter().find(|f| f.name() == &name).ok_or_else(|| {
                    ConvertError::type_mismatch(format!("missing struct field: {}", name))
                })?;
                validate_arrow_type_for_pg_oid(field.data_type(), attr.atttypid)?;
            }
            Ok(())
        },
        DataType::Map(_, _) => require_target_oid(
            target_type_oid,
            pg_sys::JSONBOID,
            "arrow map requires postgres jsonb",
        ),
        _ => require_target_oid(
            target_type_oid,
            pg_sys::JSONBOID,
            "unsupported arrow type requires postgres jsonb",
        ),
    }
}

pub fn arrow_value_to_datum(
    array: &dyn Array,
    row_idx: usize,
    target_type_oid: pg_sys::Oid,
) -> Result<(pg_sys::Datum, bool), ConvertError> {
    if array.is_null(row_idx) {
        return Ok((pg_sys::Datum::from(0usize), true));
    }

    match array.data_type() {
        DataType::Boolean => {
            require_target_oid(
                target_type_oid,
                pg_sys::BOOLOID,
                "arrow boolean requires postgres boolean",
            )?;
            let v = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| ConvertError::internal("invalid boolean array"))?
                .value(row_idx);
            Ok((
                v.into_datum()
                    .ok_or_else(|| ConvertError::internal("failed to convert bool"))?,
                false,
            ))
        }
        DataType::Int8 => {
            require_target_oid(
                target_type_oid,
                pg_sys::INT2OID,
                "arrow int8 requires postgres int2",
            )?;
            let v = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| ConvertError::internal("invalid int8 array"))?
                .value(row_idx) as i16;
            Ok((
                v.into_datum()
                    .ok_or_else(|| ConvertError::internal("failed to convert int8"))?,
                false,
            ))
        }
        DataType::Int16 => {
            require_target_oid(
                target_type_oid,
                pg_sys::INT2OID,
                "arrow int16 requires postgres int2",
            )?;
            let v = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| ConvertError::internal("invalid int16 array"))?
                .value(row_idx);
            Ok((
                v.into_datum()
                    .ok_or_else(|| ConvertError::internal("failed to convert int16"))?,
                false,
            ))
        }
        DataType::Int32 => {
            require_target_oid(
                target_type_oid,
                pg_sys::INT4OID,
                "arrow int32 requires postgres int4",
            )?;
            let v = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| ConvertError::internal("invalid int32 array"))?
                .value(row_idx);
            Ok((
                v.into_datum()
                    .ok_or_else(|| ConvertError::internal("failed to convert int32"))?,
                false,
            ))
        }
        DataType::Int64 => {
            require_target_oid(
                target_type_oid,
                pg_sys::INT8OID,
                "arrow int64 requires postgres int8",
            )?;
            let v = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| ConvertError::internal("invalid int64 array"))?
                .value(row_idx);
            Ok((
                v.into_datum()
                    .ok_or_else(|| ConvertError::internal("failed to convert int64"))?,
                false,
            ))
        }
        DataType::UInt8 => {
            require_target_oid(
                target_type_oid,
                pg_sys::INT2OID,
                "arrow uint8 requires postgres int2",
            )?;
            let v = array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| ConvertError::internal("invalid uint8 array"))?
                .value(row_idx) as i16;
            Ok((
                v.into_datum()
                    .ok_or_else(|| ConvertError::internal("failed to convert uint8"))?,
                false,
            ))
        }
        DataType::UInt16 => {
            require_target_oid(
                target_type_oid,
                pg_sys::INT4OID,
                "arrow uint16 requires postgres int4",
            )?;
            let v = array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| ConvertError::internal("invalid uint16 array"))?
                .value(row_idx) as i32;
            Ok((
                v.into_datum()
                    .ok_or_else(|| ConvertError::internal("failed to convert uint16"))?,
                false,
            ))
        }
        DataType::UInt32 => {
            require_target_oid(
                target_type_oid,
                pg_sys::INT8OID,
                "arrow uint32 requires postgres int8",
            )?;
            let v = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| ConvertError::internal("invalid uint32 array"))?
                .value(row_idx) as i64;
            Ok((
                v.into_datum()
                    .ok_or_else(|| ConvertError::internal("failed to convert uint32"))?,
                false,
            ))
        }
        DataType::UInt64 => {
            require_target_oid(
                target_type_oid,
                pg_sys::INT8OID,
                "arrow uint64 requires postgres int8",
            )?;
            let v = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| ConvertError::internal("invalid uint64 array"))?
                .value(row_idx);
            if v > i64::MAX as u64 {
                return Err(ConvertError::value_out_of_range(
                    "arrow uint64 value out of range for postgres int8",
                ));
            }
            Ok((
                (v as i64)
                    .into_datum()
                    .ok_or_else(|| ConvertError::internal("failed to convert uint64"))?,
                false,
            ))
        }
        DataType::Float16 => {
            require_target_oid(
                target_type_oid,
                pg_sys::FLOAT4OID,
                "arrow float16 requires postgres float4",
            )?;
            let v = array
                .as_any()
                .downcast_ref::<Float16Array>()
                .ok_or_else(|| ConvertError::internal("invalid float16 array"))?
                .value(row_idx)
                .to_f32();
            Ok((
                v.into_datum()
                    .ok_or_else(|| ConvertError::internal("failed to convert float16"))?,
                false,
            ))
        }
        DataType::Float32 => {
            require_target_oid(
                target_type_oid,
                pg_sys::FLOAT4OID,
                "arrow float32 requires postgres float4",
            )?;
            let v = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| ConvertError::internal("invalid float32 array"))?
                .value(row_idx);
            Ok((
                v.into_datum()
                    .ok_or_else(|| ConvertError::internal("failed to convert float32"))?,
                false,
            ))
        }
        DataType::Float64 => {
            require_target_oid(
                target_type_oid,
                pg_sys::FLOAT8OID,
                "arrow float64 requires postgres float8",
            )?;
            let v = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| ConvertError::internal("invalid float64 array"))?
                .value(row_idx);
            Ok((
                v.into_datum()
                    .ok_or_else(|| ConvertError::internal("failed to convert float64"))?,
                false,
            ))
        }
        DataType::Utf8 => {
            require_target_oid(
                target_type_oid,
                pg_sys::TEXTOID,
                "arrow utf8 requires postgres text",
            )?;
            let v = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| ConvertError::internal("invalid utf8 array"))?
                .value(row_idx);
            Ok((
                v.to_string()
                    .into_datum()
                    .ok_or_else(|| ConvertError::internal("failed to convert text"))?,
                false,
            ))
        }
        DataType::LargeUtf8 => {
            require_target_oid(
                target_type_oid,
                pg_sys::TEXTOID,
                "arrow large utf8 requires postgres text",
            )?;
            let v = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| ConvertError::internal("invalid large utf8 array"))?
                .value(row_idx);
            Ok((
                v.to_string()
                    .into_datum()
                    .ok_or_else(|| ConvertError::internal("failed to convert text"))?,
                false,
            ))
        }
        DataType::Binary => {
            require_target_oid(
                target_type_oid,
                pg_sys::BYTEAOID,
                "arrow binary requires postgres bytea",
            )?;
            let v = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| ConvertError::internal("invalid binary array"))?
                .value(row_idx);
            Ok((
                v.to_vec()
                    .into_datum()
                    .ok_or_else(|| ConvertError::internal("failed to convert bytea"))?,
                false,
            ))
        }
        DataType::LargeBinary => {
            require_target_oid(
                target_type_oid,
                pg_sys::BYTEAOID,
                "arrow large binary requires postgres bytea",
            )?;
            let v = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| ConvertError::internal("invalid large binary array"))?
                .value(row_idx);
            Ok((
                v.to_vec()
                    .into_datum()
                    .ok_or_else(|| ConvertError::internal("failed to convert bytea"))?,
                false,
            ))
        }
        DataType::FixedSizeBinary(_) => {
            require_target_oid(
                target_type_oid,
                pg_sys::BYTEAOID,
                "arrow fixed size binary requires postgres bytea",
            )?;
            let v = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| ConvertError::internal("invalid fixed size binary array"))?
                .value(row_idx);
            Ok((
                v.to_vec()
                    .into_datum()
                    .ok_or_else(|| ConvertError::internal("failed to convert bytea"))?,
                false,
            ))
        }
        DataType::Date32 => {
            require_target_oid(
                target_type_oid,
                pg_sys::DATEOID,
                "arrow date32 requires postgres date",
            )?;
            let days = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or_else(|| ConvertError::internal("invalid date32 array"))?
                .value(row_idx);
            let base = chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                .ok_or_else(|| ConvertError::internal("invalid epoch"))?;
            let dt = base
                .checked_add_signed(chrono::Duration::days(days as i64))
                .ok_or_else(|| ConvertError::value_out_of_range("arrow date32 overflow"))?;
            let date = Date::new(dt.year(), dt.month() as u8, dt.day() as u8)
                .map_err(|_| ConvertError::internal("invalid date"))?;
            Ok((
                date.into_datum()
                    .ok_or_else(|| ConvertError::internal("failed to convert date"))?,
                false,
            ))
        }
        DataType::Date64 => {
            require_target_oid(
                target_type_oid,
                pg_sys::DATEOID,
                "arrow date64 requires postgres date",
            )?;
            let millis = array
                .as_any()
                .downcast_ref::<Date64Array>()
                .ok_or_else(|| ConvertError::internal("invalid date64 array"))?
                .value(row_idx);
            let dt = chrono::DateTime::from_timestamp_millis(millis)
                .ok_or_else(|| ConvertError::value_out_of_range("arrow date64 overflow"))?;
            let date = Date::new(dt.year(), dt.month() as u8, dt.day() as u8)
                .map_err(|_| ConvertError::internal("invalid date"))?;
            Ok((
                date.into_datum()
                    .ok_or_else(|| ConvertError::internal("failed to convert date"))?,
                false,
            ))
        }
        DataType::Timestamp(unit, tz) => {
            validate_arrow_type_for_pg_oid(array.data_type(), target_type_oid)?;
            let unix_micros = match unit {
                ArrowTimeUnit::Second => {
                    let secs = array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .ok_or_else(|| ConvertError::internal("invalid timestamp(s) array"))?
                        .value(row_idx);
                    secs.saturating_mul(1_000_000)
                }
                ArrowTimeUnit::Millisecond => {
                    let millis = array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .ok_or_else(|| ConvertError::internal("invalid timestamp(ms) array"))?
                        .value(row_idx);
                    millis.saturating_mul(1_000)
                }
                ArrowTimeUnit::Microsecond => array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or_else(|| ConvertError::internal("invalid timestamp(us) array"))?
                    .value(row_idx),
                ArrowTimeUnit::Nanosecond => {
                    let nanos = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .ok_or_else(|| ConvertError::internal("invalid timestamp(ns) array"))?
                        .value(row_idx);
                    nanos / 1_000
                }
            };

            let pg_micros = unix_micros - UNIX_TO_POSTGRES_EPOCH_SECS.saturating_mul(1_000_000);

            if tz.is_some() {
                let ts = TimestampWithTimeZone::try_from(pg_micros)
                    .map_err(|_| ConvertError::value_out_of_range("invalid timestamptz"))?;
                Ok((
                    ts.into_datum()
                        .ok_or_else(|| ConvertError::internal("failed to convert timestamptz"))?,
                    false,
                ))
            } else {
                let ts = Timestamp::try_from(pg_micros)
                    .map_err(|_| ConvertError::value_out_of_range("invalid timestamp"))?;
                Ok((
                    ts.into_datum()
                        .ok_or_else(|| ConvertError::internal("failed to convert timestamp"))?,
                    false,
                ))
            }
        }
        DataType::Decimal128(_, _) => {
            require_target_oid(
                target_type_oid,
                pg_sys::NUMERICOID,
                "arrow decimal128 requires postgres numeric",
            )?;
            let v = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| ConvertError::internal("invalid decimal128 array"))?
                .value_as_string(row_idx);
            let numeric =
                AnyNumeric::try_from(v.as_str()).map_err(|_| ConvertError::internal("invalid numeric"))?;
            Ok((
                numeric.into_datum()
                    .ok_or_else(|| ConvertError::internal("failed to convert numeric"))?,
                false,
            ))
        }
        DataType::Decimal256(_, _) => {
            require_target_oid(
                target_type_oid,
                pg_sys::NUMERICOID,
                "arrow decimal256 requires postgres numeric",
            )?;
            let v = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| ConvertError::internal("invalid decimal256 array"))?
                .value_as_string(row_idx);
            let numeric =
                AnyNumeric::try_from(v.as_str()).map_err(|_| ConvertError::internal("invalid numeric"))?;
            Ok((
                numeric.into_datum()
                    .ok_or_else(|| ConvertError::internal("failed to convert numeric"))?,
                false,
            ))
        }
        DataType::Dictionary(_, _) => {
            let dict = array
                .as_any_dictionary_opt()
                .ok_or_else(|| ConvertError::internal("invalid dictionary array"))?;
            let value_idx = dictionary_key_to_usize(dict.keys(), row_idx)?;
            let values = dict.values().as_ref();
            if value_idx >= values.len() {
                return Err(ConvertError::internal("dictionary key out of range"));
            }
            arrow_value_to_datum(values, value_idx, target_type_oid)
        }
        DataType::List(_) | DataType::LargeList(_) => {
            let elem_oid = unsafe { pg_sys::get_element_type(target_type_oid) };
            if elem_oid == pg_sys::InvalidOid {
                if target_type_oid == pg_sys::JSONBOID {
                    let json = arrow_value_to_json(array, row_idx)?;
                    return Ok((
                        JsonB(json)
                            .into_datum()
                            .ok_or_else(|| ConvertError::internal("failed to convert jsonb"))?,
                        false,
                    ));
                }
                return Err(ConvertError::type_mismatch(
                    "arrow list requires postgres array (or jsonb)",
                ));
            }
            list_to_array_datum(array, row_idx, target_type_oid, elem_oid)
        }
        DataType::FixedSizeList(_, _) => {
            let elem_oid = unsafe { pg_sys::get_element_type(target_type_oid) };
            if elem_oid == pg_sys::InvalidOid {
                if target_type_oid == pg_sys::JSONBOID {
                    let json = arrow_value_to_json(array, row_idx)?;
                    return Ok((
                        JsonB(json)
                            .into_datum()
                            .ok_or_else(|| ConvertError::internal("failed to convert jsonb"))?,
                        false,
                    ));
                }
                return Err(ConvertError::type_mismatch(
                    "arrow fixed size list requires postgres array (or jsonb)",
                ));
            }
            fixed_size_list_to_array_datum(array, row_idx, target_type_oid, elem_oid)
        }
        DataType::Struct(_) => {
            let struct_array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| ConvertError::internal("invalid struct array"))?;
            struct_to_composite_datum(struct_array, row_idx, target_type_oid)
        }
        DataType::Map(_, _) => {
            require_target_oid(
                target_type_oid,
                pg_sys::JSONBOID,
                "arrow map requires postgres jsonb",
            )?;
            let json = arrow_value_to_json(array, row_idx)?;
            Ok((
                JsonB(json)
                    .into_datum()
                    .ok_or_else(|| ConvertError::internal("failed to convert jsonb"))?,
                false,
            ))
        }
        _ => {
            require_target_oid(
                target_type_oid,
                pg_sys::JSONBOID,
                "unsupported arrow type requires postgres jsonb",
            )?;
            let json = arrow_value_to_json(array, row_idx)?;
            Ok((
                JsonB(json)
                    .into_datum()
                    .ok_or_else(|| ConvertError::internal("failed to convert jsonb"))?,
                false,
            ))
        }
    }
}

fn list_to_array_datum(
    array: &dyn Array,
    row_idx: usize,
    array_type_oid: pg_sys::Oid,
    elem_oid: pg_sys::Oid,
) -> Result<(pg_sys::Datum, bool), ConvertError> {
    fn handle_list<OffsetSize: arrow::array::OffsetSizeTrait>(
        array: &dyn Array,
        row_idx: usize,
        array_type_oid: pg_sys::Oid,
        elem_oid: pg_sys::Oid,
    ) -> Result<(pg_sys::Datum, bool), ConvertError> {
        let list_array = array
            .as_any()
            .downcast_ref::<GenericListArray<OffsetSize>>()
            .ok_or_else(|| ConvertError::internal("invalid list array"))?;
        let values = list_array.value(row_idx);
        array_values_to_pg_array(values.as_ref(), array_type_oid, elem_oid)
    }

    match array.data_type() {
        DataType::List(_) => handle_list::<i32>(array, row_idx, array_type_oid, elem_oid),
        DataType::LargeList(_) => handle_list::<i64>(array, row_idx, array_type_oid, elem_oid),
        _ => Err(ConvertError::internal("not a list array")),
    }
}

fn fixed_size_list_to_array_datum(
    array: &dyn Array,
    row_idx: usize,
    array_type_oid: pg_sys::Oid,
    elem_oid: pg_sys::Oid,
) -> Result<(pg_sys::Datum, bool), ConvertError> {
    let list_array = array
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or_else(|| ConvertError::internal("invalid fixed size list array"))?;
    let values = list_array.value(row_idx);
    array_values_to_pg_array(values.as_ref(), array_type_oid, elem_oid)
}

fn array_values_to_pg_array(
    values: &dyn Array,
    array_type_oid: pg_sys::Oid,
    elem_oid: pg_sys::Oid,
) -> Result<(pg_sys::Datum, bool), ConvertError> {
    let _ = array_type_oid;
    let len = values.len();
    let mut datums = Vec::with_capacity(len);
    let mut nulls = Vec::with_capacity(len);

    for i in 0..len {
        let (d, isnull) = arrow_value_to_datum(values, i, elem_oid)?;
        datums.push(d);
        nulls.push(isnull);
    }

    unsafe {
        let mut typlen: i16 = 0;
        let mut typbyval: bool = false;
        let mut typalign: std::os::raw::c_char = 0;
        pg_sys::get_typlenbyvalalign(elem_oid, &mut typlen, &mut typbyval, &mut typalign);

        let mut dims = [len as i32];
        let mut lbs = [1i32];
        let arr = pg_sys::construct_md_array(
            datums.as_mut_ptr(),
            nulls.as_mut_ptr(),
            1,
            dims.as_mut_ptr(),
            lbs.as_mut_ptr(),
            elem_oid,
            typlen as i32,
            typbyval,
            typalign,
        );
        if arr.is_null() {
            return Err(ConvertError::internal("failed to construct array"));
        }

        let datum = pg_sys::Datum::from(arr);
        Ok((datum, false))
    }
}

fn struct_to_composite_datum(
    struct_array: &StructArray,
    row_idx: usize,
    composite_type_oid: pg_sys::Oid,
) -> Result<(pg_sys::Datum, bool), ConvertError> {
    unsafe {
        let tupdesc =
            pg_sys::lookup_type_cache(composite_type_oid, pg_sys::TYPECACHE_TUPDESC as i32);
        if tupdesc.is_null() || (*tupdesc).tupDesc.is_null() {
            return Err(ConvertError::type_mismatch(
                "failed to lookup composite tupdesc",
            ));
        }
        let tupdesc = (*tupdesc).tupDesc;

        let natts = (*tupdesc).natts as usize;
        let mut values = vec![pg_sys::Datum::from(0usize); natts];
        let mut nulls = vec![true; natts];

        for i in 0..natts {
            let attr = *(*tupdesc).attrs.as_ptr().add(i);
            if attr.attisdropped {
                values[i] = pg_sys::Datum::from(0usize);
                nulls[i] = true;
                continue;
            }

            let name = std::ffi::CStr::from_ptr(attr.attname.data.as_ptr())
                .to_string_lossy()
                .to_string();

            let idx = struct_array
                .fields()
                .iter()
                .position(|f| f.name() == &name)
                .ok_or_else(|| {
                    ConvertError::type_mismatch(format!("struct field name mismatch: {}", name))
                })?;
            let col = struct_array.column(idx);
            let (d, isnull) = arrow_value_to_datum(col.as_ref(), row_idx, attr.atttypid)?;
            values[i] = d;
            nulls[i] = isnull;
        }

        let htup = pg_sys::heap_form_tuple(tupdesc, values.as_mut_ptr(), nulls.as_mut_ptr());
        if htup.is_null() {
            return Err(ConvertError::internal("failed to form heap tuple"));
        }
        let datum = pg_sys::HeapTupleGetDatum(htup);
        Ok((datum, false))
    }
}

fn arrow_value_to_json(array: &dyn Array, row_idx: usize) -> Result<Value, ConvertError> {
    if array.is_null(row_idx) {
        return Ok(Value::Null);
    }

    match array.data_type() {
        DataType::Boolean => Ok(Value::Bool(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or_else(|| ConvertError::internal("invalid boolean array"))?
                .value(row_idx),
        )),
        DataType::Int8 => Ok(json_number(
            array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| ConvertError::internal("invalid int8 array"))?
                .value(row_idx) as i64,
        )),
        DataType::Int16 => Ok(json_number(
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| ConvertError::internal("invalid int16 array"))?
                .value(row_idx) as i64,
        )),
        DataType::Int32 => Ok(json_number(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| ConvertError::internal("invalid int32 array"))?
                .value(row_idx) as i64,
        )),
        DataType::Int64 => Ok(json_number(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| ConvertError::internal("invalid int64 array"))?
                .value(row_idx),
        )),
        DataType::UInt8 => Ok(json_number(
            array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or_else(|| ConvertError::internal("invalid uint8 array"))?
                .value(row_idx) as i64,
        )),
        DataType::UInt16 => Ok(json_number(
            array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or_else(|| ConvertError::internal("invalid uint16 array"))?
                .value(row_idx) as i64,
        )),
        DataType::UInt32 => Ok(json_number(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| ConvertError::internal("invalid uint32 array"))?
                .value(row_idx) as i64,
        )),
        DataType::UInt64 => Ok(json_number(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or_else(|| ConvertError::internal("invalid uint64 array"))?
                .value(row_idx) as i64,
        )),
        DataType::Float16 => {
            let v = array
                .as_any()
                .downcast_ref::<Float16Array>()
                .ok_or_else(|| ConvertError::internal("invalid float16 array"))?
                .value(row_idx)
                .to_f32() as f64;
            Ok(Number::from_f64(v)
                .map(Value::Number)
                .unwrap_or(Value::Null))
        }
        DataType::Float32 => {
            let v = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or_else(|| ConvertError::internal("invalid float32 array"))?
                .value(row_idx) as f64;
            Ok(Number::from_f64(v)
                .map(Value::Number)
                .unwrap_or(Value::Null))
        }
        DataType::Float64 => {
            let v = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| ConvertError::internal("invalid float64 array"))?
                .value(row_idx);
            Ok(Number::from_f64(v)
                .map(Value::Number)
                .unwrap_or(Value::Null))
        }
        DataType::Utf8 => {
            let v = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| ConvertError::internal("invalid utf8 array"))?
                .value(row_idx);
            Ok(Value::String(v.to_string()))
        }
        DataType::LargeUtf8 => {
            let v = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or_else(|| ConvertError::internal("invalid large utf8 array"))?
                .value(row_idx);
            Ok(Value::String(v.to_string()))
        }
        DataType::Binary => {
            let v = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| ConvertError::internal("invalid binary array"))?
                .value(row_idx);
            Ok(Value::String(STANDARD.encode(v)))
        }
        DataType::LargeBinary => {
            let v = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or_else(|| ConvertError::internal("invalid large binary array"))?
                .value(row_idx);
            Ok(Value::String(STANDARD.encode(v)))
        }
        DataType::FixedSizeBinary(_) => {
            let v = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or_else(|| ConvertError::internal("invalid fixed size binary array"))?
                .value(row_idx);
            Ok(Value::String(STANDARD.encode(v)))
        }
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            let mut out = Vec::new();
            let values = match array.data_type() {
                DataType::List(_) => {
                    let l = array
                        .as_any()
                        .downcast_ref::<GenericListArray<i32>>()
                        .ok_or_else(|| ConvertError::internal("invalid list array"))?;
                    l.value(row_idx)
                }
                DataType::LargeList(_) => {
                    let l = array
                        .as_any()
                        .downcast_ref::<GenericListArray<i64>>()
                        .ok_or_else(|| ConvertError::internal("invalid large list array"))?;
                    l.value(row_idx)
                }
                DataType::FixedSizeList(_, _) => {
                    let l = array
                        .as_any()
                        .downcast_ref::<FixedSizeListArray>()
                        .ok_or_else(|| ConvertError::internal("invalid fixed size list array"))?;
                    l.value(row_idx)
                }
                _ => return Err(ConvertError::internal("invalid list type")),
            };
            for i in 0..values.len() {
                out.push(arrow_value_to_json(values.as_ref(), i)?);
            }
            Ok(Value::Array(out))
        }
        DataType::Struct(fields) => {
            let struct_array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or_else(|| ConvertError::internal("invalid struct array"))?;
            let mut map = Map::new();
            for (i, f) in fields.iter().enumerate() {
                let col = struct_array.column(i);
                map.insert(
                    f.name().clone(),
                    arrow_value_to_json(col.as_ref(), row_idx)?,
                );
            }
            Ok(Value::Object(map))
        }
        DataType::Decimal128(_, _) => {
            let v = array
                .as_any()
                .downcast_ref::<Decimal128Array>()
                .ok_or_else(|| ConvertError::internal("invalid decimal128 array"))?
                .value_as_string(row_idx);
            Ok(Value::String(v))
        }
        DataType::Decimal256(_, _) => {
            let v = array
                .as_any()
                .downcast_ref::<Decimal256Array>()
                .ok_or_else(|| ConvertError::internal("invalid decimal256 array"))?
                .value_as_string(row_idx);
            Ok(Value::String(v))
        }
        DataType::Dictionary(_, _) => {
            let dict = array
                .as_any_dictionary_opt()
                .ok_or_else(|| ConvertError::internal("invalid dictionary array"))?;
            let value_idx = dictionary_key_to_usize(dict.keys(), row_idx)?;
            let values = dict.values().as_ref();
            if value_idx >= values.len() {
                return Err(ConvertError::internal("dictionary key out of range"));
            }
            arrow_value_to_json(values, value_idx)
        }
        _ => Ok(Value::String(format!(
            "<unsupported_type: {:?}>",
            array.data_type()
        ))),
    }
}

fn json_number(v: i64) -> Value {
    Value::Number(Number::from(v))
}

fn dictionary_key_to_usize(keys: &dyn Array, row_idx: usize) -> Result<usize, ConvertError> {
    match keys.data_type() {
        DataType::Int8 => {
            let v = keys
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or_else(|| ConvertError::internal("invalid dictionary keys (int8)"))?
                .value(row_idx) as i64;
            usize::try_from(v).map_err(|_| ConvertError::internal("negative dictionary key"))
        }
        DataType::Int16 => {
            let v = keys
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or_else(|| ConvertError::internal("invalid dictionary keys (int16)"))?
                .value(row_idx) as i64;
            usize::try_from(v).map_err(|_| ConvertError::internal("negative dictionary key"))
        }
        DataType::Int32 => {
            let v = keys
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or_else(|| ConvertError::internal("invalid dictionary keys (int32)"))?
                .value(row_idx) as i64;
            usize::try_from(v).map_err(|_| ConvertError::internal("negative dictionary key"))
        }
        DataType::Int64 => {
            let v = keys
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or_else(|| ConvertError::internal("invalid dictionary keys (int64)"))?
                .value(row_idx);
            usize::try_from(v).map_err(|_| ConvertError::internal("negative dictionary key"))
        }
        DataType::UInt8 => Ok(keys
            .as_any()
            .downcast_ref::<UInt8Array>()
            .ok_or_else(|| ConvertError::internal("invalid dictionary keys (uint8)"))?
            .value(row_idx) as usize),
        DataType::UInt16 => Ok(keys
            .as_any()
            .downcast_ref::<UInt16Array>()
            .ok_or_else(|| ConvertError::internal("invalid dictionary keys (uint16)"))?
            .value(row_idx) as usize),
        DataType::UInt32 => Ok(keys
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| ConvertError::internal("invalid dictionary keys (uint32)"))?
            .value(row_idx) as usize),
        DataType::UInt64 => Ok(keys
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| ConvertError::internal("invalid dictionary keys (uint64)"))?
            .value(row_idx) as usize),
        _ => Err(ConvertError::unsupported_type(
            "unsupported dictionary key type",
        )),
    }
}
