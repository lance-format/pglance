use arrow::array::{
    Array, BinaryArray, BooleanArray, Date32Array, Date64Array, FixedSizeBinaryArray,
    FixedSizeListArray, Float16Array, Float32Array, Float64Array, GenericListArray, Int16Array,
    Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, StringArray,
    StructArray, TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
    TimestampSecondArray, UInt16Array, UInt32Array, UInt64Array, UInt8Array,
};
use arrow::datatypes::{DataType, TimeUnit as ArrowTimeUnit};
use base64::{engine::general_purpose::STANDARD, Engine as _};
use chrono::Datelike;
use pgrx::datum::{Date, Timestamp, TimestampWithTimeZone};
use pgrx::pg_sys;
use pgrx::prelude::IntoDatum;
use pgrx::JsonB;
use serde_json::{Map, Number, Value};

const UNIX_TO_POSTGRES_EPOCH_SECS: i64 = 946_684_800;

pub fn arrow_value_to_datum(
    array: &dyn Array,
    row_idx: usize,
    target_type_oid: pg_sys::Oid,
) -> Result<(pg_sys::Datum, bool), &'static str> {
    if array.is_null(row_idx) {
        return Ok((pg_sys::Datum::from(0usize), true));
    }

    match array.data_type() {
        DataType::Boolean => {
            let v = array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .ok_or("invalid boolean array")?
                .value(row_idx);
            Ok((v.into_datum().ok_or("failed to convert bool")?, false))
        }
        DataType::Int8 => {
            let v = array
                .as_any()
                .downcast_ref::<Int8Array>()
                .ok_or("invalid int8 array")?
                .value(row_idx) as i16;
            Ok((v.into_datum().ok_or("failed to convert int8")?, false))
        }
        DataType::Int16 => {
            let v = array
                .as_any()
                .downcast_ref::<Int16Array>()
                .ok_or("invalid int16 array")?
                .value(row_idx);
            Ok((v.into_datum().ok_or("failed to convert int16")?, false))
        }
        DataType::Int32 => {
            let v = array
                .as_any()
                .downcast_ref::<Int32Array>()
                .ok_or("invalid int32 array")?
                .value(row_idx);
            Ok((v.into_datum().ok_or("failed to convert int32")?, false))
        }
        DataType::Int64 => {
            let v = array
                .as_any()
                .downcast_ref::<Int64Array>()
                .ok_or("invalid int64 array")?
                .value(row_idx);
            Ok((v.into_datum().ok_or("failed to convert int64")?, false))
        }
        DataType::UInt8 => {
            let v = array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .ok_or("invalid uint8 array")?
                .value(row_idx) as i16;
            Ok((v.into_datum().ok_or("failed to convert uint8")?, false))
        }
        DataType::UInt16 => {
            let v = array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .ok_or("invalid uint16 array")?
                .value(row_idx) as i32;
            Ok((v.into_datum().ok_or("failed to convert uint16")?, false))
        }
        DataType::UInt32 => {
            let v = array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or("invalid uint32 array")?
                .value(row_idx) as i64;
            Ok((v.into_datum().ok_or("failed to convert uint32")?, false))
        }
        DataType::UInt64 => {
            let v = array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .ok_or("invalid uint64 array")?
                .value(row_idx);
            if v > i64::MAX as u64 {
                return Err("uint64 out of range for int8");
            }
            Ok((
                (v as i64).into_datum().ok_or("failed to convert uint64")?,
                false,
            ))
        }
        DataType::Float16 => {
            let v = array
                .as_any()
                .downcast_ref::<Float16Array>()
                .ok_or("invalid float16 array")?
                .value(row_idx)
                .to_f32();
            Ok((v.into_datum().ok_or("failed to convert float16")?, false))
        }
        DataType::Float32 => {
            let v = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .ok_or("invalid float32 array")?
                .value(row_idx);
            Ok((v.into_datum().ok_or("failed to convert float32")?, false))
        }
        DataType::Float64 => {
            let v = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or("invalid float64 array")?
                .value(row_idx);
            Ok((v.into_datum().ok_or("failed to convert float64")?, false))
        }
        DataType::Utf8 => {
            let v = array
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or("invalid utf8 array")?
                .value(row_idx);
            Ok((
                v.to_string().into_datum().ok_or("failed to convert text")?,
                false,
            ))
        }
        DataType::LargeUtf8 => {
            let v = array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .ok_or("invalid large utf8 array")?
                .value(row_idx);
            Ok((
                v.to_string().into_datum().ok_or("failed to convert text")?,
                false,
            ))
        }
        DataType::Binary => {
            let v = array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or("invalid binary array")?
                .value(row_idx);
            Ok((
                v.to_vec().into_datum().ok_or("failed to convert bytea")?,
                false,
            ))
        }
        DataType::LargeBinary => {
            let v = array
                .as_any()
                .downcast_ref::<LargeBinaryArray>()
                .ok_or("invalid large binary array")?
                .value(row_idx);
            Ok((
                v.to_vec().into_datum().ok_or("failed to convert bytea")?,
                false,
            ))
        }
        DataType::FixedSizeBinary(_) => {
            let v = array
                .as_any()
                .downcast_ref::<FixedSizeBinaryArray>()
                .ok_or("invalid fixed size binary array")?
                .value(row_idx);
            Ok((
                v.to_vec().into_datum().ok_or("failed to convert bytea")?,
                false,
            ))
        }
        DataType::Date32 => {
            let days = array
                .as_any()
                .downcast_ref::<Date32Array>()
                .ok_or("invalid date32 array")?
                .value(row_idx);
            let base = chrono::NaiveDate::from_ymd_opt(1970, 1, 1).ok_or("invalid epoch")?;
            let dt = base
                .checked_add_signed(chrono::Duration::days(days as i64))
                .ok_or("date32 overflow")?;
            let date = Date::new(dt.year(), dt.month() as u8, dt.day() as u8)
                .map_err(|_| "invalid date")?;
            Ok((date.into_datum().ok_or("failed to convert date")?, false))
        }
        DataType::Date64 => {
            let millis = array
                .as_any()
                .downcast_ref::<Date64Array>()
                .ok_or("invalid date64 array")?
                .value(row_idx);
            let dt = chrono::DateTime::from_timestamp_millis(millis).ok_or("date64 overflow")?;
            let date = Date::new(dt.year(), dt.month() as u8, dt.day() as u8)
                .map_err(|_| "invalid date")?;
            Ok((date.into_datum().ok_or("failed to convert date")?, false))
        }
        DataType::Timestamp(unit, tz) => {
            let unix_micros = match unit {
                ArrowTimeUnit::Second => {
                    let secs = array
                        .as_any()
                        .downcast_ref::<TimestampSecondArray>()
                        .ok_or("invalid timestamp(s) array")?
                        .value(row_idx);
                    secs.saturating_mul(1_000_000)
                }
                ArrowTimeUnit::Millisecond => {
                    let millis = array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .ok_or("invalid timestamp(ms) array")?
                        .value(row_idx);
                    millis.saturating_mul(1_000)
                }
                ArrowTimeUnit::Microsecond => array
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondArray>()
                    .ok_or("invalid timestamp(us) array")?
                    .value(row_idx),
                ArrowTimeUnit::Nanosecond => {
                    let nanos = array
                        .as_any()
                        .downcast_ref::<TimestampNanosecondArray>()
                        .ok_or("invalid timestamp(ns) array")?
                        .value(row_idx);
                    nanos / 1_000
                }
            };

            let pg_micros = unix_micros - UNIX_TO_POSTGRES_EPOCH_SECS.saturating_mul(1_000_000);

            if tz.is_some() || target_type_oid == pg_sys::TIMESTAMPTZOID {
                let ts = TimestampWithTimeZone::try_from(pg_micros)
                    .map_err(|_| "invalid timestamptz")?;
                Ok((
                    ts.into_datum().ok_or("failed to convert timestamptz")?,
                    false,
                ))
            } else {
                let ts = Timestamp::try_from(pg_micros).map_err(|_| "invalid timestamp")?;
                Ok((ts.into_datum().ok_or("failed to convert timestamp")?, false))
            }
        }
        DataType::List(_) | DataType::LargeList(_) => {
            let elem_oid = unsafe { pg_sys::get_element_type(target_type_oid) };
            if elem_oid == pg_sys::InvalidOid {
                return Ok((
                    JsonB(arrow_value_to_json(array, row_idx))
                        .into_datum()
                        .ok_or("jsonb")?,
                    false,
                ));
            }
            list_to_array_datum(array, row_idx, target_type_oid, elem_oid)
        }
        DataType::FixedSizeList(_, _) => {
            let elem_oid = unsafe { pg_sys::get_element_type(target_type_oid) };
            if elem_oid == pg_sys::InvalidOid {
                return Ok((
                    JsonB(arrow_value_to_json(array, row_idx))
                        .into_datum()
                        .ok_or("jsonb")?,
                    false,
                ));
            }
            fixed_size_list_to_array_datum(array, row_idx, target_type_oid, elem_oid)
        }
        DataType::Struct(_) => {
            let struct_array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .ok_or("invalid struct array")?;
            struct_to_composite_datum(struct_array, row_idx, target_type_oid)
        }
        DataType::Map(_, _) => Ok((
            JsonB(arrow_value_to_json(array, row_idx))
                .into_datum()
                .ok_or("failed to convert jsonb")?,
            false,
        )),
        _ => Ok((
            JsonB(arrow_value_to_json(array, row_idx))
                .into_datum()
                .ok_or("failed to convert jsonb")?,
            false,
        )),
    }
}

fn list_to_array_datum(
    array: &dyn Array,
    row_idx: usize,
    array_type_oid: pg_sys::Oid,
    elem_oid: pg_sys::Oid,
) -> Result<(pg_sys::Datum, bool), &'static str> {
    fn handle_list<OffsetSize: arrow::array::OffsetSizeTrait>(
        array: &dyn Array,
        row_idx: usize,
        array_type_oid: pg_sys::Oid,
        elem_oid: pg_sys::Oid,
    ) -> Result<(pg_sys::Datum, bool), &'static str> {
        let list_array = array
            .as_any()
            .downcast_ref::<GenericListArray<OffsetSize>>()
            .ok_or("invalid list array")?;
        let values = list_array.value(row_idx);
        array_values_to_pg_array(values.as_ref(), array_type_oid, elem_oid)
    }

    match array.data_type() {
        DataType::List(_) => handle_list::<i32>(array, row_idx, array_type_oid, elem_oid),
        DataType::LargeList(_) => handle_list::<i64>(array, row_idx, array_type_oid, elem_oid),
        _ => Err("not a list array"),
    }
}

fn fixed_size_list_to_array_datum(
    array: &dyn Array,
    row_idx: usize,
    array_type_oid: pg_sys::Oid,
    elem_oid: pg_sys::Oid,
) -> Result<(pg_sys::Datum, bool), &'static str> {
    let list_array = array
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .ok_or("invalid fixed size list array")?;
    let values = list_array.value(row_idx);
    array_values_to_pg_array(values.as_ref(), array_type_oid, elem_oid)
}

fn array_values_to_pg_array(
    values: &dyn Array,
    array_type_oid: pg_sys::Oid,
    elem_oid: pg_sys::Oid,
) -> Result<(pg_sys::Datum, bool), &'static str> {
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
            return Err("failed to construct array");
        }

        let datum = pg_sys::Datum::from(arr);
        Ok((datum, false))
    }
}

fn struct_to_composite_datum(
    struct_array: &StructArray,
    row_idx: usize,
    composite_type_oid: pg_sys::Oid,
) -> Result<(pg_sys::Datum, bool), &'static str> {
    unsafe {
        let tupdesc =
            pg_sys::lookup_type_cache(composite_type_oid, pg_sys::TYPECACHE_TUPDESC as i32);
        if tupdesc.is_null() || (*tupdesc).tupDesc.is_null() {
            return Err("failed to lookup composite tupdesc");
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
                .ok_or("struct field name mismatch")?;
            let col = struct_array.column(idx);
            let (d, isnull) = arrow_value_to_datum(col.as_ref(), row_idx, attr.atttypid)?;
            values[i] = d;
            nulls[i] = isnull;
        }

        let htup = pg_sys::heap_form_tuple(tupdesc, values.as_mut_ptr(), nulls.as_mut_ptr());
        if htup.is_null() {
            return Err("failed to form heap tuple");
        }
        let datum = pg_sys::HeapTupleGetDatum(htup);
        Ok((datum, false))
    }
}

fn arrow_value_to_json(array: &dyn Array, row_idx: usize) -> Value {
    if array.is_null(row_idx) {
        return Value::Null;
    }

    match array.data_type() {
        DataType::Boolean => Value::Bool(
            array
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(row_idx),
        ),
        DataType::Int8 => json_number(
            array
                .as_any()
                .downcast_ref::<Int8Array>()
                .unwrap()
                .value(row_idx) as i64,
        ),
        DataType::Int16 => json_number(
            array
                .as_any()
                .downcast_ref::<Int16Array>()
                .unwrap()
                .value(row_idx) as i64,
        ),
        DataType::Int32 => json_number(
            array
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .value(row_idx) as i64,
        ),
        DataType::Int64 => json_number(
            array
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(row_idx),
        ),
        DataType::UInt8 => json_number(
            array
                .as_any()
                .downcast_ref::<UInt8Array>()
                .unwrap()
                .value(row_idx) as i64,
        ),
        DataType::UInt16 => json_number(
            array
                .as_any()
                .downcast_ref::<UInt16Array>()
                .unwrap()
                .value(row_idx) as i64,
        ),
        DataType::UInt32 => json_number(
            array
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap()
                .value(row_idx) as i64,
        ),
        DataType::UInt64 => json_number(
            array
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .value(row_idx) as i64,
        ),
        DataType::Float16 => {
            let v = array
                .as_any()
                .downcast_ref::<Float16Array>()
                .unwrap()
                .value(row_idx)
                .to_f32() as f64;
            Number::from_f64(v)
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        DataType::Float32 => {
            let v = array
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .value(row_idx) as f64;
            Number::from_f64(v)
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        DataType::Float64 => {
            let v = array
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row_idx);
            Number::from_f64(v)
                .map(Value::Number)
                .unwrap_or(Value::Null)
        }
        DataType::Utf8 => Value::String(
            array
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(row_idx)
                .to_string(),
        ),
        DataType::LargeUtf8 => Value::String(
            array
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(row_idx)
                .to_string(),
        ),
        DataType::Binary => Value::String(
            STANDARD.encode(
                array
                    .as_any()
                    .downcast_ref::<BinaryArray>()
                    .unwrap()
                    .value(row_idx),
            ),
        ),
        DataType::LargeBinary => Value::String(
            STANDARD.encode(
                array
                    .as_any()
                    .downcast_ref::<LargeBinaryArray>()
                    .unwrap()
                    .value(row_idx),
            ),
        ),
        DataType::FixedSizeBinary(_) => Value::String(
            STANDARD.encode(
                array
                    .as_any()
                    .downcast_ref::<FixedSizeBinaryArray>()
                    .unwrap()
                    .value(row_idx),
            ),
        ),
        DataType::List(_) | DataType::LargeList(_) | DataType::FixedSizeList(_, _) => {
            let mut out = Vec::new();
            let values = match array.data_type() {
                DataType::List(_) => {
                    let l = array
                        .as_any()
                        .downcast_ref::<GenericListArray<i32>>()
                        .unwrap();
                    l.value(row_idx)
                }
                DataType::LargeList(_) => {
                    let l = array
                        .as_any()
                        .downcast_ref::<GenericListArray<i64>>()
                        .unwrap();
                    l.value(row_idx)
                }
                DataType::FixedSizeList(_, _) => {
                    let l = array.as_any().downcast_ref::<FixedSizeListArray>().unwrap();
                    l.value(row_idx)
                }
                _ => unreachable!(),
            };
            for i in 0..values.len() {
                out.push(arrow_value_to_json(values.as_ref(), i));
            }
            Value::Array(out)
        }
        DataType::Struct(fields) => {
            let struct_array = array.as_any().downcast_ref::<StructArray>().unwrap();
            let mut map = Map::new();
            for (i, f) in fields.iter().enumerate() {
                let col = struct_array.column(i);
                map.insert(f.name().clone(), arrow_value_to_json(col.as_ref(), row_idx));
            }
            Value::Object(map)
        }
        _ => Value::String(format!("<unsupported_type: {:?}>", array.data_type())),
    }
}

fn json_number(v: i64) -> Value {
    Value::Number(Number::from(v))
}
