use arrow::datatypes::{DataType, Field};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone)]
pub struct CompositeTypeDef {
    pub type_name: String,
    pub fields: Vec<(String, String)>,
}

#[derive(Debug, Default)]
pub struct SchemaMapping {
    pub composite_types: Vec<CompositeTypeDef>,
    pub column_types: Vec<(String, String)>,
}

pub fn build_schema_mapping(
    table_name: &str,
    fields: &[Field],
) -> Result<SchemaMapping, &'static str> {
    let mut mapping = SchemaMapping::default();
    let mut seen = BTreeSet::<String>::new();
    let mut composites = BTreeMap::<String, CompositeTypeDef>::new();

    for field in fields {
        let ty = pg_type_for_arrow(table_name, field, field.name(), &mut composites, &mut seen)?;
        mapping.column_types.push((field.name().clone(), ty));
    }

    mapping.composite_types = composites.into_values().collect();
    Ok(mapping)
}

fn pg_type_for_arrow(
    table_name: &str,
    field: &Field,
    path: &str,
    composites: &mut BTreeMap<String, CompositeTypeDef>,
    seen: &mut BTreeSet<String>,
) -> Result<String, &'static str> {
    match field.data_type() {
        DataType::Boolean => Ok("boolean".to_string()),
        DataType::Int8 | DataType::UInt8 => Ok("int2".to_string()),
        DataType::Int16 => Ok("int2".to_string()),
        DataType::UInt16 => Ok("int4".to_string()),
        DataType::Int32 => Ok("int4".to_string()),
        DataType::UInt32 => Ok("int8".to_string()),
        DataType::Int64 | DataType::UInt64 => Ok("int8".to_string()),
        DataType::Float16 | DataType::Float32 => Ok("float4".to_string()),
        DataType::Float64 => Ok("float8".to_string()),
        DataType::Utf8 | DataType::LargeUtf8 => Ok("text".to_string()),
        DataType::Binary | DataType::LargeBinary | DataType::FixedSizeBinary(_) => {
            Ok("bytea".to_string())
        }
        DataType::Date32 | DataType::Date64 => Ok("date".to_string()),
        DataType::Timestamp(_, tz) => {
            if tz.is_some() {
                Ok("timestamptz".to_string())
            } else {
                Ok("timestamp".to_string())
            }
        }
        DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => Ok("numeric".to_string()),
        DataType::List(elem) | DataType::LargeList(elem) | DataType::FixedSizeList(elem, _) => {
            let elem_field = Field::new(elem.name(), elem.data_type().clone(), elem.is_nullable());
            let inner_path = format!("{}_item", path);
            let inner = pg_type_for_arrow(table_name, &elem_field, &inner_path, composites, seen)?;
            Ok(format!("{}[]", inner))
        }
        DataType::Struct(struct_fields) => {
            let type_name = composite_type_name(table_name, path);

            if !seen.insert(type_name.clone()) {
                return Ok(type_name);
            }

            let mut cols = Vec::with_capacity(struct_fields.len());
            for f in struct_fields.iter() {
                let child_path = format!("{}_{}", path, f.name());
                let ty = pg_type_for_arrow(table_name, f, &child_path, composites, seen)?;
                cols.push((f.name().clone(), ty));
            }

            composites.insert(
                type_name.clone(),
                CompositeTypeDef {
                    type_name: type_name.clone(),
                    fields: cols,
                },
            );
            Ok(type_name)
        }
        DataType::Map(_, _) => Ok("jsonb".to_string()),
        DataType::Dictionary(_, value) => {
            let v = Field::new("value", (**value).clone(), true);
            let inner_path = format!("{}_dict", path);
            pg_type_for_arrow(table_name, &v, &inner_path, composites, seen)
        }
        _ => Ok("jsonb".to_string()),
    }
}

fn composite_type_name(table: &str, path: &str) -> String {
    let mut name = format!("lance_{}_{}", table, path);
    name.make_ascii_lowercase();
    name = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    if name.len() > 60 {
        name.truncate(60);
    }
    name
}
