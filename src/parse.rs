use anyhow::Result;
use base64::Engine as _;
use parquet::file::reader::{FileReader, SerializedFileReader};
use parquet::record::{Field, Row};
use serde::de::DeserializeOwned;
use serde_json::{self, Value};

pub fn parse_json<T: DeserializeOwned>(body: &str) -> Result<T> {
    Ok(serde_json::from_str(body)?)
}

pub fn parse_parquet<T: DeserializeOwned>(bytes: &[u8]) -> Result<T> {
    let chunk_reader = bytes::Bytes::from(bytes.to_vec());
    let reader = SerializedFileReader::new(chunk_reader)?;
    let iter = reader.get_row_iter(None)?;

    let mut out = Vec::new();
    for row in iter {
        let row = row?;
        out.push(row_to_value(&row));
    }

    let value = serde_json::Value::Array(out);
    Ok(serde_json::from_value(value)?)
}

fn row_to_value(row: &Row) -> Value {
    let mut obj = serde_json::Map::new();
    for (name, field) in row.get_column_iter() {
        obj.insert(name.to_string(), field_to_value(field));
    }
    Value::Object(obj)
}

fn field_to_value(field: &Field) -> Value {
    match field {
        Field::Null => Value::Null,
        Field::Bool(b) => Value::Bool(*b),

        Field::Int(i) => Value::Number((*i).into()),
        Field::Long(i) => Value::Number((*i).into()),

        Field::Float(f) => serde_json::Number::from_f64(*f as f64)
            .map(Value::Number)
            .unwrap_or(Value::Null),
        Field::Double(d) => serde_json::Number::from_f64(*d)
            .map(Value::Number)
            .unwrap_or(Value::Null),

        Field::Str(s) => Value::String(s.clone()),
        Field::Bytes(b) => {
            Value::String(base64::engine::general_purpose::STANDARD.encode(b.data()))
        }

        Field::Group(r) => row_to_value(r),
        _ => Value::Null,
    }
}
