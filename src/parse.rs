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

//          //          //          //          //          //          //          //          //          //          //          //

//                      //                      //                      //                                  //                      //

//                      //                      //          //          //          //                      //                      //

//                      //                      //                                  //                      //                      //

//                      //                      //          //          //          //                      //                      //

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::file::properties::WriterProperties;
    use parquet::file::writer::SerializedFileWriter;
    use parquet::record::Row;
    use parquet::schema::parser::parse_message_type;
    use std::io::Cursor;
    use std::sync::Arc;

    #[test]
    fn test_parse_json_valid() {
        let input = r#"[{"foo": "bar"}]"#;
        let val: Vec<serde_json::Value> = parse_json(input).unwrap();
        assert_eq!(val[0]["foo"], "bar");
    }

    #[test]
    fn test_parse_json_invalid() {
        let input = r#"{"foo": "bar""#; // missing closing brace
        let res: Result<serde_json::Value> = parse_json(input);
        assert!(res.is_err());
    }

    #[test]
    fn test_field_to_value_variants() {
        assert_eq!(field_to_value(&Field::Null), Value::Null);
        assert_eq!(field_to_value(&Field::Bool(true)), Value::Bool(true));
        assert_eq!(field_to_value(&Field::Int(123)), Value::Number(123.into()));
        assert_eq!(field_to_value(&Field::Long(456)), Value::Number(456.into()));
        assert_eq!(
            field_to_value(&Field::Float(1.5)),
            Value::Number(serde_json::Number::from_f64(1.5).unwrap())
        );
        assert_eq!(
            field_to_value(&Field::Double(2.5)),
            Value::Number(serde_json::Number::from_f64(2.5).unwrap())
        );
        assert_eq!(
            field_to_value(&Field::Str("abc".to_string())),
            Value::String("abc".into())
        );

        let bytes = parquet::data_type::ByteArray::from("hi".as_bytes().to_vec());
        assert!(matches!(
            field_to_value(&Field::Bytes(bytes)),
            Value::String(_)
        ));

        let nested = Row::new(vec![("inner".to_string(), Field::Int(42))]);
        let val = field_to_value(&Field::Group(nested));
        assert_eq!(val["inner"], 42);
    }

    #[test]
    fn test_row_to_value() {
        let row = Row::new(vec![
            ("a".to_string(), Field::Int(1)),
            ("b".to_string(), Field::Str("x".to_string())),
        ]);
        let val = row_to_value(&row);
        assert_eq!(val["a"], 1);
        assert_eq!(val["b"], "x");
    }

    #[test]
    fn test_parse_parquet_invalid() {
        // not a parquet file
        let data = b"not parquet";
        let res: Result<Vec<serde_json::Value>> = parse_parquet(data);
        assert!(res.is_err());
    }

    #[test]
    fn test_parse_parquet_roundtrip() {
        let message_type = "
            message schema {
                REQUIRED INT32 a;
                REQUIRED BYTE_ARRAY b (UTF8);
            }
        ";
        let schema = Arc::new(parse_message_type(message_type).unwrap());

        let mut buffer = Cursor::new(Vec::new());
        {
            let props = Arc::new(WriterProperties::builder().build());
            let mut writer = SerializedFileWriter::new(&mut buffer, schema, props).unwrap();

            {
                let mut row_group_writer = writer.next_row_group().unwrap();

                // a: INT32
                {
                    let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
                    {
                        let typed = col_writer.typed::<parquet::data_type::Int32Type>();
                        typed.write_batch(&[1], None, None).unwrap();
                    } // drop typed borrow
                    col_writer.close().unwrap(); // close the column
                }

                // b: UTF8 string
                {
                    let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
                    {
                        let typed = col_writer.typed::<parquet::data_type::ByteArrayType>();
                        use parquet::data_type::ByteArray;
                        let ba = ByteArray::from("hello".as_bytes().to_vec());
                        typed.write_batch(&[ba], None, None).unwrap();
                    }
                    col_writer.close().unwrap();
                }

                row_group_writer.close().unwrap();
            }

            writer.close().unwrap();
        }

        let bytes = buffer.into_inner();
        let vals: Vec<serde_json::Value> = parse_parquet(&bytes).unwrap();
        assert_eq!(vals[0]["a"], 1);
        assert_eq!(vals[0]["b"], "hello");
    }

    #[test]
    fn test_parse_parquet_with_nested_group() {
        let message_type = "
            message schema {
                REQUIRED GROUP nested {
                    REQUIRED INT32 inner;
                }
            }
        ";
        let schema = Arc::new(parse_message_type(message_type).unwrap());

        let mut buffer = Cursor::new(Vec::new());
        {
            let props = Arc::new(WriterProperties::builder().build());
            let mut writer = SerializedFileWriter::new(&mut buffer, schema, props).unwrap();

            {
                let mut row_group_writer = writer.next_row_group().unwrap();

                // nested.inner: INT32
                {
                    let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
                    {
                        let typed = col_writer.typed::<parquet::data_type::Int32Type>();
                        typed.write_batch(&[7], None, None).unwrap();
                    }
                    col_writer.close().unwrap();
                }

                row_group_writer.close().unwrap();
            }

            writer.close().unwrap();
        }

        let bytes = buffer.into_inner();
        let vals: Vec<serde_json::Value> = parse_parquet(&bytes).unwrap();
        assert_eq!(vals[0]["nested"]["inner"], 7);
    }

    #[test]
    fn test_parse_parquet_binary_base64() {
        // Raw binary (no UTF8 annotation) → expect base64 string
        let message_type = "
            message schema {
                REQUIRED BINARY raw;
            }
        ";
        let schema = Arc::new(parse_message_type(message_type).unwrap());

        let mut buffer = Cursor::new(Vec::new());
        {
            let props = Arc::new(WriterProperties::builder().build());
            let mut writer = SerializedFileWriter::new(&mut buffer, schema, props).unwrap();

            {
                let mut row_group_writer = writer.next_row_group().unwrap();

                {
                    let mut col_writer = row_group_writer.next_column().unwrap().unwrap();
                    {
                        let typed = col_writer.typed::<parquet::data_type::ByteArrayType>();
                        use parquet::data_type::ByteArray;
                        let raw = vec![0u8, 1, 2, 255];
                        let ba = ByteArray::from(raw);
                        typed.write_batch(&[ba], None, None).unwrap();
                    }
                    col_writer.close().unwrap();
                }

                row_group_writer.close().unwrap();
            }

            writer.close().unwrap();
        }

        let bytes = buffer.into_inner();
        let vals: Vec<serde_json::Value> = parse_parquet(&bytes).unwrap();
        // base64(0x00 0x01 0x02 0xFF) = "AAEC/w=="
        assert_eq!(vals[0]["raw"], "AAEC/w==");
    }
}
