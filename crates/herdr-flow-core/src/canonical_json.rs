use alloc::{string::String, vec::Vec};
use core::{fmt, fmt::Write as _};

use serde_json::{Map, Number, Value};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CanonicalJsonError {
    NumberNotRepresentableAsF64,
    UnicodeNoncharacter,
}

impl fmt::Display for CanonicalJsonError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NumberNotRepresentableAsF64 => {
                formatter.write_str("number cannot be represented as an IEEE-754 double")
            }
            Self::UnicodeNoncharacter => {
                formatter.write_str("I-JSON strings cannot contain Unicode noncharacters")
            }
        }
    }
}

/// Converts every JSON number to its IEEE-754 representation and validates the
/// I-JSON string domain used by RFC 8785.
///
/// Reducers receive this normalized value so they cannot distinguish two raw
/// numeric spellings that have the same canonical digest.
pub fn normalize(value: &Value) -> Result<Value, CanonicalJsonError> {
    match value {
        Value::Null => Ok(Value::Null),
        Value::Bool(value) => Ok(Value::Bool(*value)),
        Value::Number(number) => {
            let value = number
                .as_f64()
                .ok_or(CanonicalJsonError::NumberNotRepresentableAsF64)?;
            let value = if value == 0.0 { 0.0 } else { value };
            let number =
                Number::from_f64(value).ok_or(CanonicalJsonError::NumberNotRepresentableAsF64)?;
            Ok(Value::Number(number))
        }
        Value::String(value) => {
            validate_string(value)?;
            Ok(Value::String(value.clone()))
        }
        Value::Array(values) => values
            .iter()
            .map(normalize)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        Value::Object(values) => {
            let mut normalized = Map::new();
            for (key, value) in values {
                validate_string(key)?;
                normalized.insert(key.clone(), normalize(value)?);
            }
            Ok(Value::Object(normalized))
        }
    }
}

/// Returns whether a value already uses the normalized representation exposed
/// to reducers.
pub(crate) fn is_normalized(value: &Value) -> bool {
    match value {
        Value::Null | Value::Bool(_) | Value::String(_) => true,
        Value::Number(number) => {
            number.is_f64()
                && number
                    .as_f64()
                    .is_some_and(|value| value != 0.0 || !value.is_sign_negative())
        }
        Value::Array(values) => values.iter().all(is_normalized),
        Value::Object(values) => values.values().all(is_normalized),
    }
}

/// Serializes a JSON value using RFC 8785 JSON Canonicalization Scheme rules.
///
/// JSON numbers are rendered through their IEEE-754 double value as required by
/// RFC 8785. Stage schemas—not the canonicalizer—enforce application-specific
/// integer ranges or require larger integers to be represented as strings.
pub fn to_vec(value: &Value) -> Result<Vec<u8>, CanonicalJsonError> {
    let normalized = normalize(value)?;
    let mut output = String::new();
    write_value(&normalized, &mut output)?;
    Ok(output.into_bytes())
}

fn write_value(value: &Value, output: &mut String) -> Result<(), CanonicalJsonError> {
    match value {
        Value::Null => output.push_str("null"),
        Value::Bool(true) => output.push_str("true"),
        Value::Bool(false) => output.push_str("false"),
        Value::Number(number) => write_number(number, output)?,
        Value::String(value) => write_string(value, output)?,
        Value::Array(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_value(value, output)?;
            }
            output.push(']');
        }
        Value::Object(values) => {
            let mut entries: Vec<_> = values.iter().collect();
            entries.sort_by(|(left, _), (right, _)| left.encode_utf16().cmp(right.encode_utf16()));

            output.push('{');
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_string(key, output)?;
                output.push(':');
                write_value(value, output)?;
            }
            output.push('}');
        }
    }
    Ok(())
}

fn write_number(number: &Number, output: &mut String) -> Result<(), CanonicalJsonError> {
    let value = number
        .as_f64()
        .ok_or(CanonicalJsonError::NumberNotRepresentableAsF64)?;
    let mut buffer = ryu_js::Buffer::new();
    output.push_str(buffer.format_finite(value));
    Ok(())
}

fn write_string(value: &str, output: &mut String) -> Result<(), CanonicalJsonError> {
    validate_string(value)?;
    output.push('"');
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\u{0008}' => output.push_str("\\b"),
            '\u{0009}' => output.push_str("\\t"),
            '\u{000a}' => output.push_str("\\n"),
            '\u{000c}' => output.push_str("\\f"),
            '\u{000d}' => output.push_str("\\r"),
            '\u{0000}'..='\u{001f}' => {
                write!(output, "\\u{:04x}", character as u32)
                    .expect("writing to String cannot fail");
            }
            _ => output.push(character),
        }
    }
    output.push('"');
    Ok(())
}

pub(crate) fn validate_string(value: &str) -> Result<(), CanonicalJsonError> {
    if value.chars().any(is_unicode_noncharacter) {
        return Err(CanonicalJsonError::UnicodeNoncharacter);
    }
    Ok(())
}

fn is_unicode_noncharacter(character: char) -> bool {
    let codepoint = character as u32;
    matches!(codepoint, 0xfdd0..=0xfdef) || codepoint & 0xffff >= 0xfffe
}

#[cfg(test)]
mod tests {
    use alloc::string::ToString;

    use super::*;
    use serde_json::json;

    #[test]
    fn canonicalizes_rfc_8785_values() {
        let value: Value = serde_json::from_str(
            r#"{"numbers":[333333333.33333329,1E30,4.50,2e-3,0.000000000000000000000000001],"string":"€$\u000f\nA'B\"\\\"/","literals":[null,true,false]}"#,
        )
        .unwrap();

        let canonical = String::from_utf8(to_vec(&value).unwrap()).unwrap();

        assert_eq!(
            canonical,
            "{\"literals\":[null,true,false],\"numbers\":[333333333.3333333,1e+30,4.5,0.002,1e-27],\"string\":\"€$\\u000f\\nA'B\\\"\\\\\\\"/\"}"
        );
    }

    #[test]
    fn sorts_object_names_by_utf16_code_units() {
        let value = json!({ "\u{e000}": 1, "\u{1f600}": 2 });

        let canonical = String::from_utf8(to_vec(&value).unwrap()).unwrap();

        assert_eq!(canonical, "{\"😀\":2,\"\":1}");
    }

    #[test]
    fn integer_and_decimal_representations_normalize_identically() {
        let integer = json!(9_007_199_254_740_992_u64);
        let decimal: Value = serde_json::from_str("9007199254740992.0").unwrap();

        assert_eq!(normalize(&integer).unwrap(), normalize(&decimal).unwrap());
        assert_eq!(to_vec(&integer).unwrap(), b"9007199254740992");
    }

    #[test]
    fn follows_rfc_8785_ieee_754_rounding() {
        let value = json!(9_007_199_254_740_993_u64);
        let normalized = normalize(&value).unwrap();

        assert_eq!(to_vec(&value).unwrap(), b"9007199254740992");
        assert_eq!(normalized.as_f64(), Some(9_007_199_254_740_992.0));
        assert_eq!(normalized.as_u64(), None);
    }

    #[test]
    fn canonicalizes_and_normalizes_negative_zero_as_positive_zero() {
        let normalized = normalize(&json!(-0.0)).unwrap();

        assert_eq!(to_vec(&json!(-0.0)).unwrap(), b"0");
        assert!(!normalized.as_f64().unwrap().is_sign_negative());
        assert!(is_normalized(&normalized));
        assert!(!is_normalized(&json!(-0.0)));
    }

    #[test]
    fn rejects_unicode_noncharacters_in_values_and_keys() {
        assert_eq!(
            to_vec(&json!("\u{fdd0}")),
            Err(CanonicalJsonError::UnicodeNoncharacter)
        );
        assert_eq!(
            to_vec(&json!({ "\u{10ffff}": true })),
            Err(CanonicalJsonError::UnicodeNoncharacter)
        );
    }

    #[test]
    fn equivalent_objects_have_identical_bytes() {
        let left: Value = serde_json::from_str(r#"{"b":2,"a":1}"#).unwrap();
        let right: Value = serde_json::from_str(r#"{"a":1,"b":2}"#).unwrap();

        assert_eq!(to_vec(&left).unwrap(), to_vec(&right).unwrap());
        assert_eq!(
            String::from_utf8(to_vec(&left).unwrap()).unwrap(),
            "{\"a\":1,\"b\":2}".to_string()
        );
    }
}
