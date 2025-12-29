use arrow_schema::DataType;
use datafusion_common::ScalarValue;

pub(super) fn string_scalar_for_type(value: &str, data_type: &DataType) -> Option<ScalarValue> {
    match data_type {
        DataType::Utf8 => Some(ScalarValue::Utf8(Some(value.to_string()))),
        DataType::LargeUtf8 => Some(ScalarValue::LargeUtf8(Some(value.to_string()))),
        DataType::Utf8View => Some(ScalarValue::Utf8View(Some(value.to_string()))),
        _ => None,
    }
}

pub(super) fn next_prefix_string(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    for idx in (0..chars.len()).rev() {
        let mut next = chars[idx] as u32 + 1;
        while next <= char::MAX as u32 {
            if let Some(ch) = char::from_u32(next) {
                chars[idx] = ch;
                chars.truncate(idx + 1);
                return Some(chars.into_iter().collect());
            }
            next += 1;
        }
    }
    None
}
