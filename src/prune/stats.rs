use arrow_schema::{DataType, Schema};
use datafusion_common::ScalarValue;
use parquet::{
    basic::{ColumnOrder, SortOrder},
    file::statistics::Statistics,
};

use super::context::RowGroupContext;

pub(super) fn stats_for_column(
    column: &str,
    ctx: &RowGroupContext<'_>,
) -> Option<(ScalarValue, ScalarValue, Option<u64>, u64)> {
    let row_group = ctx.metadata.row_group(ctx.row_group_idx);
    let col_idx = *ctx.column_lookup.get(column)?;
    let data_type = data_type_for_path(ctx.schema, column)?;
    let stats = row_group.column(col_idx).statistics()?;
    if !byte_array_ordering_supported(stats, ctx, col_idx) {
        return None;
    }
    let (min, max) = stats_to_scalars(stats, &data_type)?;
    let null_count = stats.null_count_opt();
    let row_count = row_group.num_rows() as u64;
    Some((min, max, null_count, row_count))
}

pub(super) fn byte_array_ordering_supported(
    stats: &Statistics,
    ctx: &RowGroupContext<'_>,
    col_idx: usize,
) -> bool {
    if !matches!(
        stats,
        Statistics::ByteArray(_) | Statistics::FixedLenByteArray(_)
    ) {
        return true;
    }

    let column_order = ctx.metadata.file_metadata().column_order(col_idx);
    if !matches!(
        column_order,
        ColumnOrder::TYPE_DEFINED_ORDER(SortOrder::UNSIGNED)
    ) {
        return false;
    }

    if ctx.options.allow_truncated_byte_array_ordering() {
        return true;
    }

    stats.min_is_exact() && stats.max_is_exact()
}

fn stats_to_scalars(
    stats: &Statistics,
    data_type: &DataType,
) -> Option<(ScalarValue, ScalarValue)> {
    match stats {
        Statistics::Boolean(stats) => {
            let min = stats.min_opt().copied()?;
            let max = stats.max_opt().copied()?;
            Some((
                ScalarValue::Boolean(Some(min)),
                ScalarValue::Boolean(Some(max)),
            ))
        }
        Statistics::Int32(stats) => {
            let min = stats.min_opt().copied()?;
            let max = stats.max_opt().copied()?;
            let min = ScalarValue::Int32(Some(min)).cast_to(data_type).ok()?;
            let max = ScalarValue::Int32(Some(max)).cast_to(data_type).ok()?;
            Some((min, max))
        }
        Statistics::Int64(stats) => {
            let min = stats.min_opt().copied()?;
            let max = stats.max_opt().copied()?;
            let min = ScalarValue::Int64(Some(min)).cast_to(data_type).ok()?;
            let max = ScalarValue::Int64(Some(max)).cast_to(data_type).ok()?;
            Some((min, max))
        }
        Statistics::Float(stats) => {
            let min = stats.min_opt().copied()?;
            let max = stats.max_opt().copied()?;
            Some((
                ScalarValue::Float32(Some(min)),
                ScalarValue::Float32(Some(max)),
            ))
        }
        Statistics::Double(stats) => {
            let min = stats.min_opt().copied()?;
            let max = stats.max_opt().copied()?;
            Some((
                ScalarValue::Float64(Some(min)),
                ScalarValue::Float64(Some(max)),
            ))
        }
        Statistics::ByteArray(stats) => {
            let min = stats.min_opt()?;
            let max = stats.max_opt()?;
            let min = byte_array_to_scalar(min.data(), data_type)?;
            let max = byte_array_to_scalar(max.data(), data_type)?;
            Some((min, max))
        }
        Statistics::FixedLenByteArray(stats) => {
            let min = stats.min_opt()?;
            let max = stats.max_opt()?;
            let min = byte_array_to_scalar(min.data(), data_type)?;
            let max = byte_array_to_scalar(max.data(), data_type)?;
            Some((min, max))
        }
        _ => None,
    }
}

pub(super) fn byte_array_to_scalar(bytes: &[u8], data_type: &DataType) -> Option<ScalarValue> {
    match data_type {
        DataType::Utf8 => {
            let s = String::from_utf8(bytes.to_vec()).ok()?;
            Some(ScalarValue::Utf8(Some(s)))
        }
        DataType::LargeUtf8 => {
            let s = String::from_utf8(bytes.to_vec()).ok()?;
            Some(ScalarValue::LargeUtf8(Some(s)))
        }
        DataType::Utf8View => {
            let s = String::from_utf8(bytes.to_vec()).ok()?;
            Some(ScalarValue::Utf8View(Some(s)))
        }
        DataType::Binary => Some(ScalarValue::Binary(Some(bytes.to_vec()))),
        DataType::LargeBinary => Some(ScalarValue::LargeBinary(Some(bytes.to_vec()))),
        DataType::BinaryView => Some(ScalarValue::BinaryView(Some(bytes.to_vec()))),
        DataType::FixedSizeBinary(size) => {
            let size = *size as usize;
            if bytes.len() == size {
                Some(ScalarValue::FixedSizeBinary(
                    size as i32,
                    Some(bytes.to_vec()),
                ))
            } else {
                None
            }
        }
        _ => None,
    }
}

pub(super) fn data_type_for_path(schema: &Schema, path: &str) -> Option<DataType> {
    // Try direct lookup for top-level fields
    if let Ok(field) = schema.field_with_name(path) {
        return Some(field.data_type().clone());
    }

    // Split path and traverse through nested types
    let parts: Vec<&str> = path.split('.').collect();
    if parts.is_empty() {
        return None;
    }

    // Start with the first field
    let first_field = schema.fields().iter().find(|f| f.name() == parts[0])?;
    let mut current = first_field.data_type().clone();
    let mut i = 1;

    while i < parts.len() {
        match (&current, parts[i]) {
            // Struct field traversal
            (DataType::Struct(fields), part) => {
                let field = fields.iter().find(|f| f.name() == part)?;
                current = field.data_type().clone();
                i += 1;
            }
            // Parquet LIST 3-level encoding for all list variants: field.list.<element_name>
            // Accept both Arrow field name and standard "element" (for coerced writes)
            // LargeList and FixedSizeList use the same physical encoding as List
            (DataType::List(element_field), "list")
            | (DataType::LargeList(element_field), "list")
            | (DataType::FixedSizeList(element_field, _), "list") => {
                i += 1;
                if i >= parts.len() {
                    return None;
                }
                let element_name = parts[i];
                // Accept either the Arrow element field name or "element" (coerce_types = true)
                if element_name != element_field.name() && element_name != "element" {
                    return None;
                }
                current = element_field.data_type().clone();
                i += 1;
            }
            // Parquet MAP 3-level encoding: field.<entries_name>.{key,value}
            // Accept both Arrow entries name and standard "key_value" (for coerced writes)
            (DataType::Map(entries_field, _), part)
                if part == entries_field.name() || part == "key_value" =>
            {
                i += 1;
                if i >= parts.len() {
                    return None;
                }
                // entries_field should be a struct with key and value
                if let DataType::Struct(kv_fields) = entries_field.data_type() {
                    let kv_part = parts[i];
                    let mut field = kv_fields.iter().find(|f| f.name() == kv_part);
                    if field.is_none() && kv_fields.len() >= 2 {
                        // Accept standard "key"/"value" names for coerced schemas
                        if kv_part == "key" {
                            field = Some(&kv_fields[0]);
                        } else if kv_part == "value" {
                            field = Some(&kv_fields[1]);
                        }
                    }
                    let field = field?;
                    current = field.data_type().clone();
                    i += 1;
                } else {
                    return None;
                }
            }
            _ => return None,
        }
    }

    Some(current)
}
