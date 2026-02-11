use arrow_buffer::{IntervalDayTime, IntervalMonthDayNano, i256};
use arrow_schema::{DataType, IntervalUnit, Schema, TimeUnit};
use datafusion_common::ScalarValue;
use parquet::{
    basic::{ColumnOrder, SortOrder},
    data_type::Int96,
    file::statistics::Statistics,
};
use std::sync::Arc;

use super::context::RowGroupContext;

pub(super) fn stats_for_column(
    column: &str,
    ctx: &RowGroupContext<'_>,
) -> Option<(ScalarValue, ScalarValue, Option<u64>, u64)> {
    let row_group = ctx.metadata.row_group(ctx.row_group_idx);
    let col_idx = *ctx.column_lookup.get(column)?;
    let data_type = data_type_for_path(ctx.schema, column)?;
    let stats = row_group.column(col_idx).statistics()?;
    if !byte_array_ordering_supported(stats, ctx, col_idx, &data_type) {
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
    data_type: &DataType,
) -> bool {
    if !matches!(
        stats,
        Statistics::ByteArray(_) | Statistics::FixedLenByteArray(_)
    ) {
        return true;
    }

    let column_order = ctx.metadata.file_metadata().column_order(col_idx);
    let expected_order = if matches!(
        data_type,
        DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal128(_, _)
            | DataType::Decimal256(_, _)
    ) {
        SortOrder::SIGNED
    } else if matches!(data_type, DataType::Interval(_)) {
        SortOrder::UNDEFINED
    } else {
        SortOrder::UNSIGNED
    };
    if !matches!(
        column_order,
        ColumnOrder::TYPE_DEFINED_ORDER(order) if order == expected_order
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
            match data_type {
                DataType::Date32 => Some((
                    ScalarValue::Date32(Some(min)),
                    ScalarValue::Date32(Some(max)),
                )),
                DataType::Time32(unit) => match unit {
                    TimeUnit::Second => Some((
                        ScalarValue::Time32Second(Some(min)),
                        ScalarValue::Time32Second(Some(max)),
                    )),
                    TimeUnit::Millisecond => Some((
                        ScalarValue::Time32Millisecond(Some(min)),
                        ScalarValue::Time32Millisecond(Some(max)),
                    )),
                    _ => None,
                },
                DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _) => Some((
                    decimal_from_i32(min, data_type)?,
                    decimal_from_i32(max, data_type)?,
                )),
                _ => {
                    let min = ScalarValue::Int32(Some(min)).cast_to(data_type).ok()?;
                    let max = ScalarValue::Int32(Some(max)).cast_to(data_type).ok()?;
                    Some((min, max))
                }
            }
        }
        Statistics::Int64(stats) => {
            let min = stats.min_opt().copied()?;
            let max = stats.max_opt().copied()?;
            match data_type {
                DataType::Date64 => Some((
                    ScalarValue::Date64(Some(min)),
                    ScalarValue::Date64(Some(max)),
                )),
                DataType::Timestamp(unit, tz) => Some((
                    timestamp_scalar(unit, tz, min),
                    timestamp_scalar(unit, tz, max),
                )),
                DataType::Time64(unit) => match unit {
                    TimeUnit::Microsecond => Some((
                        ScalarValue::Time64Microsecond(Some(min)),
                        ScalarValue::Time64Microsecond(Some(max)),
                    )),
                    TimeUnit::Nanosecond => Some((
                        ScalarValue::Time64Nanosecond(Some(min)),
                        ScalarValue::Time64Nanosecond(Some(max)),
                    )),
                    _ => None,
                },
                DataType::Duration(unit) => match unit {
                    TimeUnit::Second => Some((
                        ScalarValue::DurationSecond(Some(min)),
                        ScalarValue::DurationSecond(Some(max)),
                    )),
                    TimeUnit::Millisecond => Some((
                        ScalarValue::DurationMillisecond(Some(min)),
                        ScalarValue::DurationMillisecond(Some(max)),
                    )),
                    TimeUnit::Microsecond => Some((
                        ScalarValue::DurationMicrosecond(Some(min)),
                        ScalarValue::DurationMicrosecond(Some(max)),
                    )),
                    TimeUnit::Nanosecond => Some((
                        ScalarValue::DurationNanosecond(Some(min)),
                        ScalarValue::DurationNanosecond(Some(max)),
                    )),
                },
                DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _) => Some((
                    decimal_from_i64(min, data_type)?,
                    decimal_from_i64(max, data_type)?,
                )),
                _ => {
                    let min = ScalarValue::Int64(Some(min)).cast_to(data_type).ok()?;
                    let max = ScalarValue::Int64(Some(max)).cast_to(data_type).ok()?;
                    Some((min, max))
                }
            }
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
        Statistics::Int96(stats) => {
            let min = stats.min_opt().copied()?;
            let max = stats.max_opt().copied()?;
            let min = int96_to_timestamp_scalar(&min, data_type)?;
            let max = int96_to_timestamp_scalar(&max, data_type)?;
            Some((min, max))
        }
    }
}

pub(super) fn timestamp_scalar(unit: &TimeUnit, tz: &Option<Arc<str>>, value: i64) -> ScalarValue {
    let tz = tz.clone();
    match unit {
        TimeUnit::Second => ScalarValue::TimestampSecond(Some(value), tz),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(value), tz),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(value), tz),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(value), tz),
    }
}

pub(super) fn byte_array_to_scalar(bytes: &[u8], data_type: &DataType) -> Option<ScalarValue> {
    match data_type {
        DataType::Interval(unit) => interval_from_bytes(bytes, unit),
        DataType::Decimal32(_, _)
        | DataType::Decimal64(_, _)
        | DataType::Decimal128(_, _)
        | DataType::Decimal256(_, _) => decimal_from_bytes(bytes, data_type),
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

fn interval_from_bytes(bytes: &[u8], unit: &IntervalUnit) -> Option<ScalarValue> {
    if bytes.len() != 12 {
        return None;
    }
    let months = i32::from_le_bytes(bytes[0..4].try_into().ok()?);
    let days = i32::from_le_bytes(bytes[4..8].try_into().ok()?);
    let millis = i32::from_le_bytes(bytes[8..12].try_into().ok()?);
    match unit {
        IntervalUnit::YearMonth => Some(ScalarValue::IntervalYearMonth(Some(months))),
        IntervalUnit::DayTime => Some(ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(
            days, millis,
        )))),
        IntervalUnit::MonthDayNano => {
            let nanos = i64::from(millis).checked_mul(1_000_000)?;
            Some(ScalarValue::IntervalMonthDayNano(Some(
                IntervalMonthDayNano::new(months, days, nanos),
            )))
        }
    }
}

pub(super) fn int96_to_timestamp_scalar(
    value: &Int96,
    data_type: &DataType,
) -> Option<ScalarValue> {
    let DataType::Timestamp(unit, tz) = data_type else {
        return None;
    };

    let data = value.data();
    let nanos = ((data[1] as i64) << 32) + data[0] as i64;
    let days = data[2] as i32;

    const JULIAN_DAY_OF_EPOCH: i128 = 2_440_588;
    const SECONDS_IN_DAY: i128 = 86_400;
    const NANOS_IN_SECOND: i128 = 1_000_000_000;

    let days_since_epoch = i128::from(days) - JULIAN_DAY_OF_EPOCH;
    let nanos_since_epoch = days_since_epoch
        .checked_mul(SECONDS_IN_DAY.checked_mul(NANOS_IN_SECOND)?)?
        + i128::from(nanos);

    let timestamp = match unit {
        TimeUnit::Second => nanos_since_epoch / NANOS_IN_SECOND,
        TimeUnit::Millisecond => nanos_since_epoch / (NANOS_IN_SECOND / 1_000),
        TimeUnit::Microsecond => nanos_since_epoch / (NANOS_IN_SECOND / 1_000_000),
        TimeUnit::Nanosecond => nanos_since_epoch,
    };

    let timestamp = i64::try_from(timestamp).ok()?;
    let tz = tz.clone();
    Some(match unit {
        TimeUnit::Second => ScalarValue::TimestampSecond(Some(timestamp), tz),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(timestamp), tz),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(timestamp), tz),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(timestamp), tz),
    })
}

pub(super) fn decimal_from_i32(value: i32, data_type: &DataType) -> Option<ScalarValue> {
    match data_type {
        DataType::Decimal32(precision, scale) => {
            Some(ScalarValue::Decimal32(Some(value), *precision, *scale))
        }
        DataType::Decimal64(precision, scale) => Some(ScalarValue::Decimal64(
            Some(i64::from(value)),
            *precision,
            *scale,
        )),
        DataType::Decimal128(precision, scale) => Some(ScalarValue::Decimal128(
            Some(i128::from(value)),
            *precision,
            *scale,
        )),
        DataType::Decimal256(precision, scale) => Some(ScalarValue::Decimal256(
            Some(i256::from(value)),
            *precision,
            *scale,
        )),
        _ => None,
    }
}

pub(super) fn decimal_from_i64(value: i64, data_type: &DataType) -> Option<ScalarValue> {
    match data_type {
        DataType::Decimal32(precision, scale) => {
            let value = i32::try_from(value).ok()?;
            Some(ScalarValue::Decimal32(Some(value), *precision, *scale))
        }
        DataType::Decimal64(precision, scale) => {
            Some(ScalarValue::Decimal64(Some(value), *precision, *scale))
        }
        DataType::Decimal128(precision, scale) => Some(ScalarValue::Decimal128(
            Some(i128::from(value)),
            *precision,
            *scale,
        )),
        DataType::Decimal256(precision, scale) => Some(ScalarValue::Decimal256(
            Some(i256::from(value)),
            *precision,
            *scale,
        )),
        _ => None,
    }
}

pub(super) fn decimal_from_bytes(bytes: &[u8], data_type: &DataType) -> Option<ScalarValue> {
    if bytes.is_empty() {
        return None;
    }
    match data_type {
        DataType::Decimal32(precision, scale) => {
            if bytes.len() > 4 {
                return None;
            }
            let value = i32::from_be_bytes(sign_extend_be::<4>(bytes));
            Some(ScalarValue::Decimal32(Some(value), *precision, *scale))
        }
        DataType::Decimal64(precision, scale) => {
            if bytes.len() > 8 {
                return None;
            }
            let value = i64::from_be_bytes(sign_extend_be::<8>(bytes));
            Some(ScalarValue::Decimal64(Some(value), *precision, *scale))
        }
        DataType::Decimal128(precision, scale) => {
            if bytes.len() > 16 {
                return None;
            }
            let value = i128::from_be_bytes(sign_extend_be::<16>(bytes));
            Some(ScalarValue::Decimal128(Some(value), *precision, *scale))
        }
        DataType::Decimal256(precision, scale) => {
            if bytes.len() > 32 {
                return None;
            }
            let value = i256::from_be_bytes(sign_extend_be::<32>(bytes));
            Some(ScalarValue::Decimal256(Some(value), *precision, *scale))
        }
        _ => None,
    }
}

fn sign_extend_be<const N: usize>(bytes: &[u8]) -> [u8; N] {
    debug_assert!(bytes.len() <= N, "Array too large, expected <= {N}");
    let is_negative = (bytes[0] & 128u8) == 128u8;
    let mut result = if is_negative { [255u8; N] } else { [0u8; N] };
    for (d, s) in result.iter_mut().skip(N - bytes.len()).zip(bytes) {
        *d = *s;
    }
    result
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

#[cfg(test)]
mod tests {
    use super::*;

    fn int96_for_nanos_since_midnight(nanos: u64, julian_day: u32) -> Int96 {
        let low = nanos as u32;
        let high = (nanos >> 32) as u32;
        Int96::from(vec![low, high, julian_day])
    }

    #[test]
    fn interval_from_bytes_year_month() {
        let mut bytes = [0u8; 12];
        bytes[0..4].copy_from_slice(&12i32.to_le_bytes());
        let scalar = byte_array_to_scalar(&bytes, &DataType::Interval(IntervalUnit::YearMonth))
            .expect("interval scalar");
        assert_eq!(scalar, ScalarValue::IntervalYearMonth(Some(12)));
    }

    #[test]
    fn interval_from_bytes_day_time() {
        let mut bytes = [0u8; 12];
        bytes[4..8].copy_from_slice(&2i32.to_le_bytes());
        bytes[8..12].copy_from_slice(&1500i32.to_le_bytes());
        let scalar = byte_array_to_scalar(&bytes, &DataType::Interval(IntervalUnit::DayTime))
            .expect("interval scalar");
        assert_eq!(
            scalar,
            ScalarValue::IntervalDayTime(Some(IntervalDayTime::new(2, 1500)))
        );
    }

    #[test]
    fn int96_timestamp_epoch_nanos() {
        let int96 = int96_for_nanos_since_midnight(0, 2_440_588);
        let data_type = DataType::Timestamp(TimeUnit::Nanosecond, None);
        let scalar = int96_to_timestamp_scalar(&int96, &data_type).expect("timestamp scalar");
        assert_eq!(scalar, ScalarValue::TimestampNanosecond(Some(0), None));
    }

    #[test]
    fn int96_timestamp_epoch_seconds() {
        let int96 = int96_for_nanos_since_midnight(1_000_000_000, 2_440_588);
        let data_type = DataType::Timestamp(TimeUnit::Second, None);
        let scalar = int96_to_timestamp_scalar(&int96, &data_type).expect("timestamp scalar");
        assert_eq!(scalar, ScalarValue::TimestampSecond(Some(1), None));
    }

    #[test]
    fn interval_from_bytes_month_day_nano() {
        let mut bytes = [0u8; 12];
        bytes[0..4].copy_from_slice(&1i32.to_le_bytes());
        bytes[4..8].copy_from_slice(&2i32.to_le_bytes());
        bytes[8..12].copy_from_slice(&1500i32.to_le_bytes());
        let scalar = byte_array_to_scalar(&bytes, &DataType::Interval(IntervalUnit::MonthDayNano))
            .expect("interval scalar");
        assert_eq!(
            scalar,
            ScalarValue::IntervalMonthDayNano(Some(IntervalMonthDayNano::new(1, 2, 1_500_000_000)))
        );
    }

    #[test]
    fn interval_from_bytes_requires_12_bytes() {
        let bytes = [0u8; 11];
        let scalar = byte_array_to_scalar(&bytes, &DataType::Interval(IntervalUnit::MonthDayNano));
        assert!(scalar.is_none());
    }
}
