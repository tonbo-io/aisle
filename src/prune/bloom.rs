//! Bloom filter pruning for equality predicates

use datafusion_common::ScalarValue;
use parquet::bloom_filter::Sbbf;

use super::context::RowGroupContext;
use crate::ir::TriState;

pub(super) fn eval_bloom_eq(
    column: &str,
    value: &ScalarValue,
    ctx: &RowGroupContext<'_>,
) -> TriState {
    let bloom = match ctx.bloom_for_column(column) {
        Some(bloom) => bloom,
        None => return TriState::Unknown,
    };
    match bloom_maybe_contains(bloom, value) {
        Some(false) => TriState::False,
        _ => TriState::Unknown,
    }
}

pub(super) fn eval_bloom_in_list(
    column: &str,
    values: &[ScalarValue],
    ctx: &RowGroupContext<'_>,
) -> TriState {
    let bloom = match ctx.bloom_for_column(column) {
        Some(bloom) => bloom,
        None => return TriState::Unknown,
    };
    let mut any_checked = false;
    let mut any_possible = false;
    for value in values {
        match bloom_maybe_contains(bloom, value) {
            Some(true) => {
                any_checked = true;
                any_possible = true;
            }
            Some(false) => {
                any_checked = true;
            }
            None => {
                any_possible = true;
            }
        }
    }
    if any_checked && !any_possible {
        TriState::False
    } else {
        TriState::Unknown
    }
}

fn bloom_maybe_contains(filter: &Sbbf, value: &ScalarValue) -> Option<bool> {
    match value {
        ScalarValue::Boolean(Some(_)) => None,
        ScalarValue::Int8(Some(v)) => Some(filter.check(v)),
        ScalarValue::Int16(Some(v)) => Some(filter.check(v)),
        ScalarValue::Int32(Some(v)) => Some(filter.check(v)),
        ScalarValue::Int64(Some(v)) => Some(filter.check(v)),
        ScalarValue::UInt8(Some(v)) => Some(filter.check(v)),
        ScalarValue::UInt16(Some(v)) => Some(filter.check(v)),
        ScalarValue::UInt32(Some(v)) => Some(filter.check(v)),
        ScalarValue::UInt64(Some(v)) => Some(filter.check(v)),
        ScalarValue::Float32(Some(v)) => Some(filter.check(v)),
        ScalarValue::Float64(Some(v)) => Some(filter.check(v)),
        ScalarValue::Date32(Some(v)) => Some(filter.check(v)),
        ScalarValue::Date64(Some(v)) => Some(filter.check(v)),
        ScalarValue::Time32Second(Some(v)) => Some(filter.check(v)),
        ScalarValue::Time32Millisecond(Some(v)) => Some(filter.check(v)),
        ScalarValue::Time64Microsecond(Some(v)) => Some(filter.check(v)),
        ScalarValue::Time64Nanosecond(Some(v)) => Some(filter.check(v)),
        ScalarValue::TimestampSecond(Some(v), _) => Some(filter.check(v)),
        ScalarValue::TimestampMillisecond(Some(v), _) => Some(filter.check(v)),
        ScalarValue::TimestampMicrosecond(Some(v), _) => Some(filter.check(v)),
        ScalarValue::TimestampNanosecond(Some(v), _) => Some(filter.check(v)),
        ScalarValue::DurationSecond(Some(v)) => Some(filter.check(v)),
        ScalarValue::DurationMillisecond(Some(v)) => Some(filter.check(v)),
        ScalarValue::DurationMicrosecond(Some(v)) => Some(filter.check(v)),
        ScalarValue::DurationNanosecond(Some(v)) => Some(filter.check(v)),
        ScalarValue::Utf8(Some(v))
        | ScalarValue::LargeUtf8(Some(v))
        | ScalarValue::Utf8View(Some(v)) => {
            let s: &str = v.as_str();
            Some(filter.check(&s))
        }
        ScalarValue::Binary(Some(v))
        | ScalarValue::LargeBinary(Some(v))
        | ScalarValue::BinaryView(Some(v)) => Some(filter.check(v)),
        ScalarValue::FixedSizeBinary(_, Some(v)) => Some(filter.check(v)),
        _ => None,
    }
}
