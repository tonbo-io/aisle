//! Comparison operator evaluation (=, !=, <, <=, >, >=)

use arrow_schema::{DataType, TimeUnit};
use datafusion_common::ScalarValue;
use parquet::{
    arrow::arrow_reader::RowSelection,
    file::page_index::column_index::{ColumnIndexIterators, ColumnIndexMetaData},
};

use super::{context::RowGroupContext, page, page::PagePruning, stats};
use crate::expr::{CmpOp, TriState};

pub(super) fn eval_cmp(
    column: &str,
    op: CmpOp,
    value: &ScalarValue,
    ctx: &RowGroupContext<'_>,
) -> TriState {
    match stats::stats_for_column(column, ctx) {
        Some((min, max, null_count, row_count)) => {
            eval_cmp_stats(op, value, Some(&min), Some(&max), null_count, row_count)
        }
        None => TriState::Unknown,
    }
}

pub(super) fn eval_cmp_stats(
    op: CmpOp,
    value: &ScalarValue,
    min: Option<&ScalarValue>,
    max: Option<&ScalarValue>,
    null_count: Option<u64>,
    _row_count: u64,
) -> TriState {
    let (Some(min), Some(max)) = (min, max) else {
        return TriState::Unknown;
    };
    let min_cmp = min.partial_cmp(value);
    let max_cmp = max.partial_cmp(value);
    let nulls = null_count.unwrap_or(0);

    match op {
        CmpOp::Eq => {
            if min_cmp == Some(std::cmp::Ordering::Greater)
                || max_cmp == Some(std::cmp::Ordering::Less)
            {
                return TriState::False;
            }
            if min == max && min == value && nulls == 0 {
                return TriState::True;
            }
            TriState::Unknown
        }
        CmpOp::NotEq => {
            if min == max && min == value {
                return TriState::False;
            }
            if (min_cmp == Some(std::cmp::Ordering::Greater)
                || max_cmp == Some(std::cmp::Ordering::Less))
                && nulls == 0
            {
                return TriState::True;
            }
            TriState::Unknown
        }
        CmpOp::Lt => {
            if min_cmp == Some(std::cmp::Ordering::Greater)
                || min_cmp == Some(std::cmp::Ordering::Equal)
            {
                return TriState::False;
            }
            if max_cmp == Some(std::cmp::Ordering::Less) && nulls == 0 {
                return TriState::True;
            }
            TriState::Unknown
        }
        CmpOp::LtEq => {
            if min_cmp == Some(std::cmp::Ordering::Greater) {
                return TriState::False;
            }
            if (max_cmp == Some(std::cmp::Ordering::Less)
                || max_cmp == Some(std::cmp::Ordering::Equal))
                && nulls == 0
            {
                return TriState::True;
            }
            TriState::Unknown
        }
        CmpOp::Gt => {
            if max_cmp == Some(std::cmp::Ordering::Less)
                || max_cmp == Some(std::cmp::Ordering::Equal)
            {
                return TriState::False;
            }
            if min_cmp == Some(std::cmp::Ordering::Greater) && nulls == 0 {
                return TriState::True;
            }
            TriState::Unknown
        }
        CmpOp::GtEq => {
            if max_cmp == Some(std::cmp::Ordering::Less) {
                return TriState::False;
            }
            if (min_cmp == Some(std::cmp::Ordering::Greater)
                || min_cmp == Some(std::cmp::Ordering::Equal))
                && nulls == 0
            {
                return TriState::True;
            }
            TriState::Unknown
        }
    }
}

pub(super) fn page_selection_for_cmp(
    column: &str,
    op: CmpOp,
    value: &ScalarValue,
    ctx: &RowGroupContext<'_>,
) -> Option<PagePruning> {
    let row_group = ctx.metadata.row_group(ctx.row_group_idx);
    let col_idx = *ctx.column_lookup.get(column)?;
    let data_type = stats::data_type_for_path(ctx.schema, column)?;
    let nullable = ctx
        .schema
        .field_with_name(column)
        .map(|field| field.is_nullable())
        .unwrap_or(true);
    let row_group_nulls = row_group
        .column(col_idx)
        .statistics()
        .and_then(|stats| stats.null_count_opt());
    let column_index = ctx.metadata.column_index()?;
    let offset_index = ctx.metadata.offset_index()?;
    let col_index_meta = column_index.get(ctx.row_group_idx)?.get(col_idx)?;
    if matches!(
        col_index_meta,
        ColumnIndexMetaData::BYTE_ARRAY(_) | ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(_)
    ) {
        let stats = row_group.column(col_idx).statistics()?;
        if !stats::byte_array_ordering_supported(stats, ctx, col_idx, &data_type) {
            return None;
        }
    }
    let offset_meta = offset_index.get(ctx.row_group_idx)?.get(col_idx)?;
    let page_ranges = page::build_page_ranges(offset_meta, row_group.num_rows() as usize)?;
    let states = page_predicate_states(
        col_index_meta,
        op,
        value,
        &data_type,
        nullable,
        row_group_nulls,
    )?;
    if states.len() != page_ranges.len() {
        return None;
    }
    let exact = states.iter().all(|tri| *tri != TriState::Unknown);
    let selected_ranges = page_ranges
        .into_iter()
        .zip(states)
        .filter_map(|(range, tri)| (tri != TriState::False).then_some(range))
        .collect::<Vec<_>>();
    let selection = RowSelection::from_consecutive_ranges(
        selected_ranges.into_iter(),
        row_group.num_rows() as usize,
    );
    Some(PagePruning::new(selection, exact))
}

fn eval_cmp_stats_page(
    op: CmpOp,
    value: &ScalarValue,
    min: Option<&ScalarValue>,
    max: Option<&ScalarValue>,
    null_count: Option<i64>,
    nullable: bool,
) -> TriState {
    let nulls = null_count.and_then(|count| if count >= 0 { Some(count as u64) } else { None });
    let tri = eval_cmp_stats(op, value, min, max, nulls, 0);
    if tri == TriState::True && nulls.is_none() && nullable {
        TriState::Unknown
    } else {
        tri
    }
}

fn page_predicate_states(
    col_index_meta: &ColumnIndexMetaData,
    op: CmpOp,
    value: &ScalarValue,
    data_type: &DataType,
    nullable: bool,
    row_group_nulls: Option<u64>,
) -> Option<Vec<TriState>> {
    let page_null_count = |idx: usize| -> Option<i64> {
        page_null_count_or_zero(col_index_meta.null_count(idx), row_group_nulls)
    };
    match col_index_meta {
        ColumnIndexMetaData::BOOLEAN(_) => {
            let value = value.cast_to(&DataType::Boolean).ok()?;
            let value = match value {
                ScalarValue::Boolean(Some(v)) => v,
                _ => return None,
            };
            let mins = bool::min_values_iter(col_index_meta).enumerate();
            let maxs = bool::max_values_iter(col_index_meta);
            Some(
                mins.zip(maxs)
                    .map(|((idx, min), max)| {
                        let min = min.map(|v| ScalarValue::Boolean(Some(v)));
                        let max = max.map(|v| ScalarValue::Boolean(Some(v)));
                        let null_count = page_null_count(idx);
                        eval_cmp_stats_page(
                            op,
                            &ScalarValue::Boolean(Some(value)),
                            min.as_ref(),
                            max.as_ref(),
                            null_count,
                            nullable,
                        )
                    })
                    .collect(),
            )
        }
        ColumnIndexMetaData::INT32(_) => {
            let to_scalar = |v| match data_type {
                DataType::Date32 => Some(ScalarValue::Date32(Some(v))),
                DataType::Time32(unit) => match unit {
                    TimeUnit::Second => Some(ScalarValue::Time32Second(Some(v))),
                    TimeUnit::Millisecond => Some(ScalarValue::Time32Millisecond(Some(v))),
                    _ => None,
                },
                DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _) => stats::decimal_from_i32(v, data_type),
                _ => ScalarValue::Int32(Some(v)).cast_to(data_type).ok(),
            };
            let value = value.cast_to(data_type).ok()?;
            let mins = i32::min_values_iter(col_index_meta).enumerate();
            let maxs = i32::max_values_iter(col_index_meta);
            Some(
                mins.zip(maxs)
                    .map(|((idx, min), max)| {
                        let min = min.and_then(&to_scalar);
                        let max = max.and_then(&to_scalar);
                        let null_count = page_null_count(idx);
                        eval_cmp_stats_page(
                            op,
                            &value,
                            min.as_ref(),
                            max.as_ref(),
                            null_count,
                            nullable,
                        )
                    })
                    .collect(),
            )
        }
        ColumnIndexMetaData::INT64(_) => {
            let to_scalar = |v| match data_type {
                DataType::Date64 => Some(ScalarValue::Date64(Some(v))),
                DataType::Timestamp(unit, tz) => {
                    Some(stats::timestamp_scalar(unit, tz, v))
                }
                DataType::Time64(unit) => match unit {
                    TimeUnit::Microsecond => Some(ScalarValue::Time64Microsecond(Some(v))),
                    TimeUnit::Nanosecond => Some(ScalarValue::Time64Nanosecond(Some(v))),
                    _ => None,
                },
                DataType::Duration(unit) => match unit {
                    TimeUnit::Second => Some(ScalarValue::DurationSecond(Some(v))),
                    TimeUnit::Millisecond => Some(ScalarValue::DurationMillisecond(Some(v))),
                    TimeUnit::Microsecond => Some(ScalarValue::DurationMicrosecond(Some(v))),
                    TimeUnit::Nanosecond => Some(ScalarValue::DurationNanosecond(Some(v))),
                },
                DataType::Decimal32(_, _)
                | DataType::Decimal64(_, _)
                | DataType::Decimal128(_, _)
                | DataType::Decimal256(_, _) => stats::decimal_from_i64(v, data_type),
                _ => ScalarValue::Int64(Some(v)).cast_to(data_type).ok(),
            };
            let value = value.cast_to(data_type).ok()?;
            let mins = i64::min_values_iter(col_index_meta).enumerate();
            let maxs = i64::max_values_iter(col_index_meta);
            Some(
                mins.zip(maxs)
                    .map(|((idx, min), max)| {
                        let min = min.and_then(&to_scalar);
                        let max = max.and_then(&to_scalar);
                        let null_count = page_null_count(idx);
                        eval_cmp_stats_page(
                            op,
                            &value,
                            min.as_ref(),
                            max.as_ref(),
                            null_count,
                            nullable,
                        )
                    })
                    .collect(),
            )
        }
        ColumnIndexMetaData::FLOAT(_) => {
            let value = value.cast_to(&DataType::Float32).ok()?;
            let value = match value {
                ScalarValue::Float32(Some(v)) => v,
                _ => return None,
            };
            let mins = f32::min_values_iter(col_index_meta).enumerate();
            let maxs = f32::max_values_iter(col_index_meta);
            Some(
                mins.zip(maxs)
                    .map(|((idx, min), max)| {
                        let min = min.map(|v| ScalarValue::Float32(Some(v)));
                        let max = max.map(|v| ScalarValue::Float32(Some(v)));
                        let null_count = page_null_count(idx);
                        eval_cmp_stats_page(
                            op,
                            &ScalarValue::Float32(Some(value)),
                            min.as_ref(),
                            max.as_ref(),
                            null_count,
                            nullable,
                        )
                    })
                    .collect(),
            )
        }
        ColumnIndexMetaData::DOUBLE(_) => {
            let value = value.cast_to(&DataType::Float64).ok()?;
            let value = match value {
                ScalarValue::Float64(Some(v)) => v,
                _ => return None,
            };
            let mins = f64::min_values_iter(col_index_meta).enumerate();
            let maxs = f64::max_values_iter(col_index_meta);
            Some(
                mins.zip(maxs)
                    .map(|((idx, min), max)| {
                        let min = min.map(|v| ScalarValue::Float64(Some(v)));
                        let max = max.map(|v| ScalarValue::Float64(Some(v)));
                        let null_count = page_null_count(idx);
                        eval_cmp_stats_page(
                            op,
                            &ScalarValue::Float64(Some(value)),
                            min.as_ref(),
                            max.as_ref(),
                            null_count,
                            nullable,
                        )
                    })
                    .collect(),
            )
        }
        ColumnIndexMetaData::BYTE_ARRAY(_) => {
            let mins = parquet::data_type::ByteArray::min_values_iter(col_index_meta).enumerate();
            let maxs = parquet::data_type::ByteArray::max_values_iter(col_index_meta);
            let value = value.cast_to(data_type).ok()?;
            Some(
                mins.zip(maxs)
                    .map(|((idx, min), max)| {
                        let min =
                            min.and_then(|v| stats::byte_array_to_scalar(v.data(), data_type));
                        let max =
                            max.and_then(|v| stats::byte_array_to_scalar(v.data(), data_type));
                        let null_count = page_null_count(idx);
                        eval_cmp_stats_page(
                            op,
                            &value,
                            min.as_ref(),
                            max.as_ref(),
                            null_count,
                            nullable,
                        )
                    })
                    .collect(),
            )
        }
        ColumnIndexMetaData::FIXED_LEN_BYTE_ARRAY(_) => {
            let mins =
                parquet::data_type::FixedLenByteArray::min_values_iter(col_index_meta).enumerate();
            let maxs = parquet::data_type::FixedLenByteArray::max_values_iter(col_index_meta);
            let value = value.cast_to(data_type).ok()?;
            Some(
                mins.zip(maxs)
                    .map(|((idx, min), max)| {
                        let min =
                            min.and_then(|v| stats::byte_array_to_scalar(v.data(), data_type));
                        let max =
                            max.and_then(|v| stats::byte_array_to_scalar(v.data(), data_type));
                        let null_count = page_null_count(idx);
                        eval_cmp_stats_page(
                            op,
                            &value,
                            min.as_ref(),
                            max.as_ref(),
                            null_count,
                            nullable,
                        )
                    })
                    .collect(),
            )
        }
        ColumnIndexMetaData::NONE => None,
        _ => None,
    }
}

fn page_null_count_or_zero(
    page_null_count: Option<i64>,
    row_group_nulls: Option<u64>,
) -> Option<i64> {
    match page_null_count {
        Some(count) => Some(count),
        None if row_group_nulls == Some(0) => Some(0),
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use super::page_null_count_or_zero;

    #[test]
    fn page_null_count_uses_page_value_when_present() {
        assert_eq!(page_null_count_or_zero(Some(3), Some(0)), Some(3));
        assert_eq!(page_null_count_or_zero(Some(3), Some(10)), Some(3));
        assert_eq!(page_null_count_or_zero(Some(3), None), Some(3));
    }

    #[test]
    fn page_null_count_defaults_to_zero_when_row_group_has_no_nulls() {
        assert_eq!(page_null_count_or_zero(None, Some(0)), Some(0));
    }

    #[test]
    fn page_null_count_stays_missing_when_row_group_nulls_unknown_or_nonzero() {
        assert_eq!(page_null_count_or_zero(None, Some(2)), None);
        assert_eq!(page_null_count_or_zero(None, None), None);
    }
}
