use crate::ir::{CmpOp, TriState};

use super::context::RowGroupContext;
use super::{cmp, stats, strings};
use super::page::PagePruning;

pub(super) fn eval_starts_with(
    column: &str,
    prefix: &str,
    ctx: &RowGroupContext<'_>,
) -> TriState {
    if prefix.is_empty() {
        return TriState::Unknown;
    }
    let data_type = match stats::data_type_for_path(ctx.schema, column) {
        Some(data_type) => data_type,
        None => return TriState::Unknown,
    };
    let lower = match strings::string_scalar_for_type(prefix, &data_type) {
        Some(value) => value,
        None => return TriState::Unknown,
    };
    let (min, max, null_count, row_count) = match stats::stats_for_column(column, ctx) {
        Some(stats) => stats,
        None => return TriState::Unknown,
    };
    let lower_eval = cmp::eval_cmp_stats(
        CmpOp::GtEq,
        &lower,
        Some(&min),
        Some(&max),
        null_count,
        row_count,
    );
    let upper = strings::next_prefix_string(prefix)
        .and_then(|next| strings::string_scalar_for_type(&next, &data_type));
    match upper {
        Some(upper) => {
            let upper_eval = cmp::eval_cmp_stats(
                CmpOp::Lt,
                &upper,
                Some(&min),
                Some(&max),
                null_count,
                row_count,
            );
            lower_eval.and(upper_eval)
        }
        None => lower_eval,
    }
}

pub(super) fn page_selection_for_starts_with(
    column: &str,
    prefix: &str,
    ctx: &RowGroupContext<'_>,
) -> Option<PagePruning> {
    if prefix.is_empty() {
        return None;
    }
    let data_type = stats::data_type_for_path(ctx.schema, column)?;
    let lower = strings::string_scalar_for_type(prefix, &data_type)?;
    let mut low_sel = cmp::page_selection_for_cmp(column, CmpOp::GtEq, &lower, ctx)?;
    let upper = strings::next_prefix_string(prefix)
        .and_then(|next| strings::string_scalar_for_type(&next, &data_type));
    match upper {
        Some(upper) => {
            let high_sel = cmp::page_selection_for_cmp(column, CmpOp::Lt, &upper, ctx)?;
            Some(low_sel.intersection(high_sel))
        }
        None => {
            low_sel.exact = false;
            Some(low_sel)
        }
    }
}
