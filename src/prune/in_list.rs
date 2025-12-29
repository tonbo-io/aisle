use datafusion_common::ScalarValue;
use crate::ir::{CmpOp, TriState};

use super::context::RowGroupContext;
use super::{cmp, stats};
use super::page::PagePruning;

pub(super) fn eval_in_list(
    column: &str,
    values: &[ScalarValue],
    ctx: &RowGroupContext<'_>,
) -> TriState {
    let (min, max, null_count, row_count) = match stats::stats_for_column(column, ctx) {
        Some(stats) => stats,
        None => return TriState::Unknown,
    };
    let mut any_true = false;
    let mut all_false = true;
    for value in values {
        let tri = cmp::eval_cmp_stats(
            CmpOp::Eq,
            value,
            Some(&min),
            Some(&max),
            null_count,
            row_count,
        );
        match tri {
            TriState::True => {
                any_true = true;
                all_false = false;
                break;
            }
            TriState::Unknown => {
                all_false = false;
            }
            TriState::False => {}
        }
    }
    if any_true {
        TriState::True
    } else if all_false {
        TriState::False
    } else {
        TriState::Unknown
    }
}

pub(super) fn page_selection_for_in_list(
    column: &str,
    values: &[ScalarValue],
    ctx: &RowGroupContext<'_>,
) -> Option<PagePruning> {
    // IN as OR of equalities - all-or-nothing
    let mut selection: Option<PagePruning> = None;
    for value in values {
        let sel = cmp::page_selection_for_cmp(column, CmpOp::Eq, value, ctx)?; // Fail fast
        selection = match selection {
            None => Some(sel),
            Some(existing) => Some(existing.union(sel)),
        };
    }
    selection
}
