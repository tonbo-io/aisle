use datafusion_common::ScalarValue;

use super::{cmp, context::RowGroupContext, page::PagePruning};
use crate::expr::{CmpOp, TriState};

pub(super) fn eval_between(
    column: &str,
    low: &ScalarValue,
    high: &ScalarValue,
    inclusive: bool,
    ctx: &RowGroupContext<'_>,
) -> TriState {
    let op_low = if inclusive { CmpOp::GtEq } else { CmpOp::Gt };
    let op_high = if inclusive { CmpOp::LtEq } else { CmpOp::Lt };
    let low_eval = cmp::eval_cmp(column, op_low, low, ctx);
    let high_eval = cmp::eval_cmp(column, op_high, high, ctx);
    low_eval.and(high_eval)
}

pub(super) fn page_selection_for_between(
    column: &str,
    low: &ScalarValue,
    high: &ScalarValue,
    inclusive: bool,
    ctx: &RowGroupContext<'_>,
) -> Option<PagePruning> {
    // BETWEEN as AND of bounds - best-effort (use what's available)
    let op_low = if inclusive { CmpOp::GtEq } else { CmpOp::Gt };
    let op_high = if inclusive { CmpOp::LtEq } else { CmpOp::Lt };

    let low_sel = cmp::page_selection_for_cmp(column, op_low, low, ctx);
    let high_sel = cmp::page_selection_for_cmp(column, op_high, high, ctx);

    match (low_sel, high_sel) {
        (Some(low), Some(high)) => Some(low.intersection(high)),
        (Some(mut sel), None) => {
            sel.exact = false;
            Some(sel) /* Best-effort: use available bound */
        }
        (None, Some(mut sel)) => {
            sel.exact = false;
            Some(sel) /* Best-effort: use available bound */
        }
        (None, None) => None,
    }
}
