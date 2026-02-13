//! Expression evaluation for Parquet metadata pruning.
//!
//! # Architecture
//!
//! ```text
//! eval_expr (dispatcher)
//!     ├─> cmp::eval_cmp        (=, !=, <, >, ...)
//!     ├─> bloom::eval_bloom_eq (bloom filter check)
//!     ├─> between::eval_between (BETWEEN)
//!     └─> ...
//! ```

use parquet::arrow::arrow_reader::RowSelection;

pub(in crate::prune) use super::context::RowGroupContext;
use super::{
    between, bloom, cmp, dictionary, in_list, is_null, page, page::PagePruning, starts_with,
};
use crate::expr::{Expr, TriState};

pub(super) fn eval_conjunction(predicates: &[Expr], ctx: &RowGroupContext<'_>) -> TriState {
    let mut result = TriState::True;
    for predicate in predicates {
        result = result.and(eval_expr(predicate, ctx));
        if result == TriState::False {
            break;
        }
    }
    result
}

pub(super) fn eval_expr(expr: &Expr, ctx: &RowGroupContext<'_>) -> TriState {
    match expr {
        Expr::True => TriState::True,
        Expr::False => TriState::False,
        Expr::Cmp { column, op, value } => cmp::eval_cmp(column, *op, value, ctx),
        Expr::Between {
            column,
            low,
            high,
            inclusive,
        } => between::eval_between(column, low, high, *inclusive, ctx),
        Expr::InList { column, values } => in_list::eval_in_list(column, values, ctx),
        Expr::BloomFilterEq { column, value } => bloom::eval_bloom_eq(column, value, ctx),
        Expr::BloomFilterInList { column, values } => {
            bloom::eval_bloom_in_list(column, values, ctx)
        }
        Expr::DictionaryHintEq { column, value } => {
            dictionary::eval_dictionary_eq(column, value, ctx)
        }
        Expr::DictionaryHintInList { column, values } => {
            dictionary::eval_dictionary_in_list(column, values, ctx)
        }
        Expr::StartsWith { column, prefix } => starts_with::eval_starts_with(column, prefix, ctx),
        Expr::IsNull { column, negated } => is_null::eval_is_null(column, *negated, ctx),
        Expr::And(parts) => parts
            .iter()
            .fold(TriState::True, |acc, expr| acc.and(eval_expr(expr, ctx))),
        Expr::Or(parts) => parts
            .iter()
            .fold(TriState::False, |acc, expr| acc.or(eval_expr(expr, ctx))),
        Expr::Not(inner) => eval_expr(inner, ctx).not(),
    }
}

/// Evaluate a conjunction of predicates for page-level pruning
///
/// Uses best-effort AND logic: if some predicates cannot produce a selection
/// (unsupported), they are skipped and only the supported predicates are used
/// to build the page selection. This is safe because unsupported predicates
/// will be evaluated during actual data reading.
pub(super) fn page_selection_for_predicates(
    predicates: &[Expr],
    ctx: &RowGroupContext<'_>,
) -> Option<RowSelection> {
    let mut selection: Option<PagePruning> = None;
    for predicate in predicates {
        // Best-effort: skip unsupported predicates instead of failing fast
        if let Some(predicate_selection) = page_selection_for_expr(predicate, ctx) {
            selection = match selection {
                None => Some(predicate_selection),
                Some(existing) => Some(existing.intersection(predicate_selection)),
            };
        }
    }
    selection.map(|selection| selection.selection)
}

/// Evaluate an expression for page-level pruning
///
/// Returns `None` if the expression cannot be evaluated at the page level
/// (unsupported predicate type or missing metadata).
///
/// # Conservative Evaluation Rules
///
/// - **AND**: Best-effort - skip unsupported parts, intersect supported ones
/// - **OR**: All-or-nothing - return None if any part unsupported (correctness requirement)
/// - **IN**: Treated as OR, so all-or-nothing
/// - **BETWEEN**: Treated as AND of bounds, so best-effort (can use partial bounds)
/// - **NOT**: Supported only when the inner selection is exact (no unknown pages)
pub(super) fn page_selection_for_expr(
    expr: &Expr,
    ctx: &RowGroupContext<'_>,
) -> Option<PagePruning> {
    match expr {
        Expr::Cmp { column, op, value } => cmp::page_selection_for_cmp(column, *op, value, ctx),
        Expr::Between {
            column,
            low,
            high,
            inclusive,
        } => between::page_selection_for_between(column, low, high, *inclusive, ctx),
        Expr::InList { column, values } => in_list::page_selection_for_in_list(column, values, ctx),
        Expr::BloomFilterEq { .. }
        | Expr::BloomFilterInList { .. }
        | Expr::DictionaryHintEq { .. }
        | Expr::DictionaryHintInList { .. } => None,
        Expr::StartsWith { column, prefix } => {
            starts_with::page_selection_for_starts_with(column, prefix, ctx)
        }
        Expr::IsNull { column, negated } => {
            is_null::page_selection_for_is_null(column, *negated, ctx)
        }
        Expr::And(parts) => {
            // Best-effort AND: skip unsupported, intersect supported
            let mut selection: Option<PagePruning> = None;
            let mut all_supported = true;
            for part in parts {
                if let Some(part_sel) = page_selection_for_expr(part, ctx) {
                    selection = match selection {
                        None => Some(part_sel),
                        Some(existing) => Some(existing.intersection(part_sel)),
                    };
                } else {
                    all_supported = false;
                }
            }
            selection.map(|mut selection| {
                // Mark inexact if any predicate was unsupported
                // Unsupported predicates will be evaluated during data read, so we can't
                // be certain about the final result. This prevents NOT inversion.
                if !all_supported {
                    selection.exact = false;
                }
                selection
            })
        }
        Expr::Or(parts) => {
            // All-or-nothing OR: return None if any part unsupported
            let mut selection: Option<PagePruning> = None;
            for part in parts {
                let part_sel = page_selection_for_expr(part, ctx)?; // Fail fast
                selection = match selection {
                    None => Some(part_sel),
                    Some(existing) => Some(existing.union(part_sel)),
                };
            }
            selection
        }
        Expr::Not(inner) => {
            let inner = page_selection_for_expr(inner, ctx)?;
            // Safety check: Only invert exact selections
            // If the inner selection has Unknown pages (exact=false), those pages were
            // conservatively kept (selected). Inverting would incorrectly skip them,
            // causing false negatives (missing rows that should match NOT predicate).
            if !inner.exact {
                return None; // Conservative: disable NOT for inexact selections
            }
            let total_rows = ctx.metadata.row_group(ctx.row_group_idx).num_rows() as usize;
            let selection = page::invert_selection(&inner.selection, total_rows)?;
            // Inverted selection is always exact if input was exact
            Some(PagePruning::new(selection, true))
        }
        Expr::True | Expr::False => None, // Unsupported: no column to prune on
    }
}
