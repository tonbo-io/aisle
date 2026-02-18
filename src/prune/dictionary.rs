//! Dictionary hint pruning for equality predicates.

use datafusion_common::ScalarValue;

use super::{context::RowGroupContext, provider::DictionaryHintValue};
use crate::expr::TriState;

pub(super) fn eval_dictionary_eq(
    column: &str,
    value: &ScalarValue,
    ctx: &RowGroupContext<'_>,
) -> TriState {
    let hints = match ctx.dictionary_hints_for_column(column) {
        Some(hints) => hints,
        None => return TriState::Unknown,
    };
    if hints.is_empty() {
        return TriState::Unknown;
    }

    match DictionaryHintValue::from_scalar(value) {
        Some(value) => {
            if hints.contains(&value) {
                TriState::Unknown
            } else {
                TriState::False
            }
        }
        None => TriState::Unknown,
    }
}

pub(super) fn eval_dictionary_in_list(
    column: &str,
    values: &[ScalarValue],
    ctx: &RowGroupContext<'_>,
) -> TriState {
    let hints = match ctx.dictionary_hints_for_column(column) {
        Some(hints) => hints,
        None => return TriState::Unknown,
    };
    if hints.is_empty() {
        return TriState::Unknown;
    }

    let mut any_supported = false;
    let mut any_possible = false;

    for value in values {
        match DictionaryHintValue::from_scalar(value) {
            Some(value) => {
                any_supported = true;
                if hints.contains(&value) {
                    any_possible = true;
                    break;
                }
            }
            None => {
                // Unsupported literal type for dictionary hints: conservative keep.
                return TriState::Unknown;
            }
        }
    }

    if any_supported && !any_possible {
        TriState::False
    } else {
        TriState::Unknown
    }
}
