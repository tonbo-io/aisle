use super::{context::RowGroupContext, page, page::PagePruning};
use crate::ir::TriState;

pub(super) fn eval_is_null(column: &str, negated: bool, ctx: &RowGroupContext<'_>) -> TriState {
    let row_group = ctx.metadata.row_group(ctx.row_group_idx);
    let col_idx = match ctx.column_lookup.get(column) {
        Some(idx) => *idx,
        None => return TriState::Unknown,
    };
    let stats = row_group.column(col_idx).statistics();
    let row_count = row_group.num_rows() as u64;
    let null_count = stats.and_then(|s| s.null_count_opt());

    let base = match null_count {
        Some(0) => TriState::False,
        Some(count) if count == row_count => TriState::True,
        Some(_) => TriState::Unknown,
        None => TriState::Unknown,
    };
    if negated { base.not() } else { base }
}

pub(super) fn page_selection_for_is_null(
    column: &str,
    negated: bool,
    ctx: &RowGroupContext<'_>,
) -> Option<PagePruning> {
    let row_group = ctx.metadata.row_group(ctx.row_group_idx);
    let col_idx = *ctx.column_lookup.get(column)?;
    let column_index = ctx.metadata.column_index()?;
    let offset_index = ctx.metadata.offset_index()?;
    let col_index_meta = column_index.get(ctx.row_group_idx)?.get(col_idx)?;
    let offset_meta = offset_index.get(ctx.row_group_idx)?.get(col_idx)?;
    let page_ranges = page::build_page_ranges(offset_meta, row_group.num_rows() as usize)?;
    let mut exact = true;
    let mut selected_ranges = Vec::with_capacity(page_ranges.len());
    for (i, range) in page_ranges.into_iter().enumerate() {
        let base = if col_index_meta.is_null_page(i) {
            TriState::True
        } else if col_index_meta.null_count(i) == Some(0) {
            TriState::False
        } else {
            TriState::Unknown
        };
        let tri = if negated { base.not() } else { base };
        if tri == TriState::Unknown {
            exact = false;
        }
        if tri != TriState::False {
            selected_ranges.push(range);
        }
    }
    let selection = parquet::arrow::arrow_reader::RowSelection::from_consecutive_ranges(
        selected_ranges.into_iter(),
        row_group.num_rows() as usize,
    );
    Some(PagePruning::new(selection, exact))
}
