use std::collections::{HashMap, HashSet};

use arrow_schema::Schema;
use parquet::{
    arrow::arrow_reader::{RowSelection, RowSelector},
    bloom_filter::Sbbf,
    file::metadata::ParquetMetaData,
};

use crate::compile::CompileResult;
use crate::ir::{IrExpr, TriState};
use crate::selection::row_selection_to_roaring;

use super::context::{RowGroupContext, build_column_lookup};
use super::eval;
use super::options::PruneOptions;
use super::provider::AsyncBloomFilterProvider;
use super::result::PruneResult;

pub(crate) fn prune_compiled(
    metadata: &ParquetMetaData,
    schema: &Schema,
    compile: CompileResult,
    options: &PruneOptions,
) -> PruneResult {
    prune_compiled_with_bloom(metadata, schema, compile, options)
}

pub(super) fn prune_compiled_with_bloom(
    metadata: &ParquetMetaData,
    schema: &Schema,
    compile: CompileResult,
    options: &PruneOptions,
) -> PruneResult {
    let evaluator = PruneEvaluator::new(metadata, schema);
    let mut row_groups = Vec::new();
    let mut selections = Vec::new();
    let mut any_selection = false;

    for row_group_idx in 0..metadata.num_row_groups() {
        let row_count = metadata.row_group(row_group_idx).num_rows() as usize;
        let tri = evaluator.eval_row_group_conjunction(compile.prunable(), row_group_idx, None);
        if tri == TriState::False {
            continue;
        }

        let mut selection = if options.enable_page_index() {
            evaluator.eval_pages_for_predicates(compile.prunable(), row_group_idx)
        } else {
            None
        };

        if let Some(sel) = selection.as_ref() {
            if !sel.selects_any() {
                continue;
            }
            any_selection = true;
        }

        row_groups.push(row_group_idx);
        selections.push((row_count, selection.take()));
    }

    let row_selection = if any_selection {
        Some(concat_selections(&selections))
    } else {
        None
    };

    let roaring = row_selection.as_ref().and_then(|selection| {
        if options.emit_roaring() {
            let total_rows: u64 = selections.iter().map(|(rows, _)| *rows as u64).sum();

            if total_rows > u32::MAX as u64 {
                // Dataset too large for RoaringBitmap
                // This is expected for very large datasets (>4.2B rows)
                // Users should use RowSelection directly in this case
                eprintln!(
                    "Note: Dataset has {} rows (exceeds u32::MAX limit of {}). \
                     RoaringBitmap output skipped. Use RowSelection for large datasets.",
                    total_rows,
                    u32::MAX
                );
                None
            } else {
                row_selection_to_roaring(selection, total_rows)
            }
        } else {
            None
        }
    });

    PruneResult::new(row_groups, row_selection, roaring, compile)
}

pub(crate) async fn prune_compiled_with_bloom_provider<P: AsyncBloomFilterProvider>(
    metadata: &ParquetMetaData,
    schema: &Schema,
    compile: CompileResult,
    options: &PruneOptions,
    provider: &mut P,
) -> PruneResult {
    let evaluator = PruneEvaluator::new(metadata, schema);
    let mut row_groups = Vec::new();
    let mut selections = Vec::new();
    let mut any_selection = false;

    let bloom_columns = if options.enable_bloom_filter() {
        collect_bloom_columns(compile.prunable())
    } else {
        HashSet::new()
    };

    for row_group_idx in 0..metadata.num_row_groups() {
        let row_count = metadata.row_group(row_group_idx).num_rows() as usize;
        let bloom_filters = if !bloom_columns.is_empty() {
            load_bloom_filters_async(
                provider,
                row_group_idx,
                evaluator.column_lookup(),
                &bloom_columns,
            )
            .await
        } else {
            None
        };

        let tri =
            evaluator.eval_row_group_conjunction(compile.prunable(), row_group_idx, bloom_filters);
        if tri == TriState::False {
            continue;
        }

        let mut selection = if options.enable_page_index() {
            evaluator.eval_pages_for_predicates(compile.prunable(), row_group_idx)
        } else {
            None
        };

        if let Some(sel) = selection.as_ref() {
            if !sel.selects_any() {
                continue;
            }
            any_selection = true;
        }

        row_groups.push(row_group_idx);
        selections.push((row_count, selection.take()));
    }

    let row_selection = if any_selection {
        Some(concat_selections(&selections))
    } else {
        None
    };

    let roaring = row_selection.as_ref().and_then(|selection| {
        if options.emit_roaring() {
            let total_rows: u64 = selections.iter().map(|(rows, _)| *rows as u64).sum();

            if total_rows > u32::MAX as u64 {
                // Dataset too large for RoaringBitmap
                // This is expected for very large datasets (>4.2B rows)
                // Users should use RowSelection directly in this case
                eprintln!(
                    "Note: Dataset has {} rows (exceeds u32::MAX limit of {}). \
                     RoaringBitmap output skipped. Use RowSelection for large datasets.",
                    total_rows,
                    u32::MAX
                );
                None
            } else {
                row_selection_to_roaring(selection, total_rows)
            }
        } else {
            None
        }
    });

    PruneResult::new(row_groups, row_selection, roaring, compile)
}

fn concat_selections(selections: &[(usize, Option<RowSelection>)]) -> RowSelection {
    let mut combined = Vec::new();
    for (row_count, selection) in selections {
        let selection = selection
            .clone()
            .unwrap_or_else(|| RowSelection::from(vec![RowSelector::select(*row_count)]));
        let mut selectors: Vec<RowSelector> = selection.into();
        combined.append(&mut selectors);
    }
    RowSelection::from(combined)
}

fn collect_bloom_columns(predicates: &[IrExpr]) -> HashSet<String> {
    let mut columns = HashSet::new();
    for predicate in predicates {
        collect_bloom_columns_for_expr(predicate, &mut columns);
    }
    columns
}

fn collect_bloom_columns_for_expr(expr: &IrExpr, columns: &mut HashSet<String>) {
    match expr {
        IrExpr::BloomFilterEq { column, .. } | IrExpr::BloomFilterInList { column, .. } => {
            columns.insert(column.clone());
        }
        IrExpr::And(parts) | IrExpr::Or(parts) => {
            for part in parts {
                collect_bloom_columns_for_expr(part, columns);
            }
        }
        IrExpr::Not(inner) => collect_bloom_columns_for_expr(inner, columns),
        IrExpr::Cmp { .. }
        | IrExpr::Between { .. }
        | IrExpr::InList { .. }
        | IrExpr::StartsWith { .. }
        | IrExpr::IsNull { .. }
        | IrExpr::True
        | IrExpr::False => {}
    }
}

async fn load_bloom_filters_async<P: AsyncBloomFilterProvider>(
    provider: &mut P,
    row_group_idx: usize,
    column_lookup: &HashMap<String, usize>,
    bloom_columns: &HashSet<String>,
) -> Option<HashMap<usize, Sbbf>> {
    let mut requests = Vec::new();
    for column in bloom_columns {
        let Some(col_idx) = column_lookup.get(column) else {
            continue;
        };
        requests.push((row_group_idx, *col_idx));
    }
    if requests.is_empty() {
        return None;
    }

    let batch = provider.bloom_filters_batch(&requests).await;
    let mut filters = HashMap::new();
    for ((rg, col), filter) in batch {
        if rg == row_group_idx {
            filters.insert(col, filter);
        }
    }

    if filters.is_empty() {
        None
    } else {
        Some(filters)
    }
}

struct PruneEvaluator<'a> {
    metadata: &'a ParquetMetaData,
    schema: &'a Schema,
    column_lookup: HashMap<String, usize>,
}

impl<'a> PruneEvaluator<'a> {
    fn new(metadata: &'a ParquetMetaData, schema: &'a Schema) -> Self {
        let column_lookup = build_column_lookup(metadata.file_metadata().schema_descr());
        Self {
            metadata,
            schema,
            column_lookup,
        }
    }

    fn column_lookup(&self) -> &HashMap<String, usize> {
        &self.column_lookup
    }

    fn row_group_context(
        &self,
        row_group_idx: usize,
        bloom_filters: Option<HashMap<usize, Sbbf>>,
    ) -> RowGroupContext<'_> {
        RowGroupContext {
            metadata: self.metadata,
            schema: self.schema,
            column_lookup: &self.column_lookup,
            row_group_idx,
            bloom_filters,
        }
    }

    fn eval_row_group_conjunction(
        &self,
        predicates: &[IrExpr],
        row_group_idx: usize,
        bloom_filters: Option<HashMap<usize, Sbbf>>,
    ) -> TriState {
        let ctx = self.row_group_context(row_group_idx, bloom_filters);
        eval::eval_conjunction(predicates, &ctx)
    }

    fn eval_pages_for_predicates(
        &self,
        predicates: &[IrExpr],
        row_group_idx: usize,
    ) -> Option<RowSelection> {
        let ctx = self.row_group_context(row_group_idx, None);
        eval::page_selection_for_predicates(predicates, &ctx)
    }
}
