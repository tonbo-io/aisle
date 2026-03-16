use std::borrow::Cow;
use std::collections::{HashMap, HashSet};

use arrow_schema::Schema;
use parquet::{
    arrow::arrow_reader::{RowSelection, RowSelector},
    bloom_filter::Sbbf,
    file::metadata::ParquetMetaData,
};

use super::{
    context::{RowGroupContext, build_column_lookup},
    eval,
    options::PruneOptions,
    provider::{AsyncBloomFilterProvider, DictionaryHintEvidence, DictionaryHintValue},
    result::PruneResult,
};
use crate::{
    AisleResult,
    expr::rewrite::{self, MetadataHintConfig},
    expr::{Expr, TriState},
    selection::row_selection_to_roaring,
};

pub(crate) fn prune_compiled(
    metadata: &ParquetMetaData,
    schema: &Schema,
    compile: AisleResult,
    options: &PruneOptions,
    output_projection: Option<Vec<String>>,
    predicate_columns: Option<Vec<String>>,
) -> PruneResult {
    let evaluator = PruneEvaluator::new(metadata, schema);
    let hints = MetadataHintConfig {
        bloom: options.enable_bloom_filter(),
        dictionary: options.enable_dictionary_hints(),
    };
    let predicates = if hints.bloom || hints.dictionary {
        Cow::Owned(rewrite::inject_metadata_hints_all(
            compile.prunable(),
            hints,
        ))
    } else {
        Cow::Borrowed(compile.prunable())
    };
    let mut row_groups = Vec::new();
    let mut selections = Vec::new();
    let mut any_selection = false;

    for row_group_idx in 0..metadata.num_row_groups() {
        let row_count = metadata.row_group(row_group_idx).num_rows() as usize;
        let tri = evaluator.eval_row_group_conjunction(
            predicates.as_ref(),
            row_group_idx,
            None,
            None,
            options,
        );
        if tri == TriState::False {
            continue;
        }

        let mut selection = if options.enable_page_index() {
            evaluator.eval_pages_for_predicates(predicates.as_ref(), row_group_idx, options)
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
                    "Note: Dataset has {} rows (exceeds u32::MAX limit of {}). RoaringBitmap \
                     output skipped. Use RowSelection for large datasets.",
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

    PruneResult::new(
        row_groups,
        row_selection,
        roaring,
        compile,
        output_projection,
        predicate_columns,
    )
}

pub(crate) async fn prune_compiled_with_bloom_provider<P: AsyncBloomFilterProvider>(
    metadata: &ParquetMetaData,
    schema: &Schema,
    compile: AisleResult,
    options: &PruneOptions,
    provider: &mut P,
    output_projection: Option<Vec<String>>,
    predicate_columns: Option<Vec<String>>,
) -> PruneResult {
    let evaluator = PruneEvaluator::new(metadata, schema);
    let hints = MetadataHintConfig {
        bloom: options.enable_bloom_filter(),
        dictionary: options.enable_dictionary_hints(),
    };
    let predicates = if hints.bloom || hints.dictionary {
        Cow::Owned(rewrite::inject_metadata_hints_all(
            compile.prunable(),
            hints,
        ))
    } else {
        Cow::Borrowed(compile.prunable())
    };
    let mut row_groups = Vec::new();
    let mut selections = Vec::new();
    let mut any_selection = false;

    let bloom_columns = if options.enable_bloom_filter() {
        collect_bloom_columns(predicates.as_ref())
    } else {
        HashSet::new()
    };
    let dictionary_columns = if options.enable_dictionary_hints() {
        collect_dictionary_columns(predicates.as_ref())
    } else {
        HashSet::new()
    };
    let bloom_column_indices = resolve_column_indices(evaluator.column_lookup(), &bloom_columns);
    let dictionary_column_indices =
        resolve_column_indices(evaluator.column_lookup(), &dictionary_columns);

    for row_group_idx in 0..metadata.num_row_groups() {
        let row_count = metadata.row_group(row_group_idx).num_rows() as usize;
        let bloom_filters = if !bloom_column_indices.is_empty() {
            load_bloom_filters_async(provider, row_group_idx, &bloom_column_indices).await
        } else {
            None
        };
        let dictionary_hints = if !dictionary_column_indices.is_empty() {
            load_dictionary_hints_async(provider, row_group_idx, &dictionary_column_indices).await
        } else {
            None
        };

        let tri = evaluator.eval_row_group_conjunction(
            predicates.as_ref(),
            row_group_idx,
            bloom_filters,
            dictionary_hints,
            options,
        );
        if tri == TriState::False {
            continue;
        }

        let mut selection = if options.enable_page_index() {
            evaluator.eval_pages_for_predicates(predicates.as_ref(), row_group_idx, options)
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
                    "Note: Dataset has {} rows (exceeds u32::MAX limit of {}). RoaringBitmap \
                     output skipped. Use RowSelection for large datasets.",
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

    PruneResult::new(
        row_groups,
        row_selection,
        roaring,
        compile,
        output_projection,
        predicate_columns,
    )
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

fn collect_bloom_columns(predicates: &[Expr]) -> HashSet<String> {
    let mut columns = HashSet::new();
    for predicate in predicates {
        collect_bloom_columns_for_expr(predicate, &mut columns);
    }
    columns
}

fn collect_bloom_columns_for_expr(expr: &Expr, columns: &mut HashSet<String>) {
    match expr {
        Expr::BloomFilterEq { column, .. } | Expr::BloomFilterInList { column, .. } => {
            columns.insert(column.clone());
        }
        Expr::And(parts) | Expr::Or(parts) => {
            for part in parts {
                collect_bloom_columns_for_expr(part, columns);
            }
        }
        Expr::Not(inner) => collect_bloom_columns_for_expr(inner, columns),
        Expr::Cmp { .. }
        | Expr::Between { .. }
        | Expr::InList { .. }
        | Expr::DictionaryHintEq { .. }
        | Expr::DictionaryHintInList { .. }
        | Expr::StartsWith { .. }
        | Expr::IsNull { .. }
        | Expr::True
        | Expr::False => {}
    }
}

fn collect_dictionary_columns(predicates: &[Expr]) -> HashSet<String> {
    let mut columns = HashSet::new();
    for predicate in predicates {
        collect_dictionary_columns_for_expr(predicate, &mut columns);
    }
    columns
}

fn collect_dictionary_columns_for_expr(expr: &Expr, columns: &mut HashSet<String>) {
    match expr {
        Expr::DictionaryHintEq { column, .. } | Expr::DictionaryHintInList { column, .. } => {
            columns.insert(column.clone());
        }
        Expr::And(parts) | Expr::Or(parts) => {
            for part in parts {
                collect_dictionary_columns_for_expr(part, columns);
            }
        }
        Expr::Not(inner) => collect_dictionary_columns_for_expr(inner, columns),
        Expr::Cmp { .. }
        | Expr::Between { .. }
        | Expr::InList { .. }
        | Expr::BloomFilterEq { .. }
        | Expr::BloomFilterInList { .. }
        | Expr::StartsWith { .. }
        | Expr::IsNull { .. }
        | Expr::True
        | Expr::False => {}
    }
}

fn resolve_column_indices(
    column_lookup: &HashMap<String, usize>,
    columns: &HashSet<String>,
) -> Vec<usize> {
    let mut indices = Vec::with_capacity(columns.len());
    for column in columns {
        let Some(col_idx) = column_lookup.get(column) else {
            continue;
        };
        indices.push(*col_idx);
    }
    indices.sort_unstable();
    indices.dedup();
    indices
}

async fn load_bloom_filters_async<P: AsyncBloomFilterProvider>(
    provider: &mut P,
    row_group_idx: usize,
    bloom_column_indices: &[usize],
) -> Option<HashMap<usize, Sbbf>> {
    let mut requests = Vec::with_capacity(bloom_column_indices.len());
    for &column_idx in bloom_column_indices {
        requests.push((row_group_idx, column_idx));
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

async fn load_dictionary_hints_async<P: AsyncBloomFilterProvider>(
    provider: &mut P,
    row_group_idx: usize,
    dictionary_column_indices: &[usize],
) -> Option<HashMap<usize, HashSet<DictionaryHintValue>>> {
    let mut requests = Vec::with_capacity(dictionary_column_indices.len());
    for &column_idx in dictionary_column_indices {
        requests.push((row_group_idx, column_idx));
    }
    if requests.is_empty() {
        return None;
    }

    let batch = provider.dictionary_hints_batch(&requests).await;
    let mut hints = HashMap::new();
    for ((rg, col), evidence) in batch {
        if rg != row_group_idx {
            continue;
        }
        if let DictionaryHintEvidence::Exact(values) = evidence {
            hints.insert(col, values);
        }
    }

    if hints.is_empty() { None } else { Some(hints) }
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
        dictionary_hints: Option<HashMap<usize, HashSet<DictionaryHintValue>>>,
        options: &'a PruneOptions,
    ) -> RowGroupContext<'_> {
        RowGroupContext {
            metadata: self.metadata,
            schema: self.schema,
            column_lookup: &self.column_lookup,
            row_group_idx,
            bloom_filters,
            dictionary_hints,
            options,
        }
    }

    fn eval_row_group_conjunction(
        &self,
        predicates: &[Expr],
        row_group_idx: usize,
        bloom_filters: Option<HashMap<usize, Sbbf>>,
        dictionary_hints: Option<HashMap<usize, HashSet<DictionaryHintValue>>>,
        options: &PruneOptions,
    ) -> TriState {
        let ctx = self.row_group_context(row_group_idx, bloom_filters, dictionary_hints, options);
        eval::eval_conjunction(predicates, &ctx)
    }

    fn eval_pages_for_predicates(
        &self,
        predicates: &[Expr],
        row_group_idx: usize,
        options: &PruneOptions,
    ) -> Option<RowSelection> {
        let ctx = self.row_group_context(row_group_idx, None, None, options);
        eval::page_selection_for_predicates(predicates, &ctx)
    }
}
