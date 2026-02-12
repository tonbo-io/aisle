use std::collections::BTreeSet;

use parquet::{
    arrow::{ProjectionMask, arrow_reader::RowSelection},
    schema::types::SchemaDescriptor,
};
use roaring::RoaringBitmap;

use crate::{AisleResult, Expr, selection::row_selection_to_roaring};

/// Result of metadata pruning
#[derive(Clone, Debug)]
pub struct PruneResult {
    row_groups: Vec<usize>,
    row_selection: Option<RowSelection>,
    roaring: Option<RoaringBitmap>,
    compile: AisleResult,
    output_projection: Option<Vec<String>>,
    predicate_columns: Vec<String>,
    required_columns: Vec<String>,
    fallback_required_projection_all: bool,
}

impl PruneResult {
    /// Get the row groups that should be read
    pub fn row_groups(&self) -> &[usize] {
        &self.row_groups
    }

    /// Get the row selection (ranges of rows to read within row groups)
    pub fn row_selection(&self) -> Option<&RowSelection> {
        self.row_selection.as_ref()
    }

    /// Get the roaring bitmap representation (if enabled)
    pub fn roaring(&self) -> Option<&RoaringBitmap> {
        self.roaring.as_ref()
    }

    /// Get the compilation result (what predicates were compiled)
    pub fn compile_result(&self) -> &AisleResult {
        &self.compile
    }

    /// Get the output projection columns requested for this prune request.
    ///
    /// Returns `None` when no explicit output projection was configured.
    pub fn output_projection(&self) -> Option<&[String]> {
        self.output_projection.as_deref()
    }

    /// Get columns referenced by predicate expressions used for pruning.
    ///
    /// Values are deduplicated and returned in sorted order.
    pub fn predicate_columns(&self) -> &[String] {
        &self.predicate_columns
    }

    /// Get the union of predicate columns and output projection columns.
    ///
    /// Values are deduplicated and returned in sorted order.
    pub fn required_columns(&self) -> &[String] {
        &self.required_columns
    }

    /// Build a Parquet projection mask for output columns, if configured.
    pub fn output_projection_mask(
        &self,
        parquet_schema: &SchemaDescriptor,
    ) -> Option<ProjectionMask> {
        self.output_projection.as_ref().map(|columns| {
            ProjectionMask::columns(parquet_schema, columns.iter().map(String::as_str))
        })
    }

    /// Build a Parquet projection mask from [`required_columns`](Self::required_columns).
    pub fn required_projection_mask(&self, parquet_schema: &SchemaDescriptor) -> ProjectionMask {
        if self.fallback_required_projection_all || self.required_columns.is_empty() {
            ProjectionMask::all()
        } else {
            ProjectionMask::columns(
                parquet_schema,
                self.required_columns.iter().map(String::as_str),
            )
        }
    }

    /// Convert to a roaring bitmap, computing it if not already present
    ///
    /// Returns None if:
    /// - There is no row selection, or
    /// - The dataset exceeds u32::MAX rows (RoaringBitmap limitation)
    pub fn into_roaring(self, total_rows: u64) -> Option<RoaringBitmap> {
        if let Some(roaring) = self.roaring {
            return Some(roaring);
        }
        self.row_selection
            .and_then(|sel| row_selection_to_roaring(&sel, total_rows))
    }

    /// Consume the result and return all components
    pub fn into_parts(
        self,
    ) -> (
        Vec<usize>,
        Option<RowSelection>,
        Option<RoaringBitmap>,
        AisleResult,
    ) {
        (
            self.row_groups,
            self.row_selection,
            self.roaring,
            self.compile,
        )
    }

    /// Consume the result and return all components, including projection metadata.
    pub fn into_parts_with_projection(
        self,
    ) -> (
        Vec<usize>,
        Option<RowSelection>,
        Option<RoaringBitmap>,
        AisleResult,
        Option<Vec<String>>,
        Vec<String>,
        Vec<String>,
    ) {
        (
            self.row_groups,
            self.row_selection,
            self.roaring,
            self.compile,
            self.output_projection,
            self.predicate_columns,
            self.required_columns,
        )
    }
}

impl PruneResult {
    pub(super) fn new(
        row_groups: Vec<usize>,
        row_selection: Option<RowSelection>,
        roaring: Option<RoaringBitmap>,
        compile: AisleResult,
        output_projection: Option<Vec<String>>,
        predicate_columns: Option<Vec<String>>,
    ) -> Self {
        let fallback_required_projection_all = compile.has_errors() && predicate_columns.is_none();
        let predicate_columns = predicate_columns
            .map(normalize_columns)
            .unwrap_or_else(|| collect_columns_from_predicates(compile.ir_exprs()));
        let required_columns = merge_columns(&predicate_columns, output_projection.as_deref());
        Self {
            row_groups,
            row_selection,
            roaring,
            compile,
            output_projection,
            predicate_columns,
            required_columns,
            fallback_required_projection_all,
        }
    }
}

fn collect_columns_from_predicates(predicates: &[Expr]) -> Vec<String> {
    let mut columns = BTreeSet::new();
    for predicate in predicates {
        collect_columns_from_expr(predicate, &mut columns);
    }
    columns.into_iter().collect()
}

fn collect_columns_from_expr(expr: &Expr, columns: &mut BTreeSet<String>) {
    match expr {
        Expr::Cmp { column, .. }
        | Expr::Between { column, .. }
        | Expr::InList { column, .. }
        | Expr::BloomFilterEq { column, .. }
        | Expr::BloomFilterInList { column, .. }
        | Expr::StartsWith { column, .. }
        | Expr::IsNull { column, .. } => {
            columns.insert(column.clone());
        }
        Expr::And(parts) | Expr::Or(parts) => {
            for part in parts {
                collect_columns_from_expr(part, columns);
            }
        }
        Expr::Not(inner) => collect_columns_from_expr(inner, columns),
        Expr::True | Expr::False => {}
    }
}

fn merge_columns(
    predicate_columns: &[String],
    output_projection: Option<&[String]>,
) -> Vec<String> {
    let mut columns: BTreeSet<String> = predicate_columns.iter().cloned().collect();
    if let Some(output_columns) = output_projection {
        for column in output_columns {
            columns.insert(column.clone());
        }
    }
    columns.into_iter().collect()
}

fn normalize_columns(columns: Vec<String>) -> Vec<String> {
    let mut unique = BTreeSet::new();
    for column in columns {
        if !column.is_empty() {
            unique.insert(column);
        }
    }
    unique.into_iter().collect()
}
