use parquet::arrow::arrow_reader::RowSelection;
use roaring::RoaringBitmap;

use crate::{AisleResult, selection::row_selection_to_roaring};

/// Result of metadata pruning
#[derive(Clone, Debug)]
pub struct PruneResult {
    row_groups: Vec<usize>,
    row_selection: Option<RowSelection>,
    roaring: Option<RoaringBitmap>,
    compile: AisleResult,
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
}

impl PruneResult {
    pub(super) fn new(
        row_groups: Vec<usize>,
        row_selection: Option<RowSelection>,
        roaring: Option<RoaringBitmap>,
        compile: AisleResult,
    ) -> Self {
        Self {
            row_groups,
            row_selection,
            roaring,
            compile,
        }
    }
}
