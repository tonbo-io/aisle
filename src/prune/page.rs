//! Page-level pruning utilities and exactness tracking.
//!
//! # Page-Level Pruning
//!
//! Page-level pruning uses column index (page statistics) to skip individual pages
//! within a row group, providing finer-grained I/O reduction than row-group-only pruning.
//!
//! # Exactness Tracking
//!
//! The `PagePruning` struct tracks whether a page selection is "exact" (definite) or
//! "inexact" (conservative):
//!
//! - **Exact** (`exact = true`): Every page is definitively True or False
//!   - All pages returned TriState::True (keep) or TriState::False (skip)
//!   - No Unknown pages that were conservatively kept
//!   - Safe to invert for NOT operations
//!
//! - **Inexact** (`exact = false`): Some pages are uncertain or predicates unsupported
//!   - Some pages returned TriState::Unknown (conservatively kept)
//!   - Or some predicates in a conjunction couldn't be evaluated
//!   - NOT inversion is unsafe (would incorrectly prune Unknown pages)
//!
//! # Example
//!
//! ```text
//! Predicate: a > 50
//! Page 0: min=1,  max=10   → TriState::False  (all < 50, skip)
//! Page 1: min=45, max=55   → TriState::Unknown (might contain > 50, keep)
//! Page 2: min=60, max=70   → TriState::True   (all > 50, keep)
//!
//! Result: exact = false (Page 1 is Unknown)
//! NOT inversion: Rejected (would incorrectly skip Page 1's >50 values)
//! ```

use std::ops::Range;

use parquet::{
    arrow::arrow_reader::{RowSelection, RowSelector},
    file::page_index::offset_index::OffsetIndexMetaData,
};

/// Page-level selection with exactness tracking.
///
/// Wraps a `RowSelection` with a flag indicating whether the selection is definite
/// (all pages are True/False) or conservative (some pages are Unknown).
///
/// # Exactness Semantics
///
/// - `exact = true`: All pages in the selection are definitively True or False
///   - No TriState::Unknown pages
///   - Safe to invert for NOT operations
///   - Indicates high-confidence pruning decisions
///
/// - `exact = false`: Some pages are uncertain or predicates are unsupported
///   - Contains TriState::Unknown pages (conservatively kept)
///   - Or some predicates in a conjunction couldn't be evaluated
///   - NOT inversion is disabled (would cause incorrect pruning)
///
/// # Usage
///
/// ```text
/// // Exact selection (all pages are True/False)
/// let exact_sel = PagePruning::new(selection, true);
/// let inverted = page::invert_selection(&exact_sel.selection, total_rows); // OK
///
/// // Inexact selection (some Unknown pages)
/// let inexact_sel = PagePruning::new(selection, false);
/// // NOT would check: if !inexact_sel.exact { return None } // Rejected
/// ```
#[derive(Clone, Debug)]
pub(super) struct PagePruning {
    pub(super) selection: RowSelection,
    pub(super) exact: bool,
}

impl PagePruning {
    /// Creates a new `PagePruning` with the given selection and exactness flag.
    pub(super) fn new(selection: RowSelection, exact: bool) -> Self {
        Self { selection, exact }
    }

    /// Returns the intersection of two page selections.
    ///
    /// The result is exact only if **both** inputs are exact. If either side has
    /// Unknown pages, the intersection is also considered inexact (conservative).
    ///
    /// # Logic
    ///
    /// ```text
    /// exact(A ∩ B) = exact(A) AND exact(B)
    /// ```
    ///
    /// # Example
    ///
    /// ```text
    /// A: exact=true,  selection=[select 3, skip 3]
    /// B: exact=false, selection=[skip 2, select 4]
    /// Result: exact=false (B has Unknown pages, so intersection is inexact)
    /// ```
    pub(super) fn intersection(self, other: Self) -> Self {
        Self {
            selection: self.selection.intersection(&other.selection),
            exact: self.exact && other.exact,
        }
    }

    /// Returns the union of two page selections.
    ///
    /// The result is exact only if **both** inputs are exact. If either side has
    /// Unknown pages, we can't be certain about the union (conservative).
    ///
    /// # Logic
    ///
    /// ```text
    /// exact(A ∪ B) = exact(A) AND exact(B)
    /// ```
    ///
    /// # Example
    ///
    /// ```text
    /// A: exact=true,  selection=[select 2, skip 4]
    /// B: exact=false, selection=[skip 3, select 3]
    /// Result: exact=false (B has Unknown pages, so union is inexact)
    /// ```
    ///
    /// # Why Both Must Be Exact
    ///
    /// If A has Unknown pages that were conservatively kept, we don't know
    /// whether those pages truly match. Union with B doesn't change this
    /// uncertainty, so the result remains inexact.
    pub(super) fn union(self, other: Self) -> Self {
        Self {
            selection: self.selection.union(&other.selection),
            exact: self.exact && other.exact,
        }
    }
}

/// Builds row ranges for each page from offset index metadata.
///
/// Converts Parquet page locations (first row index per page) into
/// Rust ranges representing the row span of each page.
///
/// # Returns
///
/// - `Some(Vec<Range<usize>>)` - Row ranges for each page
/// - `None` - If offset index is empty
///
/// # Example
///
/// ```text
/// Input: page_locations = [
///   {first_row_index: 0},
///   {first_row_index: 100},
///   {first_row_index: 250}
/// ], row_group_rows = 300
///
/// Output: [0..100, 100..250, 250..300]
/// ```
pub(super) fn build_page_ranges(
    offset_meta: &OffsetIndexMetaData,
    row_group_rows: usize,
) -> Option<Vec<Range<usize>>> {
    let locations = offset_meta.page_locations();
    if locations.is_empty() {
        return None;
    }
    let mut ranges = Vec::with_capacity(locations.len());
    for i in 0..locations.len() {
        let start = locations[i].first_row_index as usize;
        let end = if i + 1 < locations.len() {
            locations[i + 1].first_row_index as usize
        } else {
            row_group_rows
        };
        if start < end {
            ranges.push(start..end);
        }
    }
    Some(ranges)
}

/// Inverts a row selection by flipping all select/skip selectors.
///
/// Used for NOT operations on page-level pruning. This is **only safe** when the
/// input selection is exact (all pages are definitively True/False, no Unknown pages).
///
/// # Arguments
///
/// * `selection` - The row selection to invert
/// * `total_rows` - Total number of rows in the row group
///
/// # Returns
///
/// - `Some(RowSelection)` - Inverted selection where skip↔select are flipped
/// - `None` - If the selection is invalid (covered_rows > total_rows)
///
/// # Inversion Logic
///
/// ```text
/// Original:  [select(3), skip(3)]
/// Inverted:  [skip(3), select(3)]
/// ```
///
/// # Edge Cases
///
/// 1. **Partial coverage** (covered_rows < total_rows):
///    - Trailing rows are assumed to be skipped in the original
///    - A skip selector is added for the trailing rows before inversion
///
///    ```text
///    Input: [select(3), skip(2)], total_rows=6
///    Step 1: Add trailing skip: [select(3), skip(2), skip(1)]
///    Step 2: Invert: [skip(3), select(2), select(1)]
///    ```
///
/// 2. **Over-coverage** (covered_rows > total_rows):
///    - Invalid state, returns None
///    - Defensive check to prevent logic errors
///
/// 3. **Exact coverage** (covered_rows == total_rows):
///    - No padding needed, directly inverts selectors
///
/// # Safety for NOT
///
/// This function should only be called when `exact = true`:
///
/// ```text
/// // Safe: All pages are True/False
/// let exact_sel = PagePruning::new(selection, true);
/// let inverted = invert_selection(&exact_sel.selection, total_rows); // OK
///
/// // Unsafe: Some pages are Unknown (conservatively kept)
/// let inexact_sel = PagePruning::new(selection, false);
/// // Caller must check: if !inexact_sel.exact { return None }
/// ```
///
/// If the selection contains Unknown pages that were conservatively kept (select),
/// inversion would incorrectly skip them, causing false negatives.
pub(super) fn invert_selection(
    selection: &RowSelection,
    total_rows: usize,
) -> Option<RowSelection> {
    let mut selectors: Vec<RowSelector> = selection.iter().copied().collect();
    let covered_rows: usize = selectors.iter().map(|s| s.row_count).sum();
    if covered_rows > total_rows {
        return None;
    }
    if covered_rows < total_rows {
        // Assume any trailing rows are skipped in the original selection.
        selectors.push(RowSelector::skip(total_rows - covered_rows));
    }
    let inverted = selectors
        .into_iter()
        .map(|s| {
            if s.skip {
                RowSelector::select(s.row_count)
            } else {
                RowSelector::skip(s.row_count)
            }
        })
        .collect::<Vec<_>>();
    Some(RowSelection::from(inverted))
}
