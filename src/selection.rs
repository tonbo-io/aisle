use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use roaring::RoaringBitmap;

/// Convert a RowSelection to a RoaringBitmap
///
/// # Limitations
///
/// RoaringBitmap is limited to u32::MAX (~4.2 billion) rows. This function
/// returns `None` if the dataset exceeds this limit. For large datasets,
/// use `RowSelection` directly instead of converting to RoaringBitmap.
///
/// # Arguments
///
/// * `selection` - The row selection to convert
/// * `total_rows` - Total number of rows in the dataset
///
/// # Returns
///
/// * `Some(bitmap)` - Successfully converted selection
/// * `None` - Dataset exceeds u32::MAX rows (limitation of RoaringBitmap)
pub fn row_selection_to_roaring(
    selection: &RowSelection,
    total_rows: u64,
) -> Option<RoaringBitmap> {
    // Check if dataset exceeds RoaringBitmap's u32 limitation
    if total_rows > u32::MAX as u64 {
        return None;
    }

    let mut bitmap = RoaringBitmap::new();
    let mut offset: u64 = 0;

    for selector in selection.iter() {
        let len = selector.row_count as u64;
        if selector.skip {
            offset += len;
            continue;
        }
        if len == 0 {
            continue;
        }
        let end = offset + len;

        // Validate range is within bounds
        if end > total_rows {
            // Selection is malformed (extends beyond total_rows)
            // This shouldn't happen with well-formed selections
            return None;
        }

        // This check is now redundant (total_rows already checked)
        // but kept for extra safety
        if end > u32::MAX as u64 {
            return None;
        }

        bitmap.insert_range(offset as u32..end as u32);
        offset = end;
    }

    Some(bitmap)
}

/// Convert a RoaringBitmap back to a RowSelection
///
/// Reconstructs a Parquet `RowSelection` from a compact RoaringBitmap representation.
/// This is useful for deserializing selections or converting between formats.
///
/// # Arguments
///
/// * `bitmap` - The RoaringBitmap representing selected row indices
/// * `total_rows` - Total number of rows in the dataset
///
/// # Returns
///
/// A `RowSelection` with skip/select ranges matching the bitmap.
///
/// # Examples
///
/// ```
/// use aisle::roaring_to_row_selection;
/// use roaring::RoaringBitmap;
///
/// let mut bitmap = RoaringBitmap::new();
/// bitmap.insert_range(10..20); // Select rows 10-19
/// bitmap.insert_range(50..60); // Select rows 50-59
///
/// let selection = roaring_to_row_selection(&bitmap, 100);
/// // Resulting selection: [skip(10), select(10), skip(30), select(10), skip(40)]
/// ```
pub fn roaring_to_row_selection(bitmap: &RoaringBitmap, total_rows: usize) -> RowSelection {
    let mut selectors = Vec::new();
    let mut cursor = 0usize;
    let mut iter = bitmap.iter().peekable();
    while cursor < total_rows {
        let Some(&next) = iter.peek() else {
            if cursor < total_rows {
                selectors.push(RowSelector::skip(total_rows - cursor));
            }
            break;
        };
        let next = next as usize;
        if next > cursor {
            selectors.push(RowSelector::skip(next - cursor));
            cursor = next;
        }
        let mut run_len = 0usize;
        while let Some(&value) = iter.peek() {
            let value = value as usize;
            if value != cursor + run_len {
                break;
            }
            run_len += 1;
            iter.next();
        }
        if run_len > 0 {
            selectors.push(RowSelector::select(run_len));
            cursor += run_len;
        }
    }
    RowSelection::from(selectors)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn row_selection_round_trip() {
        let selection = RowSelection::from(vec![
            RowSelector::skip(3),
            RowSelector::select(2),
            RowSelector::skip(1),
            RowSelector::select(3),
        ]);
        let bitmap = row_selection_to_roaring(&selection, 9).expect("should convert");
        let back = roaring_to_row_selection(&bitmap, 9);
        let original: Vec<RowSelector> = selection.into();
        let round_trip: Vec<RowSelector> = back.into();
        assert_eq!(original, round_trip);
    }

    #[test]
    fn roaring_rejects_large_datasets() {
        // Dataset exceeds u32::MAX
        let large_total = u32::MAX as u64 + 1;
        let selection = RowSelection::from(vec![RowSelector::select(1000)]);

        let result = row_selection_to_roaring(&selection, large_total);

        assert!(
            result.is_none(),
            "Should return None for datasets exceeding u32::MAX"
        );
    }

    #[test]
    fn roaring_accepts_max_u32_dataset() {
        // Dataset at exactly u32::MAX should work
        let max_u32 = u32::MAX as u64;
        let selection = RowSelection::from(vec![RowSelector::select(1000)]);

        let result = row_selection_to_roaring(&selection, max_u32);

        assert!(
            result.is_some(),
            "Should accept datasets at exactly u32::MAX"
        );
    }

    #[test]
    fn roaring_rejects_malformed_selection() {
        // Selection extends beyond total_rows
        let selection = RowSelection::from(vec![RowSelector::select(3), RowSelector::select(3)]);

        let result = row_selection_to_roaring(&selection, 5);

        assert!(
            result.is_none(),
            "Should return None for selections that exceed total_rows"
        );
    }
}
