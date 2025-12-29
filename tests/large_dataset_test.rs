/// Test that demonstrates graceful handling of datasets exceeding u32::MAX
///
/// This test verifies that:
/// 1. The library doesn't silently truncate large datasets
/// 2. RowSelection works for any dataset size
/// 3. RoaringBitmap gracefully returns None for >4.2B row datasets
use aisle::{roaring_to_row_selection, row_selection_to_roaring};
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};

#[test]
fn test_large_dataset_roaring_returns_none() {
    // Simulate a dataset with more than u32::MAX rows
    let large_dataset_rows = u32::MAX as u64 + 1000;

    // Create a selection (this has no size limit)
    let selection = RowSelection::from(vec![
        RowSelector::skip(1000),
        RowSelector::select(500),
        RowSelector::skip(2000),
        RowSelector::select(1500),
    ]);

    // Try to convert to RoaringBitmap - should return None for large datasets
    let roaring = row_selection_to_roaring(&selection, large_dataset_rows);

    assert!(
        roaring.is_none(),
        "RoaringBitmap conversion should return None for datasets > u32::MAX"
    );
}

#[test]
fn test_boundary_condition_at_u32_max() {
    // Dataset with exactly u32::MAX rows should work
    let max_rows = u32::MAX as u64;

    let selection = RowSelection::from(vec![RowSelector::skip(100), RowSelector::select(1000)]);

    let roaring = row_selection_to_roaring(&selection, max_rows);

    assert!(
        roaring.is_some(),
        "RoaringBitmap should work for datasets at exactly u32::MAX"
    );

    // Verify the bitmap has the correct number of selected rows
    let bitmap = roaring.unwrap();
    assert_eq!(
        bitmap.len(),
        1000,
        "Bitmap should contain exactly 1000 selected rows"
    );

    // Verify round-trip preserves the selected row count
    let recovered = roaring_to_row_selection(&bitmap, max_rows as usize);
    let recovered_selectors: Vec<RowSelector> = recovered.into();
    let selected_count: usize = recovered_selectors
        .iter()
        .filter(|s| !s.skip)
        .map(|s| s.row_count)
        .sum();

    assert_eq!(
        selected_count, 1000,
        "Round-trip should preserve 1000 selected rows"
    );
}

#[test]
fn test_row_selection_works_for_any_size() {
    // RowSelection has no size limitation - demonstrate it works with large values
    let very_large = u64::MAX / 2;

    let selection = RowSelection::from(vec![
        RowSelector::skip(very_large as usize / 2),
        RowSelector::select(1000),
    ]);

    // RowSelection can represent this just fine
    let selectors: Vec<RowSelector> = selection.into();
    assert_eq!(selectors.len(), 2);

    // But RoaringBitmap cannot
    let selection_again = RowSelection::from(vec![
        RowSelector::skip(very_large as usize / 2),
        RowSelector::select(1000),
    ]);
    let roaring = row_selection_to_roaring(&selection_again, very_large);
    assert!(
        roaring.is_none(),
        "Should return None for very large datasets"
    );
}

#[test]
fn test_small_dataset_still_works() {
    // Verify that normal-sized datasets still work correctly
    let small_dataset = 1_000_000u64; // 1 million rows - well below u32::MAX

    let selection = RowSelection::from(vec![
        RowSelector::skip(100_000),
        RowSelector::select(500_000),
        RowSelector::skip(200_000),
        RowSelector::select(200_000),
    ]);

    let roaring = row_selection_to_roaring(&selection, small_dataset);

    assert!(
        roaring.is_some(),
        "RoaringBitmap should work for normal-sized datasets"
    );

    // Verify the bitmap is correct
    let bitmap = roaring.unwrap();
    assert_eq!(
        bitmap.len(),
        700_000,
        "Should select 700k rows (500k + 200k)"
    );
}
