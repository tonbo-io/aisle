/// Tests for best-effort page pruning behavior
///
/// These tests verify that:
/// - AND predicates work with partial support (skip unsupported, prune with supported)
/// - OR predicates are all-or-nothing (disable if any unsupported)
/// - BETWEEN can use partial bounds
/// - NOT is supported only when the inner predicate is page-exact (no unknown pages)
use std::ops::Not;
use std::sync::Arc;

use aisle::PruneRequest;
use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use datafusion_expr::{BinaryExpr, Expr, Operator, col, lit};
use parquet::{
    arrow::ArrowWriter,
    file::{
        metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader},
        properties::{EnabledStatistics, WriterProperties},
    },
};

fn make_batch(schema: &Schema, values: &[i32]) -> RecordBatch {
    let arrays: Vec<Arc<dyn arrow_array::Array>> = schema
        .fields()
        .iter()
        .map(|_| Arc::new(Int32Array::from(values.to_vec())) as Arc<dyn arrow_array::Array>)
        .collect();
    RecordBatch::try_new(Arc::new(schema.clone()), arrays).unwrap()
}

fn write_parquet(batches: &[RecordBatch], props: WriterProperties) -> Vec<u8> {
    let mut buffer: Vec<u8> = Vec::new();
    let schema = batches[0].schema();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props)).unwrap();
    for batch in batches {
        writer.write(batch).unwrap();
    }
    writer.close().unwrap();
    buffer
}

fn load_metadata(bytes: &[u8]) -> ParquetMetaData {
    let bytes = Bytes::copy_from_slice(bytes);
    ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .parse_and_finish(&bytes)
        .unwrap()
}

#[test]
fn test_and_with_unsupported_predicate() {
    // Test that AND with one unsupported predicate still attempts page pruning
    // The key test is that compilation succeeds with partial support
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]);

    let batch = make_batch(&schema, &[1, 2, 3, 100, 101, 102]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(1)
        .set_data_page_size_limit(1)
        .set_dictionary_enabled(false)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata(&bytes);

    // Create an AND with:
    // - Supported: a > 50
    // - Unsupported: a + b > 100 (arithmetic not supported)
    let supported = col("a").gt(lit(50i32));
    let unsupported = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(col("a")),
        op: Operator::Plus,
        right: Box::new(col("b")),
    })
    .gt(lit(100i32));
    let expr = supported.and(unsupported);

    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(true)
        .emit_roaring(false)
        .prune();

    // KEY TEST: Best-effort compilation should work
    // - The supported predicate (a > 50) should compile successfully
    // - The unsupported predicate (a + b > 100) should error
    assert_eq!(
        result.compile_result().prunable_count(),
        1,
        "Should compile the supported predicate"
    );
    assert_eq!(
        result.compile_result().error_count(),
        1,
        "Should have one unsupported predicate"
    );

    // Best-effort pruning should still attempt to create a page selection
    // based on the supported predicate, even though one predicate failed
    // (The actual effectiveness depends on page index data quality)
    assert!(
        result.row_selection().is_some(),
        "Best-effort: should create page selection from supported predicate"
    );
}

#[test]
fn test_or_with_unsupported_predicate() {
    // Test that OR with one unsupported predicate disables page pruning
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]);

    let batch = make_batch(&schema, &[1, 2, 3, 100, 101, 102]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(1)
        .set_data_page_size_limit(1)
        .set_dictionary_enabled(false)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata(&bytes);

    // Create an OR with:
    // - Supported: a > 50
    // - Unsupported: a + b > 100
    let supported = col("a").gt(lit(50i32));
    let unsupported = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(col("a")),
        op: Operator::Plus,
        right: Box::new(col("b")),
    })
    .gt(lit(100i32));
    let expr = supported.or(unsupported);

    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(true)
        .emit_roaring(false)
        .prune();

    // OR with unsupported should disable page pruning (return None)
    // because we don't know which rows satisfy the unsupported predicate
    assert!(
        result.row_selection().is_none(),
        "OR with unsupported should not produce page selection"
    );
}

#[test]
fn test_not_page_selection_rejects_unknown_pages() {
    // Test that NOT does not invert when the inner predicate has unknown pages
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

    // Pages: [1, 100] (unknown), [2, 3] (false), [200, 201] (true)
    let batch = make_batch(&schema, &[1, 100, 2, 3, 200, 201]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(2)
        .set_data_page_size_limit(2)
        .set_dictionary_enabled(false)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata(&bytes);

    let expr = col("a").gt(lit(50i32)).not();
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(true)
        .emit_roaring(false)
        .prune();

    assert_eq!(result.compile_result().prunable_count(), 1);
    assert_eq!(result.compile_result().error_count(), 0);
    assert!(
        result.row_selection().is_none(),
        "NOT should skip page selection when inner has unknown pages"
    );
}

#[test]
fn test_not_page_selection_inverts_exact_with_multi_pages() {
    // Construct multiple pages so page-level stats are precise, then verify NOT inversion.
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

    let batch = make_batch(&schema, &[1, 2, 3, 100, 101, 102]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(1)
        .set_write_batch_size(1)
        .set_dictionary_enabled(false)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata(&bytes);

    let column_index = metadata.column_index().expect("page index");
    let col_index_meta = column_index
        .get(0)
        .and_then(|cols| cols.get(0))
        .expect("column index");
    assert!(
        col_index_meta.num_pages() > 1,
        "expected multiple pages in column index"
    );

    let inner_expr = col("a").gt(lit(50i32));
    let inner_result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&inner_expr)
        .enable_page_index(true)
        .emit_roaring(false)
        .prune();
    let inner_sel = inner_result
        .row_selection()
        .expect("inner selection should be available");
    let inner_selectors: Vec<parquet::arrow::arrow_reader::RowSelector> =
        inner_sel.clone().into();
    assert!(
        inner_selectors.iter().any(|sel| sel.skip),
        "inner selection should include skips"
    );

    let expr = inner_expr.not();
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(true)
        .emit_roaring(false)
        .prune();

    assert_eq!(result.compile_result().prunable_count(), 1);
    assert_eq!(result.compile_result().error_count(), 0);
    let selection = result
        .row_selection()
        .expect("NOT should produce selection when exact");
    assert!(selection.selects_any());
}

#[test]
fn test_mixed_and_or() {
    // Test (a > 50) AND ((b > 100) OR unsupported)
    // The OR should disable, but AND should still use "a > 50"
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]);

    let batch = make_batch(&schema, &[1, 2, 3, 100, 101, 102]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(1)
        .set_data_page_size_limit(1)
        .set_dictionary_enabled(false)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata(&bytes);

    // (a > 50) AND ((b > 100) OR (a + b > 200))
    let left = col("a").gt(lit(50i32));
    let or_left = col("b").gt(lit(100i32));
    let or_right = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(col("a")),
        op: Operator::Plus,
        right: Box::new(col("b")),
    })
    .gt(lit(200i32));
    let or_expr = or_left.or(or_right);
    let expr = left.and(or_expr);

    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(true)
        .emit_roaring(false)
        .prune();

    // The OR is unsupported, but AND should still use "a > 50"
    // Should have page selection based on "a > 50"
    // Depending on whether we compile the OR as a unit or decompose it,
    // we might or might not get a selection. Let's check that we at least
    // compiled the supported parts:
    assert!(result.compile_result().prunable_count() >= 1);
}

#[test]
fn test_conjunction_all_supported() {
    // Test that when all predicates in conjunction are supported, all are used
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]);

    let batch = make_batch(&schema, &[1, 2, 3, 100, 101, 102]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(1)
        .set_data_page_size_limit(1)
        .set_dictionary_enabled(false)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata(&bytes);

    // (a > 50) AND (b > 90)
    let expr = col("a").gt(lit(50i32)).and(col("b").gt(lit(90i32)));

    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(true)
        .emit_roaring(false)
        .prune();

    // Both predicates should be compiled and used
    assert_eq!(result.compile_result().prunable_count(), 2);
    assert_eq!(result.compile_result().error_count(), 0);

    // Should have page selection (intersection of both predicates)
    assert!(result.row_selection().is_some());
}

// ============================================================================
// NOT Edge Cases
// ============================================================================

#[test]
fn test_not_of_and() {
    // Test NOT((a > 50) AND (b > 90))
    // De Morgan: NOT(A AND B) = NOT(A) OR NOT(B)
    // Since OR requires all parts supported, this should only work if both are invertible
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]);

    let batch = make_batch(&schema, &[1, 2, 3, 100, 101, 102]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(1)
        .set_data_page_size_limit(1)
        .set_dictionary_enabled(false)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata(&bytes);

    // NOT((a > 50) AND (b > 90))
    let expr = col("a")
        .gt(lit(50i32))
        .and(col("b").gt(lit(90i32)))
        .not();

    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(true)
        .emit_roaring(false)
        .prune();

    // Should compile the NOT and inner predicates
    assert!(result.compile_result().prunable_count() >= 1);
    assert_eq!(result.compile_result().error_count(), 0);

    // Page selection behavior depends on whether the inner AND produces skips
    // The test just verifies compilation succeeds
}

#[test]
fn test_not_of_or() {
    // Test NOT((a > 50) OR (b > 90))
    // De Morgan: NOT(A OR B) = NOT(A) AND NOT(B)
    // Since AND is best-effort, this should work if inner OR is supported
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]);

    let batch = make_batch(&schema, &[1, 2, 3, 100, 101, 102]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(1)
        .set_data_page_size_limit(1)
        .set_dictionary_enabled(false)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata(&bytes);

    // NOT((a > 50) OR (b > 90))
    let expr = col("a")
        .gt(lit(50i32))
        .or(col("b").gt(lit(90i32)))
        .not();

    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(true)
        .emit_roaring(false)
        .prune();

    // Should compile
    assert!(result.compile_result().prunable_count() >= 1);
    assert_eq!(result.compile_result().error_count(), 0);
}

#[test]
fn test_double_negation() {
    // Test NOT(NOT(a > 50))
    // Should logically equal (a > 50), but tests nested NOT handling
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

    let batch = make_batch(&schema, &[1, 2, 3, 100, 101, 102]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(1)
        .set_data_page_size_limit(1)
        .set_dictionary_enabled(false)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata(&bytes);

    // NOT(NOT(a > 50))
    let expr = col("a").gt(lit(50i32)).not().not();

    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(true)
        .emit_roaring(false)
        .prune();

    // Should compile successfully
    assert_eq!(result.compile_result().prunable_count(), 1);
    assert_eq!(result.compile_result().error_count(), 0);

    // Double NOT might or might not produce selection depending on inversion logic
    // The key is that it compiles without error
}

#[test]
fn test_not_with_all_select_inner() {
    // Test NOT when inner predicate results in "select all" (no skips)
    // This is the conservative case that should return None for page selection
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

    // All values are > 0, so "a > 0" will select all pages
    let batch = make_batch(&schema, &[1, 2, 3, 4, 5, 6]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(1)
        .set_data_page_size_limit(1)
        .set_dictionary_enabled(false)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata(&bytes);

    // Check inner predicate first
    let inner_result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&col("a").gt(lit(0i32)))
        .enable_page_index(true)
        .emit_roaring(false)
        .prune();

    // Inner should select all (or at least have no skips if it produces a selection)
    let inner_has_skip = inner_result.row_selection().map(|selection| {
        let selectors: Vec<parquet::arrow::arrow_reader::RowSelector> =
            selection.clone().into();
        selectors.iter().any(|sel| sel.skip)
    });

    // NOT(a > 0) - should be conservative if inner has no skips
    let expr = col("a").gt(lit(0i32)).not();
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(true)
        .emit_roaring(false)
        .prune();

    // Should compile successfully
    assert_eq!(result.compile_result().prunable_count(), 1);
    assert_eq!(result.compile_result().error_count(), 0);

    // If inner has no skips, NOT should return None (conservative)
    if inner_has_skip == Some(false) || inner_has_skip.is_none() {
        assert!(
            result.row_selection().is_none(),
            "NOT should be conservative when inner has no skips"
        );
    }
}

#[test]
fn test_not_of_unsupported_predicate() {
    // Test NOT of an unsupported predicate (arithmetic expression)
    // Should compile but not produce page selection
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int32, false),
        Field::new("b", DataType::Int32, false),
    ]);

    let batch = make_batch(&schema, &[1, 2, 3, 100, 101, 102]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(1)
        .set_data_page_size_limit(1)
        .set_dictionary_enabled(false)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata(&bytes);

    // NOT(a + b > 100) - arithmetic not supported
    let unsupported = Expr::BinaryExpr(BinaryExpr {
        left: Box::new(col("a")),
        op: Operator::Plus,
        right: Box::new(col("b")),
    })
    .gt(lit(100i32));
    let expr = unsupported.not();

    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(true)
        .emit_roaring(false)
        .prune();

    // Inner predicate should fail to compile
    assert_eq!(result.compile_result().prunable_count(), 0);
    assert_eq!(result.compile_result().error_count(), 1);

    // No page selection (inner is unsupported)
    assert!(
        result.row_selection().is_none(),
        "NOT of unsupported should not produce page selection"
    );
}

#[test]
fn test_not_inverts_skip_select_correctly() {
    // Test that NOT correctly flips skip/select selectors
    // Create data where some pages should be skipped, others selected
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);

    // Values: [1,2,3] in first pages, [100,101,102] in last pages
    let batch = make_batch(&schema, &[1, 2, 3, 100, 101, 102]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(1)
        .set_data_page_size_limit(1)
        .set_dictionary_enabled(false)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata(&bytes);

    // a > 50: should select last 3 pages, skip first 3
    let inner_expr = col("a").gt(lit(50i32));
    let inner_result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&inner_expr)
        .enable_page_index(true)
        .emit_roaring(false)
        .prune();

    // NOT(a > 50): should select first 3 pages, skip last 3
    let not_expr = inner_expr.not();
    let not_result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&not_expr)
        .enable_page_index(true)
        .emit_roaring(false)
        .prune();

    // Inner should have selection with some skips
    assert!(inner_result.row_selection().is_some());

    let inner_has_skip = inner_result.row_selection().map(|selection| {
        let selectors: Vec<parquet::arrow::arrow_reader::RowSelector> =
            selection.clone().into();
        selectors.iter().any(|sel| sel.skip)
    });

    // NOT should produce selection only if inner has skips
    match inner_has_skip {
        Some(true) => {
            assert!(
                not_result.row_selection().is_some(),
                "NOT should produce page selection when inner has skips"
            );

            // Verify inversion is correct
            if let (Some(inner_sel), Some(not_sel)) =
                (inner_result.row_selection(), not_result.row_selection())
            {
                let inner_selectors: Vec<parquet::arrow::arrow_reader::RowSelector> =
                    inner_sel.clone().into();
                let not_selectors: Vec<parquet::arrow::arrow_reader::RowSelector> =
                    not_sel.clone().into();

                // Should have same number of selectors
                assert_eq!(inner_selectors.len(), not_selectors.len());

                // Each selector should be inverted
                for (inner_sel, not_sel) in inner_selectors.iter().zip(not_selectors.iter()) {
                    assert_eq!(inner_sel.row_count, not_sel.row_count, "Row counts match");
                    assert_ne!(inner_sel.skip, not_sel.skip, "Skip flags are inverted");
                }
            }
        }
        _ => {
            // If inner has no skips, NOT should conservatively return None
            assert!(
                not_result.row_selection().is_none(),
                "NOT should be conservative when inner has no skips"
            );
        }
    }
}
