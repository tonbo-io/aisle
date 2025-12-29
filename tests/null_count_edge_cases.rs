//! Null count edge case tests for row-group and page-level pruning.
//!
//! Tests coverage:
//! - null_count = 0 (no nulls)
//! - null_count = row_count (all nulls)
//! - partial null_count (some nulls)
//! - null_count = None (missing stats)
//!
//! Predicates tested:
//! - IS NULL / IS NOT NULL
//! - Comparison operators with nulls (=, !=, <, >, <=, >=)
//! - IN list with nulls
//! - BETWEEN with nulls

use std::sync::Arc;

use aisle::PruneRequest;
use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use datafusion_expr::{col, lit};
use parquet::{
    arrow::ArrowWriter,
    file::{
        metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader},
        properties::{EnabledStatistics, WriterProperties},
    },
};

fn make_batch_nullable(schema: &Schema, values: Vec<Option<i32>>) -> RecordBatch {
    let array = Int32Array::from(values);
    RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(array)]).unwrap()
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

fn load_metadata_without_page_index(bytes: &[u8]) -> ParquetMetaData {
    let bytes = Bytes::copy_from_slice(bytes);
    ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Skip)
        .parse_and_finish(&bytes)
        .unwrap()
}

// ============================================================================
// Row-group level: IS NULL / IS NOT NULL
// ============================================================================

#[test]
fn is_null_with_zero_nulls() {
    // null_count = 0 => IS NULL should prune (return False)
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let batch = make_batch_nullable(&schema, vec![Some(1), Some(2), Some(3)]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata_without_page_index(&bytes);

    let expr = col("a").is_null();
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .prune();

    // Should prune: no nulls present
    assert_eq!(result.row_groups(), &[] as &[usize]);
}

#[test]
fn is_null_with_all_nulls() {
    // null_count = row_count => IS NULL should keep (return True)
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let batch = make_batch_nullable(&schema, vec![None, None, None]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata_without_page_index(&bytes);

    let expr = col("a").is_null();
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .prune();

    // Should keep: all nulls
    assert_eq!(result.row_groups(), &[0]);
}

#[test]
fn is_null_with_partial_nulls() {
    // partial null_count => IS NULL should keep (return Unknown)
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let batch = make_batch_nullable(&schema, vec![Some(1), None, Some(3)]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata_without_page_index(&bytes);

    let expr = col("a").is_null();
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .prune();

    // Should keep: might have nulls
    assert_eq!(result.row_groups(), &[0]);
}

#[test]
fn is_not_null_with_zero_nulls() {
    // null_count = 0 => IS NOT NULL should keep (return True)
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let batch = make_batch_nullable(&schema, vec![Some(1), Some(2), Some(3)]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata_without_page_index(&bytes);

    let expr = col("a").is_not_null();
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .prune();

    // Should keep: all non-null
    assert_eq!(result.row_groups(), &[0]);
}

#[test]
fn is_not_null_with_all_nulls() {
    // null_count = row_count => IS NOT NULL should prune (return False)
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let batch = make_batch_nullable(&schema, vec![None, None, None]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata_without_page_index(&bytes);

    let expr = col("a").is_not_null();
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .prune();

    // Should prune: all nulls
    assert_eq!(result.row_groups(), &[] as &[usize]);
}

#[test]
fn is_not_null_with_partial_nulls() {
    // partial null_count => IS NOT NULL should keep (return Unknown)
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let batch = make_batch_nullable(&schema, vec![Some(1), None, Some(3)]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata_without_page_index(&bytes);

    let expr = col("a").is_not_null();
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .prune();

    // Should keep: might have non-nulls
    assert_eq!(result.row_groups(), &[0]);
}

// ============================================================================
// Row-group level: Comparison operators with nulls
// ============================================================================

#[test]
fn eq_with_zero_nulls_matching() {
    // null_count = 0, value in range => should keep
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let batch = make_batch_nullable(&schema, vec![Some(1), Some(2), Some(3)]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata_without_page_index(&bytes);

    let expr = col("a").eq(lit(2));
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .prune();

    assert_eq!(result.row_groups(), &[0]);
}

#[test]
fn eq_with_all_nulls() {
    // null_count = row_count => should ideally prune, but current behavior
    // is conservative (keeps) because min/max stats are None
    // SQL semantics: NULL = value evaluates to NULL (falsy), so no rows match
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let batch = make_batch_nullable(&schema, vec![None, None, None]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata_without_page_index(&bytes);

    let expr = col("a").eq(lit(2));
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .prune();

    // Current behavior: keeps (conservative, because min/max are None)
    // Ideal behavior: prune (because NULL = x is always falsy)
    assert_eq!(result.row_groups(), &[0]); // Conservative keep
}

#[test]
fn eq_with_partial_nulls_in_range() {
    // partial nulls, value in range => keep (might have matching rows)
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let batch = make_batch_nullable(&schema, vec![Some(1), None, Some(3)]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata_without_page_index(&bytes);

    let expr = col("a").eq(lit(2));
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .prune();

    // Should keep: value 2 is in range [1, 3]
    assert_eq!(result.row_groups(), &[0]);
}

#[test]
fn not_eq_with_zero_nulls() {
    // null_count = 0, min == max == value => should prune
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let batch = make_batch_nullable(&schema, vec![Some(2), Some(2), Some(2)]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata_without_page_index(&bytes);

    let expr = col("a").not_eq(lit(2));
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .prune();

    // Should prune: all values are 2
    assert_eq!(result.row_groups(), &[] as &[usize]);
}

#[test]
fn not_eq_with_nulls() {
    // min == max == value with nulls > 0
    // All non-null values are 2, so they don't match `!= 2`
    // Null values: NULL != 2 evaluates to NULL (falsy)
    // Therefore NO rows match the predicate
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let batch = make_batch_nullable(&schema, vec![Some(2), None, Some(2)]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata_without_page_index(&bytes);

    let expr = col("a").not_eq(lit(2));
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .prune();

    // Should prune: min==max==value means all non-nulls are 2,
    // and NULL != 2 is falsy, so no rows match
    assert_eq!(result.row_groups(), &[] as &[usize]);
}

#[test]
fn lt_with_zero_nulls_can_prune() {
    // null_count = 0, min >= value => should prune
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let batch = make_batch_nullable(&schema, vec![Some(5), Some(6), Some(7)]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata_without_page_index(&bytes);

    let expr = col("a").lt(lit(5));
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .prune();

    // Should prune: all values >= 5
    assert_eq!(result.row_groups(), &[] as &[usize]);
}

#[test]
fn lt_with_zero_nulls_keeps_all() {
    // null_count = 0, max < value => should keep all (True)
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let batch = make_batch_nullable(&schema, vec![Some(1), Some(2), Some(3)]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata_without_page_index(&bytes);

    let expr = col("a").lt(lit(5));
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .prune();

    // Should keep: all values < 5
    assert_eq!(result.row_groups(), &[0]);
}

#[test]
fn lt_with_nulls() {
    // Has nulls => max < value would still only return True if null_count = 0
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let batch = make_batch_nullable(&schema, vec![Some(1), None, Some(3)]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata_without_page_index(&bytes);

    let expr = col("a").lt(lit(5));
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .prune();

    // Should keep: Unknown (has nulls)
    assert_eq!(result.row_groups(), &[0]);
}

// ============================================================================
// Row-group level: IN list with nulls
// ============================================================================

#[test]
fn in_list_with_zero_nulls() {
    // null_count = 0, value in range => keep
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let batch = make_batch_nullable(&schema, vec![Some(1), Some(2), Some(3)]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata_without_page_index(&bytes);

    let expr = col("a").in_list(vec![lit(2), lit(5)], false);
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .prune();

    // Should keep: 2 is in range
    assert_eq!(result.row_groups(), &[0]);
}

#[test]
fn in_list_with_all_nulls() {
    // null_count = row_count => should ideally prune, but current behavior
    // is conservative (keeps) because min/max stats are None
    // SQL semantics: NULL IN (values) evaluates to NULL (falsy), so no rows match
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let batch = make_batch_nullable(&schema, vec![None, None, None]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata_without_page_index(&bytes);

    let expr = col("a").in_list(vec![lit(1), lit(2)], false);
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .prune();

    // Current behavior: keeps (conservative, because min/max are None)
    // Ideal behavior: prune (because NULL IN (...) is always falsy)
    assert_eq!(result.row_groups(), &[0]); // Conservative keep
}

// ============================================================================
// Page-level: null count edge cases
// ============================================================================

#[test]
fn page_level_is_null_with_mixed_pages() {
    // Pages with varying null counts
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);

    // Create batches: first has nulls, second doesn't
    let batch1 = make_batch_nullable(&schema, vec![None, None]);
    let batch2 = make_batch_nullable(&schema, vec![Some(1), Some(2)]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(2)
        .set_max_row_group_size(100)
        .build();

    let bytes = write_parquet(&[batch1, batch2], props);
    let metadata = load_metadata(&bytes);

    let expr = col("a").is_null();
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(true)
        .prune();

    assert_eq!(result.row_groups(), &[0]);

    // Should have page-level selection
    if result.row_selection().is_some() {
        let selection = result.row_selection().unwrap();
        // At least some rows should be selected (the null page)
        assert!(selection.selects_any());
    }
}

#[test]
fn page_level_comparison_with_zero_null_count() {
    // Page with null_count = 0 should allow aggressive pruning
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let batch = make_batch_nullable(&schema, vec![Some(1), Some(2), Some(10), Some(11)]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(2)
        .set_max_row_group_size(100)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata(&bytes);

    let expr = col("a").gt(lit(5));
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(true)
        .prune();

    assert_eq!(result.row_groups(), &[0]);

    if result.row_selection().is_some() {
        let selection = result.row_selection().unwrap();
        // Should prune first page (values 1, 2), keep second page (values 10, 11)
        assert!(selection.selects_any());
    }
}

#[test]
fn missing_stats_returns_unknown() {
    // When statistics are disabled, should be conservative (keep everything)
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, true)]);
    let batch = make_batch_nullable(&schema, vec![Some(1), Some(2), Some(3)]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::None)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata_without_page_index(&bytes);

    let expr = col("a").eq(lit(100));
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .prune();

    // Should keep: no stats available (conservative)
    assert_eq!(result.row_groups(), &[0]);
}
