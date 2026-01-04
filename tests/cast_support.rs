use std::sync::Arc;

use aisle::{Expr, PruneRequest, PruneResult};
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use datafusion_common::ScalarValue;
use parquet::{
    arrow::ArrowWriter,
    file::{
        metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader},
        properties::{EnabledStatistics, WriterProperties},
    },
};

fn make_int_batch(schema: &Schema, values: &[i64]) -> RecordBatch {
    let array = Int64Array::from(values.to_vec());
    RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(array)]).unwrap()
}

fn make_string_batch(schema: &Schema, values: &[&str]) -> RecordBatch {
    let array = StringArray::from(values.to_vec());
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
        .with_page_index_policy(PageIndexPolicy::Skip)
        .parse_and_finish(&bytes)
        .unwrap()
}

fn prune_with_test_options(
    metadata: &ParquetMetaData,
    schema: &Schema,
    expr: &Expr,
) -> PruneResult {
    PruneRequest::new(metadata, schema)
        .with_predicate(expr)
        .enable_page_index(false)
        .enable_bloom_filter(false)
        .emit_roaring(false)
        .prune()
}

#[test]
fn allows_noop_column_cast() {
    // Schema: id INT64
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let batch1 = make_int_batch(&schema, &[1, 2]);
    let batch2 = make_int_batch(&schema, &[100, 200]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_size(2)
        .build();

    let bytes = write_parquet(&[batch1, batch2], props);
    let metadata = load_metadata(&bytes);

    let expr = Expr::eq("id", ScalarValue::Int64(Some(150)));

    let result = prune_with_test_options(&metadata, &schema, &expr);

    // Should prune row group 0 (values 1, 2)
    assert_eq!(result.row_groups(), &[1]);
    assert!(result.compile_result().errors().is_empty());
}

#[test]
fn allows_noop_try_cast() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let batch1 = make_int_batch(&schema, &[1, 2]);
    let batch2 = make_int_batch(&schema, &[100, 200]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_size(2)
        .build();

    let bytes = write_parquet(&[batch1, batch2], props);
    let metadata = load_metadata(&bytes);

    let expr = Expr::eq("id", ScalarValue::Int64(Some(150)));

    let result = prune_with_test_options(&metadata, &schema, &expr);

    assert_eq!(result.row_groups(), &[1]);
    assert!(result.compile_result().errors().is_empty());
}

#[test]
fn rejects_non_trivial_column_cast() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let batch = make_int_batch(&schema, &[1, 2]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata(&bytes);

    // Type mismatch: INT64 column compared to string literal
    let expr = Expr::eq("id", ScalarValue::Utf8(Some("100".to_string())));

    let result = prune_with_test_options(&metadata, &schema, &expr);

    // Type mismatch should be conservative (keep all row groups)
    assert_eq!(result.row_groups(), &[0]);
}

#[test]
fn casts_literals_at_compile_time() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let batch1 = make_int_batch(&schema, &[1, 2]);
    let batch2 = make_int_batch(&schema, &[100, 200]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_size(2)
        .build();

    let bytes = write_parquet(&[batch1, batch2], props);
    let metadata = load_metadata(&bytes);

    let expr = Expr::eq("id", ScalarValue::Int64(Some(150)));

    let result = prune_with_test_options(&metadata, &schema, &expr);

    // Should prune correctly (literal cast happens at compile time)
    assert_eq!(result.row_groups(), &[1]);
    assert!(result.compile_result().errors().is_empty());
}

#[test]
fn handles_nested_casts() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let batch1 = make_int_batch(&schema, &[1, 2]);
    let batch2 = make_int_batch(&schema, &[100, 200]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_size(2)
        .build();

    let bytes = write_parquet(&[batch1, batch2], props);
    let metadata = load_metadata(&bytes);

    let expr = Expr::eq("id", ScalarValue::Int64(Some(150)));

    let result = prune_with_test_options(&metadata, &schema, &expr);

    assert_eq!(result.row_groups(), &[1]);
    assert!(result.compile_result().errors().is_empty());
}

#[test]
fn cast_in_between() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let batch1 = make_int_batch(&schema, &[1, 2]);
    let batch2 = make_int_batch(&schema, &[50, 60]);
    let batch3 = make_int_batch(&schema, &[100, 200]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_size(2)
        .build();

    let bytes = write_parquet(&[batch1, batch2, batch3], props);
    let metadata = load_metadata(&bytes);

    let expr = Expr::between(
        "id",
        ScalarValue::Int64(Some(40)),
        ScalarValue::Int64(Some(70)),
        true,
    );

    let result = prune_with_test_options(&metadata, &schema, &expr);

    // Should keep only row group 1 (50, 60)
    assert_eq!(result.row_groups(), &[1]);
    assert!(result.compile_result().errors().is_empty());
}

#[test]
fn cast_in_in_list() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let batch1 = make_int_batch(&schema, &[1, 2]);
    let batch2 = make_int_batch(&schema, &[50, 60]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_size(2)
        .build();

    let bytes = write_parquet(&[batch1, batch2], props);
    let metadata = load_metadata(&bytes);

    let expr = Expr::in_list(
        "id",
        vec![ScalarValue::Int64(Some(1)), ScalarValue::Int64(Some(2))],
    );

    let result = prune_with_test_options(&metadata, &schema, &expr);

    assert_eq!(result.row_groups(), &[0]);
    assert!(result.compile_result().errors().is_empty());
}

#[test]
fn cast_in_like() {
    let schema = Schema::new(vec![Field::new("name", DataType::Utf8, false)]);
    let batch1 = make_string_batch(&schema, &["alice", "bob"]);
    let batch2 = make_string_batch(&schema, &["charlie", "david"]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_size(2)
        .build();

    let bytes = write_parquet(&[batch1, batch2], props);
    let metadata = load_metadata(&bytes);

    let expr = Expr::starts_with("name", "cha");

    let result = prune_with_test_options(&metadata, &schema, &expr);

    assert_eq!(result.row_groups(), &[1]);
    assert!(result.compile_result().errors().is_empty());
}

#[test]
fn cast_in_is_null() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, true)]);
    let batch = make_int_batch(&schema, &[1, 2]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata(&bytes);

    let expr = Expr::is_null("id");

    let result = prune_with_test_options(&metadata, &schema, &expr);

    // Row group has no nulls, should be pruned
    assert_eq!(result.row_groups(), &[] as &[usize]);
    assert!(result.compile_result().errors().is_empty());
}

#[test]
fn invalid_literal_cast_fails() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let batch = make_int_batch(&schema, &[1, 2]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata(&bytes);

    // Type mismatch: string literal against INT64 column
    let expr = Expr::eq("id", ScalarValue::Utf8(Some("not_a_number".to_string())));

    let result = prune_with_test_options(&metadata, &schema, &expr);

    // Should be conservative (keep all row groups)
    assert_eq!(result.row_groups(), &[0]);
}

#[test]
fn both_column_and_literal_cast() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let batch1 = make_int_batch(&schema, &[1, 2]);
    let batch2 = make_int_batch(&schema, &[100, 200]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_size(2)
        .build();

    let bytes = write_parquet(&[batch1, batch2], props);
    let metadata = load_metadata(&bytes);

    let expr = Expr::eq("id", ScalarValue::Int64(Some(150)));

    let result = prune_with_test_options(&metadata, &schema, &expr);

    assert_eq!(result.row_groups(), &[1]);
    assert!(result.compile_result().errors().is_empty());
}
