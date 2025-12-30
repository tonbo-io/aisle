use std::sync::Arc;

use aisle::{PruneRequest, PruneResult};
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use datafusion_expr::{Expr, cast, col, lit, try_cast};
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

    // No-op CAST: id is already INT64
    let expr = cast(col("id"), DataType::Int64).eq(lit(150i64));

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

    // No-op TRY_CAST
    let expr = try_cast(col("id"), DataType::Int64).eq(lit(150i64));

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

    // Non-trivial CAST: id is INT64, cast to STRING
    let expr = cast(col("id"), DataType::Utf8).eq(lit("100"));

    let result = prune_with_test_options(&metadata, &schema, &expr);

    // Should have compilation errors
    assert!(!result.compile_result().errors().is_empty());
    let error = result.compile_result().errors()[0].to_string();
    assert!(error.contains("CAST on column 'id'"));
    assert!(error.contains("not supported"));
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

    // Cast literal from Int32 to Int64
    let expr = col("id").eq(cast(lit(150i32), DataType::Int64));

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

    // Double no-op cast
    let expr = cast(cast(col("id"), DataType::Int64), DataType::Int64).eq(lit(150i64));

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

    // CAST in BETWEEN: column is no-op, literals are cast
    let expr = cast(col("id"), DataType::Int64).between(
        cast(lit(40i32), DataType::Int64),
        cast(lit(70i32), DataType::Int64),
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

    // CAST in IN list
    let expr = cast(col("id"), DataType::Int64).in_list(
        vec![
            cast(lit(1i32), DataType::Int64),
            cast(lit(2i32), DataType::Int64),
        ],
        false,
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

    // No-op cast in LIKE
    let expr = Expr::Like(datafusion_expr::expr::Like::new(
        false,
        Box::new(cast(col("name"), DataType::Utf8)),
        Box::new(lit("cha%")),
        None,
        false,
    ));

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

    // No-op cast in IS NULL
    let expr = cast(col("id"), DataType::Int64).is_null();

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

    // Invalid literal cast: can't cast "not_a_number" to INT64
    let expr = col("id").eq(cast(lit("not_a_number"), DataType::Int64));

    let result = prune_with_test_options(&metadata, &schema, &expr);

    // Should have compilation errors (literal cast fails)
    assert!(!result.compile_result().errors().is_empty());
    let error = result.compile_result().errors()[0].to_string();
    // The error happens because the literal cast fails, making it not a valid literal
    assert!(error.contains("Unsupported expression") || error.contains("Cannot cast"));
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

    // Both sides have casts
    let expr = cast(col("id"), DataType::Int64).eq(cast(lit(150i32), DataType::Int64));

    let result = prune_with_test_options(&metadata, &schema, &expr);

    assert_eq!(result.row_groups(), &[1]);
    assert!(result.compile_result().errors().is_empty());
}
