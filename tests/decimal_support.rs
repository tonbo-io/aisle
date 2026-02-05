use std::sync::Arc;

use aisle::{Expr, PruneRequest};
use arrow_array::{ArrayRef, Decimal128Array, Decimal256Array, RecordBatch};
use arrow_buffer::i256;
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use datafusion_common::ScalarValue;
use parquet::{
    arrow::{ArrowWriter, arrow_reader::RowSelector},
    file::{
        metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader},
        properties::{EnabledStatistics, WriterProperties},
    },
};

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

fn load_metadata_without_page_index(bytes: &[u8]) -> ParquetMetaData {
    let bytes = Bytes::copy_from_slice(bytes);
    ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Skip)
        .parse_and_finish(&bytes)
        .unwrap()
}

fn load_metadata_with_page_index(bytes: &[u8]) -> ParquetMetaData {
    let bytes = Bytes::copy_from_slice(bytes);
    ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .parse_and_finish(&bytes)
        .unwrap()
}

fn make_batch(schema: &Schema, array: ArrayRef) -> RecordBatch {
    RecordBatch::try_new(Arc::new(schema.clone()), vec![array]).unwrap()
}

fn decimal128_array(values: &[i128], precision: u8, scale: i8) -> ArrayRef {
    let array =
        Decimal128Array::from(values.to_vec()).with_precision_and_scale(precision, scale);
    Arc::new(array.unwrap())
}

fn decimal256_array(values: &[i256], precision: u8, scale: i8) -> ArrayRef {
    let array =
        Decimal256Array::from(values.to_vec()).with_precision_and_scale(precision, scale);
    Arc::new(array.unwrap())
}

#[test]
fn row_group_prunes_decimal128() {
    let precision = 10u8;
    let scale = 0i8;
    let data_type = DataType::Decimal128(precision, scale);
    let schema = Schema::new(vec![Field::new("a", data_type.clone(), false)]);
    let batch1 = make_batch(&schema, decimal128_array(&[1, 2, 3], precision, scale));
    let batch2 = make_batch(&schema, decimal128_array(&[100, 101, 102], precision, scale));

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_size(3)
        .build();

    let bytes = write_parquet(&[batch1, batch2], props);
    let metadata = load_metadata_without_page_index(&bytes);

    let expr = Expr::gt(
        "a",
        ScalarValue::Decimal128(Some(50), precision, scale),
    );
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .prune();

    assert_eq!(result.row_groups(), &[1]);
}

#[test]
fn row_group_prunes_decimal256() {
    let precision = 39u8;
    let scale = 0i8;
    let data_type = DataType::Decimal256(precision, scale);
    let schema = Schema::new(vec![Field::new("a", data_type.clone(), false)]);
    let batch1 = make_batch(
        &schema,
        decimal256_array(
            &[i256::from_i128(1), i256::from_i128(2), i256::from_i128(3)],
            precision,
            scale,
        ),
    );
    let batch2 = make_batch(
        &schema,
        decimal256_array(
            &[
                i256::from_i128(100),
                i256::from_i128(101),
                i256::from_i128(102),
            ],
            precision,
            scale,
        ),
    );

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_size(3)
        .build();

    let bytes = write_parquet(&[batch1, batch2], props);
    let metadata = load_metadata_without_page_index(&bytes);

    let expr = Expr::gt(
        "a",
        ScalarValue::Decimal256(Some(i256::from_i128(50)), precision, scale),
    );
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .prune();

    assert_eq!(result.row_groups(), &[1]);
}

#[test]
fn page_level_prunes_decimal128_pages() {
    let precision = 10u8;
    let scale = 0i8;
    let data_type = DataType::Decimal128(precision, scale);
    let schema = Schema::new(vec![Field::new("a", data_type.clone(), false)]);
    let batch = make_batch(
        &schema,
        decimal128_array(&[1, 2, 3, 100, 101, 102], precision, scale),
    );

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(1)
        .set_write_batch_size(1)
        .set_dictionary_enabled(false)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata_with_page_index(&bytes);

    let expr = Expr::gt(
        "a",
        ScalarValue::Decimal128(Some(50), precision, scale),
    );
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(true)
        .emit_roaring(false)
        .prune();

    assert_eq!(result.row_groups(), &[0]);
    let selection = result.row_selection().expect("expected page selection");
    let selectors: Vec<RowSelector> = selection.clone().into();
    assert!(
        selectors.iter().any(|sel| sel.skip),
        "expected page selection with skips"
    );
}

#[test]
fn page_level_prunes_decimal256_pages() {
    let precision = 39u8;
    let scale = 0i8;
    let data_type = DataType::Decimal256(precision, scale);
    let schema = Schema::new(vec![Field::new("a", data_type.clone(), false)]);
    let batch = make_batch(
        &schema,
        decimal256_array(
            &[
                i256::from_i128(1),
                i256::from_i128(2),
                i256::from_i128(3),
                i256::from_i128(100),
                i256::from_i128(101),
                i256::from_i128(102),
            ],
            precision,
            scale,
        ),
    );

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(1)
        .set_write_batch_size(1)
        .set_dictionary_enabled(false)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata_with_page_index(&bytes);

    let expr = Expr::gt(
        "a",
        ScalarValue::Decimal256(Some(i256::from_i128(50)), precision, scale),
    );
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(true)
        .emit_roaring(false)
        .prune();

    assert_eq!(result.row_groups(), &[0]);
    let selection = result.row_selection().expect("expected page selection");
    let selectors: Vec<RowSelector> = selection.clone().into();
    assert!(
        selectors.iter().any(|sel| sel.skip),
        "expected page selection with skips"
    );
}
