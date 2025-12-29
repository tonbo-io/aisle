use std::sync::Arc;

use aisle::PruneRequest;
use arrow_array::{
    Int8Array, Int16Array, Int32Array, Int64Array, RecordBatch, UInt8Array, UInt16Array,
    UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use datafusion_common::ScalarValue;
use datafusion_expr::{col, lit};
use parquet::{
    arrow::ArrowWriter,
    file::{
        metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader},
        properties::{EnabledStatistics, WriterProperties},
    },
};

fn make_batch(schema: &Schema, data_type: &DataType, values: &[i64]) -> RecordBatch {
    let array: Arc<dyn arrow_array::Array> = match data_type {
        DataType::Int8 => Arc::new(Int8Array::from(
            values.iter().map(|v| *v as i8).collect::<Vec<_>>(),
        )),
        DataType::Int16 => Arc::new(Int16Array::from(
            values.iter().map(|v| *v as i16).collect::<Vec<_>>(),
        )),
        DataType::Int32 => Arc::new(Int32Array::from(
            values.iter().map(|v| *v as i32).collect::<Vec<_>>(),
        )),
        DataType::Int64 => Arc::new(Int64Array::from(values.to_vec())),
        DataType::UInt8 => Arc::new(UInt8Array::from(
            values.iter().map(|v| *v as u8).collect::<Vec<_>>(),
        )),
        DataType::UInt16 => Arc::new(UInt16Array::from(
            values.iter().map(|v| *v as u16).collect::<Vec<_>>(),
        )),
        DataType::UInt32 => Arc::new(UInt32Array::from(
            values.iter().map(|v| *v as u32).collect::<Vec<_>>(),
        )),
        DataType::UInt64 => Arc::new(UInt64Array::from(
            values.iter().map(|v| *v as u64).collect::<Vec<_>>(),
        )),
        _ => panic!("unsupported data type for int family test"),
    };

    RecordBatch::try_new(Arc::new(schema.clone()), vec![array]).unwrap()
}

fn scalar_for_type(data_type: &DataType, value: i64) -> ScalarValue {
    match data_type {
        DataType::Int8 => ScalarValue::Int8(Some(value as i8)),
        DataType::Int16 => ScalarValue::Int16(Some(value as i16)),
        DataType::Int32 => ScalarValue::Int32(Some(value as i32)),
        DataType::Int64 => ScalarValue::Int64(Some(value)),
        DataType::UInt8 => ScalarValue::UInt8(Some(value as u8)),
        DataType::UInt16 => ScalarValue::UInt16(Some(value as u16)),
        DataType::UInt32 => ScalarValue::UInt32(Some(value as u32)),
        DataType::UInt64 => ScalarValue::UInt64(Some(value as u64)),
        _ => panic!("unsupported data type for int family test"),
    }
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

fn load_metadata_without_page_index(bytes: &[u8]) -> ParquetMetaData {
    let bytes = Bytes::copy_from_slice(bytes);
    ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Skip)
        .parse_and_finish(&bytes)
        .unwrap()
}

fn load_metadata(bytes: &[u8]) -> ParquetMetaData {
    let bytes = Bytes::copy_from_slice(bytes);
    ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .parse_and_finish(&bytes)
        .unwrap()
}

#[test]
fn row_group_prunes_int_families() {
    let types = vec![
        DataType::Int8,
        DataType::Int16,
        DataType::Int32,
        DataType::Int64,
        DataType::UInt8,
        DataType::UInt16,
        DataType::UInt32,
        DataType::UInt64,
    ];

    for data_type in types {
        let schema = Schema::new(vec![Field::new("a", data_type.clone(), false)]);
        let batch = make_batch(&schema, &data_type, &[1, 2, 3]);

        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build();

        let bytes = write_parquet(&[batch], props);
        let metadata = load_metadata_without_page_index(&bytes);

        let prune_expr = col("a").gt(lit(scalar_for_type(&data_type, 10)));
        let pruned = PruneRequest::new(&metadata, &schema)
            .with_predicate(&prune_expr)
            .enable_page_index(false)
            .prune();
        assert_eq!(
            pruned.row_groups(),
            &[] as &[usize],
            "expected prune for {data_type:?}"
        );

        let keep_expr = col("a").eq(lit(scalar_for_type(&data_type, 2)));
        let kept = PruneRequest::new(&metadata, &schema)
            .with_predicate(&keep_expr)
            .enable_page_index(false)
            .prune();
        assert_eq!(kept.row_groups(), &[0], "expected keep for {data_type:?}");
    }
}

#[test]
fn page_level_prunes_int8() {
    let data_type = DataType::Int8;
    let schema = Schema::new(vec![Field::new("a", data_type.clone(), false)]);
    let batch = make_batch(&schema, &data_type, &[1, 2, 3, 100, 101, 102]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(1)
        .set_write_batch_size(1)
        .set_dictionary_enabled(false)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata(&bytes);

    let expr = col("a").gt(lit(scalar_for_type(&data_type, 50)));
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(true)
        .emit_roaring(false)
        .prune();

    assert_eq!(result.row_groups(), &[0]);
    let selection = result.row_selection().expect("expected page selection");
    let selectors: Vec<parquet::arrow::arrow_reader::RowSelector> = selection.clone().into();
    assert!(
        selectors.iter().any(|sel| sel.skip),
        "expected page selection with skips"
    );
}
