use std::sync::Arc;

use aisle::{Expr, PruneRequest};
use arrow_array::{
    ArrayRef, Date32Array, Date64Array, RecordBatch, TimestampMicrosecondArray,
    TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
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

fn timestamp_array(unit: TimeUnit, tz: Option<&str>, values: &[i64]) -> ArrayRef {
    match unit {
        TimeUnit::Second => {
            let array = TimestampSecondArray::from(values.to_vec());
            let array = if let Some(tz) = tz {
                array.with_timezone(tz.to_string())
            } else {
                array
            };
            Arc::new(array)
        }
        TimeUnit::Millisecond => {
            let array = TimestampMillisecondArray::from(values.to_vec());
            let array = if let Some(tz) = tz {
                array.with_timezone(tz.to_string())
            } else {
                array
            };
            Arc::new(array)
        }
        TimeUnit::Microsecond => {
            let array = TimestampMicrosecondArray::from(values.to_vec());
            let array = if let Some(tz) = tz {
                array.with_timezone(tz.to_string())
            } else {
                array
            };
            Arc::new(array)
        }
        TimeUnit::Nanosecond => {
            let array = TimestampNanosecondArray::from(values.to_vec());
            let array = if let Some(tz) = tz {
                array.with_timezone(tz.to_string())
            } else {
                array
            };
            Arc::new(array)
        }
    }
}

fn timestamp_scalar(unit: TimeUnit, tz: Option<&str>, value: i64) -> ScalarValue {
    let tz = tz.map(|tz| Arc::from(tz));
    match unit {
        TimeUnit::Second => ScalarValue::TimestampSecond(Some(value), tz),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(value), tz),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(value), tz),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(value), tz),
    }
}

#[test]
fn row_group_prunes_date_types() {
    let cases = vec![
        (DataType::Date32, vec![1i64, 2, 3], vec![100i64, 101, 102], 50i64),
        (
            DataType::Date64,
            vec![1_000i64, 2_000, 3_000],
            vec![100_000i64, 101_000, 102_000],
            50_000i64,
        ),
    ];

    for (data_type, group1, group2, threshold) in cases {
        let schema = Schema::new(vec![Field::new("a", data_type.clone(), false)]);
        let batch1 = match data_type {
            DataType::Date32 => make_batch(
                &schema,
                Arc::new(Date32Array::from(
                    group1.iter().map(|v| *v as i32).collect::<Vec<_>>(),
                )),
            ),
            DataType::Date64 => make_batch(&schema, Arc::new(Date64Array::from(group1))),
            _ => unreachable!("unexpected date type"),
        };
        let batch2 = match data_type {
            DataType::Date32 => make_batch(
                &schema,
                Arc::new(Date32Array::from(
                    group2.iter().map(|v| *v as i32).collect::<Vec<_>>(),
                )),
            ),
            DataType::Date64 => make_batch(&schema, Arc::new(Date64Array::from(group2))),
            _ => unreachable!("unexpected date type"),
        };

        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_max_row_group_size(3)
            .build();

        let bytes = write_parquet(&[batch1, batch2], props);
        let metadata = load_metadata_without_page_index(&bytes);

        let scalar = match data_type {
            DataType::Date32 => ScalarValue::Date32(Some(threshold as i32)),
            DataType::Date64 => ScalarValue::Date64(Some(threshold)),
            _ => unreachable!("unexpected date type"),
        };
        let expr = Expr::gt("a", scalar);
        let result = PruneRequest::new(&metadata, &schema)
            .with_predicate(&expr)
            .enable_page_index(false)
            .prune();

        assert_eq!(result.row_groups(), &[1], "expected prune for {data_type:?}");
    }
}

#[test]
fn row_group_prunes_timestamp_units() {
    let units = vec![
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ];

    for unit in units {
        let data_type = DataType::Timestamp(unit, None);
        let schema = Schema::new(vec![Field::new("a", data_type.clone(), false)]);
        let batch1 = make_batch(&schema, timestamp_array(unit, None, &[1, 2, 3]));
        let batch2 = make_batch(&schema, timestamp_array(unit, None, &[100, 101, 102]));

        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_max_row_group_size(3)
            .build();

        let bytes = write_parquet(&[batch1, batch2], props);
        let metadata = load_metadata_without_page_index(&bytes);

        let expr = Expr::gt("a", timestamp_scalar(unit, None, 50));
        let result = PruneRequest::new(&metadata, &schema)
            .with_predicate(&expr)
            .enable_page_index(false)
            .prune();

        assert_eq!(result.row_groups(), &[1], "expected prune for {unit:?}");
    }
}

#[test]
fn row_group_prunes_timestamp_with_timezone() {
    let tz = "+00:00";
    let unit = TimeUnit::Millisecond;
    let data_type = DataType::Timestamp(unit, Some(Arc::from(tz)));
    let schema = Schema::new(vec![Field::new("a", data_type.clone(), false)]);
    let batch1 = make_batch(&schema, timestamp_array(unit, Some(tz), &[1, 2, 3]));
    let batch2 = make_batch(&schema, timestamp_array(unit, Some(tz), &[100, 101, 102]));

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_size(3)
        .build();

    let bytes = write_parquet(&[batch1, batch2], props);
    let metadata = load_metadata_without_page_index(&bytes);

    let expr = Expr::gt("a", timestamp_scalar(unit, Some(tz), 50));
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .prune();

    assert_eq!(result.row_groups(), &[1], "expected prune for tz timestamps");
}

#[test]
fn page_level_prunes_timestamp_pages() {
    let unit = TimeUnit::Millisecond;
    let data_type = DataType::Timestamp(unit, None);
    let schema = Schema::new(vec![Field::new("a", data_type.clone(), false)]);
    let batch = make_batch(&schema, timestamp_array(unit, None, &[1, 2, 3, 100, 101, 102]));

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(1)
        .set_write_batch_size(1)
        .set_dictionary_enabled(false)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata_with_page_index(&bytes);

    let expr = Expr::gt("a", timestamp_scalar(unit, None, 50));
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
fn page_level_prunes_date32_pages() {
    let data_type = DataType::Date32;
    let schema = Schema::new(vec![Field::new("a", data_type.clone(), false)]);
    let batch = make_batch(
        &schema,
        Arc::new(Date32Array::from(vec![1, 2, 3, 100, 101, 102])),
    );

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(1)
        .set_write_batch_size(1)
        .set_dictionary_enabled(false)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata_with_page_index(&bytes);

    let expr = Expr::gt("a", ScalarValue::Date32(Some(50)));
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
fn page_level_prunes_date64_pages() {
    let data_type = DataType::Date64;
    let schema = Schema::new(vec![Field::new("a", data_type.clone(), false)]);
    let batch = make_batch(
        &schema,
        Arc::new(Date64Array::from(vec![
            1_000, 2_000, 3_000, 100_000, 101_000, 102_000,
        ])),
    );

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(1)
        .set_write_batch_size(1)
        .set_dictionary_enabled(false)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata_with_page_index(&bytes);

    let expr = Expr::gt("a", ScalarValue::Date64(Some(50_000)));
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
