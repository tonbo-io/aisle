use std::sync::Arc;

use aisle::{Expr, PruneRequest};
use arrow_array::{
    ArrayRef, DurationMicrosecondArray, DurationMillisecondArray, DurationNanosecondArray,
    DurationSecondArray, RecordBatch, Time32MillisecondArray, Time32SecondArray,
    Time64MicrosecondArray, Time64NanosecondArray,
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

fn time32_array(unit: TimeUnit, values: &[i32]) -> ArrayRef {
    match unit {
        TimeUnit::Second => Arc::new(Time32SecondArray::from(values.to_vec())),
        TimeUnit::Millisecond => Arc::new(Time32MillisecondArray::from(values.to_vec())),
        _ => unreachable!("invalid unit for Time32"),
    }
}

fn time64_array(unit: TimeUnit, values: &[i64]) -> ArrayRef {
    match unit {
        TimeUnit::Microsecond => Arc::new(Time64MicrosecondArray::from(values.to_vec())),
        TimeUnit::Nanosecond => Arc::new(Time64NanosecondArray::from(values.to_vec())),
        _ => unreachable!("invalid unit for Time64"),
    }
}

fn duration_array(unit: TimeUnit, values: &[i64]) -> ArrayRef {
    match unit {
        TimeUnit::Second => Arc::new(DurationSecondArray::from(values.to_vec())),
        TimeUnit::Millisecond => Arc::new(DurationMillisecondArray::from(values.to_vec())),
        TimeUnit::Microsecond => Arc::new(DurationMicrosecondArray::from(values.to_vec())),
        TimeUnit::Nanosecond => Arc::new(DurationNanosecondArray::from(values.to_vec())),
    }
}

fn time32_scalar(unit: TimeUnit, value: i32) -> ScalarValue {
    match unit {
        TimeUnit::Second => ScalarValue::Time32Second(Some(value)),
        TimeUnit::Millisecond => ScalarValue::Time32Millisecond(Some(value)),
        _ => unreachable!("invalid unit for Time32"),
    }
}

fn time64_scalar(unit: TimeUnit, value: i64) -> ScalarValue {
    match unit {
        TimeUnit::Microsecond => ScalarValue::Time64Microsecond(Some(value)),
        TimeUnit::Nanosecond => ScalarValue::Time64Nanosecond(Some(value)),
        _ => unreachable!("invalid unit for Time64"),
    }
}

fn duration_scalar(unit: TimeUnit, value: i64) -> ScalarValue {
    match unit {
        TimeUnit::Second => ScalarValue::DurationSecond(Some(value)),
        TimeUnit::Millisecond => ScalarValue::DurationMillisecond(Some(value)),
        TimeUnit::Microsecond => ScalarValue::DurationMicrosecond(Some(value)),
        TimeUnit::Nanosecond => ScalarValue::DurationNanosecond(Some(value)),
    }
}

#[test]
fn row_group_prunes_time32_units() {
    let units = vec![TimeUnit::Second, TimeUnit::Millisecond];
    for unit in units {
        let data_type = DataType::Time32(unit);
        let schema = Schema::new(vec![Field::new("a", data_type.clone(), false)]);
        let batch1 = make_batch(&schema, time32_array(unit, &[1, 2, 3]));
        let batch2 = make_batch(&schema, time32_array(unit, &[100, 101, 102]));

        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_max_row_group_size(3)
            .build();

        let bytes = write_parquet(&[batch1, batch2], props);
        let metadata = load_metadata_without_page_index(&bytes);

        let expr = Expr::gt("a", time32_scalar(unit, 50));
        let result = PruneRequest::new(&metadata, &schema)
            .with_predicate(&expr)
            .enable_page_index(false)
            .prune();

        assert_eq!(result.row_groups(), &[1], "expected prune for {unit:?}");
    }
}

#[test]
fn row_group_prunes_time64_units() {
    let units = vec![TimeUnit::Microsecond, TimeUnit::Nanosecond];
    for unit in units {
        let data_type = DataType::Time64(unit);
        let schema = Schema::new(vec![Field::new("a", data_type.clone(), false)]);
        let batch1 = make_batch(&schema, time64_array(unit, &[1, 2, 3]));
        let batch2 = make_batch(&schema, time64_array(unit, &[100, 101, 102]));

        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_max_row_group_size(3)
            .build();

        let bytes = write_parquet(&[batch1, batch2], props);
        let metadata = load_metadata_without_page_index(&bytes);

        let expr = Expr::gt("a", time64_scalar(unit, 50));
        let result = PruneRequest::new(&metadata, &schema)
            .with_predicate(&expr)
            .enable_page_index(false)
            .prune();

        assert_eq!(result.row_groups(), &[1], "expected prune for {unit:?}");
    }
}

#[test]
fn row_group_prunes_duration_units() {
    let units = vec![
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ];
    for unit in units {
        let data_type = DataType::Duration(unit);
        let schema = Schema::new(vec![Field::new("a", data_type.clone(), false)]);
        let batch1 = make_batch(&schema, duration_array(unit, &[1, 2, 3]));
        let batch2 = make_batch(&schema, duration_array(unit, &[100, 101, 102]));

        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_max_row_group_size(3)
            .build();

        let bytes = write_parquet(&[batch1, batch2], props);
        let metadata = load_metadata_without_page_index(&bytes);

        let expr = Expr::gt("a", duration_scalar(unit, 50));
        let result = PruneRequest::new(&metadata, &schema)
            .with_predicate(&expr)
            .enable_page_index(false)
            .prune();

        assert_eq!(result.row_groups(), &[1], "expected prune for {unit:?}");
    }
}

#[test]
fn page_level_prunes_time32_pages() {
    let units = vec![TimeUnit::Second, TimeUnit::Millisecond];
    for unit in units {
        let data_type = DataType::Time32(unit);
        let schema = Schema::new(vec![Field::new("a", data_type.clone(), false)]);
        let batch = make_batch(&schema, time32_array(unit, &[1, 2, 3, 100, 101, 102]));

        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_data_page_row_count_limit(1)
            .set_write_batch_size(1)
            .set_dictionary_enabled(false)
            .build();

        let bytes = write_parquet(&[batch], props);
        let metadata = load_metadata_with_page_index(&bytes);

        let expr = Expr::gt("a", time32_scalar(unit, 50));
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
}

#[test]
fn page_level_prunes_time64_pages() {
    let units = vec![TimeUnit::Microsecond, TimeUnit::Nanosecond];
    for unit in units {
        let data_type = DataType::Time64(unit);
        let schema = Schema::new(vec![Field::new("a", data_type.clone(), false)]);
        let batch = make_batch(&schema, time64_array(unit, &[1, 2, 3, 100, 101, 102]));

        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_data_page_row_count_limit(1)
            .set_write_batch_size(1)
            .set_dictionary_enabled(false)
            .build();

        let bytes = write_parquet(&[batch], props);
        let metadata = load_metadata_with_page_index(&bytes);

        let expr = Expr::gt("a", time64_scalar(unit, 50));
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
}

#[test]
fn page_level_prunes_duration_pages() {
    let units = vec![
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ];
    for unit in units {
        let data_type = DataType::Duration(unit);
        let schema = Schema::new(vec![Field::new("a", data_type.clone(), false)]);
        let batch = make_batch(&schema, duration_array(unit, &[1, 2, 3, 100, 101, 102]));

        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_data_page_row_count_limit(1)
            .set_write_batch_size(1)
            .set_dictionary_enabled(false)
            .build();

        let bytes = write_parquet(&[batch], props);
        let metadata = load_metadata_with_page_index(&bytes);

        let expr = Expr::gt("a", duration_scalar(unit, 50));
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
}
