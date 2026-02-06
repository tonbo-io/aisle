use std::sync::Arc;

use aisle::{Expr, PruneRequest};
use arrow_schema::{DataType, Field, Schema, TimeUnit};
use bytes::Bytes;
use datafusion_common::ScalarValue;
use parquet::{
    arrow::arrow_reader::RowSelector,
    data_type::{Int96, Int96Type},
    file::{
        metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader},
        properties::{EnabledStatistics, WriterProperties},
        writer::SerializedFileWriter,
    },
    schema::parser::parse_message_type,
};

const JULIAN_DAY_OF_EPOCH: i64 = 2_440_588;
const NANOS_PER_DAY: i128 = 86_400_i128 * 1_000_000_000_i128;

fn int96_from_timestamp_nanos(nanos: i64) -> Int96 {
    let nanos = i128::from(nanos);
    let days_since_epoch = nanos / NANOS_PER_DAY;
    let nanos_of_day = nanos % NANOS_PER_DAY;
    let julian_day = days_since_epoch + i128::from(JULIAN_DAY_OF_EPOCH);
    let nanos_of_day = nanos_of_day as u64;
    let low = nanos_of_day as u32;
    let high = (nanos_of_day >> 32) as u32;
    Int96::from(vec![low, high, julian_day as u32])
}

fn timestamp_scalar(unit: TimeUnit, value: i64) -> ScalarValue {
    match unit {
        TimeUnit::Second => ScalarValue::TimestampSecond(Some(value), None),
        TimeUnit::Millisecond => ScalarValue::TimestampMillisecond(Some(value), None),
        TimeUnit::Microsecond => ScalarValue::TimestampMicrosecond(Some(value), None),
        TimeUnit::Nanosecond => ScalarValue::TimestampNanosecond(Some(value), None),
    }
}

fn to_unit_value(nanos: i64, unit: TimeUnit) -> i64 {
    match unit {
        TimeUnit::Second => nanos / 1_000_000_000,
        TimeUnit::Millisecond => nanos / 1_000_000,
        TimeUnit::Microsecond => nanos / 1_000,
        TimeUnit::Nanosecond => nanos,
    }
}

fn write_int96_parquet(row_groups: &[Vec<i64>], props: WriterProperties) -> Vec<u8> {
    let message_type = "message schema { REQUIRED INT96 ts; }";
    let schema = Arc::new(parse_message_type(message_type).unwrap());
    let props = Arc::new(props);
    let mut writer = SerializedFileWriter::new(Vec::new(), schema, props).unwrap();

    for group in row_groups {
        let mut row_group_writer = writer.next_row_group().unwrap();
        if let Some(mut col_writer) = row_group_writer.next_column().unwrap() {
            let values: Vec<Int96> = group
                .iter()
                .copied()
                .map(int96_from_timestamp_nanos)
                .collect();
            col_writer
                .typed::<Int96Type>()
                .write_batch(&values, None, None)
                .unwrap();
            col_writer.close().unwrap();
        }
        row_group_writer.close().unwrap();
    }

    writer.into_inner().unwrap()
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

#[test]
fn row_group_prunes_int96_timestamps() {
    let units = [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ];
    for unit in units {
        let schema = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(unit, None),
            false,
        )]);

        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build();

        let bytes = write_int96_parquet(
            &[
                vec![0, 1_000_000_000, 2_000_000_000],
                vec![10_000_000_000, 11_000_000_000, 12_000_000_000],
            ],
            props,
        );
        let metadata = load_metadata_without_page_index(&bytes);

        let threshold = to_unit_value(5_000_000_000, unit);
        let expr = Expr::gt("ts", timestamp_scalar(unit, threshold));
        let result = PruneRequest::new(&metadata, &schema)
            .with_predicate(&expr)
            .enable_page_index(false)
            .prune();

        assert_eq!(result.row_groups(), &[1], "expected prune for {unit:?}");
    }
}

#[test]
fn page_level_prunes_int96_pages() {
    let units = [
        TimeUnit::Second,
        TimeUnit::Millisecond,
        TimeUnit::Microsecond,
        TimeUnit::Nanosecond,
    ];
    for unit in units {
        let schema = Schema::new(vec![Field::new(
            "ts",
            DataType::Timestamp(unit, None),
            false,
        )]);

        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Page)
            .set_data_page_row_count_limit(1)
            .set_write_batch_size(1)
            .set_dictionary_enabled(false)
            .build();

        let bytes = write_int96_parquet(
            &[vec![0, 10_000_000_000, 20_000_000_000, 30_000_000_000]],
            props,
        );
        let metadata = load_metadata_with_page_index(&bytes);

        let threshold = to_unit_value(15_000_000_000, unit);
        let expr = Expr::gt("ts", timestamp_scalar(unit, threshold));
        let result = PruneRequest::new(&metadata, &schema)
            .with_predicate(&expr)
            .enable_page_index(true)
            .emit_roaring(false)
            .prune();

        assert_eq!(result.row_groups(), &[0], "expected page prune for {unit:?}");
        let selection = result.row_selection().expect("expected page selection");
        let selectors: Vec<RowSelector> = selection.clone().into();
        assert!(
            selectors.iter().any(|sel| sel.skip),
            "expected page selection with skips for {unit:?}"
        );
    }
}
