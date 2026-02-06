use std::sync::Arc;

use aisle::{Expr, PruneRequest};
use arrow_buffer::IntervalDayTime;
use arrow_schema::{DataType, Field, IntervalUnit, Schema};
use datafusion_common::ScalarValue;
use parquet::{
    basic::{BoundaryOrder, ColumnOrder, Type as PhysicalType},
    data_type::FixedLenByteArray,
    file::metadata::{
        ColumnChunkMetaData, ColumnIndexBuilder, FileMetaData, OffsetIndexBuilder,
        ParquetMetaData, ParquetMetaDataBuilder, RowGroupMetaData,
    },
    file::statistics::Statistics,
    schema::{parser::parse_message_type, types::SchemaDescriptor},
};

fn interval_year_month_bytes(months: i32) -> Vec<u8> {
    let mut bytes = [0u8; 12];
    bytes[0..4].copy_from_slice(&months.to_le_bytes());
    bytes.to_vec()
}

fn interval_day_time_bytes(days: i32, millis: i32) -> Vec<u8> {
    let mut bytes = [0u8; 12];
    bytes[4..8].copy_from_slice(&days.to_le_bytes());
    bytes[8..12].copy_from_slice(&millis.to_le_bytes());
    bytes.to_vec()
}

fn file_metadata(schema_descr: Arc<SchemaDescriptor>, num_rows: i64) -> FileMetaData {
    let column_orders = schema_descr
        .columns()
        .iter()
        .map(|col| {
            let sort_order = ColumnOrder::sort_order_for_type(
                col.logical_type_ref(),
                col.converted_type(),
                col.physical_type(),
            );
            ColumnOrder::TYPE_DEFINED_ORDER(sort_order)
        })
        .collect::<Vec<_>>();
    FileMetaData::new(1, num_rows, None, None, schema_descr, Some(column_orders))
}

fn schema_descriptor() -> Arc<SchemaDescriptor> {
    let message_type = "message schema { REQUIRED FIXED_LEN_BYTE_ARRAY (12) interval (INTERVAL); }";
    let schema = Arc::new(parse_message_type(message_type).unwrap());
    Arc::new(SchemaDescriptor::new(schema))
}

fn interval_row_group_metadata(
    schema_descr: &Arc<SchemaDescriptor>,
    min: Vec<u8>,
    max: Vec<u8>,
    num_rows: i64,
) -> RowGroupMetaData {
    let col_descr = schema_descr.column(0);
    let stats = Statistics::fixed_len_byte_array(
        Some(FixedLenByteArray::from(min)),
        Some(FixedLenByteArray::from(max)),
        None,
        Some(0),
        false,
    );
    let column = ColumnChunkMetaData::builder(col_descr.clone())
        .set_statistics(stats)
        .set_num_values(num_rows)
        .build()
        .unwrap();

    RowGroupMetaData::builder(schema_descr.clone())
        .set_num_rows(num_rows)
        .set_total_byte_size(0)
        .add_column_metadata(column)
        .build()
        .unwrap()
}

fn build_interval_page_metadata(
    page_min: Vec<Vec<u8>>,
    page_max: Vec<Vec<u8>>,
    row_group_min: Vec<u8>,
    row_group_max: Vec<u8>,
) -> ParquetMetaData {
    let schema_descr = schema_descriptor();
    let num_rows = page_min.len() as i64;

    let row_group = interval_row_group_metadata(&schema_descr, row_group_min, row_group_max, num_rows);
    let file_meta = file_metadata(schema_descr.clone(), num_rows);

    let mut column_index = ColumnIndexBuilder::new(PhysicalType::FIXED_LEN_BYTE_ARRAY);
    for (min, max) in page_min.into_iter().zip(page_max) {
        column_index.append(false, min, max, 0);
    }
    column_index.set_boundary_order(BoundaryOrder::UNORDERED);

    let mut offset_index = OffsetIndexBuilder::new();
    for i in 0..num_rows {
        offset_index.append_row_count(1);
        offset_index.append_offset_and_size(i as i64, 1);
    }

    ParquetMetaDataBuilder::new(file_meta)
        .add_row_group(row_group)
        .set_column_index(Some(vec![vec![column_index.build().unwrap()]]))
        .set_offset_index(Some(vec![vec![offset_index.build()]]))
        .build()
}

#[test]
fn row_group_prunes_interval_year_month_not_eq() {
    let schema = Schema::new(vec![Field::new(
        "interval",
        DataType::Interval(IntervalUnit::YearMonth),
        false,
    )]);

    let schema_descr = schema_descriptor();
    let row_group_0 = interval_row_group_metadata(
        &schema_descr,
        interval_year_month_bytes(12),
        interval_year_month_bytes(12),
        3,
    );
    let row_group_1 = interval_row_group_metadata(
        &schema_descr,
        interval_year_month_bytes(13),
        interval_year_month_bytes(13),
        3,
    );

    let file_meta = file_metadata(schema_descr.clone(), 6);
    let metadata = ParquetMetaDataBuilder::new(file_meta)
        .set_row_groups(vec![row_group_0, row_group_1])
        .build();

    let expr = Expr::not_eq("interval", ScalarValue::IntervalYearMonth(Some(12)));
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .prune();

    assert_eq!(result.row_groups(), &[1]);
}

#[test]
fn row_group_prunes_interval_day_time_not_eq() {
    let schema = Schema::new(vec![Field::new(
        "interval",
        DataType::Interval(IntervalUnit::DayTime),
        false,
    )]);

    let schema_descr = schema_descriptor();
    let row_group_0 = interval_row_group_metadata(
        &schema_descr,
        interval_day_time_bytes(1, 0),
        interval_day_time_bytes(1, 0),
        3,
    );
    let row_group_1 = interval_row_group_metadata(
        &schema_descr,
        interval_day_time_bytes(2, 0),
        interval_day_time_bytes(2, 0),
        3,
    );

    let file_meta = file_metadata(schema_descr.clone(), 6);
    let metadata = ParquetMetaDataBuilder::new(file_meta)
        .set_row_groups(vec![row_group_0, row_group_1])
        .build();

    let value = IntervalDayTime::new(1, 0);
    let expr = Expr::not_eq("interval", ScalarValue::IntervalDayTime(Some(value)));
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .prune();

    assert_eq!(result.row_groups(), &[1]);
}

#[test]
fn page_level_prunes_interval_year_month_pages() {
    let schema = Schema::new(vec![Field::new(
        "interval",
        DataType::Interval(IntervalUnit::YearMonth),
        false,
    )]);

    let metadata = build_interval_page_metadata(
        vec![
            interval_year_month_bytes(1),
            interval_year_month_bytes(2),
            interval_year_month_bytes(3),
            interval_year_month_bytes(4),
        ],
        vec![
            interval_year_month_bytes(1),
            interval_year_month_bytes(2),
            interval_year_month_bytes(3),
            interval_year_month_bytes(4),
        ],
        interval_year_month_bytes(1),
        interval_year_month_bytes(4),
    );

    let expr = Expr::not_eq("interval", ScalarValue::IntervalYearMonth(Some(2)));
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

#[test]
fn page_level_prunes_interval_day_time_pages() {
    let schema = Schema::new(vec![Field::new(
        "interval",
        DataType::Interval(IntervalUnit::DayTime),
        false,
    )]);

    let metadata = build_interval_page_metadata(
        vec![
            interval_day_time_bytes(1, 0),
            interval_day_time_bytes(2, 0),
            interval_day_time_bytes(3, 0),
            interval_day_time_bytes(4, 0),
        ],
        vec![
            interval_day_time_bytes(1, 0),
            interval_day_time_bytes(2, 0),
            interval_day_time_bytes(3, 0),
            interval_day_time_bytes(4, 0),
        ],
        interval_day_time_bytes(1, 0),
        interval_day_time_bytes(4, 0),
    );

    let value = IntervalDayTime::new(2, 0);
    let expr = Expr::not_eq("interval", ScalarValue::IntervalDayTime(Some(value)));
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
