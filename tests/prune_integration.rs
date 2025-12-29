use std::sync::Arc;

use aisle::PruneRequest;
use arrow_array::{Int32Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use datafusion_expr::{col, lit};
use parquet::{
    arrow::{ArrowWriter, arrow_reader::RowSelector},
    file::{
        metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader},
        properties::{EnabledStatistics, WriterProperties},
    },
};

fn make_batch(schema: &Schema, values: &[i32]) -> RecordBatch {
    let array = Int32Array::from(values.to_vec());
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

#[test]
fn prunes_row_groups() {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
    let batch1 = make_batch(&schema, &[1, 2, 3, 4, 5]);
    let batch2 = make_batch(&schema, &[10, 11, 12, 13, 14]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_max_row_group_size(5)
        .build();

    let bytes = write_parquet(&[batch1, batch2], props);
    let metadata = load_metadata(&bytes);

    let expr = col("a").gt(lit(9));
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(true)
        .emit_roaring(true)
        .prune();
    assert_eq!(result.row_groups(), &[1]);
    let selection = result.row_selection().expect("selection").clone();
    let selectors: Vec<RowSelector> = selection.into();
    assert_eq!(selectors, vec![RowSelector::select(5)]);

    let bitmap = result.roaring().expect("roaring");
    assert_eq!(bitmap.len(), 5);
}

#[test]
fn prunes_pages_with_index() {
    let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
    let batch = make_batch(&schema, &[1, 2, 3, 4, 5, 6]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_data_page_row_count_limit(1)
        .set_data_page_size_limit(1)
        .set_dictionary_enabled(false)
        .set_max_row_group_size(100)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata(&bytes);
    assert!(metadata.column_index().is_some());
    assert!(metadata.offset_index().is_some());
    let page_locations = metadata.offset_index().unwrap()[0][0].page_locations();

    let expr = col("a").gt(lit(3));
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(true)
        .emit_roaring(false)
        .prune();
    assert_eq!(result.row_groups(), &[0]);
    let selection = result.row_selection().expect("selection").clone();
    let selectors: Vec<RowSelector> = selection.into();
    if page_locations.len() > 1 {
        assert_eq!(
            selectors,
            vec![RowSelector::skip(3), RowSelector::select(3)]
        );
    } else {
        assert_eq!(selectors, vec![RowSelector::select(6)]);
    }
}
