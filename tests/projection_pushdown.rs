use std::sync::Arc;

use aisle::{Expr, PruneRequest, RowFilter as AisleRowFilter};
use arrow_array::{ArrayRef, Int32Array, RecordBatch, StructArray};
use arrow_schema::{DataType, Field, Fields, Schema};
use bytes::Bytes;
use datafusion_common::ScalarValue;
use parquet::{
    arrow::{
        ArrowWriter,
        arrow_reader::{ParquetRecordBatchReaderBuilder, RowFilter as ParquetRowFilter},
    },
    file::{
        metadata::{ParquetMetaData, ParquetMetaDataReader},
        properties::{EnabledStatistics, WriterProperties},
    },
};

fn write_parquet(batches: &[RecordBatch], props: WriterProperties) -> Bytes {
    let mut buffer = Vec::new();
    let schema = batches[0].schema();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props)).unwrap();
    for batch in batches {
        writer.write(batch).unwrap();
    }
    writer.close().unwrap();
    Bytes::from(buffer)
}

fn load_metadata(bytes: &Bytes) -> ParquetMetaData {
    ParquetMetaDataReader::new()
        .parse_and_finish(bytes)
        .unwrap()
}

fn make_wide_data() -> (Bytes, Arc<Schema>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int32, false),
        Field::new("payload_a", DataType::Int32, false),
        Field::new("payload_b", DataType::Int32, false),
    ]));

    let keys: Vec<i32> = (0..10).collect();
    let payload_a: Vec<i32> = keys.iter().map(|v| v + 100).collect();
    let payload_b: Vec<i32> = keys.iter().map(|v| v + 200).collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int32Array::from(keys)),
            Arc::new(Int32Array::from(payload_a)),
            Arc::new(Int32Array::from(payload_b)),
        ],
    )
    .unwrap();

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_size(5)
        .build();

    (write_parquet(&[batch], props), schema)
}

fn scan_all_columns(
    bytes: &Bytes,
    result: &aisle::PruneResult,
    predicate: &Expr,
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
) -> Vec<(i32, i32, i32)> {
    let row_filter = ParquetRowFilter::new(vec![Box::new(AisleRowFilter::new(
        predicate.clone(),
        parquet_schema,
    ))]);

    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
        .unwrap()
        .with_row_groups(result.row_groups().to_vec())
        .with_row_filter(row_filter)
        .build()
        .unwrap();

    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        let key = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let payload_a = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let payload_b = batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for idx in 0..batch.num_rows() {
            rows.push((key.value(idx), payload_a.value(idx), payload_b.value(idx)));
        }
    }
    rows
}

fn scan_projected_payload_a(
    bytes: &Bytes,
    result: &aisle::PruneResult,
    predicate: &Expr,
    parquet_schema: &parquet::schema::types::SchemaDescriptor,
) -> Vec<i32> {
    let row_filter = ParquetRowFilter::new(vec![Box::new(AisleRowFilter::new(
        predicate.clone(),
        parquet_schema,
    ))]);

    let mut builder = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
        .unwrap()
        .with_row_groups(result.row_groups().to_vec())
        .with_row_filter(row_filter);
    if let Some(mask) = result.output_projection_mask(parquet_schema) {
        builder = builder.with_projection(mask);
    }
    let reader = builder.build().unwrap();

    let mut values = Vec::new();
    for batch in reader {
        let batch = batch.unwrap();
        assert_eq!(batch.num_columns(), 1);
        let payload_a = batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        for idx in 0..batch.num_rows() {
            values.push(payload_a.value(idx));
        }
    }
    values
}

#[test]
fn extracts_required_columns_for_mixed_predicates() {
    let (bytes, schema) = make_wide_data();
    let metadata = load_metadata(&bytes);

    let predicate = Expr::and(vec![
        Expr::gt("key", ScalarValue::Int32(Some(2))),
        Expr::or(vec![
            Expr::is_not_null("payload_a"),
            Expr::between(
                "payload_b",
                ScalarValue::Int32(Some(200)),
                ScalarValue::Int32(Some(205)),
                true,
            ),
        ]),
    ]);

    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&predicate)
        .with_output_projection(["payload_b"])
        .prune();

    assert_eq!(
        result.predicate_columns(),
        &[
            "key".to_string(),
            "payload_a".to_string(),
            "payload_b".to_string()
        ]
    );
    assert_eq!(
        result.required_columns(),
        &[
            "key".to_string(),
            "payload_a".to_string(),
            "payload_b".to_string()
        ]
    );
}

#[test]
fn extracts_required_columns_for_nested_predicates() {
    let nested_fields = Fields::from(vec![
        Field::new("score", DataType::Int32, false),
        Field::new("region", DataType::Int32, false),
    ]);
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("info", DataType::Struct(nested_fields), false),
    ]));

    let ids = Arc::new(Int32Array::from(vec![1, 2, 3])) as ArrayRef;
    let score = Arc::new(Int32Array::from(vec![10, 20, 30])) as ArrayRef;
    let region = Arc::new(Int32Array::from(vec![1, 1, 2])) as ArrayRef;
    let struct_array = StructArray::from(vec![
        (Arc::new(Field::new("score", DataType::Int32, false)), score),
        (
            Arc::new(Field::new("region", DataType::Int32, false)),
            region,
        ),
    ]);
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![ids, Arc::new(struct_array) as ArrayRef],
    )
    .unwrap();

    let bytes = write_parquet(
        &[batch],
        WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .build(),
    );
    let metadata = load_metadata(&bytes);

    let predicate = Expr::gt("info.score", ScalarValue::Int32(Some(15)));
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&predicate)
        .with_output_projection(["id"])
        .prune();

    assert_eq!(result.predicate_columns(), &["info.score".to_string()]);
    assert_eq!(
        result.required_columns(),
        &["id".to_string(), "info.score".to_string()]
    );
}

#[test]
fn applying_output_projection_reduces_columns_without_changing_rows() {
    let (bytes, schema) = make_wide_data();
    let metadata = load_metadata(&bytes);
    let predicate = Expr::gt("key", ScalarValue::Int32(Some(6)));
    let parquet_schema = metadata.file_metadata().schema_descr();

    let no_projection = PruneRequest::new(&metadata, &schema)
        .with_predicate(&predicate)
        .prune();
    let with_projection = PruneRequest::new(&metadata, &schema)
        .with_predicate(&predicate)
        .with_output_projection(["payload_a"])
        .prune();

    let all_rows = scan_all_columns(&bytes, &no_projection, &predicate, parquet_schema);
    let projected_payload =
        scan_projected_payload_a(&bytes, &with_projection, &predicate, parquet_schema);

    assert_eq!(
        all_rows
            .iter()
            .map(|(_, payload_a, _)| *payload_a)
            .collect::<Vec<_>>(),
        projected_payload
    );
}

#[test]
fn projection_configuration_is_noop_when_not_applied_to_reader() {
    let (bytes, schema) = make_wide_data();
    let metadata = load_metadata(&bytes);
    let predicate = Expr::gt("key", ScalarValue::Int32(Some(6)));
    let parquet_schema = metadata.file_metadata().schema_descr();

    let no_projection = PruneRequest::new(&metadata, &schema)
        .with_predicate(&predicate)
        .prune();
    let with_projection = PruneRequest::new(&metadata, &schema)
        .with_predicate(&predicate)
        .with_output_projection(["payload_a"])
        .prune();

    assert_eq!(no_projection.row_groups(), with_projection.row_groups());
    assert_eq!(
        no_projection.row_selection().is_some(),
        with_projection.row_selection().is_some()
    );

    let no_projection_rows = scan_all_columns(&bytes, &no_projection, &predicate, parquet_schema);
    let projection_unused_rows =
        scan_all_columns(&bytes, &with_projection, &predicate, parquet_schema);
    assert_eq!(no_projection_rows, projection_unused_rows);
}
