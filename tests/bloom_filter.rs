use std::sync::Arc;

use aisle::{Expr, PruneRequest};
use arrow_array::{RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion_common::ScalarValue;
use parquet::{
    arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder},
    file::properties::{EnabledStatistics, WriterProperties},
};

fn make_batch(schema: &Schema, values: &[&str]) -> RecordBatch {
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

#[tokio::test]
async fn prunes_row_groups_with_bloom_filter_eq() {
    let schema = Schema::new(vec![Field::new("s", DataType::Utf8, false)]);
    let batch1 = make_batch(&schema, &["fa", "fz"]);
    let batch2 = make_batch(&schema, &["foo", "fx"]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_bloom_filter_enabled(true)
        .set_max_row_group_size(2)
        .build();

    let bytes = write_parquet(&[batch1, batch2], props);
    let path = std::env::temp_dir().join(format!(
        "aisle_bloom_eq_{}.parquet",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::write(&path, &bytes).unwrap();

    let file = tokio::fs::File::open(&path).await.unwrap();
    let mut builder = ParquetRecordBatchStreamBuilder::new(file).await.unwrap();

    // This test specifically tests bloom filter pruning, so we construct
    // the bloom filter variant directly (internal test)
    let value = ScalarValue::Utf8(Some("foo".to_string()));
    let expr = Expr::BloomFilterEq {
        column: "s".to_string(),
        value,
    };

    let metadata = builder.metadata().clone();
    let schema = builder.schema().clone();
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .enable_bloom_filter(true)
        .emit_roaring(false)
        .prune_async(&mut builder)
        .await;

    assert_eq!(result.row_groups(), &[1]);

    let _ = std::fs::remove_file(&path);
}
