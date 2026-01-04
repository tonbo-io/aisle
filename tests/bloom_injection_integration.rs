use std::sync::Arc;

use std::future::Future;

use aisle::{Expr, PruneRequest};
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use datafusion_common::ScalarValue;
use parquet::{
    arrow::AsyncArrowWriter,
    bloom_filter::Sbbf,
    file::{
        metadata::{PageIndexPolicy, ParquetMetaDataReader},
        properties::{EnabledStatistics, WriterProperties},
    },
};

async fn create_test_parquet(stats: EnabledStatistics) -> (Vec<u8>, Arc<Schema>) {
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![1, 2, 3]))],
    )
    .unwrap();

    let mut buffer = Vec::new();
    let props = WriterProperties::builder()
        .set_statistics_enabled(stats)
        .set_bloom_filter_enabled(true)
        .build();

    let mut writer = AsyncArrowWriter::try_new(&mut buffer, schema.clone(), Some(props)).unwrap();
    writer.write(&batch).await.unwrap();
    writer.close().await.unwrap();

    (buffer, schema)
}

#[derive(Default)]
struct CountingProvider {
    calls: Vec<(usize, usize)>,
}

impl aisle::AsyncBloomFilterProvider for CountingProvider {
    fn bloom_filter(
        &mut self,
        row_group_idx: usize,
        column_idx: usize,
    ) -> impl Future<Output = Option<Sbbf>> + '_ {
        self.calls.push((row_group_idx, column_idx));
        async { None }
    }
}

#[tokio::test]
async fn test_bloom_injection_disabled() {
    let (buffer, schema) = create_test_parquet(EnabledStatistics::None).await;
    let bytes = Bytes::from(buffer);
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Skip)
        .parse_and_finish(&bytes)
        .unwrap();

    // With bloom filters disabled, should NOT request bloom filters
    let expr = Expr::eq("id", ScalarValue::Int64(Some(42)));
    let mut provider = CountingProvider::default();
    let _result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_bloom_filter(false) // Disabled
        .prune_async(&mut provider)
        .await;

    assert!(
        provider.calls.is_empty(),
        "Provider should not be called when bloom filters are disabled"
    );
}

#[tokio::test]
async fn test_bloom_injection_enabled() {
    let (buffer, schema) = create_test_parquet(EnabledStatistics::None).await;
    let bytes = Bytes::from(buffer);
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Skip)
        .parse_and_finish(&bytes)
        .unwrap();

    // With bloom filters enabled, should request bloom filters
    let expr = Expr::eq("id", ScalarValue::Int64(Some(42)));
    let mut provider = CountingProvider::default();
    let _result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_bloom_filter(true) // Enabled
        .prune_async(&mut provider)
        .await;

    assert!(
        !provider.calls.is_empty(),
        "Provider should be called when bloom filters are enabled"
    );
}

#[tokio::test]
async fn test_bloom_injection_respects_negation() {
    let (buffer, schema) = create_test_parquet(EnabledStatistics::None).await;
    let bytes = Bytes::from(buffer);
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Skip)
        .parse_and_finish(&bytes)
        .unwrap();

    // NOT(eq) with bloom enabled should NOT request bloom filters
    let expr = Expr::not(Expr::eq("id", ScalarValue::Int64(Some(42))));
    let mut provider = CountingProvider::default();
    let _result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_bloom_filter(true) // Enabled
        .prune_async(&mut provider)
        .await;

    assert!(
        provider.calls.is_empty(),
        "Provider should not be called for negated predicates"
    );
}
