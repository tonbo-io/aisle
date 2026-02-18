use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use aisle::{
    AsyncBloomFilterProvider, DictionaryHintEvidence, DictionaryHintValue, Expr, PruneRequest,
};
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use datafusion_common::ScalarValue;
use parquet::{
    arrow::ArrowWriter,
    bloom_filter::Sbbf,
    file::{
        metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader},
        properties::{EnabledStatistics, WriterProperties},
    },
};

fn make_string_batch(schema: &Schema, values: &[&str]) -> RecordBatch {
    let array = StringArray::from(values.to_vec());
    RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(array)]).unwrap()
}

fn write_parquet_bytes(batches: &[RecordBatch], props: WriterProperties) -> Vec<u8> {
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
        .with_page_index_policy(PageIndexPolicy::Skip)
        .parse_and_finish(&bytes)
        .unwrap()
}

#[derive(Default)]
struct MockDictionaryProvider {
    dictionary_calls: Vec<(usize, usize)>,
    hints: HashMap<(usize, usize), HashSet<DictionaryHintValue>>,
}

impl AsyncBloomFilterProvider for MockDictionaryProvider {
    async fn bloom_filter(&mut self, _row_group_idx: usize, _column_idx: usize) -> Option<Sbbf> {
        None
    }

    async fn dictionary_hints(
        &mut self,
        row_group_idx: usize,
        column_idx: usize,
    ) -> DictionaryHintEvidence {
        self.dictionary_calls.push((row_group_idx, column_idx));
        match self.hints.get(&(row_group_idx, column_idx)) {
            Some(values) => DictionaryHintEvidence::Exact(values.clone()),
            None => DictionaryHintEvidence::Unknown,
        }
    }
}

fn target_expr() -> Expr {
    Expr::eq("s", ScalarValue::Utf8(Some("target".to_string())))
}

#[tokio::test]
async fn dictionary_hints_prune_when_enabled() {
    let schema = Schema::new(vec![Field::new("s", DataType::Utf8, false)]);
    let batch1 = make_string_batch(&schema, &["alpha", "beta"]);
    let batch2 = make_string_batch(&schema, &["target", "omega"]);

    // Disable stats so pruning depends on dictionary hints only.
    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::None)
        .set_dictionary_enabled(true)
        .set_max_row_group_size(2)
        .build();

    let bytes = write_parquet_bytes(&[batch1, batch2], props);
    let metadata = load_metadata(&bytes);

    let mut provider = MockDictionaryProvider::default();
    provider.hints.insert(
        (0, 0),
        HashSet::from([
            DictionaryHintValue::Utf8("alpha".to_string()),
            DictionaryHintValue::Utf8("beta".to_string()),
        ]),
    );
    provider.hints.insert(
        (1, 0),
        HashSet::from([
            DictionaryHintValue::Utf8("target".to_string()),
            DictionaryHintValue::Utf8("omega".to_string()),
        ]),
    );

    let expr = target_expr();
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .enable_bloom_filter(false)
        .enable_dictionary_hints(true)
        .emit_roaring(false)
        .prune_async(&mut provider)
        .await;

    assert_eq!(result.row_groups(), &[1]);
    assert_eq!(provider.dictionary_calls.len(), 2);
}

#[tokio::test]
async fn dictionary_hints_fallback_to_unknown_when_missing() {
    let schema = Schema::new(vec![Field::new("s", DataType::Utf8, false)]);
    let batch1 = make_string_batch(&schema, &["alpha", "beta"]);
    let batch2 = make_string_batch(&schema, &["target", "omega"]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::None)
        .set_dictionary_enabled(true)
        .set_max_row_group_size(2)
        .build();

    let bytes = write_parquet_bytes(&[batch1, batch2], props);
    let metadata = load_metadata(&bytes);

    // Missing/ambiguous hints: must be conservative and keep all row groups.
    let mut provider = MockDictionaryProvider::default();
    provider.hints.insert((0, 0), HashSet::new());
    let expr = target_expr();
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .enable_bloom_filter(false)
        .enable_dictionary_hints(true)
        .emit_roaring(false)
        .prune_async(&mut provider)
        .await;

    assert_eq!(result.row_groups(), &[0, 1]);
    assert_eq!(provider.dictionary_calls.len(), 2);
}

#[tokio::test]
async fn dictionary_hints_disabled_no_regression() {
    let schema = Schema::new(vec![Field::new("s", DataType::Utf8, false)]);
    let batch1 = make_string_batch(&schema, &["alpha", "beta"]);
    let batch2 = make_string_batch(&schema, &["target", "omega"]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::None)
        .set_dictionary_enabled(true)
        .set_max_row_group_size(2)
        .build();

    let bytes = write_parquet_bytes(&[batch1, batch2], props);
    let metadata = load_metadata(&bytes);

    let mut provider = MockDictionaryProvider::default();
    provider.hints.insert(
        (0, 0),
        HashSet::from([
            DictionaryHintValue::Utf8("alpha".to_string()),
            DictionaryHintValue::Utf8("beta".to_string()),
        ]),
    );
    provider.hints.insert(
        (1, 0),
        HashSet::from([
            DictionaryHintValue::Utf8("target".to_string()),
            DictionaryHintValue::Utf8("omega".to_string()),
        ]),
    );

    let expr = target_expr();
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .enable_bloom_filter(false)
        .enable_dictionary_hints(false)
        .emit_roaring(false)
        .prune_async(&mut provider)
        .await;

    assert_eq!(result.row_groups(), &[0, 1]);
    assert!(
        provider.dictionary_calls.is_empty(),
        "dictionary provider should not be called when hints are disabled"
    );
}

#[tokio::test]
async fn dictionary_hints_not_called_for_unsupported_literal_types() {
    let schema = Schema::new(vec![Field::new("n", DataType::Int64, false)]);
    let batch1 = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int64Array::from(vec![1, 2]))],
    )
    .unwrap();
    let batch2 = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(Int64Array::from(vec![42, 99]))],
    )
    .unwrap();

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::None)
        .set_dictionary_enabled(true)
        .set_max_row_group_size(2)
        .build();

    let bytes = write_parquet_bytes(&[batch1, batch2], props);
    let metadata = load_metadata(&bytes);

    let mut provider = MockDictionaryProvider::default();
    let expr = Expr::eq("n", ScalarValue::Int64(Some(42)));
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .enable_bloom_filter(false)
        .enable_dictionary_hints(true)
        .emit_roaring(false)
        .prune_async(&mut provider)
        .await;

    assert_eq!(result.row_groups(), &[0, 1]);
    assert!(
        provider.dictionary_calls.is_empty(),
        "dictionary provider should not be called for unsupported literal types"
    );
}
