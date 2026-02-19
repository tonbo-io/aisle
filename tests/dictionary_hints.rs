use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use aisle::{
    AsyncBloomFilterProvider, CachedDictionaryHintProvider, DictionaryHintEvidence,
    DictionaryHintValue, Expr, PruneRequest,
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

fn make_two_group_string_metadata() -> (Schema, ParquetMetaData) {
    make_two_group_string_metadata_with_values(&["alpha", "beta"], &["target", "omega"])
}

fn make_two_group_string_metadata_with_values(
    group0_values: &[&str],
    group1_values: &[&str],
) -> (Schema, ParquetMetaData) {
    let schema = Schema::new(vec![Field::new("s", DataType::Utf8, false)]);
    let batch1 = make_string_batch(&schema, group0_values);
    let batch2 = make_string_batch(&schema, group1_values);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::None)
        .set_dictionary_enabled(true)
        .set_max_row_group_size(2)
        .build();

    let bytes = write_parquet_bytes(&[batch1, batch2], props);
    (schema, load_metadata(&bytes))
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

#[derive(Default)]
struct BatchedDictionaryProvider {
    dictionary_calls: usize,
    batch_calls: usize,
    hints: HashMap<(usize, usize), HashSet<DictionaryHintValue>>,
}

impl AsyncBloomFilterProvider for BatchedDictionaryProvider {
    async fn bloom_filter(&mut self, _row_group_idx: usize, _column_idx: usize) -> Option<Sbbf> {
        None
    }

    async fn dictionary_hints(
        &mut self,
        row_group_idx: usize,
        column_idx: usize,
    ) -> DictionaryHintEvidence {
        self.dictionary_calls += 1;
        match self.hints.get(&(row_group_idx, column_idx)) {
            Some(values) => DictionaryHintEvidence::Exact(values.clone()),
            None => DictionaryHintEvidence::Unknown,
        }
    }

    async fn dictionary_hints_batch<'a>(
        &'a mut self,
        requests: &'a [(usize, usize)],
    ) -> HashMap<(usize, usize), DictionaryHintEvidence> {
        self.batch_calls += 1;
        requests
            .iter()
            .map(|&(row_group_idx, column_idx)| {
                let evidence = match self.hints.get(&(row_group_idx, column_idx)) {
                    Some(values) => DictionaryHintEvidence::Exact(values.clone()),
                    None => DictionaryHintEvidence::Unknown,
                };
                ((row_group_idx, column_idx), evidence)
            })
            .collect()
    }
}

#[derive(Default)]
struct TransientUnknownProvider {
    dictionary_calls_per_key: HashMap<(usize, usize), usize>,
    hints: HashMap<(usize, usize), HashSet<DictionaryHintValue>>,
}

impl AsyncBloomFilterProvider for TransientUnknownProvider {
    async fn bloom_filter(&mut self, _row_group_idx: usize, _column_idx: usize) -> Option<Sbbf> {
        None
    }

    async fn dictionary_hints(
        &mut self,
        row_group_idx: usize,
        column_idx: usize,
    ) -> DictionaryHintEvidence {
        let call_count = self
            .dictionary_calls_per_key
            .entry((row_group_idx, column_idx))
            .and_modify(|count| *count += 1)
            .or_insert(1);
        if *call_count == 1 {
            return DictionaryHintEvidence::Unknown;
        }
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
    let (schema, metadata) = make_two_group_string_metadata();

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
    let (schema, metadata) = make_two_group_string_metadata();

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
    let (schema, metadata) = make_two_group_string_metadata();

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

#[tokio::test]
async fn dictionary_hints_batch_provider_called_per_row_group() {
    let (schema, metadata) = make_two_group_string_metadata();

    let mut provider = BatchedDictionaryProvider::default();
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
    assert_eq!(provider.batch_calls, 2);
    assert_eq!(provider.dictionary_calls, 0);
}

#[tokio::test]
async fn cached_dictionary_provider_reuses_evidence_across_prune_calls() {
    let (schema, metadata) = make_two_group_string_metadata();

    let mut inner = MockDictionaryProvider::default();
    inner.hints.insert(
        (0, 0),
        HashSet::from([
            DictionaryHintValue::Utf8("alpha".to_string()),
            DictionaryHintValue::Utf8("beta".to_string()),
        ]),
    );
    inner.hints.insert(
        (1, 0),
        HashSet::from([
            DictionaryHintValue::Utf8("target".to_string()),
            DictionaryHintValue::Utf8("omega".to_string()),
        ]),
    );

    let mut provider = CachedDictionaryHintProvider::new(inner);
    let expr = target_expr();

    let first = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .enable_bloom_filter(false)
        .enable_dictionary_hints(true)
        .emit_roaring(false)
        .prune_async(&mut provider)
        .await;

    let second = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .enable_bloom_filter(false)
        .enable_dictionary_hints(true)
        .emit_roaring(false)
        .prune_async(&mut provider)
        .await;

    assert_eq!(first.row_groups(), &[1]);
    assert_eq!(second.row_groups(), &[1]);
    assert_eq!(provider.inner().dictionary_calls.len(), 2);
    assert_eq!(provider.cached_entries(), 2);
}

#[tokio::test]
async fn cached_dictionary_provider_retarget_without_clear_should_not_false_prune() {
    let (schema_a, metadata_a) =
        make_two_group_string_metadata_with_values(&["alpha", "beta"], &["target", "omega"]);
    let (schema_b, metadata_b) =
        make_two_group_string_metadata_with_values(&["target", "beta"], &["alpha", "omega"]);

    let mut inner = MockDictionaryProvider::default();
    inner.hints.insert(
        (0, 0),
        HashSet::from([
            DictionaryHintValue::Utf8("alpha".to_string()),
            DictionaryHintValue::Utf8("beta".to_string()),
        ]),
    );
    inner.hints.insert(
        (1, 0),
        HashSet::from([
            DictionaryHintValue::Utf8("target".to_string()),
            DictionaryHintValue::Utf8("omega".to_string()),
        ]),
    );
    let mut provider = CachedDictionaryHintProvider::new(inner);

    let expr = target_expr();

    let first = PruneRequest::new(&metadata_a, &schema_a)
        .with_predicate(&expr)
        .enable_page_index(false)
        .enable_bloom_filter(false)
        .enable_dictionary_hints(true)
        .emit_roaring(false)
        .prune_async(&mut provider)
        .await;
    assert_eq!(first.row_groups(), &[1]);

    let mut inner_b = MockDictionaryProvider::default();
    inner_b.hints.insert(
        (0, 0),
        HashSet::from([
            DictionaryHintValue::Utf8("target".to_string()),
            DictionaryHintValue::Utf8("beta".to_string()),
        ]),
    );
    inner_b.hints.insert(
        (1, 0),
        HashSet::from([
            DictionaryHintValue::Utf8("alpha".to_string()),
            DictionaryHintValue::Utf8("omega".to_string()),
        ]),
    );
    let _old = provider.retarget_inner(inner_b);
    assert_eq!(provider.cached_entries(), 0);

    let second = PruneRequest::new(&metadata_b, &schema_b)
        .with_predicate(&expr)
        .enable_page_index(false)
        .enable_bloom_filter(false)
        .enable_dictionary_hints(true)
        .emit_roaring(false)
        .prune_async(&mut provider)
        .await;

    // Safety contract: retargeting provider state should not reuse stale evidence.
    assert_eq!(second.row_groups(), &[0]);
}

#[tokio::test]
async fn cached_dictionary_provider_does_not_pin_transient_unknowns() {
    let (schema, metadata) = make_two_group_string_metadata();

    let mut inner = TransientUnknownProvider::default();
    inner.hints.insert(
        (0, 0),
        HashSet::from([
            DictionaryHintValue::Utf8("alpha".to_string()),
            DictionaryHintValue::Utf8("beta".to_string()),
        ]),
    );
    inner.hints.insert(
        (1, 0),
        HashSet::from([
            DictionaryHintValue::Utf8("target".to_string()),
            DictionaryHintValue::Utf8("omega".to_string()),
        ]),
    );
    let mut provider = CachedDictionaryHintProvider::new(inner);

    let expr = target_expr();
    let first = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .enable_bloom_filter(false)
        .enable_dictionary_hints(true)
        .emit_roaring(false)
        .prune_async(&mut provider)
        .await;
    let second = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .enable_bloom_filter(false)
        .enable_dictionary_hints(true)
        .emit_roaring(false)
        .prune_async(&mut provider)
        .await;

    // First pass sees transient unknowns and keeps all row groups.
    assert_eq!(first.row_groups(), &[0, 1]);
    // Second pass retries unknowns and recovers exact pruning.
    assert_eq!(second.row_groups(), &[1]);
    assert_eq!(provider.inner().dictionary_calls_per_key.get(&(0, 0)), Some(&2));
    assert_eq!(provider.inner().dictionary_calls_per_key.get(&(1, 0)), Some(&2));
    assert_eq!(provider.cached_entries(), 2);
}
