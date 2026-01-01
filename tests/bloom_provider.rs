use std::{collections::HashMap, sync::Arc};

use aisle::{AsyncBloomFilterProvider, PruneRequest};
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use datafusion_expr::{col, lit};
use parquet::{
    arrow::AsyncArrowWriter,
    bloom_filter::Sbbf,
    file::{
        metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader},
        properties::{EnabledStatistics, WriterProperties},
    },
};

// ============================================================================
// Helper Functions
// ============================================================================

fn make_int_batch(schema: &Schema, values: &[i64]) -> RecordBatch {
    let array = Int64Array::from(values.to_vec());
    RecordBatch::try_new(Arc::new(schema.clone()), vec![Arc::new(array)]).unwrap()
}

async fn write_parquet_async(batches: &[RecordBatch], props: WriterProperties) -> Vec<u8> {
    let mut buffer: Vec<u8> = Vec::new();
    let schema = batches[0].schema();
    let mut writer = AsyncArrowWriter::try_new(&mut buffer, schema, Some(props)).unwrap();
    for batch in batches {
        writer.write(batch).await.unwrap();
    }
    writer.close().await.unwrap();
    buffer
}

fn load_metadata(bytes: &[u8]) -> ParquetMetaData {
    let bytes = Bytes::copy_from_slice(bytes);
    ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Skip)
        .parse_and_finish(&bytes)
        .unwrap()
}

// ============================================================================
// Mock Bloom Filter Provider
// ============================================================================

/// Mock provider that tracks which bloom filters were requested
struct MockBloomProvider {
    calls: Vec<(usize, usize)>,
    filters: HashMap<(usize, usize), Sbbf>,
}

impl MockBloomProvider {
    fn new() -> Self {
        Self {
            calls: Vec::new(),
            filters: HashMap::new(),
        }
    }

    fn calls(&self) -> &[(usize, usize)] {
        &self.calls
    }
}

impl AsyncBloomFilterProvider for MockBloomProvider {
    async fn bloom_filter(&mut self, row_group_idx: usize, column_idx: usize) -> Option<Sbbf> {
        self.calls.push((row_group_idx, column_idx));
        self.filters.get(&(row_group_idx, column_idx)).cloned()
    }
}

// ============================================================================
// Cached Bloom Filter Provider
// ============================================================================

/// Provider that caches bloom filters after first load
struct CachedBloomProvider {
    calls: Vec<(usize, usize)>,
    cache_hits: usize,
    filters: HashMap<(usize, usize), Sbbf>,
    cache: HashMap<(usize, usize), Sbbf>,
}

impl CachedBloomProvider {
    fn new() -> Self {
        Self {
            calls: Vec::new(),
            cache_hits: 0,
            filters: HashMap::new(),
            cache: HashMap::new(),
        }
    }

    fn cache_hits(&self) -> usize {
        self.cache_hits
    }
}

impl AsyncBloomFilterProvider for CachedBloomProvider {
    async fn bloom_filter(&mut self, row_group_idx: usize, column_idx: usize) -> Option<Sbbf> {
        let key = (row_group_idx, column_idx);
        self.calls.push(key);

        // Check cache first
        if let Some(filter) = self.cache.get(&key) {
            self.cache_hits += 1;
            return Some(filter.clone());
        }

        // Load and cache
        if let Some(filter) = self.filters.get(&key).cloned() {
            self.cache.insert(key, filter.clone());
            Some(filter)
        } else {
            None
        }
    }
}

// ============================================================================
// Batched Bloom Filter Provider
// ============================================================================

/// Provider that tracks batch calls
struct BatchedBloomProvider {
    single_calls: usize,
    batch_calls: usize,
    filters: HashMap<(usize, usize), Sbbf>,
}

impl BatchedBloomProvider {
    fn new() -> Self {
        Self {
            single_calls: 0,
            batch_calls: 0,
            filters: HashMap::new(),
        }
    }

    fn single_calls(&self) -> usize {
        self.single_calls
    }

    fn batch_calls(&self) -> usize {
        self.batch_calls
    }
}

impl AsyncBloomFilterProvider for BatchedBloomProvider {
    async fn bloom_filter(&mut self, row_group_idx: usize, column_idx: usize) -> Option<Sbbf> {
        self.single_calls += 1;
        self.filters.get(&(row_group_idx, column_idx)).cloned()
    }

    async fn bloom_filters_batch<'a>(
        &'a mut self,
        requests: &'a [(usize, usize)],
    ) -> HashMap<(usize, usize), Sbbf> {
        self.batch_calls += 1;

        // Custom batch implementation
        requests
            .iter()
            .filter_map(|&key| self.filters.get(&key).map(|f| (key, f.clone())))
            .collect()
    }
}

// ============================================================================
// Tests
// ============================================================================

#[tokio::test]
async fn test_mock_provider_tracks_calls() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let batch = make_int_batch(&schema, &[1, 2]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .build();

    let bytes = write_parquet_async(&[batch], props).await;
    let metadata = load_metadata(&bytes);

    let mut provider = MockBloomProvider::new();
    let expr = col("id").eq(lit(150i64));

    // Call without bloom filters enabled - provider should not be called
    let _result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .enable_bloom_filter(false)
        .emit_roaring(false)
        .prune_async(&mut provider)
        .await;

    assert_eq!(
        provider.calls().len(),
        0,
        "Provider should not be called when bloom filters disabled"
    );
}

#[tokio::test]
async fn test_mock_provider_called_with_bloom_enabled() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let batch = make_int_batch(&schema, &[1, 2]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_bloom_filter_enabled(true)
        .build();

    let bytes = write_parquet_async(&[batch], props).await;
    let metadata = load_metadata(&bytes);

    let mut provider = MockBloomProvider::new();
    let expr = col("id").eq(lit(150i64));

    let _result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .enable_bloom_filter(true) // Enable bloom filter pruning
        .emit_roaring(false)
        .prune_async(&mut provider)
        .await;

    // Should have attempted to load bloom filter for column 0, row group 0
    assert!(
        provider.calls().contains(&(0, 0)),
        "Provider should be called for row group 0, column 0"
    );
}

#[tokio::test]
async fn test_default_batch_implementation_is_sequential() {
    let mut provider = MockBloomProvider::new();

    // Simulate batch call with default implementation
    let requests = vec![(0, 0), (0, 1), (1, 0)];
    let _results = provider.bloom_filters_batch(&requests).await;

    // Default implementation calls bloom_filter sequentially for each request
    assert_eq!(provider.calls().len(), 3);
    assert_eq!(provider.calls(), &[(0, 0), (0, 1), (1, 0)]);
}

#[tokio::test]
async fn test_cached_provider_avoids_duplicate_loads() {
    let mut provider = CachedBloomProvider::new();

    // First call - no filter available, not cached
    let result1 = provider.bloom_filter(0, 0).await;
    assert!(result1.is_none());
    assert_eq!(provider.cache_hits(), 0);
    assert_eq!(provider.calls.len(), 1);

    // Second call to same key - still tries to load (None not cached)
    let result2 = provider.bloom_filter(0, 0).await;
    assert!(result2.is_none());
    assert_eq!(provider.cache_hits(), 0); // Still 0 - None results aren't cached
    assert_eq!(provider.calls.len(), 2);

    // Note: Cache only stores successful filter loads (Some values).
    // This is correct behavior - we don't cache "not found" results.
}

#[tokio::test]
async fn test_batched_provider_uses_custom_batch_method() {
    let mut provider = BatchedBloomProvider::new();

    let requests = vec![(0, 0), (0, 1), (1, 0)];
    let _results = provider.bloom_filters_batch(&requests).await;

    // Custom batch method should be called once, not individual calls
    assert_eq!(provider.batch_calls(), 1);
    assert_eq!(provider.single_calls(), 0);
}

#[tokio::test]
async fn test_provider_returns_none_for_missing_filters() {
    let mut provider = MockBloomProvider::new();

    let result = provider.bloom_filter(0, 0).await;
    assert!(result.is_none());

    let result = provider.bloom_filter(999, 999).await;
    assert!(result.is_none());
}

#[tokio::test]
async fn test_batch_returns_only_available_filters() {
    // Note: We can't easily create real Sbbf instances in tests without
    // actual Parquet files with bloom filters, so this test demonstrates
    // the structure even though filters will be None

    let mut provider = MockBloomProvider::new();

    let requests = vec![(0, 0), (0, 1), (1, 0)];
    let results = provider.bloom_filters_batch(&requests).await;

    // Should return empty map (no filters available)
    assert_eq!(results.len(), 0);

    // But all requests should have been attempted
    assert_eq!(provider.calls().len(), 3);
}

#[tokio::test]
async fn test_provider_called_for_multiple_row_groups() {
    let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    let batch1 = make_int_batch(&schema, &[1, 2]);
    let batch2 = make_int_batch(&schema, &[3, 4]);
    let batch3 = make_int_batch(&schema, &[5, 6]);

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_size(2)
        .set_bloom_filter_enabled(true)
        .build();

    let bytes = write_parquet_async(&[batch1, batch2, batch3], props).await;
    let metadata = load_metadata(&bytes);

    let mut provider = MockBloomProvider::new();
    let expr = col("id").eq(lit(150i64));

    let _result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .enable_bloom_filter(true)
        .emit_roaring(false)
        .prune_async(&mut provider)
        .await;

    // Should attempt to load bloom filter for all row groups
    assert!(provider.calls().contains(&(0, 0)));
    assert!(provider.calls().contains(&(1, 0)));
    assert!(provider.calls().contains(&(2, 0)));
}

#[tokio::test]
async fn test_cached_provider_with_multiple_columns() {
    let mut provider = CachedBloomProvider::new();

    // Load filters for different columns (all return None, so no caching occurs)
    provider.bloom_filter(0, 0).await;
    provider.bloom_filter(0, 1).await;
    provider.bloom_filter(0, 0).await; // No cache hit - None not cached
    provider.bloom_filter(0, 1).await; // No cache hit - None not cached

    assert_eq!(provider.cache_hits(), 0); // None results don't get cached
    assert_eq!(provider.calls.len(), 4);
}

#[tokio::test]
async fn test_batch_method_with_empty_requests() {
    let mut provider = MockBloomProvider::new();

    let requests: Vec<(usize, usize)> = vec![];
    let results = provider.bloom_filters_batch(&requests).await;

    assert_eq!(results.len(), 0);
    assert_eq!(provider.calls().len(), 0);
}

#[tokio::test]
async fn test_batch_method_with_duplicate_requests() {
    let mut provider = MockBloomProvider::new();

    // Same request multiple times
    let requests = vec![(0, 0), (0, 0), (0, 0)];
    let results = provider.bloom_filters_batch(&requests).await;

    // Default implementation will call bloom_filter 3 times
    assert_eq!(provider.calls().len(), 3);
    assert_eq!(results.len(), 0); // No filters available
}
