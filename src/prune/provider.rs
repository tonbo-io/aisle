use std::{
    collections::{HashMap, HashSet},
    future::Future,
};

use datafusion_common::ScalarValue;
use parquet::{
    arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder},
    bloom_filter::Sbbf,
};

/// Canonical value representation used by dictionary-hint providers.
///
/// Only string and binary values are included in this MVP. Unsupported value
/// types are ignored conservatively by dictionary-hint evaluation.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub enum DictionaryHintValue {
    Utf8(String),
    Binary(Vec<u8>),
}

impl DictionaryHintValue {
    /// Convert a [`ScalarValue`] into a dictionary-hint value when supported.
    pub fn from_scalar(value: &ScalarValue) -> Option<Self> {
        match value {
            ScalarValue::Utf8(Some(v))
            | ScalarValue::LargeUtf8(Some(v))
            | ScalarValue::Utf8View(Some(v)) => Some(Self::Utf8(v.clone())),
            ScalarValue::Binary(Some(v))
            | ScalarValue::LargeBinary(Some(v))
            | ScalarValue::BinaryView(Some(v))
            | ScalarValue::FixedSizeBinary(_, Some(v)) => Some(Self::Binary(v.clone())),
            _ => None,
        }
    }
}

/// Evidence returned by dictionary-hint providers.
///
/// Pruning is only allowed for [`DictionaryHintEvidence::Exact`] values.
/// Any uncertain evidence must be reported as [`DictionaryHintEvidence::Unknown`]
/// to preserve conservative correctness.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DictionaryHintEvidence {
    /// Complete and exact dictionary values for a row-group column.
    Exact(HashSet<DictionaryHintValue>),
    /// Missing or inexact evidence (must not be used for definitive pruning).
    Unknown,
}

/// Provider adapter that caches dictionary hint evidence across calls.
///
/// This wrapper is useful when repeated pruning requests touch the same
/// `(row_group_idx, column_idx)` pairs, for example in metadata-only loops
/// over the same file.
///
/// Cache entries store only [`DictionaryHintEvidence::Exact`] evidence.
/// [`DictionaryHintEvidence::Unknown`] is never cached so transient unknowns
/// can recover on later calls.
#[derive(Debug, Clone)]
pub struct CachedDictionaryHintProvider<P> {
    inner: P,
    cache: HashMap<(usize, usize), DictionaryHintEvidence>,
}

impl<P> CachedDictionaryHintProvider<P> {
    /// Create a new cached provider wrapper.
    pub fn new(inner: P) -> Self {
        Self {
            inner,
            cache: HashMap::new(),
        }
    }

    /// Create a new cached provider wrapper with explicit cache capacity.
    pub fn with_capacity(inner: P, capacity: usize) -> Self {
        Self {
            inner,
            cache: HashMap::with_capacity(capacity),
        }
    }

    /// Access the wrapped provider by reference.
    pub fn inner(&self) -> &P {
        &self.inner
    }

    /// Replace the wrapped provider and invalidate cached dictionary evidence.
    ///
    /// Use this when changing source file/metadata or otherwise retargeting the
    /// provider. Returns the previously wrapped provider.
    pub fn retarget_inner(&mut self, inner: P) -> P {
        self.cache.clear();
        std::mem::replace(&mut self.inner, inner)
    }

    /// Consume this wrapper and return the wrapped provider.
    pub fn into_inner(self) -> P {
        self.inner
    }

    /// Remove all cached dictionary hint evidence.
    pub fn clear_dictionary_cache(&mut self) {
        self.cache.clear();
    }

    /// Number of cached `(row_group_idx, column_idx)` entries.
    pub fn cached_entries(&self) -> usize {
        self.cache.len()
    }
}

impl<P: AsyncBloomFilterProvider> AsyncBloomFilterProvider for CachedDictionaryHintProvider<P> {
    fn bloom_filter(
        &mut self,
        row_group_idx: usize,
        column_idx: usize,
    ) -> impl Future<Output = Option<Sbbf>> + '_ {
        self.inner.bloom_filter(row_group_idx, column_idx)
    }

    fn bloom_filters_batch<'a>(
        &'a mut self,
        requests: &'a [(usize, usize)],
    ) -> impl Future<Output = HashMap<(usize, usize), Sbbf>> + 'a {
        self.inner.bloom_filters_batch(requests)
    }

    fn dictionary_hints(
        &mut self,
        row_group_idx: usize,
        column_idx: usize,
    ) -> impl Future<Output = DictionaryHintEvidence> + '_ {
        async move {
            let key = (row_group_idx, column_idx);
            if let Some(evidence) = self.cache.get(&key) {
                return evidence.clone();
            }

            let evidence = self.inner.dictionary_hints(row_group_idx, column_idx).await;
            if let DictionaryHintEvidence::Exact(_) = &evidence {
                self.cache.insert(key, evidence.clone());
            }
            evidence
        }
    }

    fn dictionary_hints_batch<'a>(
        &'a mut self,
        requests: &'a [(usize, usize)],
    ) -> impl Future<Output = HashMap<(usize, usize), DictionaryHintEvidence>> + 'a {
        async move {
            let mut result = HashMap::with_capacity(requests.len());
            let mut missing = Vec::new();

            for &request in requests {
                if let Some(evidence) = self.cache.get(&request) {
                    result.insert(request, evidence.clone());
                } else {
                    missing.push(request);
                }
            }

            if !missing.is_empty() {
                let loaded = self.inner.dictionary_hints_batch(&missing).await;
                for request in missing {
                    let evidence = loaded
                        .get(&request)
                        .cloned()
                        .unwrap_or(DictionaryHintEvidence::Unknown);
                    if let DictionaryHintEvidence::Exact(_) = &evidence {
                        self.cache.insert(request, evidence.clone());
                    }
                    result.insert(request, evidence);
                }
            }

            result
        }
    }
}

/// Async bloom filter provider trait for custom I/O strategies.
///
/// This trait allows users to implement custom bloom filter loading strategies,
/// including concurrent loading, caching, connection pooling, or batching.
///
/// # Default Implementation
///
/// [`ParquetRecordBatchStreamBuilder`] implements this trait automatically,
/// providing sequential bloom filter loading via the Parquet async API.
///
/// # Custom Implementations
///
/// Advanced users can implement this trait to optimize bloom filter loading
/// for their specific storage backend (S3, GCS, Azure, etc.):
///
/// ```rust,ignore
/// use aisle::AsyncBloomFilterProvider;
/// use parquet::bloom_filter::Sbbf;
/// use std::collections::HashMap;
///
/// struct ConcurrentBloomProvider {
///     // Connection pool, cache, etc.
/// }
///
/// impl AsyncBloomFilterProvider for ConcurrentBloomProvider {
///     async fn bloom_filter(
///         &mut self,
///         row_group_idx: usize,
///         column_idx: usize,
///     ) -> Option<Sbbf> {
///         // Custom logic: check cache, load if needed
///         self.cache.get(&(row_group_idx, column_idx)).cloned()
///     }
///
///     // Override batch method for concurrent loading
///     async fn bloom_filters_batch<'a>(
///         &'a mut self,
///         requests: &'a [(usize, usize)],
///     ) -> HashMap<(usize, usize), Sbbf> {
///         // Load all filters concurrently via get_byte_ranges
///         self.load_batch_concurrent(requests).await
///     }
/// }
/// ```
///
/// # Performance Considerations
///
/// The default implementation loads bloom filters **sequentially** within each
/// row group. For remote storage (S3/GCS), this can be slow. Custom providers
/// can optimize by:
///
/// - **Batching**: Override `bloom_filters_batch` to use `get_byte_ranges`
/// - **Caching**: Store frequently-used bloom filters in memory
/// - **Connection pooling**: Use multiple connections for parallel requests
/// - **Prefetching**: Load bloom filters for upcoming row groups
///
/// # Example: Cached Provider
///
/// ```rust,ignore
/// use std::collections::HashMap;
/// use parquet::bloom_filter::Sbbf;
///
/// struct CachedBloomProvider {
///     reader: AsyncFileReader,
///     cache: HashMap<(usize, usize), Sbbf>,
/// }
///
/// impl AsyncBloomFilterProvider for CachedBloomProvider {
///     async fn bloom_filter(
///         &mut self,
///         row_group_idx: usize,
///         column_idx: usize,
///     ) -> Option<Sbbf> {
///         let key = (row_group_idx, column_idx);
///
///         // Return cached if available
///         if let Some(filter) = self.cache.get(&key) {
///             return Some(filter.clone());
///         }
///
///         // Load and cache
///         if let Some(filter) = self.load_from_reader(key).await {
///             self.cache.insert(key, filter.clone());
///             Some(filter)
///         } else {
///             None
///         }
///     }
/// }
/// ```
pub trait AsyncBloomFilterProvider {
    /// Load the bloom filter for a specific column in a row group.
    ///
    /// Returns `None` if the bloom filter is not available (missing, corrupted,
    /// or I/O error).
    ///
    /// # Parameters
    ///
    /// - `row_group_idx`: Zero-based row group index
    /// - `column_idx`: Zero-based column index in the Parquet schema
    ///
    /// # Returns
    ///
    /// - `Some(Sbbf)`: Successfully loaded bloom filter
    /// - `None`: Bloom filter unavailable (missing or error)
    ///
    /// # Concurrency
    ///
    /// This method takes `&mut self`, so concurrent calls are not possible
    /// at the trait level. Custom providers can implement internal concurrency
    /// using channels, pools, or override `bloom_filters_batch` for batching.
    fn bloom_filter(
        &mut self,
        row_group_idx: usize,
        column_idx: usize,
    ) -> impl Future<Output = Option<Sbbf>> + '_;

    /// Batch bloom filter lookup for multiple (row_group, column) pairs.
    ///
    /// Default implementation executes requests sequentially by calling
    /// `bloom_filter`. **Custom providers should override this method**
    /// to enable concurrent or batched I/O for better performance.
    ///
    /// # Parameters
    ///
    /// - `requests`: Slice of `(row_group_idx, column_idx)` pairs to load
    ///
    /// # Returns
    ///
    /// HashMap mapping `(row_group_idx, column_idx)` to loaded bloom filters.
    /// Missing or failed filters are omitted from the result.
    ///
    /// # Performance
    ///
    /// The default sequential implementation is simple but can be slow for
    /// remote storage. Override this method to:
    ///
    /// - Use `AsyncFileReader::get_byte_ranges` for single HTTP request
    /// - Load filters concurrently via connection pool
    /// - Implement request coalescing or deduplication
    ///
    /// # Example: Batched Implementation
    ///
    /// ```rust,ignore
    /// async fn bloom_filters_batch<'a>(
    ///     &'a mut self,
    ///     requests: &'a [(usize, usize)],
    /// ) -> HashMap<(usize, usize), Sbbf> {
    ///     // Collect byte ranges for all bloom filters
    ///     let ranges: Vec<Range<u64>> = requests
    ///         .iter()
    ///         .map(|&(rg, col)| self.get_bloom_range(rg, col))
    ///         .collect();
    ///
    ///     // Single batched HTTP request
    ///     let bytes_vec = self.reader.get_byte_ranges(ranges).await.unwrap();
    ///
    ///     // Parse and return
    ///     requests.iter()
    ///         .zip(bytes_vec)
    ///         .filter_map(|(key, bytes)| {
    ///             Sbbf::from_bytes(&bytes).ok().map(|f| (*key, f))
    ///         })
    ///         .collect()
    /// }
    /// ```
    fn bloom_filters_batch<'a>(
        &'a mut self,
        requests: &'a [(usize, usize)],
    ) -> impl Future<Output = HashMap<(usize, usize), Sbbf>> + 'a {
        async move {
            let mut result = HashMap::new();
            for &(row_group_idx, column_idx) in requests {
                if let Some(filter) = self.bloom_filter(row_group_idx, column_idx).await {
                    result.insert((row_group_idx, column_idx), filter);
                }
            }
            result
        }
    }

    /// Load dictionary hint evidence for a specific column in a row group.
    ///
    /// # Correctness Contract
    ///
    /// Return [`DictionaryHintEvidence::Exact`] **only** when the returned set is
    /// complete for that `(row_group_idx, column_idx)` pair.
    ///
    /// If values are partial, sampled, stale, or unavailable, return
    /// [`DictionaryHintEvidence::Unknown`]. This ensures Aisle remains conservative
    /// and does not prune matching data.
    fn dictionary_hints(
        &mut self,
        _row_group_idx: usize,
        _column_idx: usize,
    ) -> impl Future<Output = DictionaryHintEvidence> + '_ {
        async { DictionaryHintEvidence::Unknown }
    }

    /// Batch dictionary hint evidence lookup for multiple (row_group, column) pairs.
    ///
    /// Default implementation executes requests sequentially by calling
    /// [`dictionary_hints`](Self::dictionary_hints).
    fn dictionary_hints_batch<'a>(
        &'a mut self,
        requests: &'a [(usize, usize)],
    ) -> impl Future<Output = HashMap<(usize, usize), DictionaryHintEvidence>> + 'a {
        async move {
            let mut result = HashMap::new();
            for &(row_group_idx, column_idx) in requests {
                let hints = self.dictionary_hints(row_group_idx, column_idx).await;
                result.insert((row_group_idx, column_idx), hints);
            }
            result
        }
    }
}

impl<T: AsyncFileReader + Send + 'static> AsyncBloomFilterProvider
    for ParquetRecordBatchStreamBuilder<T>
{
    fn bloom_filter(
        &mut self,
        row_group_idx: usize,
        column_idx: usize,
    ) -> impl Future<Output = Option<Sbbf>> + '_ {
        async move {
            match self
                .get_row_group_column_bloom_filter(row_group_idx, column_idx)
                .await
            {
                Ok(Some(filter)) => Some(filter),
                _ => None,
            }
        }
    }
}
