use std::{
    collections::{HashMap, HashSet},
    path::Path,
    sync::Arc,
    time::Duration,
};

use aisle::{
    AsyncBloomFilterProvider, CachedDictionaryHintProvider, DictionaryHintEvidence,
    DictionaryHintValue, PruneRequest, PruneResult,
};
use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use datafusion_expr::{Expr, col, lit};
use futures::{StreamExt, lock::Mutex as AsyncMutex};
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, RowSelection};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::bloom_filter::Sbbf;
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use rand::{SeedableRng, seq::SliceRandom};

#[derive(Debug, Default)]
struct ScanStats {
    rows_read: usize,
    rows_matched: usize,
}

#[derive(Debug)]
struct ScenarioMetrics {
    row_groups_kept: usize,
    total_row_groups: usize,
    pages_kept: usize,
    total_pages: usize,
    rows_read: usize,
    rows_matched: usize,
    total_rows: usize,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum ProviderMode {
    NoCache,
    BatchOnly,
    BatchPlusCache,
}

impl ProviderMode {
    const ALL: [Self; 3] = [Self::NoCache, Self::BatchOnly, Self::BatchPlusCache];

    fn label(self) -> &'static str {
        match self {
            Self::NoCache => "no cache",
            Self::BatchOnly => "batch only",
            Self::BatchPlusCache => "batch + cache",
        }
    }

    fn metadata_bench_id(self) -> &'static str {
        match self {
            // Keep the original benchmark id stable for the "no cache" mode.
            Self::NoCache => "aisle_metadata_only_candidate",
            Self::BatchOnly => "aisle_metadata_only_batch_only",
            Self::BatchPlusCache => "aisle_metadata_only_batch_plus_cache",
        }
    }

    fn prune_scan_bench_id(self) -> &'static str {
        match self {
            // Keep the original benchmark id stable for the "no cache" mode.
            Self::NoCache => "aisle_prune_plus_scan_candidate",
            Self::BatchOnly => "aisle_prune_plus_scan_batch_only",
            Self::BatchPlusCache => "aisle_prune_plus_scan_batch_plus_cache",
        }
    }

    fn remote_metadata_bench_id(self) -> &'static str {
        match self {
            Self::NoCache => "aisle_metadata_only_remote_no_cache",
            Self::BatchOnly => "aisle_metadata_only_remote_batch_only",
            Self::BatchPlusCache => "aisle_metadata_only_remote_batch_plus_cache",
        }
    }

    fn remote_prune_scan_bench_id(self) -> &'static str {
        match self {
            Self::NoCache => "aisle_prune_plus_scan_remote_no_cache",
            Self::BatchOnly => "aisle_prune_plus_scan_remote_batch_only",
            Self::BatchPlusCache => "aisle_prune_plus_scan_remote_batch_plus_cache",
        }
    }
}

const REMOTE_METADATA_BATCH_PLUS_CACHE_COLD_ID: &str =
    "aisle_metadata_only_remote_batch_plus_cache_cold";
const REMOTE_PRUNE_SCAN_BATCH_PLUS_CACHE_COLD_ID: &str =
    "aisle_prune_plus_scan_remote_batch_plus_cache_cold";

fn count_matches(batch: &RecordBatch, target_key: &str) -> usize {
    let array = batch
        .column(1)
        .as_any()
        .downcast_ref::<StringArray>()
        .expect("key column should be Utf8");

    (0..array.len())
        .filter(|&i| !array.is_null(i) && array.value(i) == target_key)
        .count()
}

async fn scan_builder<T: AsyncFileReader + Unpin + Send + 'static>(
    builder: parquet::arrow::ParquetRecordBatchStreamBuilder<T>,
    row_groups: &[usize],
    row_selection: Option<RowSelection>,
    target_key: &str,
) -> ScanStats {
    let mut stats = ScanStats::default();
    let mut builder = builder.with_row_groups(row_groups.to_vec());

    if let Some(selection) = row_selection {
        builder = builder.with_row_selection(selection);
    }

    let mut stream = builder.build().expect("parquet stream");

    while let Some(batch) = stream.next().await {
        let batch = batch.expect("record batch");
        stats.rows_read += batch.num_rows();
        stats.rows_matched += count_matches(&batch, target_key);
    }

    stats
}

async fn scan_file_async(
    path: &Path,
    row_groups: &[usize],
    row_selection: Option<RowSelection>,
    target_key: &str,
    enable_page_index: bool,
) -> ScanStats {
    let file = tokio::fs::File::open(path).await.expect("open parquet");
    let options = ArrowReaderOptions::new().with_page_index(enable_page_index);
    let builder = parquet::arrow::ParquetRecordBatchStreamBuilder::new_with_options(file, options)
        .await
        .expect("stream builder");
    scan_builder(builder, row_groups, row_selection, target_key).await
}

fn pages_for_row_groups(
    metadata: &ParquetMetaData,
    row_groups: &[usize],
    column_idx: usize,
) -> usize {
    let Some(offset_indexes) = metadata.offset_index() else {
        return 0;
    };

    row_groups
        .iter()
        .filter_map(|&row_group_idx| offset_indexes.get(row_group_idx))
        .filter_map(|columns| columns.get(column_idx))
        .map(|offset_index| offset_index.page_locations().len())
        .sum()
}

fn create_dictionary_candidate_data(
    row_groups: usize,
    rows_per_group: usize,
) -> (
    tempfile::NamedTempFile,
    ParquetMetaData,
    Arc<Schema>,
    String,
    HashMap<(usize, usize), HashSet<DictionaryHintValue>>,
) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("key", DataType::Utf8, false),
    ]));

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_dictionary_enabled(true)
        .set_max_row_group_size(rows_per_group)
        .set_data_page_row_count_limit(512)
        .build();

    let target_key = "target_07".to_string();
    let common_values = [
        "common_alpha",
        "common_beta",
        "common_gamma",
        "common_delta",
        "common_theta",
        "common_lambda",
    ];

    let temp_file = tempfile::NamedTempFile::new().expect("temp file");
    let file = std::fs::File::create(temp_file.path()).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).expect("arrow writer");
    let mut dictionary_hints: HashMap<(usize, usize), HashSet<DictionaryHintValue>> =
        HashMap::new();

    let mut rng = rand::rngs::StdRng::seed_from_u64(7);

    for row_group_idx in 0..row_groups {
        let mut values = Vec::with_capacity(rows_per_group);
        values.push("a_anchor".to_string());
        values.push("z_anchor".to_string());

        let has_target = row_group_idx % 16 == 7;
        let target_repeats = if has_target { rows_per_group / 8 } else { 0 };
        for _ in 0..target_repeats {
            values.push(target_key.clone());
        }

        while values.len() < rows_per_group {
            let next_common = common_values[(values.len() + row_group_idx) % common_values.len()];
            values.push(next_common.to_string());
        }

        let hint_values: HashSet<DictionaryHintValue> = values
            .iter()
            .cloned()
            .map(DictionaryHintValue::Utf8)
            .collect();
        dictionary_hints.insert((row_group_idx, 1), hint_values);

        let mut ids: Vec<i64> = (0..rows_per_group)
            .map(|i| (row_group_idx * rows_per_group + i) as i64)
            .collect();

        let mut zipped: Vec<(i64, String)> = ids.drain(..).zip(values.into_iter()).collect();
        zipped.shuffle(&mut rng);
        let (ids, values): (Vec<i64>, Vec<String>) = zipped.into_iter().unzip();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(values)),
            ],
        )
        .expect("record batch");

        writer.write(&batch).expect("write row group");
    }

    writer.close().expect("close parquet writer");

    let bytes = std::fs::read(temp_file.path()).expect("read parquet bytes");
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .parse_and_finish(&Bytes::from(bytes))
        .expect("read metadata");

    (temp_file, metadata, schema, target_key, dictionary_hints)
}

fn create_dictionary_remote_sim_data(
    row_groups: usize,
    rows_per_group: usize,
    key_columns: usize,
) -> (
    tempfile::NamedTempFile,
    ParquetMetaData,
    Arc<Schema>,
    String,
    Vec<String>,
    HashMap<(usize, usize), HashSet<DictionaryHintValue>>,
) {
    let key_names: Vec<String> = (0..key_columns).map(|i| format!("key_{i:02}")).collect();
    let mut fields = vec![Field::new("id", DataType::Int64, false)];
    for key_name in &key_names {
        fields.push(Field::new(key_name, DataType::Utf8, false));
    }
    let schema = Arc::new(Schema::new(fields));

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_dictionary_enabled(true)
        .set_max_row_group_size(rows_per_group)
        .set_data_page_row_count_limit(512)
        .build();

    let target_key = "target_remote_07".to_string();
    let common_values = [
        "common_alpha",
        "common_beta",
        "common_gamma",
        "common_delta",
        "common_theta",
        "common_lambda",
    ];

    let temp_file = tempfile::NamedTempFile::new().expect("temp file");
    let file = std::fs::File::create(temp_file.path()).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).expect("arrow writer");
    let mut dictionary_hints: HashMap<(usize, usize), HashSet<DictionaryHintValue>> =
        HashMap::new();

    let mut rng = rand::rngs::StdRng::seed_from_u64(17);

    for row_group_idx in 0..row_groups {
        let mut values = Vec::with_capacity(rows_per_group);
        values.push("a_anchor".to_string());
        values.push("z_anchor".to_string());

        let has_target = row_group_idx % 8 == 3;
        let target_repeats = if has_target { rows_per_group / 8 } else { 0 };
        for _ in 0..target_repeats {
            values.push(target_key.clone());
        }

        while values.len() < rows_per_group {
            let next_common = common_values[(values.len() + row_group_idx) % common_values.len()];
            values.push(next_common.to_string());
        }

        let hint_values: HashSet<DictionaryHintValue> = values
            .iter()
            .cloned()
            .map(DictionaryHintValue::Utf8)
            .collect();

        let mut ids: Vec<i64> = (0..rows_per_group)
            .map(|i| (row_group_idx * rows_per_group + i) as i64)
            .collect();

        let mut zipped: Vec<(i64, String)> = ids.drain(..).zip(values.into_iter()).collect();
        zipped.shuffle(&mut rng);
        let (ids, values): (Vec<i64>, Vec<String>) = zipped.into_iter().unzip();

        let mut columns = Vec::with_capacity(1 + key_columns);
        columns.push(Arc::new(Int64Array::from(ids)) as Arc<dyn Array>);
        for key_col_idx in 0..key_columns {
            columns.push(Arc::new(StringArray::from(values.clone())) as Arc<dyn Array>);
            dictionary_hints.insert((row_group_idx, key_col_idx + 1), hint_values.clone());
        }

        let batch = RecordBatch::try_new(schema.clone(), columns).expect("record batch");
        writer.write(&batch).expect("write row group");
    }

    writer.close().expect("close parquet writer");

    let bytes = std::fs::read(temp_file.path()).expect("read parquet bytes");
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .parse_and_finish(&Bytes::from(bytes))
        .expect("read metadata");

    (
        temp_file,
        metadata,
        schema,
        target_key,
        key_names,
        dictionary_hints,
    )
}

fn build_all_columns_eq_predicate(key_columns: &[String], target_key: &str) -> Expr {
    let mut expr = col(&key_columns[0]).eq(lit(target_key.to_string()));
    for key_column in &key_columns[1..] {
        expr = expr.and(col(key_column).eq(lit(target_key.to_string())));
    }
    expr
}

#[derive(Clone)]
struct NoCacheDictionaryHintProvider {
    hints: Arc<HashMap<(usize, usize), HashSet<DictionaryHintValue>>>,
}

impl NoCacheDictionaryHintProvider {
    fn new(hints: Arc<HashMap<(usize, usize), HashSet<DictionaryHintValue>>>) -> Self {
        Self { hints }
    }
}

impl AsyncBloomFilterProvider for NoCacheDictionaryHintProvider {
    async fn bloom_filter(&mut self, _row_group_idx: usize, _column_idx: usize) -> Option<Sbbf> {
        None
    }

    async fn dictionary_hints(
        &mut self,
        row_group_idx: usize,
        column_idx: usize,
    ) -> DictionaryHintEvidence {
        match self.hints.get(&(row_group_idx, column_idx)) {
            Some(values) => DictionaryHintEvidence::Exact(values.clone()),
            None => DictionaryHintEvidence::Unknown,
        }
    }
}

#[derive(Clone)]
struct BatchOnlyDictionaryHintProvider {
    hints: Arc<HashMap<(usize, usize), HashSet<DictionaryHintValue>>>,
}

impl BatchOnlyDictionaryHintProvider {
    fn new(hints: Arc<HashMap<(usize, usize), HashSet<DictionaryHintValue>>>) -> Self {
        Self { hints }
    }
}

impl AsyncBloomFilterProvider for BatchOnlyDictionaryHintProvider {
    async fn bloom_filter(&mut self, _row_group_idx: usize, _column_idx: usize) -> Option<Sbbf> {
        None
    }

    async fn dictionary_hints(
        &mut self,
        row_group_idx: usize,
        column_idx: usize,
    ) -> DictionaryHintEvidence {
        match self.hints.get(&(row_group_idx, column_idx)) {
            Some(values) => DictionaryHintEvidence::Exact(values.clone()),
            None => DictionaryHintEvidence::Unknown,
        }
    }

    async fn dictionary_hints_batch<'a>(
        &'a mut self,
        requests: &'a [(usize, usize)],
    ) -> HashMap<(usize, usize), DictionaryHintEvidence> {
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

#[derive(Clone, Copy)]
struct SimulatedRemoteLatency {
    base_micros: u64,
    per_item_micros: u64,
}

impl SimulatedRemoteLatency {
    fn single_delay(self) -> Duration {
        Duration::from_micros(self.base_micros + self.per_item_micros)
    }

    fn batch_delay(self, item_count: usize) -> Duration {
        Duration::from_micros(
            self.base_micros
                .saturating_add(self.per_item_micros.saturating_mul(item_count as u64)),
        )
    }
}

#[derive(Clone)]
struct SimNoCacheDictionaryHintProvider {
    hints: Arc<HashMap<(usize, usize), HashSet<DictionaryHintValue>>>,
    latency: SimulatedRemoteLatency,
}

impl SimNoCacheDictionaryHintProvider {
    fn new(
        hints: Arc<HashMap<(usize, usize), HashSet<DictionaryHintValue>>>,
        latency: SimulatedRemoteLatency,
    ) -> Self {
        Self { hints, latency }
    }
}

impl AsyncBloomFilterProvider for SimNoCacheDictionaryHintProvider {
    async fn bloom_filter(&mut self, _row_group_idx: usize, _column_idx: usize) -> Option<Sbbf> {
        None
    }

    async fn dictionary_hints(
        &mut self,
        row_group_idx: usize,
        column_idx: usize,
    ) -> DictionaryHintEvidence {
        tokio::time::sleep(self.latency.single_delay()).await;
        match self.hints.get(&(row_group_idx, column_idx)) {
            Some(values) => DictionaryHintEvidence::Exact(values.clone()),
            None => DictionaryHintEvidence::Unknown,
        }
    }
}

#[derive(Clone)]
struct SimBatchOnlyDictionaryHintProvider {
    hints: Arc<HashMap<(usize, usize), HashSet<DictionaryHintValue>>>,
    latency: SimulatedRemoteLatency,
}

impl SimBatchOnlyDictionaryHintProvider {
    fn new(
        hints: Arc<HashMap<(usize, usize), HashSet<DictionaryHintValue>>>,
        latency: SimulatedRemoteLatency,
    ) -> Self {
        Self { hints, latency }
    }
}

impl AsyncBloomFilterProvider for SimBatchOnlyDictionaryHintProvider {
    async fn bloom_filter(&mut self, _row_group_idx: usize, _column_idx: usize) -> Option<Sbbf> {
        None
    }

    async fn dictionary_hints(
        &mut self,
        row_group_idx: usize,
        column_idx: usize,
    ) -> DictionaryHintEvidence {
        tokio::time::sleep(self.latency.single_delay()).await;
        match self.hints.get(&(row_group_idx, column_idx)) {
            Some(values) => DictionaryHintEvidence::Exact(values.clone()),
            None => DictionaryHintEvidence::Unknown,
        }
    }

    async fn dictionary_hints_batch<'a>(
        &'a mut self,
        requests: &'a [(usize, usize)],
    ) -> HashMap<(usize, usize), DictionaryHintEvidence> {
        tokio::time::sleep(self.latency.batch_delay(requests.len())).await;
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

async fn prune_with_provider<P: AsyncBloomFilterProvider>(
    metadata: &ParquetMetaData,
    schema: &Arc<Schema>,
    predicate: &Expr,
    provider: &mut P,
) -> PruneResult {
    PruneRequest::new(metadata, schema)
        .with_df_predicate(predicate)
        .enable_page_index(false)
        .enable_bloom_filter(false)
        .enable_dictionary_hints(true)
        .prune_async(provider)
        .await
}

fn evaluate_metrics_with_provider<P: AsyncBloomFilterProvider>(
    runtime: &tokio::runtime::Runtime,
    path: &Path,
    metadata: &ParquetMetaData,
    schema: &Arc<Schema>,
    predicate: &Expr,
    target_key: &str,
    mut provider: P,
) -> ScenarioMetrics {
    let result = runtime.block_on(prune_with_provider(
        metadata,
        schema,
        predicate,
        &mut provider,
    ));

    let kept_row_groups = result.row_groups().to_vec();
    let all_row_groups: Vec<usize> = (0..metadata.num_row_groups()).collect();
    let total_rows = metadata.num_row_groups() * metadata.row_group(0).num_rows() as usize;

    let stats = runtime.block_on(scan_file_async(
        path,
        &kept_row_groups,
        None,
        target_key,
        false,
    ));

    ScenarioMetrics {
        row_groups_kept: kept_row_groups.len(),
        total_row_groups: metadata.num_row_groups(),
        pages_kept: pages_for_row_groups(metadata, &kept_row_groups, 1),
        total_pages: pages_for_row_groups(metadata, &all_row_groups, 1),
        rows_read: stats.rows_read,
        rows_matched: stats.rows_matched,
        total_rows,
    }
}

fn bench_dictionary_hints_candidate(c: &mut Criterion) {
    let row_groups = 64;
    let rows_per_group = 2_048;
    let (temp_file, metadata, schema, target_key, dictionary_hints) =
        create_dictionary_candidate_data(row_groups, rows_per_group);
    let dictionary_hints = Arc::new(dictionary_hints);

    let predicate = col("key").eq(lit(target_key.clone()));
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");

    for mode in ProviderMode::ALL {
        let metrics = match mode {
            ProviderMode::NoCache => evaluate_metrics_with_provider(
                &runtime,
                temp_file.path(),
                &metadata,
                &schema,
                &predicate,
                &target_key,
                NoCacheDictionaryHintProvider::new(Arc::clone(&dictionary_hints)),
            ),
            ProviderMode::BatchOnly => evaluate_metrics_with_provider(
                &runtime,
                temp_file.path(),
                &metadata,
                &schema,
                &predicate,
                &target_key,
                BatchOnlyDictionaryHintProvider::new(Arc::clone(&dictionary_hints)),
            ),
            ProviderMode::BatchPlusCache => evaluate_metrics_with_provider(
                &runtime,
                temp_file.path(),
                &metadata,
                &schema,
                &predicate,
                &target_key,
                CachedDictionaryHintProvider::with_capacity(
                    BatchOnlyDictionaryHintProvider::new(Arc::clone(&dictionary_hints)),
                    metadata.num_row_groups(),
                ),
            ),
        };

        println!(
            "Dictionary-hints candidate ({}) : query=key = '{}', row_groups_kept={}/{}, pages_kept={}/{}, rows_read={}, rows_matched={}, decode_proxy={:.1}%",
            mode.label(),
            target_key,
            metrics.row_groups_kept,
            metrics.total_row_groups,
            metrics.pages_kept,
            metrics.total_pages,
            metrics.rows_read,
            metrics.rows_matched,
            (metrics.rows_read as f64 / metrics.total_rows as f64) * 100.0
        );
    }

    let mut group = c.benchmark_group("dictionary_hints_candidate");

    group.bench_function(ProviderMode::NoCache.metadata_bench_id(), |b| {
        b.to_async(&runtime).iter(|| async {
            let mut provider = NoCacheDictionaryHintProvider::new(Arc::clone(&dictionary_hints));
            let result = prune_with_provider(
                black_box(&metadata),
                black_box(&schema),
                black_box(&predicate),
                &mut provider,
            )
            .await;
            black_box(result.row_groups());
        });
    });

    group.bench_function(ProviderMode::NoCache.prune_scan_bench_id(), |b| {
        b.to_async(&runtime).iter(|| async {
            let mut provider = NoCacheDictionaryHintProvider::new(Arc::clone(&dictionary_hints));
            let result = prune_with_provider(
                black_box(&metadata),
                black_box(&schema),
                black_box(&predicate),
                &mut provider,
            )
            .await;
            let stats = scan_file_async(
                temp_file.path(),
                result.row_groups(),
                None,
                &target_key,
                false,
            )
            .await;
            black_box(stats);
        });
    });

    group.bench_function(ProviderMode::BatchOnly.metadata_bench_id(), |b| {
        b.to_async(&runtime).iter(|| async {
            let mut provider = BatchOnlyDictionaryHintProvider::new(Arc::clone(&dictionary_hints));
            let result = prune_with_provider(
                black_box(&metadata),
                black_box(&schema),
                black_box(&predicate),
                &mut provider,
            )
            .await;
            black_box(result.row_groups());
        });
    });

    group.bench_function(ProviderMode::BatchOnly.prune_scan_bench_id(), |b| {
        b.to_async(&runtime).iter(|| async {
            let mut provider = BatchOnlyDictionaryHintProvider::new(Arc::clone(&dictionary_hints));
            let result = prune_with_provider(
                black_box(&metadata),
                black_box(&schema),
                black_box(&predicate),
                &mut provider,
            )
            .await;
            let stats = scan_file_async(
                temp_file.path(),
                result.row_groups(),
                None,
                &target_key,
                false,
            )
            .await;
            black_box(stats);
        });
    });

    let metadata_cached_provider = Arc::new(AsyncMutex::new(
        CachedDictionaryHintProvider::with_capacity(
            BatchOnlyDictionaryHintProvider::new(Arc::clone(&dictionary_hints)),
            metadata.num_row_groups(),
        ),
    ));
    runtime.block_on(async {
        let mut provider = metadata_cached_provider.lock().await;
        let _ = prune_with_provider(&metadata, &schema, &predicate, &mut *provider).await;
    });
    group.bench_function(ProviderMode::BatchPlusCache.metadata_bench_id(), |b| {
        let provider = Arc::clone(&metadata_cached_provider);
        let metadata = &metadata;
        let schema = &schema;
        let predicate = &predicate;
        b.to_async(&runtime).iter(|| {
            let provider = Arc::clone(&provider);
            async move {
                let mut provider = provider.lock().await;
                let result = prune_with_provider(
                    black_box(metadata),
                    black_box(schema),
                    black_box(predicate),
                    &mut *provider,
                )
                .await;
                black_box(result.row_groups());
            }
        });
    });

    let scan_cached_provider = Arc::new(AsyncMutex::new(
        CachedDictionaryHintProvider::with_capacity(
            BatchOnlyDictionaryHintProvider::new(Arc::clone(&dictionary_hints)),
            metadata.num_row_groups(),
        ),
    ));
    runtime.block_on(async {
        let mut provider = scan_cached_provider.lock().await;
        let _ = prune_with_provider(&metadata, &schema, &predicate, &mut *provider).await;
    });
    group.bench_function(ProviderMode::BatchPlusCache.prune_scan_bench_id(), |b| {
        let provider = Arc::clone(&scan_cached_provider);
        let metadata = &metadata;
        let schema = &schema;
        let predicate = &predicate;
        let target_key = &target_key;
        let path = temp_file.path().to_path_buf();
        b.to_async(&runtime).iter(|| {
            let provider = Arc::clone(&provider);
            let path = path.clone();
            async move {
                let mut provider = provider.lock().await;
                let result = prune_with_provider(
                    black_box(metadata),
                    black_box(schema),
                    black_box(predicate),
                    &mut *provider,
                )
                .await;
                let stats =
                    scan_file_async(&path, result.row_groups(), None, target_key, false).await;
                black_box(stats);
            }
        });
    });

    group.finish();
}

fn bench_dictionary_hints_remote_sim(c: &mut Criterion) {
    let row_groups = 24;
    let rows_per_group = 1_024;
    let key_columns = 8;
    let (temp_file, metadata, schema, target_key, key_names, dictionary_hints) =
        create_dictionary_remote_sim_data(row_groups, rows_per_group, key_columns);
    let dictionary_hints = Arc::new(dictionary_hints);

    let predicate = build_all_columns_eq_predicate(&key_names, &target_key);
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let latency = SimulatedRemoteLatency {
        base_micros: 200,
        per_item_micros: 30,
    };

    for mode in ProviderMode::ALL {
        let metrics = match mode {
            ProviderMode::NoCache => evaluate_metrics_with_provider(
                &runtime,
                temp_file.path(),
                &metadata,
                &schema,
                &predicate,
                &target_key,
                SimNoCacheDictionaryHintProvider::new(Arc::clone(&dictionary_hints), latency),
            ),
            ProviderMode::BatchOnly => evaluate_metrics_with_provider(
                &runtime,
                temp_file.path(),
                &metadata,
                &schema,
                &predicate,
                &target_key,
                SimBatchOnlyDictionaryHintProvider::new(Arc::clone(&dictionary_hints), latency),
            ),
            ProviderMode::BatchPlusCache => evaluate_metrics_with_provider(
                &runtime,
                temp_file.path(),
                &metadata,
                &schema,
                &predicate,
                &target_key,
                CachedDictionaryHintProvider::with_capacity(
                    SimBatchOnlyDictionaryHintProvider::new(Arc::clone(&dictionary_hints), latency),
                    metadata.num_row_groups() * key_columns,
                ),
            ),
        };

        println!(
            "Dictionary-hints remote-sim ({}) : query=ALL_KEYS = '{}', row_groups_kept={}/{}, pages_kept={}/{}, rows_read={}, rows_matched={}, decode_proxy={:.1}%",
            mode.label(),
            target_key,
            metrics.row_groups_kept,
            metrics.total_row_groups,
            metrics.pages_kept,
            metrics.total_pages,
            metrics.rows_read,
            metrics.rows_matched,
            (metrics.rows_read as f64 / metrics.total_rows as f64) * 100.0
        );
    }

    let mut group = c.benchmark_group("dictionary_hints_remote_sim");

    group.bench_function(ProviderMode::NoCache.remote_metadata_bench_id(), |b| {
        b.to_async(&runtime).iter(|| async {
            let mut provider =
                SimNoCacheDictionaryHintProvider::new(Arc::clone(&dictionary_hints), latency);
            let result = prune_with_provider(
                black_box(&metadata),
                black_box(&schema),
                black_box(&predicate),
                &mut provider,
            )
            .await;
            black_box(result.row_groups());
        });
    });

    group.bench_function(ProviderMode::NoCache.remote_prune_scan_bench_id(), |b| {
        b.to_async(&runtime).iter(|| async {
            let mut provider =
                SimNoCacheDictionaryHintProvider::new(Arc::clone(&dictionary_hints), latency);
            let result = prune_with_provider(
                black_box(&metadata),
                black_box(&schema),
                black_box(&predicate),
                &mut provider,
            )
            .await;
            let stats = scan_file_async(
                temp_file.path(),
                result.row_groups(),
                None,
                &target_key,
                false,
            )
            .await;
            black_box(stats);
        });
    });

    group.bench_function(ProviderMode::BatchOnly.remote_metadata_bench_id(), |b| {
        b.to_async(&runtime).iter(|| async {
            let mut provider =
                SimBatchOnlyDictionaryHintProvider::new(Arc::clone(&dictionary_hints), latency);
            let result = prune_with_provider(
                black_box(&metadata),
                black_box(&schema),
                black_box(&predicate),
                &mut provider,
            )
            .await;
            black_box(result.row_groups());
        });
    });

    group.bench_function(ProviderMode::BatchOnly.remote_prune_scan_bench_id(), |b| {
        b.to_async(&runtime).iter(|| async {
            let mut provider =
                SimBatchOnlyDictionaryHintProvider::new(Arc::clone(&dictionary_hints), latency);
            let result = prune_with_provider(
                black_box(&metadata),
                black_box(&schema),
                black_box(&predicate),
                &mut provider,
            )
            .await;
            let stats = scan_file_async(
                temp_file.path(),
                result.row_groups(),
                None,
                &target_key,
                false,
            )
            .await;
            black_box(stats);
        });
    });

    let metadata_cached_provider = Arc::new(AsyncMutex::new(
        CachedDictionaryHintProvider::with_capacity(
            SimBatchOnlyDictionaryHintProvider::new(Arc::clone(&dictionary_hints), latency),
            metadata.num_row_groups() * key_columns,
        ),
    ));
    runtime.block_on(async {
        let mut provider = metadata_cached_provider.lock().await;
        let _ = prune_with_provider(&metadata, &schema, &predicate, &mut *provider).await;
    });
    group.bench_function(
        ProviderMode::BatchPlusCache.remote_metadata_bench_id(),
        |b| {
            let provider = Arc::clone(&metadata_cached_provider);
            let metadata = &metadata;
            let schema = &schema;
            let predicate = &predicate;
            b.to_async(&runtime).iter(|| {
                let provider = Arc::clone(&provider);
                async move {
                    let mut provider = provider.lock().await;
                    let result = prune_with_provider(
                        black_box(metadata),
                        black_box(schema),
                        black_box(predicate),
                        &mut *provider,
                    )
                    .await;
                    black_box(result.row_groups());
                }
            });
        },
    );

    let scan_cached_provider = Arc::new(AsyncMutex::new(
        CachedDictionaryHintProvider::with_capacity(
            SimBatchOnlyDictionaryHintProvider::new(Arc::clone(&dictionary_hints), latency),
            metadata.num_row_groups() * key_columns,
        ),
    ));
    runtime.block_on(async {
        let mut provider = scan_cached_provider.lock().await;
        let _ = prune_with_provider(&metadata, &schema, &predicate, &mut *provider).await;
    });
    group.bench_function(
        ProviderMode::BatchPlusCache.remote_prune_scan_bench_id(),
        |b| {
            let provider = Arc::clone(&scan_cached_provider);
            let metadata = &metadata;
            let schema = &schema;
            let predicate = &predicate;
            let target_key = &target_key;
            let path = temp_file.path().to_path_buf();
            b.to_async(&runtime).iter(|| {
                let provider = Arc::clone(&provider);
                let path = path.clone();
                async move {
                    let mut provider = provider.lock().await;
                    let result = prune_with_provider(
                        black_box(metadata),
                        black_box(schema),
                        black_box(predicate),
                        &mut *provider,
                    )
                    .await;
                    let stats =
                        scan_file_async(&path, result.row_groups(), None, target_key, false).await;
                    black_box(stats);
                }
            });
        },
    );

    group.bench_function(REMOTE_METADATA_BATCH_PLUS_CACHE_COLD_ID, |b| {
        b.to_async(&runtime).iter(|| async {
            let mut provider = CachedDictionaryHintProvider::with_capacity(
                SimBatchOnlyDictionaryHintProvider::new(Arc::clone(&dictionary_hints), latency),
                metadata.num_row_groups() * key_columns,
            );
            let result = prune_with_provider(
                black_box(&metadata),
                black_box(&schema),
                black_box(&predicate),
                &mut provider,
            )
            .await;
            black_box(result.row_groups());
        });
    });

    group.bench_function(REMOTE_PRUNE_SCAN_BATCH_PLUS_CACHE_COLD_ID, |b| {
        b.to_async(&runtime).iter(|| async {
            let mut provider = CachedDictionaryHintProvider::with_capacity(
                SimBatchOnlyDictionaryHintProvider::new(Arc::clone(&dictionary_hints), latency),
                metadata.num_row_groups() * key_columns,
            );
            let result = prune_with_provider(
                black_box(&metadata),
                black_box(&schema),
                black_box(&predicate),
                &mut provider,
            )
            .await;
            let stats = scan_file_async(
                temp_file.path(),
                result.row_groups(),
                None,
                &target_key,
                false,
            )
            .await;
            black_box(stats);
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    bench_dictionary_hints_candidate,
    bench_dictionary_hints_remote_sim
);
criterion_main!(benches);
