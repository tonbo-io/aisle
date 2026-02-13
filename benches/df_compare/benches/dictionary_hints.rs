use std::path::Path;
use std::sync::Arc;
use std::collections::{HashMap, HashSet};

use aisle::{AsyncBloomFilterProvider, DictionaryHintValue, PruneRequest};
use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use datafusion_expr::{Expr, col, lit};
use futures::StreamExt;
use parquet::bloom_filter::Sbbf;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, RowSelection};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::{ArrowWriter, ParquetRecordBatchStreamBuilder};
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
    builder: ParquetRecordBatchStreamBuilder<T>,
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
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(file, options)
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

#[derive(Clone)]
struct StaticDictionaryHintProvider {
    hints: Arc<HashMap<(usize, usize), HashSet<DictionaryHintValue>>>,
}

impl StaticDictionaryHintProvider {
    fn new(hints: HashMap<(usize, usize), HashSet<DictionaryHintValue>>) -> Self {
        Self {
            hints: Arc::new(hints),
        }
    }
}

impl AsyncBloomFilterProvider for StaticDictionaryHintProvider {
    async fn bloom_filter(&mut self, _row_group_idx: usize, _column_idx: usize) -> Option<Sbbf> {
        None
    }

    async fn dictionary_hints(
        &mut self,
        row_group_idx: usize,
        column_idx: usize,
    ) -> Option<HashSet<DictionaryHintValue>> {
        self.hints.get(&(row_group_idx, column_idx)).cloned()
    }
}

fn evaluate_metrics(
    runtime: &tokio::runtime::Runtime,
    path: &Path,
    metadata: &ParquetMetaData,
    schema: &Arc<Schema>,
    predicate: &Expr,
    target_key: &str,
    provider: StaticDictionaryHintProvider,
) -> ScenarioMetrics {
    let mut provider = provider;
    let result = runtime.block_on(async {
        PruneRequest::new(metadata, schema)
            .with_df_predicate(predicate)
            .enable_page_index(false)
            .enable_bloom_filter(false)
            .enable_dictionary_hints(true)
            .prune_async(&mut provider)
            .await
    });

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
    let provider_template = StaticDictionaryHintProvider::new(dictionary_hints);

    let predicate = col("key").eq(lit(target_key.clone()));
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");

    let metrics = evaluate_metrics(
        &runtime,
        temp_file.path(),
        &metadata,
        &schema,
        &predicate,
        &target_key,
        provider_template.clone(),
    );

    println!(
        "Dictionary-hints candidate (enabled): query=key = '{}', row_groups_kept={}/{}, pages_kept={}/{}, rows_read={}, rows_matched={}, decode_proxy={:.1}%",
        target_key,
        metrics.row_groups_kept,
        metrics.total_row_groups,
        metrics.pages_kept,
        metrics.total_pages,
        metrics.rows_read,
        metrics.rows_matched,
        (metrics.rows_read as f64 / metrics.total_rows as f64) * 100.0
    );

    let mut group = c.benchmark_group("dictionary_hints_candidate");

    group.bench_function("aisle_metadata_only_candidate", |b| {
        b.iter(|| {
            let result = PruneRequest::new(black_box(&metadata), black_box(&schema))
                .with_df_predicate(black_box(&predicate))
                .enable_page_index(false)
                .enable_bloom_filter(false)
                .prune();
            black_box(result.row_groups());
        });
    });

    group.bench_function("aisle_prune_plus_scan_candidate", |b| {
        b.to_async(&runtime).iter(|| async {
            let mut provider = provider_template.clone();
            let result = PruneRequest::new(black_box(&metadata), black_box(&schema))
                .with_df_predicate(black_box(&predicate))
                .enable_page_index(false)
                .enable_bloom_filter(false)
                .enable_dictionary_hints(true)
                .prune_async(&mut provider)
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

criterion_group!(benches, bench_dictionary_hints_candidate);
criterion_main!(benches);
