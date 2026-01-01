use std::sync::Arc;

use aisle::{PruneOptions, Pruner};
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_expr::{col, lit, Expr};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use datafusion::physical_plan::metrics::{MetricValue, MetricsSet};
use datafusion::physical_plan::{collect as df_collect, ExecutionPlan};
use datafusion::execution::TaskContext;
use futures::StreamExt;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::arrow_reader::{ArrowReaderOptions, RowSelection};
use parquet::arrow::async_reader::AsyncFileReader;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader, PageIndexPolicy};
use parquet::file::properties::{EnabledStatistics, WriterProperties};

#[derive(Debug, Default)]
struct ScanStats {
    rows_read: usize,
    rows_matched: usize,
}

#[derive(Debug, Default)]
struct DfMetricsSummary {
    output_rows: usize,
    rows_read_est: Option<usize>,
    bytes_scanned: Option<usize>,
    row_groups_stats: Option<(usize, usize)>,
    row_groups_bloom: Option<(usize, usize)>,
    page_index_rows: Option<(usize, usize)>,
}

fn count_matches(batch: &RecordBatch, target_id: i64) -> usize {
    let array = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let mut matches = 0usize;
    for i in 0..array.len() {
        if array.value(i) == target_id {
            matches += 1;
        }
    }
    matches
}

async fn scan_builder<T: AsyncFileReader + Unpin + Send + 'static>(
    builder: ParquetRecordBatchStreamBuilder<T>,
    row_groups: &[usize],
    row_selection: Option<RowSelection>,
    target_id: i64,
) -> ScanStats {
    let mut stats = ScanStats::default();
    let mut builder = builder.with_row_groups(row_groups.to_vec());

    if let Some(selection) = row_selection {
        builder = builder.with_row_selection(selection);
    }

    let mut stream = builder.build().unwrap();

    while let Some(batch) = stream.next().await {
        let batch = batch.unwrap();
        stats.rows_read += batch.num_rows();
        stats.rows_matched += count_matches(&batch, target_id);
    }

    stats
}

async fn scan_file_async(
    path: &std::path::Path,
    row_groups: &[usize],
    row_selection: Option<RowSelection>,
    target_id: i64,
    enable_page_index: bool,
) -> ScanStats {
    let file = tokio::fs::File::open(path).await.unwrap();
    let options = ArrowReaderOptions::new().with_page_index(enable_page_index);
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(file, options)
        .await
        .unwrap();
    scan_builder(builder, row_groups, row_selection, target_id).await
}

/// Create test data with OVERLAPPING row group ranges but DISTINCT page ranges
fn create_overlapping_data(
    row_groups: usize,
    rows_per_group: usize,
) -> (tempfile::NamedTempFile, ParquetMetaData, Arc<Schema>) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_bloom_filter_enabled(true)
        .set_max_row_group_size(rows_per_group)
        .set_data_page_row_count_limit(2000)
        .build();

    let temp_file = tempfile::Builder::new()
        .suffix(".parquet")
        .tempfile()
        .unwrap();
    let file = std::fs::File::create(temp_file.path()).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    use rand::{seq::SliceRandom, SeedableRng};
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);

    let cluster_size: i64 = 2000;
    let clusters_per_group = 5;
    assert_eq!(
        rows_per_group,
        cluster_size as usize * clusters_per_group,
        "rows_per_group must equal cluster_size * clusters_per_group"
    );

    let num_clusters = 100;
    let clusters: Vec<i64> = (0..num_clusters)
        .map(|i| i as i64 * cluster_size)
        .collect();

    for _rg in 0..row_groups {
        let mut selected_clusters = clusters.clone();
        selected_clusters.shuffle(&mut rng);
        selected_clusters.truncate(clusters_per_group);
        selected_clusters.sort();

        let mut all_ids = Vec::with_capacity(rows_per_group);
        for cluster_start in selected_clusters {
            for offset in 0..cluster_size {
                all_ids.push(cluster_start + offset);
            }
        }

        let values: Vec<i64> = all_ids.iter().map(|id| id * 10).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(all_ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap();

        writer.write(&batch).unwrap();
    }

    writer.close().unwrap();

    let bytes = std::fs::read(temp_file.path()).unwrap();
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .parse_and_finish(&Bytes::from(bytes))
        .unwrap();

    (temp_file, metadata, schema)
}

async fn df_rows_for_plan(plan: Arc<dyn ExecutionPlan>, ctx: Arc<TaskContext>) -> usize {
    let batches = df_collect(plan, ctx).await.unwrap();
    batches.iter().map(|b| b.num_rows()).sum()
}

fn collect_plan_metrics(plan: &Arc<dyn ExecutionPlan>, out: &mut MetricsSet) {
    if let Some(metrics) = plan.metrics() {
        for metric in metrics.iter() {
            out.push(Arc::clone(metric));
        }
    }

    for child in plan.children() {
        collect_plan_metrics(child, out);
    }
}

fn aggregate_plan_metrics(plan: &Arc<dyn ExecutionPlan>) -> MetricsSet {
    let mut metrics = MetricsSet::new();
    collect_plan_metrics(plan, &mut metrics);
    metrics.aggregate_by_name()
}

fn get_pruning(metrics: &MetricsSet, name: &str) -> Option<(usize, usize)> {
    match metrics.sum_by_name(name) {
        Some(MetricValue::PruningMetrics { pruning_metrics, .. }) => {
            Some((pruning_metrics.pruned(), pruning_metrics.matched()))
        }
        _ => None,
    }
}

fn get_count(metrics: &MetricsSet, name: &str) -> Option<usize> {
    match metrics.sum_by_name(name) {
        Some(MetricValue::Count { count, .. }) => Some(count.value()),
        _ => None,
    }
}

fn estimate_rows_read(
    total_row_groups: usize,
    rows_per_group: usize,
    row_groups_stats: Option<(usize, usize)>,
    row_groups_bloom: Option<(usize, usize)>,
    page_index_rows: Option<(usize, usize)>,
) -> Option<usize> {
    let groups_after_stats = row_groups_stats
        .map(|(_, matched)| matched)
        .unwrap_or(total_row_groups);
    let groups_after_bloom = row_groups_bloom
        .and_then(|(pruned, matched)| {
            if pruned + matched > 0 {
                Some(matched)
            } else {
                None
            }
        })
        .unwrap_or(groups_after_stats);

    let rows_after_rowgroup = groups_after_bloom.saturating_mul(rows_per_group);
    let rows_after_page = match page_index_rows {
        Some((pruned, matched)) if pruned + matched > 0 => matched,
        _ => rows_after_rowgroup,
    };

    Some(rows_after_page)
}

fn df_run_and_metrics(
    runtime: &tokio::runtime::Runtime,
    plan: Arc<dyn ExecutionPlan>,
    ctx: Arc<TaskContext>,
    total_row_groups: usize,
    rows_per_group: usize,
) -> DfMetricsSummary {
    let output_rows = runtime.block_on(async {
        let batches = df_collect(plan.clone(), ctx).await.unwrap();
        batches.iter().map(|b| b.num_rows()).sum()
    });

    let metrics = aggregate_plan_metrics(&plan);
    let row_groups_stats = get_pruning(&metrics, "row_groups_pruned_statistics");
    let row_groups_bloom = get_pruning(&metrics, "row_groups_pruned_bloom_filter");
    let page_index_rows = get_pruning(&metrics, "page_index_rows_pruned");
    let bytes_scanned = get_count(&metrics, "bytes_scanned");
    let rows_read_est = estimate_rows_read(
        total_row_groups,
        rows_per_group,
        row_groups_stats,
        row_groups_bloom,
        page_index_rows,
    );

    DfMetricsSummary {
        output_rows,
        rows_read_est,
        bytes_scanned,
        row_groups_stats,
        row_groups_bloom,
        page_index_rows,
    }
}

fn fmt_opt(value: Option<usize>) -> String {
    value.map(|v| v.to_string()).unwrap_or_else(|| "n/a".to_string())
}

fn fmt_pruning(value: Option<(usize, usize)>) -> String {
    value
        .map(|(pruned, matched)| format!("{pruned} pruned, {matched} matched"))
        .unwrap_or_else(|| "n/a".to_string())
}

fn bench_end_to_end(c: &mut Criterion) {
    let row_groups = 100;
    let rows_per_group = 10_000;
    let (temp_file, metadata, schema) = create_overlapping_data(row_groups, rows_per_group);

    let target_id = 51_000i64;
    let predicate: Expr = col("id").eq(lit(target_id));

    let pruner_row = Pruner::try_with_options(
        schema.clone(),
        PruneOptions::builder()
            .enable_page_index(false)
            .enable_bloom_filter(false)
            .build(),
    )
    .unwrap();
    let compiled_row = pruner_row.try_compile(&predicate).unwrap();

    let pruner_page = Pruner::try_with_options(
        schema.clone(),
        PruneOptions::builder()
            .enable_page_index(true)
            .enable_bloom_filter(false)
            .build(),
    )
    .unwrap();
    let compiled_page = pruner_page.try_compile(&predicate).unwrap();

    let pruner_bloom = Pruner::try_with_options(
        schema.clone(),
        PruneOptions::builder()
            .enable_page_index(false)
            .enable_bloom_filter(true)
            .build(),
    )
    .unwrap();
    let compiled_bloom = pruner_bloom.try_compile(&predicate).unwrap();

    let pruner_both = Pruner::try_with_options(
        schema.clone(),
        PruneOptions::builder()
            .enable_page_index(true)
            .enable_bloom_filter(true)
            .build(),
    )
    .unwrap();
    let compiled_both = pruner_both.try_compile(&predicate).unwrap();

    let runtime = tokio::runtime::Runtime::new().unwrap();

    let df_plan_row = runtime.block_on(async {
        let config = SessionConfig::new()
            .with_target_partitions(1)
            .with_parquet_pruning(true)
            .with_parquet_page_index_pruning(false)
            .with_parquet_bloom_filter_pruning(false);
        let ctx = SessionContext::new_with_config(config);
        let df = ctx
            .read_parquet(
                temp_file.path().to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await
            .unwrap();
        let df = df.filter(predicate.clone()).unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        (plan, ctx.task_ctx())
    });

    let df_plan_page = runtime.block_on(async {
        let config = SessionConfig::new()
            .with_target_partitions(1)
            .with_parquet_pruning(true)
            .with_parquet_page_index_pruning(true)
            .with_parquet_bloom_filter_pruning(false);
        let ctx = SessionContext::new_with_config(config);
        let df = ctx
            .read_parquet(
                temp_file.path().to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await
            .unwrap();
        let df = df.filter(predicate.clone()).unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        (plan, ctx.task_ctx())
    });

    let df_plan_bloom = runtime.block_on(async {
        let config = SessionConfig::new()
            .with_target_partitions(1)
            .with_parquet_pruning(true)
            .with_parquet_page_index_pruning(false)
            .with_parquet_bloom_filter_pruning(true);
        let ctx = SessionContext::new_with_config(config);
        let df = ctx
            .read_parquet(
                temp_file.path().to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await
            .unwrap();
        let df = df.filter(predicate.clone()).unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        (plan, ctx.task_ctx())
    });

    let df_plan_both = runtime.block_on(async {
        let config = SessionConfig::new()
            .with_target_partitions(1)
            .with_parquet_pruning(true)
            .with_parquet_page_index_pruning(true)
            .with_parquet_bloom_filter_pruning(true);
        let ctx = SessionContext::new_with_config(config);
        let df = ctx
            .read_parquet(
                temp_file.path().to_str().unwrap(),
                ParquetReadOptions::default(),
            )
            .await
            .unwrap();
        let df = df.filter(predicate.clone()).unwrap();
        let plan = df.create_physical_plan().await.unwrap();
        (plan, ctx.task_ctx())
    });

    let df_metrics_row = df_run_and_metrics(
        &runtime,
        df_plan_row.0.clone(),
        df_plan_row.1.clone(),
        row_groups,
        rows_per_group,
    );
    let df_metrics_page = df_run_and_metrics(
        &runtime,
        df_plan_page.0.clone(),
        df_plan_page.1.clone(),
        row_groups,
        rows_per_group,
    );
    let df_metrics_bloom = df_run_and_metrics(
        &runtime,
        df_plan_bloom.0.clone(),
        df_plan_bloom.1.clone(),
        row_groups,
        rows_per_group,
    );
    let df_metrics_both = df_run_and_metrics(
        &runtime,
        df_plan_both.0.clone(),
        df_plan_both.1.clone(),
        row_groups,
        rows_per_group,
    );

    println!(
        "DataFusion (row-group only): output_rows={}, rows_read_est={}, bytes_scanned={}, row_groups_stats={}, row_groups_bloom={}, page_index_rows={}",
        df_metrics_row.output_rows,
        fmt_opt(df_metrics_row.rows_read_est),
        fmt_opt(df_metrics_row.bytes_scanned),
        fmt_pruning(df_metrics_row.row_groups_stats),
        fmt_pruning(df_metrics_row.row_groups_bloom),
        fmt_pruning(df_metrics_row.page_index_rows),
    );
    println!(
        "DataFusion (page index): output_rows={}, rows_read_est={}, bytes_scanned={}, row_groups_stats={}, row_groups_bloom={}, page_index_rows={}",
        df_metrics_page.output_rows,
        fmt_opt(df_metrics_page.rows_read_est),
        fmt_opt(df_metrics_page.bytes_scanned),
        fmt_pruning(df_metrics_page.row_groups_stats),
        fmt_pruning(df_metrics_page.row_groups_bloom),
        fmt_pruning(df_metrics_page.page_index_rows),
    );
    println!(
        "DataFusion (bloom filter): output_rows={}, rows_read_est={}, bytes_scanned={}, row_groups_stats={}, row_groups_bloom={}, page_index_rows={}",
        df_metrics_bloom.output_rows,
        fmt_opt(df_metrics_bloom.rows_read_est),
        fmt_opt(df_metrics_bloom.bytes_scanned),
        fmt_pruning(df_metrics_bloom.row_groups_stats),
        fmt_pruning(df_metrics_bloom.row_groups_bloom),
        fmt_pruning(df_metrics_bloom.page_index_rows),
    );
    println!(
        "DataFusion (page index + bloom): output_rows={}, rows_read_est={}, bytes_scanned={}, row_groups_stats={}, row_groups_bloom={}, page_index_rows={}",
        df_metrics_both.output_rows,
        fmt_opt(df_metrics_both.rows_read_est),
        fmt_opt(df_metrics_both.bytes_scanned),
        fmt_pruning(df_metrics_both.row_groups_stats),
        fmt_pruning(df_metrics_both.row_groups_bloom),
        fmt_pruning(df_metrics_both.page_index_rows),
    );

    let mut group = c.benchmark_group("end_to_end_point_query");

    group.bench_function("datafusion_rowgroup_only", |b| {
        b.to_async(&runtime).iter(|| async {
            let rows = df_rows_for_plan(df_plan_row.0.clone(), df_plan_row.1.clone()).await;
            let stats = ScanStats {
                rows_read: rows,
                rows_matched: rows,
            };
            black_box(stats);
        });
    });

    group.bench_function("datafusion_with_page_index", |b| {
        b.to_async(&runtime).iter(|| async {
            let rows = df_rows_for_plan(df_plan_page.0.clone(), df_plan_page.1.clone()).await;
            let stats = ScanStats {
                rows_read: rows,
                rows_matched: rows,
            };
            black_box(stats);
        });
    });

    group.bench_function("datafusion_with_bloom_filter", |b| {
        b.to_async(&runtime).iter(|| async {
            let rows =
                df_rows_for_plan(df_plan_bloom.0.clone(), df_plan_bloom.1.clone()).await;
            let stats = ScanStats {
                rows_read: rows,
                rows_matched: rows,
            };
            black_box(stats);
        });
    });

    group.bench_function("datafusion_with_both", |b| {
        b.to_async(&runtime).iter(|| async {
            let rows = df_rows_for_plan(df_plan_both.0.clone(), df_plan_both.1.clone()).await;
            let stats = ScanStats {
                rows_read: rows,
                rows_matched: rows,
            };
            black_box(stats);
        });
    });

    group.bench_function("aisle_rowgroup_only", |b| {
        b.to_async(&runtime).iter(|| async {
            let result = compiled_row.prune(black_box(&metadata));
            let stats = scan_file_async(
                temp_file.path(),
                result.row_groups(),
                None,
                target_id,
                false,
            )
            .await;
            black_box(stats);
        });
    });

    group.bench_function("aisle_with_page_index", |b| {
        b.to_async(&runtime).iter(|| async {
            let result = compiled_page.prune(black_box(&metadata));
            let selection = result.row_selection().cloned();
            let stats = scan_file_async(
                temp_file.path(),
                result.row_groups(),
                selection,
                target_id,
                true,
            )
            .await;
            black_box(stats);
        });
    });

    group.bench_function("aisle_with_bloom_filter", |b| {
        b.to_async(&runtime).iter(|| async {
            let file = tokio::fs::File::open(temp_file.path()).await.unwrap();
            let options = ArrowReaderOptions::new().with_page_index(false);
            let mut builder = ParquetRecordBatchStreamBuilder::new_with_options(file, options)
                .await
                .unwrap();
            let result = compiled_bloom.prune_with_async_reader(&mut builder).await;
            let stats = scan_builder(builder, result.row_groups(), None, target_id).await;
            black_box(stats);
        });
    });

    group.bench_function("aisle_with_both", |b| {
        b.to_async(&runtime).iter(|| async {
            let file = tokio::fs::File::open(temp_file.path()).await.unwrap();
            let options = ArrowReaderOptions::new().with_page_index(true);
            let mut builder = ParquetRecordBatchStreamBuilder::new_with_options(file, options)
                .await
                .unwrap();
            let result = compiled_both.prune_with_async_reader(&mut builder).await;
            let selection = result.row_selection().cloned();
            let stats = scan_builder(builder, result.row_groups(), selection, target_id).await;
            black_box(stats);
        });
    });

    group.finish();
}

criterion_group!(benches, bench_end_to_end);
criterion_main!(benches);
