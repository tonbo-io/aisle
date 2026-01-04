/// Point Query Benchmark - Demonstrates Page Index Advantages
///
/// Point queries (id = X) are where page indexes shine most:
/// - Row-group stats keep many row groups (overlapping ranges)
/// - Page indexes skip most pages within those row groups
/// - Bloom filters can eliminate row groups entirely (when available)
use std::sync::Arc;

use aisle::PruneRequest;
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use datafusion_common::pruning::PruningStatistics;
use datafusion_common::{Column, Result};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_expr::{col, lit};
use datafusion_physical_expr::planner::create_physical_expr;
use datafusion_pruning::PruningPredicate;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::arrow_reader::statistics::StatisticsConverter;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader, RowGroupMetaData};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use parquet::schema::types::SchemaDescriptor;

/// Create test data with OVERLAPPING row group ranges but DISTINCT page ranges
///
/// Strategy:
/// - Create 10 "clusters" of IDs: [0-10k], [10k-20k], ..., [90k-100k]
/// - Each row group contains 5 RANDOM clusters (creates row-group overlaps)
/// - Within each cluster, IDs are sequential (creates tight page boundaries)
/// - Pages align with cluster boundaries (each page = one cluster)
///
/// Result:
/// - Row-group pruning: ineffective (all row groups contain overlapping clusters)
/// - Page-level pruning: effective (only pages matching target cluster are read)
fn create_overlapping_data(
    row_groups: usize,
    rows_per_group: usize,
) -> (tempfile::NamedTempFile, ParquetMetaData) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_bloom_filter_enabled(true)
        .set_max_row_group_size(rows_per_group)
        .set_data_page_row_count_limit(2000) // Each cluster becomes ~1 page
        .build();

    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let file = std::fs::File::create(temp_file.path()).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    use rand::{SeedableRng, seq::SliceRandom};
    let mut rng = rand::rngs::StdRng::seed_from_u64(42);

    // Define clusters: each cluster has 2000 sequential IDs
    let cluster_size = 2000;
    let num_clusters = 100;
    let clusters: Vec<i64> = (0..num_clusters).map(|i| i * cluster_size).collect();

    for _rg in 0..row_groups {
        // Each row group contains 5 random clusters (creates overlapping ranges!)
        let mut selected_clusters = clusters.clone();
        selected_clusters.shuffle(&mut rng);
        selected_clusters.truncate(5);
        selected_clusters.sort(); // Keep clusters sorted for better page alignment

        let mut all_ids = Vec::new();

        // For each selected cluster, generate sequential IDs
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

    // IMPORTANT: We must tell the reader to load page indexes!
    // By default, ParquetMetaDataReader skips page indexes even if they exist.
    use parquet::file::metadata::PageIndexPolicy;
    let bytes = std::fs::read(temp_file.path()).unwrap();
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional) // Load page indexes if available
        .parse_and_finish(&Bytes::from(bytes))
        .unwrap();

    // Verify bloom filters are written
    assert!(
        metadata
            .row_group(0)
            .column(0)
            .bloom_filter_offset()
            .is_some(),
        "Bloom filters not written to file!"
    );

    (temp_file, metadata)
}

struct RowGroupPruningStatistics<'a> {
    parquet_schema: &'a SchemaDescriptor,
    row_group_metadatas: Vec<&'a RowGroupMetaData>,
    arrow_schema: &'a Schema,
}

impl<'a> RowGroupPruningStatistics<'a> {
    fn new(
        parquet_schema: &'a SchemaDescriptor,
        row_group_metadatas: &'a [RowGroupMetaData],
        arrow_schema: &'a Schema,
    ) -> Self {
        Self {
            parquet_schema,
            row_group_metadatas: row_group_metadatas.iter().collect(),
            arrow_schema,
        }
    }

    fn metadata_iter(&'a self) -> impl Iterator<Item = &'a RowGroupMetaData> + 'a {
        self.row_group_metadatas.iter().copied()
    }

    fn statistics_converter<'b>(&'a self, column: &'b Column) -> Result<StatisticsConverter<'a>> {
        Ok(StatisticsConverter::try_new(
            &column.name,
            self.arrow_schema,
            self.parquet_schema,
        )?)
    }
}

impl PruningStatistics for RowGroupPruningStatistics<'_> {
    fn min_values(&self, column: &Column) -> Option<arrow_array::ArrayRef> {
        self.statistics_converter(column)
            .and_then(|c| Ok(c.row_group_mins(self.metadata_iter())?))
            .ok()
    }

    fn max_values(&self, column: &Column) -> Option<arrow_array::ArrayRef> {
        self.statistics_converter(column)
            .and_then(|c| Ok(c.row_group_maxes(self.metadata_iter())?))
            .ok()
    }

    fn num_containers(&self) -> usize {
        self.row_group_metadatas.len()
    }

    fn null_counts(&self, column: &Column) -> Option<arrow_array::ArrayRef> {
        self.statistics_converter(column)
            .and_then(|c| Ok(c.row_group_null_counts(self.metadata_iter())?))
            .ok()
            .map(|counts| Arc::new(counts) as arrow_array::ArrayRef)
    }

    fn row_counts(&self, column: &Column) -> Option<arrow_array::ArrayRef> {
        self.statistics_converter(column)
            .and_then(|c| Ok(c.row_group_row_counts(self.metadata_iter())?))
            .ok()
            .flatten()
            .map(|counts| Arc::new(counts) as arrow_array::ArrayRef)
    }

    fn contained(
        &self,
        _column: &Column,
        _values: &std::collections::HashSet<datafusion_common::ScalarValue>,
    ) -> Option<arrow_array::BooleanArray> {
        None
    }
}

fn bench_point_query_comparison(c: &mut Criterion) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let row_groups = 100;
    let rows_per_group = 10_000;
    let (temp_file, metadata) = create_overlapping_data(row_groups, rows_per_group);

    let runtime = tokio::runtime::Runtime::new().unwrap();

    // Point query: target a specific cluster
    // Cluster 25 = IDs [50000, 52000)
    // This cluster appears in ~50% of row groups (due to random selection)
    // But within each matching row group, only 1 page (out of 5) should be read
    let target_id = 51000i64; // Middle of cluster 25
    let predicate = col("id").eq(lit(target_id));

    let mut group = c.benchmark_group("point_query_overlapping_data");

    // First, let's see how many row groups each method keeps
    let aisle_result = PruneRequest::new(&metadata, &schema)
        .with_df_predicate(&predicate)
        .enable_page_index(false) // Row-group level only
        .prune();

    let df_schema = datafusion_common::DFSchema::try_from(schema.clone()).unwrap();
    let physical_expr =
        create_physical_expr(&predicate, &df_schema, &ExecutionProps::new()).unwrap();
    let pruning_predicate = PruningPredicate::try_new(physical_expr, schema.clone()).unwrap();
    let stats = RowGroupPruningStatistics::new(
        metadata.file_metadata().schema_descr(),
        metadata.row_groups(),
        &schema,
    );
    let df_result = pruning_predicate.prune(&stats).unwrap();
    let df_kept: Vec<usize> = df_result
        .iter()
        .enumerate()
        .filter_map(|(idx, keep)| keep.then_some(idx))
        .collect();

    println!("\n=== Point Query: id = {} ===", target_id);
    println!("Dataset: {} row groups with overlapping ranges", row_groups);
    println!(
        "Aisle (row-group level): kept {} row groups",
        aisle_result.row_groups().len()
    );
    println!(
        "DataFusion (row-group level): kept {} row groups",
        df_kept.len()
    );
    println!();

    // Benchmark: Aisle WITHOUT page index
    group.bench_function("aisle_rowgroup_only", |b| {
        b.iter(|| {
            let result = PruneRequest::new(black_box(&metadata), black_box(&schema))
                .with_df_predicate(black_box(&predicate))
                .enable_page_index(false)
                .prune();
            black_box(result.row_groups());
        });
    });

    // Benchmark: Aisle WITH page index
    group.bench_function("aisle_with_page_index", |b| {
        b.iter(|| {
            let result = PruneRequest::new(black_box(&metadata), black_box(&schema))
                .with_df_predicate(black_box(&predicate))
                .enable_page_index(true)
                .prune();
            black_box(result.row_groups());
        });
    });

    // Benchmark: Aisle WITH bloom filter (page index disabled)
    group.bench_function("aisle_with_bloom_filter", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let file = tokio::fs::File::open(temp_file.path()).await.unwrap();
                let options =
                    parquet::arrow::arrow_reader::ArrowReaderOptions::new().with_page_index(true);
                let mut builder =
                    parquet::arrow::ParquetRecordBatchStreamBuilder::new_with_options(
                        file, options,
                    )
                    .await
                    .unwrap();

                // Use metadata from builder (no page indexes loaded)
                let builder_metadata = builder.metadata().clone();
                let builder_schema = builder.schema().clone();

                let result =
                    PruneRequest::new(black_box(&builder_metadata), black_box(&builder_schema))
                        .with_df_predicate(black_box(&predicate))
                        .enable_page_index(false)
                        .enable_bloom_filter(true)
                        .prune_async(&mut builder)
                        .await;

                black_box(result.row_groups());
            });
        });
    });

    // Benchmark: Aisle WITH BOTH page index AND bloom filter
    // CRITICAL: Use metadata with page indexes (from setup), but builder for bloom filters
    group.bench_function("aisle_with_both", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let file = tokio::fs::File::open(temp_file.path()).await.unwrap();
                let mut builder = parquet::arrow::ParquetRecordBatchStreamBuilder::new(file)
                    .await
                    .unwrap();

                // Use the metadata with page indexes (from create_overlapping_data)
                // NOT builder.metadata() which doesn't have page indexes!
                let result = PruneRequest::new(black_box(&metadata), black_box(&schema))
                    .with_df_predicate(black_box(&predicate))
                    .enable_page_index(true)
                    .enable_bloom_filter(true)
                    .prune_async(&mut builder)
                    .await;

                black_box(result.row_groups());
            });
        });
    });

    // Benchmark: DataFusion (row-group only)
    group.bench_function("datafusion_rowgroup_only", |b| {
        b.iter(|| {
            let df_schema = datafusion_common::DFSchema::try_from(schema.clone()).unwrap();
            let physical_expr =
                create_physical_expr(black_box(&predicate), &df_schema, &ExecutionProps::new())
                    .unwrap();
            let pruning_predicate =
                PruningPredicate::try_new(physical_expr, schema.clone()).unwrap();
            let stats = RowGroupPruningStatistics::new(
                metadata.file_metadata().schema_descr(),
                metadata.row_groups(),
                &schema,
            );
            let values = pruning_predicate.prune(&stats).unwrap();
            black_box(values);
        });
    });

    // Now measure actual data reading
    group.finish();

    // End-to-end comparison with actual I/O
    println!("\n=== End-to-End with Actual Data Reading ===\n");

    // Estimate page index metadata size
    // For each row group, each column, each page: min/max values + null counts
    // Plus offset index with page locations
    let page_index_size = if metadata.column_index().is_some() {
        let num_row_groups = metadata.num_row_groups();
        let num_columns = metadata.file_metadata().schema().get_fields().len();

        // Estimate pages per row group (from first row group)
        let pages_per_rg = if num_row_groups > 0 {
            metadata
                .offset_index()
                .and_then(|idx| idx.get(0))
                .and_then(|rg| rg.get(0))
                .map(|col| col.page_locations().len())
                .unwrap_or(5)
        } else {
            5
        };

        // Column index: ~32 bytes per page (min + max + null_count for Int64)
        let col_idx_size = num_row_groups * num_columns * pages_per_rg * 32;

        // Offset index: ~24 bytes per page (offset + compressed_size + first_row_index)
        let off_idx_size = num_row_groups * num_columns * pages_per_rg * 24;

        col_idx_size + off_idx_size
    } else {
        0
    };

    println!(
        "Page index metadata overhead: ~{} KB",
        page_index_size / 1024
    );
    println!("(This would be additional I/O in S3 scenario)\n");

    // Aisle with page index (sync)
    let aisle_page_result = PruneRequest::new(&metadata, &schema)
        .with_df_predicate(&predicate)
        .enable_page_index(true)
        .prune();

    // Aisle with bloom filter (async, no page index)
    let aisle_bloom_result = runtime.block_on(async {
        let file = tokio::fs::File::open(temp_file.path()).await.unwrap();
        let options = parquet::arrow::arrow_reader::ArrowReaderOptions::new().with_page_index(true);
        let mut builder =
            parquet::arrow::ParquetRecordBatchStreamBuilder::new_with_options(file, options)
                .await
                .unwrap();

        // Use builder metadata (no page indexes) - this is intentional for bloom-only test
        let builder_metadata = builder.metadata().clone();
        let builder_schema = builder.schema().clone();

        PruneRequest::new(&builder_metadata, &builder_schema)
            .with_df_predicate(&predicate)
            .enable_page_index(false)
            .enable_bloom_filter(true)
            .prune_async(&mut builder)
            .await
    });

    // Aisle with BOTH (async with page index + bloom filter)
    // CRITICAL: Use metadata with page indexes, not builder.metadata()!
    let aisle_both_result = runtime.block_on(async {
        let file = tokio::fs::File::open(temp_file.path()).await.unwrap();
        let mut builder = parquet::arrow::ParquetRecordBatchStreamBuilder::new(file)
            .await
            .unwrap();

        // Use the metadata with page indexes from create_overlapping_data()
        PruneRequest::new(&metadata, &schema)
            .with_df_predicate(&predicate)
            .enable_page_index(true)
            .enable_bloom_filter(true)
            .prune_async(&mut builder)
            .await
    });

    println!("\n--- Pruning Results Comparison ---");
    println!("Page index only:");
    println!("  Row groups: {}", aisle_page_result.row_groups().len());
    if let Some(row_selection) = aisle_page_result.row_selection() {
        let selected_count: usize = row_selection
            .iter()
            .filter_map(|s| (!s.skip).then_some(s.row_count))
            .sum();
        let total_rows: usize = row_selection.iter().map(|s| s.row_count).sum();
        println!(
            "  Row selection: {} out of {} rows ({:.1}%)",
            selected_count,
            total_rows,
            (selected_count as f64 / total_rows as f64) * 100.0
        );
    }

    println!("Bloom filter only:");
    println!("  Row groups: {}", aisle_bloom_result.row_groups().len());

    println!("Both (page index + bloom filter):");
    println!("  Row groups: {}", aisle_both_result.row_groups().len());
    if let Some(row_selection) = aisle_both_result.row_selection() {
        let selected_count: usize = row_selection
            .iter()
            .filter_map(|s| (!s.skip).then_some(s.row_count))
            .sum();
        let total_rows: usize = row_selection.iter().map(|s| s.row_count).sum();
        println!(
            "  Row selection: {} out of {} rows ({:.1}%)",
            selected_count,
            total_rows,
            (selected_count as f64 / total_rows as f64) * 100.0
        );
        println!("  ✓ Page-level pruning working! (has row selection)");
    } else {
        println!("  ⚠️  WARNING: No row selection - page indexes may not be loaded!");
    }
    println!("---\n");

    // End-to-end timing for page index only
    let bytes = std::fs::read(temp_file.path()).unwrap();
    let start = std::time::Instant::now();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes.clone()))
        .unwrap()
        .with_row_groups(aisle_page_result.row_groups().to_vec());

    if let Some(row_selection) = aisle_page_result.row_selection() {
        reader = reader.with_row_selection(row_selection.clone());
    }

    let mut reader = reader.build().unwrap();
    let mut aisle_page_rows = 0;
    while let Some(batch) = reader.next() {
        aisle_page_rows += batch.unwrap().num_rows();
    }
    let aisle_page_time = start.elapsed();

    // End-to-end timing for bloom filter only
    let start = std::time::Instant::now();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes.clone()))
        .unwrap()
        .with_row_groups(aisle_bloom_result.row_groups().to_vec())
        .build()
        .unwrap();

    let mut aisle_bloom_rows = 0;
    while let Some(batch) = reader.next() {
        aisle_bloom_rows += batch.unwrap().num_rows();
    }
    let aisle_bloom_time = start.elapsed();

    // End-to-end timing for BOTH
    let start = std::time::Instant::now();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes.clone()))
        .unwrap()
        .with_row_groups(aisle_both_result.row_groups().to_vec());

    if let Some(row_selection) = aisle_both_result.row_selection() {
        reader = reader.with_row_selection(row_selection.clone());
    }

    let mut reader = reader.build().unwrap();
    let mut aisle_both_rows = 0;
    while let Some(batch) = reader.next() {
        aisle_both_rows += batch.unwrap().num_rows();
    }
    let aisle_both_time = start.elapsed();

    // DataFusion (reads full row groups)
    let start = std::time::Instant::now();
    let df_kept_count = df_kept.len();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
        .unwrap()
        .with_row_groups(df_kept)
        .build()
        .unwrap();

    let mut df_rows = 0;
    while let Some(batch) = reader.next() {
        df_rows += batch.unwrap().num_rows();
    }
    let df_time = start.elapsed();

    println!("=== End-to-End Performance Results ===\n");

    println!("DataFusion (row-group stats only):");
    println!("  Time: {:?}", df_time);
    println!("  Rows read: {}", df_rows);
    println!("  Row groups: {}", df_kept_count);

    println!("\nAisle (page index only):");
    println!("  Time: {:?}", aisle_page_time);
    println!("  Rows read: {}", aisle_page_rows);
    println!("  Row groups: {}", aisle_page_result.row_groups().len());

    println!("\nAisle (bloom filter only):");
    println!("  Time: {:?}", aisle_bloom_time);
    println!("  Rows read: {}", aisle_bloom_rows);
    println!("  Row groups: {}", aisle_bloom_result.row_groups().len());

    println!("\nAisle (BOTH page index + bloom filter):");
    println!("  Time: {:?}", aisle_both_time);
    println!("  Rows read: {}", aisle_both_rows);
    println!("  Row groups: {}", aisle_both_result.row_groups().len());

    if df_rows > 0 {
        println!("\n=== Comparison vs DataFusion ===\n");

        // Page index only
        let page_io_reduction = (1.0 - aisle_page_rows as f64 / df_rows as f64) * 100.0;
        let page_speedup = df_time.as_micros() as f64 / aisle_page_time.as_micros() as f64;
        println!("Page index only:");
        println!("  I/O Reduction: {:.1}%", page_io_reduction);
        println!("  Speedup: {:.2}x", page_speedup);

        // Bloom filter only
        let bloom_io_reduction = (1.0 - aisle_bloom_rows as f64 / df_rows as f64) * 100.0;
        let bloom_speedup = df_time.as_micros() as f64 / aisle_bloom_time.as_micros() as f64;
        println!("\nBloom filter only:");
        println!("  I/O Reduction: {:.1}%", bloom_io_reduction);
        println!("  Speedup: {:.2}x", bloom_speedup);

        // Both combined
        let both_io_reduction = (1.0 - aisle_both_rows as f64 / df_rows as f64) * 100.0;
        let both_speedup = df_time.as_micros() as f64 / aisle_both_time.as_micros() as f64;
        println!("\nBOTH (page index + bloom filter):");
        println!("  I/O Reduction: {:.1}%", both_io_reduction);
        println!("  Speedup: {:.2}x", both_speedup);
        println!("  ✓ Combined power of page-level + bloom filter pruning!");
    }
}

criterion_group!(benches, bench_point_query_comparison);
criterion_main!(benches);
