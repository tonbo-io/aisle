use std::sync::Arc;

use aisle::{ExprRowFilter, PruneRequest, Pruner};
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use datafusion_expr::{col, lit};
use parquet::{
    arrow::{
        ArrowWriter,
        arrow_reader::{ParquetRecordBatchReaderBuilder, RowFilter},
    },
    file::{
        metadata::{ParquetMetaData, ParquetMetaDataReader},
        properties::{EnabledStatistics, WriterProperties},
    },
};

// ============================================================================
// Helper Functions
// ============================================================================

fn create_int_batch(schema: Arc<Schema>, start: i64, count: usize) -> RecordBatch {
    let ids: Vec<i64> = (start..start + count as i64).collect();
    let values: Vec<i64> = ids.iter().map(|id| id * 10).collect();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
        ],
    )
    .unwrap()
}

fn create_parquet_metadata(
    schema: Arc<Schema>,
    row_groups: usize,
    rows_per_group: usize,
    batch_fn: impl Fn(Arc<Schema>, i64, usize) -> RecordBatch,
) -> (Bytes, ParquetMetaData, Arc<Schema>) {
    let mut buffer = Vec::new();
    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_max_row_group_size(rows_per_group)
        .build();

    let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), Some(props)).unwrap();

    for i in 0..row_groups {
        let batch = batch_fn(schema.clone(), (i * rows_per_group) as i64, rows_per_group);
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();

    let bytes = Bytes::from(buffer);
    let metadata = ParquetMetaDataReader::new()
        .parse_and_finish(&bytes)
        .unwrap();

    (bytes, metadata, schema)
}

// ============================================================================
// Benchmarks: Scalability
// ============================================================================

fn bench_scalability(c: &mut Criterion) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let mut group = c.benchmark_group("scalability");

    for row_groups in [10, 100, 1000, 10000] {
        let (_, metadata, schema) =
            create_parquet_metadata(schema.clone(), row_groups, 1000, create_int_batch);

        group.throughput(Throughput::Elements(row_groups as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(row_groups),
            &row_groups,
            |b, _| {
                let predicate = col("value")
                    .gt_eq(lit(1000i64))
                    .and(col("value").lt(lit(5000i64)));
                b.iter(|| {
                    PruneRequest::new(black_box(&metadata), black_box(&schema))
                        .with_df_predicate(black_box(&predicate))
                        .enable_page_index(false)
                        .enable_bloom_filter(false)
                        .prune()
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Baseline Comparison: Aisle vs Arrow Row Filtering
// ============================================================================

/// Baseline comparison: Aisle vs standard Arrow row filtering
///
/// This benchmark compares truly equivalent workloads:
/// - Both do exact row-level filtering (id == 500)
/// - Baseline reads all row groups, Aisle prunes first
fn bench_baseline_comparison(c: &mut Criterion) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let mut group = c.benchmark_group("baseline_comparison");

    for row_groups in [10, 100, 1000] {
        let (bytes, metadata, schema) =
            create_parquet_metadata(schema.clone(), row_groups, 1000, create_int_batch);

        // Baseline: Arrow reader with RowFilter (ExprRowFilter)
        // Reads ALL row groups, uses Parquet's built-in row filtering
        // This is the standard approach without metadata pruning
        group.bench_with_input(
            BenchmarkId::new("arrow_row_filter", row_groups),
            &row_groups,
            |b, _| {
                let predicate = col("id").eq(lit(500i64));

                b.iter(|| {
                    // Use ExprRowFilter for exact filtering (same as Aisle)
                    let expr_row_filter = ExprRowFilter::new(
                        predicate.clone(),
                        metadata.file_metadata().schema_descr(),
                    );

                    let row_filter = RowFilter::new(vec![Box::new(expr_row_filter)]);
                    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
                        .unwrap()
                        .with_row_filter(row_filter) // Built-in filtering, no pruning
                        .build()
                        .unwrap();

                    let mut matching_rows = 0;
                    for batch in reader {
                        let batch = batch.unwrap();
                        matching_rows += batch.num_rows();
                    }
                    black_box(matching_rows)
                });
            },
        );

        // Aisle: Metadata pruning + exact row filtering with ExprRowFilter
        // Prunes row groups using metadata, then filters rows to id == 500
        // This is the FAIR comparison - both do exact filtering
        // Uses Pruner to cache schema indexing (avoids repeated schema parsing)
        group.bench_with_input(
            BenchmarkId::new("aisle_exact_filter", row_groups),
            &row_groups,
            |b, _| {
                let predicate = col("id").eq(lit(500i64));

                // Cache schema indexing (reused across multiple Parquet files in production)
                let pruner = Pruner::try_new(schema.clone()).unwrap();

                b.iter(|| {
                    // Step 1: Prune row groups (includes predicate compilation + metadata
                    // evaluation) NOTE: Compilation cost is per-iteration, but
                    // amortized in production when the same predicate is used
                    // across multiple files
                    let result = pruner.prune(&metadata, &predicate);

                    // Step 2: Create ExprRowFilter for exact filtering (full expression)
                    let expr_row_filter = ExprRowFilter::new(
                        predicate.clone(),
                        metadata.file_metadata().schema_descr(),
                    );

                    // Step 3: Read with both row group pruning AND exact row filtering
                    let row_filter = RowFilter::new(vec![Box::new(expr_row_filter)]);
                    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
                        .unwrap()
                        .with_row_groups(result.row_groups().to_vec())
                        .with_row_filter(row_filter)
                        .build()
                        .unwrap();

                    let mut matching_rows = 0;
                    for batch in reader {
                        let batch = batch.unwrap();
                        matching_rows += batch.num_rows();
                    }
                    black_box(matching_rows)
                });
            },
        );
    }

    group.finish();
}

/// Performance breakdown: individual Aisle components
///
/// Shows the cost of each stage in the Aisle pipeline.
/// NOTE: These are NOT comparable to the baseline since they don't do exact filtering.
fn bench_aisle_breakdown(c: &mut Criterion) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let mut group = c.benchmark_group("aisle_breakdown");

    for row_groups in [10, 100, 1000] {
        let (bytes, metadata, schema) =
            create_parquet_metadata(schema.clone(), row_groups, 1000, create_int_batch);

        // Just metadata evaluation (no I/O)
        // Shows the cost of: schema indexing (cached) + compilation + metadata eval
        group.bench_with_input(
            BenchmarkId::new("metadata_only", row_groups),
            &row_groups,
            |b, _| {
                let predicate = col("id").eq(lit(500i64));
                let pruner = Pruner::try_new(schema.clone()).unwrap();

                b.iter(|| {
                    let result = pruner.prune(black_box(&metadata), black_box(&predicate));
                    black_box(result)
                });
            },
        );

        // Prune + read (no row-level filtering)
        // NOTE: Reads ALL rows from pruned groups, not just matching rows
        group.bench_with_input(
            BenchmarkId::new("prune_and_read", row_groups),
            &row_groups,
            |b, _| {
                let predicate = col("id").eq(lit(500i64));
                let pruner = Pruner::try_new(schema.clone()).unwrap();

                b.iter(|| {
                    let result = pruner.prune(&metadata, &predicate);

                    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())
                        .unwrap()
                        .with_row_groups(result.row_groups().to_vec())
                        .build()
                        .unwrap();

                    let mut total_rows = 0;
                    for batch in reader {
                        let batch = batch.unwrap();
                        total_rows += batch.num_rows();
                    }
                    black_box(total_rows)
                });
            },
        );
    }

    group.finish();
}

// ============================================================================
// Criterion Configuration
// ============================================================================

// Main benchmark group - fair comparison (both do exact filtering)
criterion_group!(
    benches,
    bench_baseline_comparison, // ← Direct comparison: Arrow vs Aisle (both exact)
    bench_scalability,         // ← Shows how Aisle scales with data size
);

// Performance breakdown - component costs (not for comparison)
criterion_group!(
    breakdown,
    bench_aisle_breakdown, // ← Shows metadata/I/O costs separately
);

// Detailed predicate benchmarks - useful for development/profiling
// Uncomment to run specific predicate type benchmarks
// criterion_group!(
//     predicate_benches,
//     bench_equality_predicate,
//     bench_range_predicate,
//     bench_in_list_predicate,
//     bench_string_prefix,
//     bench_and_chains,
//     bench_or_branches,
// );

criterion_main!(benches);
