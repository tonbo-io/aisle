/// Bloom Filter Benchmark - Demonstrates Definite Absence Detection
///
/// Bloom filters excel at proving a value is DEFINITELY ABSENT from a row group.
/// This benchmark shows:
/// - Equality predicates where bloom filters eliminate row groups entirely
/// - Comparison with row-group statistics (which can only say "maybe present")

use std::sync::Arc;

use aisle::PruneRequest;
use arrow_array::{Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use datafusion_expr::{col, lit};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader, PageIndexPolicy};
use parquet::file::properties::{EnabledStatistics, WriterProperties};
use rand::{Rng, SeedableRng};

/// Create test data with OVERLAPPING ranges but SPARSE values
///
/// Strategy:
/// - Create 100 row groups, each spanning range [0, 1M) in their min/max stats
/// - But each row group only contains EVEN IDs or ODD IDs (alternating)
/// - Each row group has 1000 IDs sampled sparsely from the full range
///
/// Result:
/// - Row-group stats: All overlap [0, 1M) → stats can't eliminate ANY row group
/// - Bloom filter: Can prove absence for specific values
///   - Query for ODD id in EVEN-only row group → bloom filter eliminates it
///   - Query for value not in any row group → bloom filter eliminates all
fn create_bloom_filter_test_data(
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
        .set_data_page_row_count_limit(2000)
        .build();

    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let file = std::fs::File::create(temp_file.path()).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    let mut rng = rand::rngs::StdRng::seed_from_u64(42);
    let total_id_range = 1_000_000i64;

    for rg in 0..row_groups {
        // Alternate: even row groups have EVEN ids, odd row groups have ODD ids
        let parity = (rg % 2) as i64;

        // Generate sparse IDs across the full range, but only matching parity
        let mut ids: Vec<i64> = Vec::new();
        while ids.len() < rows_per_group {
            let candidate = rng.gen_range(0..total_id_range);
            // Only include if it matches this row group's parity
            if candidate % 2 == parity {
                ids.push(candidate);
            }
        }
        ids.sort(); // Keep sorted for better compression

        let values: Vec<i64> = ids.iter().map(|id| id * 10).collect();

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from(values)),
            ],
        )
        .unwrap();

        writer.write(&batch).unwrap();
    }

    writer.close().unwrap();

    // Read metadata with page indexes
    let bytes = std::fs::read(temp_file.path()).unwrap();
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .parse_and_finish(&Bytes::from(bytes))
        .unwrap();

    // Verify bloom filters are written
    assert!(
        metadata.row_group(0).column(0).bloom_filter_offset().is_some(),
        "Bloom filters not written to file!"
    );

    (temp_file, metadata)
}

fn bench_bloom_filter_effectiveness(c: &mut Criterion) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));

    let row_groups = 100;
    let rows_per_group = 1000;
    let (temp_file, metadata) = create_bloom_filter_test_data(row_groups, rows_per_group);

    // Test 1: Query for an ODD number that actually exists
    // - Row-group stats: All overlap [0, 1M) → can't prune, keep all 100 row groups
    // - Bloom filter: Only exists in ODD row groups → eliminate EVEN row groups

    // First, read actual data to find a value that exists
    let bytes = std::fs::read(temp_file.path()).unwrap();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
        .unwrap()
        .with_row_groups(vec![1]) // Read from an ODD row group
        .build()
        .unwrap();

    let first_batch = reader.next().unwrap().unwrap();
    let id_array = first_batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let actual_odd_id = id_array.value(0); // Get first ID from ODD row group

    let odd_predicate = col("id").eq(lit(actual_odd_id));

    println!("\n=== Bloom Filter Effectiveness Test ===");
    println!("Dataset: {} row groups × {} rows = {} total",
        row_groups, rows_per_group, row_groups * rows_per_group);
    println!("Data pattern: Alternating EVEN/ODD row groups");
    println!("Row-group stats: All overlap [0, 1M) - stats can't prune!\n");

    // Sync API (row-group stats only)
    let stats_only_result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&odd_predicate)
        .enable_page_index(false)
        .enable_bloom_filter(false)
        .prune();

    println!("Query: id = {} (ODD number from row group 1)", actual_odd_id);
    println!("  Stats only: kept {} row groups (stats overlap, can't prune)",
        stats_only_result.row_groups().len());

    // Async API (bloom filter)
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let bloom_result = runtime.block_on(async {
        let file = tokio::fs::File::open(temp_file.path()).await.unwrap();
        let mut builder = parquet::arrow::ParquetRecordBatchStreamBuilder::new(file)
            .await
            .unwrap();

        let metadata = builder.metadata().clone();
        let schema = builder.schema().clone();

        PruneRequest::new(&metadata, &schema)
            .with_predicate(&odd_predicate)
            .enable_page_index(false)
            .enable_bloom_filter(true)
            .prune_async(&mut builder)
            .await
    });

    println!("  Bloom filter: kept {} row groups (should eliminate EVEN row groups)",
        bloom_result.row_groups().len());
    if bloom_result.row_groups().len() > 0 {
        println!("    ✓ Value found in {} ODD row groups", bloom_result.row_groups().len());
    }
    println!();

    // Test 2: Query for EVEN number - read from an EVEN row group
    let bytes = std::fs::read(temp_file.path()).unwrap();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
        .unwrap()
        .with_row_groups(vec![0]) // Read from an EVEN row group
        .build()
        .unwrap();

    let first_batch = reader.next().unwrap().unwrap();
    let id_array = first_batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
    let actual_even_id = id_array.value(0); // Get first ID from EVEN row group

    let even_predicate = col("id").eq(lit(actual_even_id));

    let stats_even = PruneRequest::new(&metadata, &schema)
        .with_predicate(&even_predicate)
        .enable_bloom_filter(false)
        .prune();

    let bloom_even = runtime.block_on(async {
        let file = tokio::fs::File::open(temp_file.path()).await.unwrap();
        let mut builder = parquet::arrow::ParquetRecordBatchStreamBuilder::new(file)
            .await
            .unwrap();

        let metadata = builder.metadata().clone();
        let schema = builder.schema().clone();

        PruneRequest::new(&metadata, &schema)
            .with_predicate(&even_predicate)
            .enable_bloom_filter(true)
            .prune_async(&mut builder)
            .await
    });

    println!("Query: id = {} (EVEN number from row group 0)", actual_even_id);
    println!("  Stats only: kept {} row groups (can't prune)", stats_even.row_groups().len());
    println!("  Bloom filter: kept {} row groups (should eliminate ODD row groups)", bloom_even.row_groups().len());
    if bloom_even.row_groups().len() > 0 {
        println!("    ✓ Value found in {} EVEN row groups", bloom_even.row_groups().len());
    }
    println!();

    // Benchmark: Metadata evaluation overhead
    let mut group = c.benchmark_group("bloom_filter_metadata");

    group.bench_function("stats_only", |b| {
        b.iter(|| {
            let result = PruneRequest::new(black_box(&metadata), black_box(&schema))
                .with_predicate(black_box(&odd_predicate))
                .enable_bloom_filter(false)
                .prune();
            black_box(result.row_groups());
        });
    });

    group.bench_function("with_bloom_filter", |b| {
        b.iter(|| {
            runtime.block_on(async {
                let file = tokio::fs::File::open(temp_file.path()).await.unwrap();
                let mut builder = parquet::arrow::ParquetRecordBatchStreamBuilder::new(file)
                    .await
                    .unwrap();

                let metadata = builder.metadata().clone();
                let schema = builder.schema().clone();

                let result = PruneRequest::new(black_box(&metadata), black_box(&schema))
                    .with_predicate(black_box(&odd_predicate))
                    .enable_bloom_filter(true)
                    .prune_async(&mut builder)
                    .await;

                black_box(result.row_groups());
            });
        });
    });

    group.finish();

    // Benchmark: End-to-end with actual data reading
    println!("\n=== End-to-End Performance (ODD number query) ===\n");

    let start = std::time::Instant::now();
    let bytes = std::fs::read(temp_file.path()).unwrap();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes.clone()))
        .unwrap()
        .with_row_groups(stats_only_result.row_groups().to_vec())
        .build()
        .unwrap();

    let mut stats_rows = 0;
    while let Some(batch) = reader.next() {
        stats_rows += batch.unwrap().num_rows();
    }
    let stats_time = start.elapsed();

    let start = std::time::Instant::now();
    let mut reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
        .unwrap()
        .with_row_groups(bloom_result.row_groups().to_vec())
        .build()
        .unwrap();

    let mut bloom_rows = 0;
    while let Some(batch) = reader.next() {
        bloom_rows += batch.unwrap().num_rows();
    }
    let bloom_time = start.elapsed();

    println!("Stats only (no bloom filter):");
    println!("  Time: {:?}", stats_time);
    println!("  Rows read: {} (before filtering)", stats_rows);
    println!("  Row groups: {}", stats_only_result.row_groups().len());

    println!("\nBloom filter:");
    println!("  Time: {:?}", bloom_time);
    println!("  Rows read: {}", bloom_rows);
    println!("  Row groups: {}", bloom_result.row_groups().len());

    if stats_rows > 0 {
        let io_reduction = (1.0 - bloom_rows as f64 / stats_rows as f64) * 100.0;
        let speedup = stats_time.as_micros() as f64 / bloom_time.as_micros() as f64;
        println!("\nSummary:");
        println!("  I/O Reduction: {:.1}%", io_reduction);
        println!("  Speedup: {:.2}x", speedup);
        println!("  Row groups eliminated: {} → {} ({}% reduction)",
            stats_only_result.row_groups().len(),
            bloom_result.row_groups().len(),
            (1.0 - bloom_result.row_groups().len() as f64 / stats_only_result.row_groups().len() as f64) * 100.0);
    }
}

criterion_group!(benches, bench_bloom_filter_effectiveness);
criterion_main!(benches);
