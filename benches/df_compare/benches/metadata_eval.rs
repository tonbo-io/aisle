use std::sync::Arc;

use aisle::{PruneOptions, PruneRequest, Pruner};
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_array::builder::BinaryBuilder;
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion_expr::{col, lit};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::ParquetRecordBatchStreamBuilder;
use parquet::arrow::ArrowWriter;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader};
use parquet::file::properties::{EnabledStatistics, WriterProperties};

/// Data distribution pattern
#[derive(Debug, Clone, Copy)]
enum DataDistribution {
    /// Sequential, non-overlapping ranges (best case for pruning)
    Ordered,
    /// Random data with overlapping ranges (realistic case)
    Random,
    /// Skewed distribution (some row groups much larger)
    Skewed,
}

/// Create schema with multiple data types (Int64, Utf8, Binary)
fn create_multi_type_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, true), // nullable
        Field::new("name", DataType::Utf8, true),   // string column
        Field::new("data", DataType::Binary, true), // binary column
    ]))
}

fn create_int_batch(
    schema: Arc<Schema>,
    start: i64,
    count: usize,
    distribution: DataDistribution,
    null_ratio: f64,
) -> RecordBatch {
    use rand::{Rng, SeedableRng};
    let mut rng = rand::rngs::StdRng::seed_from_u64(start as u64);

    let ids: Vec<i64> = match distribution {
        DataDistribution::Ordered => (start..start + count as i64).collect(),
        DataDistribution::Random => {
            let mut ids: Vec<i64> = (0..count).map(|_| rng.gen_range(0..1_000_000)).collect();
            ids.sort(); // Keep sorted within batch but allow overlaps between batches
            ids
        }
        DataDistribution::Skewed => {
            // Most values near start, some outliers
            (0..count)
                .map(|i| {
                    if rng.gen_bool(0.9) {
                        start + (i as i64 % 100)
                    } else {
                        start + rng.gen_range(0..10000)
                    }
                })
                .collect()
        }
    };

    let values: Vec<Option<i64>> = ids
        .iter()
        .map(|id| {
            if rng.gen_bool(null_ratio) {
                None
            } else {
                Some(id * 10)
            }
        })
        .collect();

    let names: Vec<Option<String>> = ids
        .iter()
        .map(|id| {
            if rng.gen_bool(null_ratio) {
                None
            } else {
                Some(format!("user_{:06}", id % 10000))
            }
        })
        .collect();

    let mut binary_builder = BinaryBuilder::new();
    for id in &ids {
        if rng.gen_bool(null_ratio) {
            binary_builder.append_null();
        } else {
            binary_builder.append_value(vec![(*id % 256) as u8; 16]);
        }
    }
    let data = binary_builder.finish();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(ids)),
            Arc::new(Int64Array::from(values)),
            Arc::new(StringArray::from(names)),
            Arc::new(data),
        ],
    )
    .unwrap()
}

fn create_simple_int_batch(schema: Arc<Schema>, start: i64, count: usize) -> RecordBatch {
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
) -> (tempfile::NamedTempFile, ParquetMetaData, Arc<Schema>) {
    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page) // Enable page-level statistics
        .set_column_index_truncate_length(Some(64))
        .set_max_row_group_size(rows_per_group)
        .set_bloom_filter_enabled(true) // Enable bloom filters
        .build();

    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let file = std::fs::File::create(temp_file.path()).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    for i in 0..row_groups {
        let batch = create_simple_int_batch(schema.clone(), (i * rows_per_group) as i64, rows_per_group);
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();

    // CRITICAL: Load page indexes into memory
    // Without this, page-level pruning silently falls back to row-group only
    let bytes = std::fs::read(temp_file.path()).unwrap();
    use parquet::file::metadata::PageIndexPolicy;
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .parse_and_finish(&Bytes::from(bytes))
        .unwrap();

    (temp_file, metadata, schema)
}

fn create_parquet_metadata_advanced(
    schema: Arc<Schema>,
    row_groups: usize,
    rows_per_group: usize,
    distribution: DataDistribution,
    null_ratio: f64,
) -> (tempfile::NamedTempFile, ParquetMetaData, Arc<Schema>) {
    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page) // Enable page-level statistics
        .set_column_index_truncate_length(Some(64))
        .set_max_row_group_size(rows_per_group)
        .set_bloom_filter_enabled(true) // Enable bloom filters
        .build();

    let temp_file = tempfile::NamedTempFile::new().unwrap();
    let file = std::fs::File::create(temp_file.path()).unwrap();
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();

    for i in 0..row_groups {
        let batch = create_int_batch(
            schema.clone(),
            (i * rows_per_group) as i64,
            rows_per_group,
            distribution,
            null_ratio,
        );
        writer.write(&batch).unwrap();
    }
    writer.close().unwrap();

    // CRITICAL: Load page indexes into memory
    // Without this, page-level pruning silently falls back to row-group only
    let bytes = std::fs::read(temp_file.path()).unwrap();
    use parquet::file::metadata::PageIndexPolicy;
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Optional)
        .parse_and_finish(&Bytes::from(bytes))
        .unwrap();

    (temp_file, metadata, schema)
}

fn bench_df_compare(c: &mut Criterion) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let predicate = col("id").eq(lit(500i64));

    let mut group = c.benchmark_group("metadata_row_group");
    let runtime = tokio::runtime::Runtime::new().expect("tokio runtime");
    let mut temp_files = Vec::new();

    for row_groups in [10, 100, 1000] {
        let (temp_file, metadata, schema) =
            create_parquet_metadata(schema.clone(), row_groups, 1000);

        let row_options = PruneOptions::builder()
            .enable_page_index(false)
            .enable_bloom_filter(false)
            .build();
        let page_options = PruneOptions::builder()
            .enable_page_index(true)
            .enable_bloom_filter(false)
            .build();
        let bloom_options = PruneOptions::builder()
            .enable_page_index(true)
            .enable_bloom_filter(true)
            .build();

        let pruner_row = Pruner::try_with_options(schema.clone(), row_options)
            .expect("pruner");
        let compiled_row = pruner_row.try_compile(&predicate).expect("compiled predicate");

        let pruner_page = Pruner::try_with_options(schema.clone(), page_options)
            .expect("pruner");
        let compiled_page = pruner_page.try_compile(&predicate).expect("compiled predicate");

        let pruner_bloom = Pruner::try_with_options(schema.clone(), bloom_options)
            .expect("pruner");
        let compiled_bloom = pruner_bloom.try_compile(&predicate).expect("compiled predicate");

        group.bench_with_input(
            BenchmarkId::new("aisle_row_group", row_groups),
            &row_groups,
            |b, _| {
                b.iter(|| {
                    let result = compiled_row.prune(black_box(&metadata));
                    black_box(result.row_groups());
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("aisle_row_group_page_index", row_groups),
            &row_groups,
            |b, _| {
                b.iter(|| {
                    let result = compiled_page.prune(black_box(&metadata));
                    black_box(result.row_groups());
                });
            },
        );

        let temp_path = temp_file.path().to_path_buf();
        temp_files.push(temp_file);
        group.bench_with_input(
            BenchmarkId::new("aisle_row_group_page_index_bloom", row_groups),
            &row_groups,
            |b, _| {
                b.to_async(&runtime).iter(|| async {
                    let file = tokio::fs::File::open(&temp_path).await.unwrap();
                    let options = ArrowReaderOptions::new().with_page_index(true);
                    let mut builder =
                        ParquetRecordBatchStreamBuilder::new_with_options(file, options)
                            .await
                            .unwrap();
                    let result = compiled_bloom.prune_with_async_reader(&mut builder).await;
                    black_box(result.row_groups());
                });
            },
        );
    }

    group.finish();
}

fn bench_data_distribution(c: &mut Criterion) {
    let schema = create_multi_type_schema();
    let predicate = col("id").eq(lit(50000i64));
    let row_groups = 100;

    let mut group = c.benchmark_group("data_distribution");

    for dist in [
        DataDistribution::Ordered,
        DataDistribution::Random,
        DataDistribution::Skewed,
    ] {
        let dist_name = format!("{:?}", dist);
        let (_temp_file, metadata, schema) = create_parquet_metadata_advanced(
            schema.clone(),
            row_groups,
            1000,
            dist,
            0.0,
        );

        group.bench_with_input(
            BenchmarkId::new("aisle", &dist_name),
            &dist_name,
            |b, _| {
                b.iter(|| {
                    let result = PruneRequest::new(black_box(&metadata), black_box(&schema))
                        .with_predicate(black_box(&predicate))
                        .prune();
                    black_box(result.row_groups());
                });
            },
        );
    }

    group.finish();
}

fn bench_predicate_types(c: &mut Criterion) {
    let schema = create_multi_type_schema();
    let row_groups = 100;
    let (_temp_file, metadata, schema) = create_parquet_metadata_advanced(
        schema.clone(),
        row_groups,
        1000,
        DataDistribution::Ordered,
        0.1, // 10% nulls
    );

    let predicates = vec![
        ("equality", col("id").eq(lit(500i64))),
        ("range_gt", col("id").gt(lit(1000i64))),
        ("range_between", col("id").between(lit(1000i64), lit(5000i64))),
        ("in_list", col("id").in_list(vec![lit(100i64), lit(500i64), lit(1000i64)], false)),
        ("is_null", col("value").is_null()),
        ("is_not_null", col("value").is_not_null()),
        ("string_prefix", col("name").like(lit("user_00%"))),
        ("complex_and", col("id").gt(lit(1000i64)).and(col("value").lt(lit(20000i64)))),
        ("complex_or", col("id").lt(lit(100i64)).or(col("id").gt(lit(90000i64)))),
    ];

    let mut group = c.benchmark_group("predicate_types");

    for (name, predicate) in predicates {
        group.bench_with_input(BenchmarkId::new("aisle", name), name, |b, _| {
            b.iter(|| {
                let result = PruneRequest::new(black_box(&metadata), black_box(&schema))
                    .with_predicate(black_box(&predicate))
                    .prune();
                black_box(result.row_groups());
            });
        });
    }

    group.finish();
}

fn bench_selectivity(c: &mut Criterion) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let row_groups = 100;

    let mut group = c.benchmark_group("selectivity");

    // Low selectivity: 1% of row groups match
    let (_temp_file, metadata, schema_clone) = create_parquet_metadata(
        schema.clone(),
        row_groups,
        1000,
    );
    let predicate_low = col("id").eq(lit(500i64)); // Matches 1 row group

    group.bench_function("aisle_low_1pct", |b| {
        b.iter(|| {
            let result = PruneRequest::new(black_box(&metadata), black_box(&schema_clone))
                .with_predicate(black_box(&predicate_low))
                .prune();
            black_box(result.row_groups());
        });
    });

    // Medium selectivity: ~30% of row groups match
    let predicate_medium = col("id").between(lit(20000i64), lit(50000i64));

    group.bench_function("aisle_medium_30pct", |b| {
        b.iter(|| {
            let result = PruneRequest::new(black_box(&metadata), black_box(&schema_clone))
                .with_predicate(black_box(&predicate_medium))
                .prune();
            black_box(result.row_groups());
        });
    });

    // High selectivity: ~80% of row groups match
    let predicate_high = col("id").gt(lit(20000i64));

    group.bench_function("aisle_high_80pct", |b| {
        b.iter(|| {
            let result = PruneRequest::new(black_box(&metadata), black_box(&schema_clone))
                .with_predicate(black_box(&predicate_high))
                .prune();
            black_box(result.row_groups());
        });
    });

    // Worst case: 100% of row groups match (measures overhead)
    let predicate_worst = col("id").gt(lit(-1i64));

    group.bench_function("aisle_worst_100pct", |b| {
        b.iter(|| {
            let result = PruneRequest::new(black_box(&metadata), black_box(&schema_clone))
                .with_predicate(black_box(&predicate_worst))
                .prune();
            black_box(result.row_groups());
        });
    });

    group.finish();
}

fn bench_page_index(c: &mut Criterion) {
    let schema = create_multi_type_schema();
    let predicate = col("id").between(lit(45000i64), lit(55000i64));
    let row_groups = 100;

    let (_temp_file, metadata, schema) = create_parquet_metadata_advanced(
        schema.clone(),
        row_groups,
        1000,
        DataDistribution::Ordered,
        0.0,
    );

    // VALIDATION: Ensure page indexes are actually loaded
    assert!(
        metadata.column_index().is_some(),
        "Page indexes not loaded! This benchmark would silently fall back to row-group only."
    );
    assert!(
        metadata.offset_index().is_some(),
        "Offset indexes not loaded! This benchmark would silently fall back to row-group only."
    );

    // VALIDATION: Ensure page-level pruning actually works
    let test_result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&predicate)
        .enable_page_index(true)
        .prune();
    assert!(
        test_result.row_selection().is_some(),
        "Page-level pruning did not produce row selection! Page indexes may not be working."
    );

    let mut group = c.benchmark_group("page_index");

    group.bench_function("without_page_index", |b| {
        b.iter(|| {
            let result = PruneRequest::new(black_box(&metadata), black_box(&schema))
                .with_predicate(black_box(&predicate))
                .enable_page_index(false)
                .prune();
            black_box(result.row_groups());
        });
    });

    group.bench_function("with_page_index", |b| {
        b.iter(|| {
            let result = PruneRequest::new(black_box(&metadata), black_box(&schema))
                .with_predicate(black_box(&predicate))
                .enable_page_index(true)
                .prune();
            black_box(result.row_selection());
        });
    });

    group.finish();
}

fn bench_scaling(c: &mut Criterion) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
    ]));
    let predicate = col("id").eq(lit(50000i64));

    let mut group = c.benchmark_group("scaling");

    for row_groups in [10, 100, 1000, 5000] {
        let (_temp_file, metadata, schema_clone) = create_parquet_metadata(
            schema.clone(),
            row_groups,
            1000,
        );

        group.bench_with_input(
            BenchmarkId::new("aisle", row_groups),
            &row_groups,
            |b, _| {
                b.iter(|| {
                    let result = PruneRequest::new(black_box(&metadata), black_box(&schema_clone))
                        .with_predicate(black_box(&predicate))
                        .prune();
                    black_box(result.row_groups());
                });
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_df_compare,
    bench_data_distribution,
    bench_predicate_types,
    bench_selectivity,
    bench_page_index,
    bench_scaling,
);
criterion_main!(benches);
