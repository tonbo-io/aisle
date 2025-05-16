use std::sync::Arc;

use aisle::{
    ProjectionMask,
    predicate::AislePredicate,
    reader::{ParquetRecordBatchStreamBuilder, predicate::AislePredicateFn},
};
use arrow::{
    array::{ArrayRef, Datum, RecordBatch, StringArray, UInt8Array, UInt64Array},
    datatypes::{DataType, Field, Schema},
};
use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use fusio::{DynFs, disk::TokioFs, fs::OpenOptions, path::Path};
use fusio_parquet::{reader::AsyncReader, writer::AsyncWriter};
use futures_util::StreamExt;
use parquet::{
    arrow::{
        AsyncArrowWriter,
        arrow_reader::{ArrowPredicate, ArrowPredicateFn, ArrowReaderOptions},
    },
    basic::Compression,
    file::properties::{EnabledStatistics, WriterProperties},
    format::SortingColumn,
    schema::types::ColumnPath,
};
use rand::{Rng, thread_rng};

fn get_ordered_record_batch(record_size: usize) -> RecordBatch {
    let mut ids = vec![];
    let mut ages = vec![];
    let mut names = vec![];
    for i in 0..record_size {
        ids.push(i as u64);
        ages.push((i % 256) as u8);
        names.push(format!("{:08}", i));
    }
    RecordBatch::try_from_iter(vec![
        ("id", Arc::new(UInt64Array::from(ids)) as ArrayRef),
        ("name", Arc::new(StringArray::from(names)) as ArrayRef),
        ("age", Arc::new(UInt8Array::from(ages)) as ArrayRef),
    ])
    .unwrap()
}

fn get_random_record_batch(record_size: usize) -> RecordBatch {
    let mut ids = vec![];
    let mut ages = vec![];
    let mut names = vec![];
    let mut rng = thread_rng();
    for i in 0..record_size {
        ids.push(rng.gen_range(0..record_size) as u64);
        ages.push((rng.gen_range(0..record_size) % 256) as u8);
        names.push(format!("{:08}", rng.gen_range(0..record_size * 10 + i)));
    }
    RecordBatch::try_from_iter(vec![
        ("id", Arc::new(UInt64Array::from(ids)) as ArrayRef),
        ("name", Arc::new(StringArray::from(names)) as ArrayRef),
        ("age", Arc::new(UInt8Array::from(ages)) as ArrayRef),
    ])
    .unwrap()
}

async fn prepare_test_file(record_size: usize, sorted: bool) -> Path {
    let dir = "./bench_data";
    let filename = format!(
        "bench_{}_{}.parquet",
        sorted.then_some("sorted").unwrap_or("random"),
        record_size
    );

    if !std::path::Path::new(dir).exists() {
        std::fs::create_dir_all(dir).unwrap();
    }

    let path = Path::new(dir).unwrap().child(filename.as_str());

    if !std::path::Path::new(&format!("{}/{}", dir, filename)).exists() {
        let fs = Arc::new(TokioFs {}) as Arc<dyn DynFs>;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::UInt8, false),
        ]));

        let mut properties_builder = WriterProperties::builder().set_compression(Compression::LZ4);
        if sorted {
            let column_paths = ColumnPath::new(vec!["id".into()]);
            let sorting_columns = vec![SortingColumn::new(0, true, true)];
            properties_builder = properties_builder
                .set_column_bloom_filter_enabled(column_paths.clone(), true)
                .set_column_statistics_enabled(column_paths, EnabledStatistics::Page)
                .set_sorting_columns(Some(sorting_columns));
        }
        let mut writer = AsyncArrowWriter::try_new(
            AsyncWriter::new(
                fs.open_options(&path, OpenOptions::default().create(true).write(true))
                    .await
                    .unwrap(),
            ),
            schema,
            Some(properties_builder.build()),
        )
        .unwrap();

        let record_batch = if sorted {
            get_ordered_record_batch(record_size)
        } else {
            get_random_record_batch(record_size)
        };
        writer.write(&record_batch).await.unwrap();
        writer.close().await.unwrap();
    }

    path
}

async fn get_async_reader(path: &Path) -> AsyncReader {
    let fs = Arc::new(TokioFs {});
    let file = fs
        .open_options(path, OpenOptions::default().read(true))
        .await
        .unwrap();
    let size = file.size().await.unwrap();
    AsyncReader::new(file, size).await.unwrap()
}

async fn bench_standard_reader(path: &Path, range: (u64, u64), options: ArrowReaderOptions) {
    use arrow::compute::kernels::cmp::{gt, lt};
    use parquet::arrow::arrow_reader::RowFilter;

    let reader = get_async_reader(path).await;

    let builder =
        parquet::arrow::ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
            .await
            .unwrap();
    let parquet_schema = builder.parquet_schema();

    let left_range_p = ArrowPredicateFn::new(
        ProjectionMask::roots(parquet_schema, vec![0]),
        move |batch| {
            let datum = Arc::new(UInt64Array::new_scalar(range.0)) as Arc<dyn Datum>;
            gt(batch.column(0), datum.as_ref())
        },
    );
    let right_range_p = ArrowPredicateFn::new(
        ProjectionMask::roots(parquet_schema, vec![0]),
        move |batch| {
            let datum = Arc::new(UInt64Array::new_scalar(range.1)) as Arc<dyn Datum>;
            lt(batch.column(0), datum.as_ref())
        },
    );

    let predicates: Vec<Box<dyn ArrowPredicate>> =
        vec![Box::new(left_range_p), Box::new(right_range_p)];
    let mut reader = builder
        .with_row_filter(RowFilter::new(predicates).into())
        .build()
        .unwrap();

    while let Some(_) = reader.next().await {}
}

async fn bench_pushdown_reader(path: &Path, range: (u64, u64), options: ArrowReaderOptions) {
    use aisle::{
        filter::RowFilter,
        ord::{gt, lt},
    };

    let reader = get_async_reader(path).await;

    let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
        .await
        .unwrap();
    let parquet_schema = builder.parquet_schema();

    let left_range_p = AislePredicateFn::new(
        ProjectionMask::roots(parquet_schema, vec![0]),
        move |batch| {
            let datum = Arc::new(UInt64Array::new_scalar(range.0)) as Arc<dyn Datum>;
            gt(batch.column(0), datum.as_ref())
        },
    );
    let right_range_p = AislePredicateFn::new(
        ProjectionMask::roots(parquet_schema, vec![0]),
        move |batch| {
            let datum = Arc::new(UInt64Array::new_scalar(range.1)) as Arc<dyn Datum>;
            lt(batch.column(0), datum.as_ref())
        },
    );

    let predicates: Vec<Box<dyn AislePredicate>> =
        vec![Box::new(left_range_p), Box::new(right_range_p)];
    let mut reader = builder
        .with_row_filter(RowFilter::new(predicates))
        .build()
        .unwrap();

    while let Some(_) = reader.next().await {}
}

fn bench_compare_readers(c: &mut Criterion, group_name: &str, sorted: bool, with_page_index: bool) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let sizes = vec![
        1024 * 1024,
        4 * 1024 * 1024,
        8 * 1024 * 1024,
        16 * 1024 * 1024,
    ];
    let mut group = c.benchmark_group(group_name);
    for size in sizes {
        let path = rt.block_on(prepare_test_file(size, sorted));

        group.bench_with_input(
            BenchmarkId::new("parquet_reader", size),
            &size,
            |b, &size| {
                b.to_async(&rt).iter(|| async {
                    let mut rng = thread_rng();
                    let left = rng.gen_range(0..size);
                    let right = rng.gen_range(left..size);
                    bench_standard_reader(
                        &path,
                        (left as u64, right as u64),
                        ArrowReaderOptions::new().with_page_index(with_page_index),
                    )
                    .await
                });
            },
        );

        group.bench_with_input(BenchmarkId::new("aisle_reader", size), &size, |b, &size| {
            b.to_async(&rt).iter(|| async {
                let mut rng = thread_rng();
                let left = rng.gen_range(0..size);
                let right = rng.gen_range(left..size);
                bench_pushdown_reader(
                    &path,
                    (left as u64, right as u64),
                    ArrowReaderOptions::new().with_page_index(with_page_index),
                )
                .await
            });
        });
    }
    group.finish();
}

fn compare_readers_sorted_without_page_index(c: &mut Criterion) {
    bench_compare_readers(c, "sorted_without_page_index", true, false);
}

fn compare_readers_sorted_with_page_index(c: &mut Criterion) {
    bench_compare_readers(c, "sorted_with_page_index", true, true);
}

fn compare_readers_random_without_page_index(c: &mut Criterion) {
    bench_compare_readers(c, "random_without_page_index", false, false);
}

fn compare_readers_random_with_page_index(c: &mut Criterion) {
    bench_compare_readers(c, "random_with_page_index", false, true);
}

criterion_group!(
    benches,
    compare_readers_sorted_without_page_index,
    compare_readers_random_without_page_index,
    compare_readers_sorted_with_page_index,
    compare_readers_random_with_page_index,
);
criterion_main!(benches);
