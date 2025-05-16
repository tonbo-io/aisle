use std::sync::Arc;

use aisle::{
    predicate::AislePredicate,
    reader::{predicate::AislePredicateFn, ParquetRecordBatchStreamBuilder},
    ProjectionMask,
};
use arrow::{
    array::{ArrayRef, Datum, RecordBatch, StringArray, UInt64Array, UInt8Array},
    datatypes::{DataType, Field, Schema},
};
use fusio::{disk::TokioFs, fs::OpenOptions, path::Path, DynFs};
use fusio_parquet::{reader::AsyncReader, writer::AsyncWriter};
use futures_util::StreamExt;
use parquet::{
    arrow::{
        arrow_reader::{ArrowPredicate, ArrowPredicateFn, ArrowReaderOptions},
        AsyncArrowWriter,
    },
    basic::Compression,
    file::properties::{EnabledStatistics, WriterProperties},
    format::SortingColumn,
    schema::types::ColumnPath,
};
use rand::{thread_rng, Rng};

const QUERY_TIMES: usize = 100;

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
        "bench_{}_{}M.parquet",
        sorted.then_some("sorted").unwrap_or("random"),
        record_size / 1048576
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

async fn bench_compare_readers(size: usize, ranges: Vec<(usize, usize)>, sorted: bool) -> Vec<f64> {
    let path = prepare_test_file(size, sorted).await;
    let mut times = vec![];
    let start = std::time::Instant::now();
    for (left, right) in ranges.iter() {
        bench_standard_reader(
            &path,
            (*left as u64, *right as u64),
            ArrowReaderOptions::new().with_page_index(false),
        )
        .await;
    }
    let time = start.elapsed().as_millis() as f64;
    times.push(time / ranges.len() as f64);

    let start = std::time::Instant::now();
    for (left, right) in ranges.iter() {
        bench_pushdown_reader(
            &path,
            (*left as u64, *right as u64),
            ArrowReaderOptions::new().with_page_index(false),
        )
        .await;
    }
    let time = start.elapsed().as_millis() as f64;
    times.push(time / ranges.len() as f64);

    let start = std::time::Instant::now();
    for (left, right) in ranges.iter() {
        bench_standard_reader(
            &path,
            (*left as u64, *right as u64),
            ArrowReaderOptions::new().with_page_index(true),
        )
        .await;
    }
    let time = start.elapsed().as_millis() as f64;
    times.push(time / ranges.len() as f64);

    let start = std::time::Instant::now();
    for (left, right) in ranges.iter() {
        bench_pushdown_reader(
            &path,
            (*left as u64, *right as u64),
            ArrowReaderOptions::new().with_page_index(true),
        )
        .await;
    }
    let time = start.elapsed().as_millis() as f64;
    times.push(time / ranges.len() as f64);

    times
}

async fn compare_readers(sorted: bool, size: usize) -> Vec<f64> {
    let mut rng = thread_rng();
    let loop_num = rng.gen_range(QUERY_TIMES..QUERY_TIMES * 2);
    let mut ranges = Vec::with_capacity(loop_num);
    for _ in 0..loop_num {
        let left = rng.gen_range(0..size);
        let right = rng.gen_range(left..size);
        ranges.push((left, right));
    }
    bench_compare_readers(size, ranges, sorted).await
}

#[tokio::main]
async fn main() {
    let mut ordered = vec![];
    let mut unordered = vec![];
    for size in [
        1024 * 1024,
        2 * 1024 * 1024,
        4 * 1024 * 1024,
        8 * 1024 * 1024,
        16 * 1024 * 1024,
        32 * 1024 * 1024,
    ] {
        ordered.push(compare_readers(true, size).await);
        unordered.push(compare_readers(false, size).await);
    }
    println!("\t\tread ordered data (time in milliseconds)\t\t");
    println!(
        "+-----------------------------------------------------------------------------------------+"
    );
    println!(
        "|num records|      Parquet      |    Aisle    |Parquet + Page index |  Aisle + Page index |"
    );
    for (i, times) in ordered.iter().enumerate() {
        println!(
            "|{:>6}M    |   {:>12.4}    |{:>12.4} |\t{:>12.4}\t    |\t{:>12.4}\t  |",
            2_u32.pow(i as u32),
            times[0],
            times[1],
            times[2],
            times[3]
        );
    }
    println!(
        "+-----------------------------------------------------------------------------------------+"
    );
    println!();
    println!("\t\tread totally random data (time in milliseconds)\t\t");
    println!(
        "+-----------------------------------------------------------------------------------------+"
    );
    println!(
        "|num records|      Parquet      |    Aisle    |Parquet + Page index |  Aisle + Page index |"
    );
    for (i, times) in unordered.iter().enumerate() {
        println!(
            "|{:>6}M    |   {:>12.4}    |{:>12.4} |\t{:>12.4}\t    |\t{:>12.4}\t  |",
            2_u32.pow(i as u32),
            times[0],
            times[1],
            times[2],
            times[3]
        );
    }
    println!(
        "+-----------------------------------------------------------------------------------------+"
    );
}
