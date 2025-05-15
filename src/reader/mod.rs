use std::{result::Result, sync::Arc};

use arrow_schema::SchemaRef;
use filter::{RowFilter, evaluate_predicate, filter_row_groups};
pub use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::ArrowReaderOptions,
        async_reader::{AsyncFileReader, ParquetRecordBatchStream},
    },
    errors::ParquetError,
    file::metadata::ParquetMetaData,
    schema::types::SchemaDescriptor,
};

pub mod filter;
pub mod predicate;

pub struct ParquetRecordBatchStreamBuilder<T>
where
    T: AsyncFileReader,
{
    builder: parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder<T>,
    projection: ProjectionMask,
    row_groups: Option<Vec<usize>>,
    filter: Option<RowFilter>,
    limit: Option<usize>,
    enable_page_index: bool,
}

impl<T> ParquetRecordBatchStreamBuilder<T>
where
    T: AsyncFileReader + 'static,
{
    /// Create a new [`ParquetRecordBatchStreamBuilder`] with the provided async source.
    pub async fn new(reader: T) -> Result<Self, ParquetError> {
        let builder =
            parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new(reader).await?;

        Ok(Self {
            builder,
            projection: ProjectionMask::all(),
            row_groups: None,
            filter: None,
            limit: None,
            enable_page_index: false,
        })
    }

    /// Create a new [`ParquetRecordBatchStreamBuilder`] with the provided async source
    /// and [`ArrowReaderOptions`].
    pub async fn new_with_options(
        reader: T,
        options: ArrowReaderOptions,
    ) -> Result<Self, ParquetError> {
        let enable_page_index = options.page_index();
        let builder =
            parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new_with_options(
                reader, options,
            )
            .await?;

        Ok(Self {
            builder,
            projection: ProjectionMask::all(),
            row_groups: None,
            filter: None,
            limit: None,
            enable_page_index,
        })
    }

    /// Build a new [`ParquetRecordBatchStream`]
    pub fn build(mut self) -> Result<ParquetRecordBatchStream<T>, ParquetError> {
        let mut builder = self.builder;
        let metadata = builder.metadata();
        let arrow_schema = builder.schema();

        let mut row_groups = match self.row_groups.take() {
            Some(row_groups) => row_groups,
            None => (0..metadata.num_row_groups()).collect::<Vec<usize>>(),
        };

        if let Some(mut filter) = self.filter {
            for predicate in filter.predicates.iter_mut() {
                row_groups =
                    filter_row_groups(metadata, arrow_schema, &row_groups, predicate.as_mut())?;
            }

            // FIXME: replace with enable_page_index
            // if metadata.column_index().is_some() {
            if self.enable_page_index {
                let mut selection = None;
                for predicate in filter.predicates.iter_mut() {
                    selection = Some(evaluate_predicate(
                        metadata,
                        arrow_schema,
                        &row_groups,
                        selection,
                        predicate.as_mut(),
                    )?);
                }
                if let Some(selection) = selection {
                    builder = builder.with_row_selection(selection);
                }
            }

            builder = builder.with_row_filter(filter.into());
        }

        if let Some(limit) = self.limit {
            builder = builder.with_limit(limit);
        }

        builder
            .with_projection(self.projection)
            .with_row_groups(row_groups)
            .build()
    }

    /// convert this builder to [`parquet::arrow::ParquetRecordBatchStreamBuilder`]
    pub fn into_raw_builder(self) -> parquet::arrow::ParquetRecordBatchStreamBuilder<T> {
        let mut builder = self.builder;

        if let Some(row_groups) = self.row_groups {
            builder = builder.with_row_groups(row_groups)
        }

        if let Some(filter) = self.filter {
            builder = builder.with_row_filter(filter.into());
        }

        if let Some(limit) = self.limit {
            builder = builder.with_limit(limit);
        }
        builder.with_projection(self.projection)
    }
}

impl<T> ParquetRecordBatchStreamBuilder<T>
where
    T: AsyncFileReader + 'static,
{
    /// Only read data from the provided row group indexes
    ///
    /// This is also called row group filtering
    pub fn with_row_groups(self, row_groups: Vec<usize>) -> Self {
        Self {
            row_groups: Some(row_groups),
            ..self
        }
    }

    /// Provide a [`RowFilter`] to skip decoding rows
    ///
    /// Row filters are applied to row group statistics and page statistics and finally applied to rows
    ///
    /// See [`parquet::arrow::ParquetRecordBatchStreamBuilder::with_row_filter`] for more detail
    pub fn with_row_filter(self, filter: RowFilter) -> Self {
        Self {
            filter: Some(filter),
            ..self
        }
    }

    /// Only read data from the provided column indexes
    pub fn with_projection(self, mask: ProjectionMask) -> Self {
        Self {
            projection: mask,
            ..self
        }
    }

    /// Provide a limit to the number of rows to be read
    ///
    /// See [`parquet::arrow::ParquetRecordBatchStreamBuilder::with_limit`] for more detail
    pub fn with_limit(self, limit: usize) -> Self {
        Self {
            limit: Some(limit),
            ..self
        }
    }

    /// Returns a reference to the [`ParquetMetaData`] for this parquet file
    pub fn metadata(&self) -> &Arc<ParquetMetaData> {
        self.builder.metadata()
    }

    /// Returns the parquet [`SchemaDescriptor`] for this parquet file
    pub fn parquet_schema(&self) -> &SchemaDescriptor {
        self.builder.parquet_schema()
    }

    /// Returns the arrow [`SchemaRef`] for this parquet file
    pub fn schema(&self) -> &SchemaRef {
        self.builder.schema()
    }

    /// Read bloom filter for a column in a row group
    ///
    /// Returns `None` if the column does not have a bloom filter
    ///
    /// We should call this function after other forms pruning, such as projection and predicate
    /// pushdown.
    pub async fn get_row_group_column_bloom_filter(
        &mut self,
        row_group_idx: usize,
        column_idx: usize,
    ) -> Result<Option<parquet::bloom_filter::Sbbf>, ParquetError> {
        self.builder
            .get_row_group_column_bloom_filter(row_group_idx, column_idx)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, AsArray, Datum, RecordBatch, StringArray, UInt8Array, UInt64Array},
        datatypes::UInt64Type,
    };
    use arrow_schema::{DataType, Field, Schema};
    use fusio::{DynFs, disk::TokioFs, fs::OpenOptions, path::Path};
    use fusio_parquet::{reader::AsyncReader, writer::AsyncWriter};
    use futures_util::StreamExt;
    use parquet::{
        arrow::{
            AsyncArrowWriter, ProjectionMask,
            arrow_reader::{ArrowReaderOptions, statistics::StatisticsConverter},
            async_reader::AsyncFileReader,
        },
        basic::Compression,
        errors::ParquetError,
        file::{
            metadata::ParquetMetaData,
            properties::{EnabledStatistics, WriterProperties},
        },
        format::SortingColumn,
        schema::types::ColumnPath,
    };
    use rand::{Rng, thread_rng};

    use super::predicate::AislePredicate;
    use crate::{
        ord::{gt, gt_eq, lt, lt_eq},
        reader::{ParquetRecordBatchStreamBuilder, filter::RowFilter, predicate::AislePredicateFn},
    };

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
            ids.push(rng.gen_range(0..record_size * 10 + i) as u64);
            ages.push((rng.gen_range(0..record_size * 10 + i) % 256) as u8);
            names.push(format!("{:08}", rng.gen_range(0..record_size * 10 + i)));
        }
        RecordBatch::try_from_iter(vec![
            ("id", Arc::new(UInt64Array::from(ids)) as ArrayRef),
            ("name", Arc::new(StringArray::from(names)) as ArrayRef),
            ("age", Arc::new(UInt8Array::from(ages)) as ArrayRef),
        ])
        .unwrap()
    }

    async fn load_data(
        parquet_path: Path,
        writer_properties: WriterProperties,
        record_size: usize,
    ) {
        let ordered = writer_properties.sorting_columns().is_some();
        let fs = Arc::new(TokioFs {}) as Arc<dyn DynFs>;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::UInt8, false),
        ]));

        let mut writer = AsyncArrowWriter::try_new(
            AsyncWriter::new(
                fs.open_options(
                    &parquet_path,
                    OpenOptions::default().create(true).write(true),
                )
                .await
                .unwrap(),
            ),
            schema.clone(),
            Some(writer_properties),
        )
        .unwrap();
        if ordered {
            writer
                .write(&get_ordered_record_batch(record_size))
                .await
                .unwrap();
        } else {
            writer
                .write(&get_random_record_batch(record_size))
                .await
                .unwrap();
        }
        writer.close().await.unwrap();
    }

    async fn try_load_data(
        dir: &str,
        filename: &str,
        writer_properties: WriterProperties,
        record_size: usize,
    ) {
        let parquet_path = format!("{}/{}", dir, filename);

        if !std::path::Path::new(&parquet_path).exists() {
            if !std::path::Path::new(dir).exists() {
                std::fs::create_dir_all(dir).unwrap();
            }
            let path = Path::new(dir).unwrap().child(filename);
            load_data(path, writer_properties, record_size).await;
        }
    }

    async fn get_parquet_metadata(path: &Path) -> Result<Arc<ParquetMetaData>, ParquetError> {
        let fs = Arc::new(TokioFs);

        let file = fs
            .open_options(path, OpenOptions::default().read(true))
            .await
            .unwrap();
        let size = file.size().await.unwrap();
        let mut reader = AsyncReader::new(file, size).await.unwrap();
        reader
            .get_metadata(Some(&ArrowReaderOptions::new().with_page_index(true)))
            .await
    }

    async fn read_parquet(
        path: &Path,
        predicates: Vec<Box<dyn AislePredicate>>,
        enable_page_filter: bool,
        enable_row_group_filter: bool,
    ) -> Result<Vec<RecordBatch>, ParquetError> {
        let fs = Arc::new(TokioFs);

        let mut batches = vec![];
        let file = fs
            .open_options(path, OpenOptions::default().read(true))
            .await
            .unwrap();
        let size = file.size().await.unwrap();
        let reader = AsyncReader::new(file, size).await.unwrap();

        if !enable_page_filter && !enable_row_group_filter {
            let builder = parquet::arrow::ParquetRecordBatchStreamBuilder::new(reader)
                .await
                .unwrap();

            let mut reader = builder
                .with_row_filter(RowFilter::new(predicates).into())
                .build()?;

            while let Some(batch) = reader.next().await {
                batches.push(batch?);
            }

            Ok(batches)
        } else {
            let options = ArrowReaderOptions::new().with_page_index(enable_page_filter);
            let builder = ParquetRecordBatchStreamBuilder::new_with_options(reader, options)
                .await
                .unwrap();

            let mut reader = builder
                .with_row_filter(RowFilter::new(predicates))
                .build()?;

            while let Some(batch) = reader.next().await {
                batches.push(batch?);
            }

            Ok(batches)
        }
    }

    fn writer_properties(
        column_paths: Option<Vec<String>>,
        sorting_column_indics: Option<Vec<usize>>,
    ) -> WriterProperties {
        let mut builder = WriterProperties::builder()
            .set_compression(Compression::LZ4)
            .set_created_by(concat!("aisle version ", env!("CARGO_PKG_VERSION")).to_owned());
        if let Some(column_paths) = column_paths {
            let column_paths = ColumnPath::new(column_paths);
            builder = builder
                .set_column_bloom_filter_enabled(column_paths.clone(), true)
                .set_column_statistics_enabled(column_paths.clone(), EnabledStatistics::Page);
            if let Some(sorting_columns) = sorting_column_indics {
                let sorting_columns: Vec<SortingColumn> = sorting_columns
                    .iter()
                    .map(|idx| SortingColumn::new(*idx as i32, true, true))
                    .collect();
                builder = builder.set_sorting_columns(Some(sorting_columns));
            }
        }
        builder.build()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_read_parquet_scan() {
        let properties = writer_properties(Some(vec!["id".into()]), Some(vec![0]));

        let filename = "data.parquet";
        let dir = "./data";
        try_load_data(dir, filename, properties, 8 * 1024 * 1024).await;

        let parquet_file_path = Path::new(dir).unwrap().child(filename);

        let metadata = get_parquet_metadata(&parquet_file_path).await.unwrap();
        let parquet_schema = metadata.file_metadata().schema_descr();

        let right_range_p = AislePredicateFn::new(
            ProjectionMask::roots(parquet_schema, vec![0]),
            move |batch| {
                let datum = Arc::new(UInt64Array::new_scalar(100 * 1024)) as Arc<dyn Datum>;
                gt(batch.column(0), datum.as_ref())
            },
        );
        let left_range_p = AislePredicateFn::new(
            ProjectionMask::roots(parquet_schema, vec![0]),
            move |batch| {
                let datum = Arc::new(UInt64Array::new_scalar(100 * 1024 + 100)) as Arc<dyn Datum>;
                lt(batch.column(0), datum.as_ref())
            },
        );

        let predicates: Vec<Box<dyn AislePredicate>> = vec![
            Box::new(right_range_p.clone()),
            Box::new(left_range_p.clone()),
        ];
        let predicates2: Vec<Box<dyn AislePredicate>> =
            vec![Box::new(right_range_p), Box::new(left_range_p)];

        {
            let expected: Vec<u64> = read_parquet(&parquet_file_path, predicates2, false, false)
                .await
                .unwrap()
                .iter()
                .flat_map(|batch| {
                    let ids = batch.column(0).as_primitive::<UInt64Type>();
                    ids.values().to_vec()
                })
                .collect();
            let actual: Vec<u64> = read_parquet(&parquet_file_path, predicates, true, true)
                .await
                .unwrap()
                .iter()
                .flat_map(|batch| {
                    let ids = batch.column(0).as_primitive::<UInt64Type>();
                    ids.values().to_vec()
                })
                .collect();
            assert_eq!(expected, actual);
        }
    }

    async fn read_parquet_scan_cross_page(
        writer_properties: WriterProperties,
        record_size: usize,
        dir: &str,
        filename: &str,
    ) {
        try_load_data(dir, filename, writer_properties, record_size).await;

        let parquet_file_path = Path::new(dir).unwrap().child(filename);

        let fs = TokioFs {};
        let file = fs
            .open_options(&parquet_file_path, OpenOptions::default().read(true))
            .await
            .unwrap();
        let size = file.size().await.unwrap();
        let reader = AsyncReader::new(file, size).await.unwrap();

        let mut builder = ParquetRecordBatchStreamBuilder::new(reader).await.unwrap();
        builder.enable_page_index = true;

        let metadata = builder.metadata();
        let arrow_schema = builder.schema();
        let parquet_schema = metadata.file_metadata().schema_descr();

        if let Some(column_index) = metadata.column_index() {
            let offset_index = metadata.offset_index().unwrap();
            let row_group_indices = (0..metadata.num_row_groups()).collect::<Vec<usize>>();
            let convert = StatisticsConverter::try_new("id", arrow_schema, parquet_schema).unwrap();
            let mins = convert
                .data_page_mins(column_index, offset_index, &row_group_indices)
                .unwrap();
            let maxes = convert
                .data_page_maxes(column_index, offset_index, &row_group_indices)
                .unwrap();

            let mut rng = thread_rng();
            let mut ranges = vec![];

            {
                // range include page
                let page_index = rng.gen_range(0..mins.len());
                let left = mins.get().0.as_primitive::<UInt64Type>().value(page_index) - 10;
                let right = maxes.get().0.as_primitive::<UInt64Type>().value(page_index) + 10;
                ranges.push(vec![(left, right)]);

                // range include page left bound
                let left = mins.get().0.as_primitive::<UInt64Type>().value(page_index) - 20;
                let right = maxes.get().0.as_primitive::<UInt64Type>().value(page_index) - 10;
                ranges.push(vec![(left, right)]);

                // range include page right bound
                let left = mins.get().0.as_primitive::<UInt64Type>().value(page_index) + 10;
                let right = maxes.get().0.as_primitive::<UInt64Type>().value(page_index) + 20;
                ranges.push(vec![(left, right)]);

                // overlapped two ranges
                let left1 = mins.get().0.as_primitive::<UInt64Type>().value(page_index) - 10;
                let right1 = maxes.get().0.as_primitive::<UInt64Type>().value(page_index) + 20;
                let left2 = right - 10;
                let right2 = maxes.get().0.as_primitive::<UInt64Type>().value(page_index) + 100;
                ranges.push(vec![(left1, right1), (left2, right2)]);

                // multiple ranges
                let page_idx1 = rng.gen_range(0..mins.len());
                let left1 = mins.get().0.as_primitive::<UInt64Type>().value(page_idx1) - 10;
                let right1 = maxes.get().0.as_primitive::<UInt64Type>().value(page_idx1) + 20;

                let page_idx2 = rng.gen_range(0..mins.len());
                let left2 = mins.get().0.as_primitive::<UInt64Type>().value(page_idx2) + 10;
                let right2 = maxes.get().0.as_primitive::<UInt64Type>().value(page_idx2) + 20;

                let page_idx3 = rng.gen_range(0..mins.len());
                let left3 = mins.get().0.as_primitive::<UInt64Type>().value(page_idx3) - 10;
                let right3 = maxes.get().0.as_primitive::<UInt64Type>().value(page_idx3) - 20;

                ranges.push(vec![(left1, right1), (left2, right2), (left3, right3)]);
            }

            for range in ranges {
                let mut predicates: Vec<Box<dyn AislePredicate>> = Vec::with_capacity(range.len());
                let mut predicates2: Vec<Box<dyn AislePredicate>> = Vec::with_capacity(range.len());
                for (left, right) in range {
                    let left_range_p = AislePredicateFn::new(
                        ProjectionMask::roots(parquet_schema, vec![0]),
                        move |batch| {
                            let datum = Arc::new(UInt64Array::new_scalar(left)) as Arc<dyn Datum>;
                            gt(batch.column(0), datum.as_ref())
                        },
                    );

                    let right_range_p = AislePredicateFn::new(
                        ProjectionMask::roots(parquet_schema, vec![0]),
                        move |batch| {
                            let datum = Arc::new(UInt64Array::new_scalar(right)) as Arc<dyn Datum>;
                            lt(batch.column(0), datum.as_ref())
                        },
                    );
                    predicates.push(Box::new(left_range_p.clone()));
                    predicates.push(Box::new(right_range_p.clone()));

                    predicates2.push(Box::new(left_range_p));
                    predicates2.push(Box::new(right_range_p));
                }

                {
                    let expected: Vec<u64> =
                        read_parquet(&parquet_file_path, predicates2, false, false)
                            .await
                            .unwrap()
                            .iter()
                            .flat_map(|batch| {
                                let ids = batch.column(0).as_primitive::<UInt64Type>();
                                ids.values().to_vec()
                            })
                            .collect();

                    let actual: Vec<u64> = read_parquet(&parquet_file_path, predicates, true, true)
                        .await
                        .unwrap()
                        .iter()
                        .flat_map(|batch| {
                            let ids = batch.column(0).as_primitive::<UInt64Type>();
                            ids.values().to_vec()
                        })
                        .collect();

                    assert_eq!(expected, actual);
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_read_parquet_scan_cross_page_ordered() {
        let properties = writer_properties(Some(vec!["id".to_string()]), Some(vec![0]));
        read_parquet_scan_cross_page(properties, 8 * 1024 * 1024, "./data", "data.parquet").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_read_parquet_scan_cross_page_unordered() {
        let properties = writer_properties(None, None);
        read_parquet_scan_cross_page(properties, 8 * 1024 * 1024, "./data", "random.parquet").await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_read_parquet_eq() {
        let properties = writer_properties(Some(vec!["id".into()]), Some(vec![0]));

        let filename = "data.parquet";
        let dir = "./data";
        try_load_data(dir, filename, properties, 8 * 1024 * 1024).await;

        let parquet_file_path = Path::new(dir).unwrap().child(filename);

        let metadata = get_parquet_metadata(&parquet_file_path).await.unwrap();
        let parquet_schema = metadata.file_metadata().schema_descr();

        let mut rng = thread_rng();
        let key = rng.gen_range(0..8 * 1024 * 1024);
        let right_range_p = AislePredicateFn::new(
            ProjectionMask::roots(parquet_schema, vec![0]),
            move |batch| {
                let datum = Arc::new(UInt64Array::new_scalar(key)) as Arc<dyn Datum>;
                gt_eq(batch.column(0), datum.as_ref())
            },
        );
        let left_range_p = AislePredicateFn::new(
            ProjectionMask::roots(parquet_schema, vec![0]),
            move |batch| {
                let datum = Arc::new(UInt64Array::new_scalar(key)) as Arc<dyn Datum>;
                lt_eq(batch.column(0), datum.as_ref())
            },
        );

        let predicates: Vec<Box<dyn AislePredicate>> = vec![
            Box::new(right_range_p.clone()),
            Box::new(left_range_p.clone()),
        ];
        let predicates2: Vec<Box<dyn AislePredicate>> =
            vec![Box::new(right_range_p), Box::new(left_range_p)];

        {
            let expected: Vec<u64> = read_parquet(&parquet_file_path, predicates2, false, false)
                .await
                .unwrap()
                .iter()
                .flat_map(|batch| {
                    let ids = batch.column(0).as_primitive::<UInt64Type>();
                    ids.values().to_vec()
                })
                .collect();
            let actual: Vec<u64> = read_parquet(&parquet_file_path, predicates, true, true)
                .await
                .unwrap()
                .iter()
                .flat_map(|batch| {
                    let ids = batch.column(0).as_primitive::<UInt64Type>();
                    ids.values().to_vec()
                })
                .collect();
            assert_eq!(expected, actual);
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_read_parquet_compare() {
        let properties = writer_properties(Some(vec!["id".to_string()]), Some(vec![0]));

        let filename = "data.parquet";
        let dir = "./data";
        let record_num = 8 * 1024 * 1024;
        try_load_data(dir, filename, properties, record_num).await;

        let parquet_file_path = Path::new(dir).unwrap().child(filename);

        let metadata = get_parquet_metadata(&parquet_file_path).await.unwrap();
        let parquet_schema = metadata.file_metadata().schema_descr();

        let loops = 10;
        let mut ranges = vec![];
        for _ in 0..loops {
            let mut rng = thread_rng();
            let left = rng.gen_range(0..record_num);
            let right = rng.gen_range(left..record_num);

            let left_range_p = AislePredicateFn::new(
                ProjectionMask::roots(parquet_schema, vec![0]),
                move |batch| {
                    let datum = Arc::new(UInt64Array::new_scalar(left as u64)) as Arc<dyn Datum>;
                    gt(batch.column(0), datum.as_ref())
                },
            );
            let right_range_p = AislePredicateFn::new(
                ProjectionMask::roots(parquet_schema, vec![0]),
                move |batch| {
                    let datum = Arc::new(UInt64Array::new_scalar(right as u64)) as Arc<dyn Datum>;
                    lt(batch.column(0), datum.as_ref())
                },
            );
            ranges.push((left_range_p, right_range_p));
        }
        let start = std::time::Instant::now();
        for (left, right) in ranges.iter() {
            let predicates: Vec<Box<dyn AislePredicate>> =
                vec![Box::new(left.clone()), Box::new(right.clone())];

            read_parquet(&parquet_file_path, predicates, false, false)
                .await
                .unwrap();
        }
        let time = start.elapsed().as_millis() as f64 / 1000.0;

        let start = std::time::Instant::now();
        for (left, right) in ranges.iter() {
            let predicates: Vec<Box<dyn AislePredicate>> =
                vec![Box::new(left.clone()), Box::new(right.clone())];

            read_parquet(&parquet_file_path, predicates, true, true)
                .await
                .unwrap();
        }
        let time2 = start.elapsed().as_millis() as f64 / 1000.0;
        println!("----------------------------");
        println!("read parquet without pushdown: {:.4}", time / loops as f64);
        println!("read parquet with pushdown: {:.4}", time2 / loops as f64);
        println!("----------------------------");
    }
}
