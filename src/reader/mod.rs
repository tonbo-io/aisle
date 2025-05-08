use std::result::Result;

use filter::{RowFilter, evaluate_predicate};
pub use parquet::arrow::ProjectionMask;
use parquet::{
    arrow::{
        arrow_reader::ArrowReaderOptions,
        async_reader::{AsyncFileReader, ParquetRecordBatchStream},
    },
    errors::ParquetError,
    file::metadata::ParquetMetaData,
    schema::types::SchemaDescriptor,
};

pub mod filter;

pub struct ParquetRecordBatchStreamBuilder<T>
where
    T: AsyncFileReader,
{
    // pub(crate) schema: SchemaRef,
    builder: parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder<T>,
    projection: ProjectionMask,
    row_groups: Option<Vec<usize>>,
    filter: Option<RowFilter>,
    limit: Option<usize>,
    enable_page_filter: bool,
}

impl<T> ParquetRecordBatchStreamBuilder<T>
where
    T: AsyncFileReader + 'static,
{
    pub async fn new(reader: T) -> Result<Self, ParquetError> {
        let builder =
            parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new(reader).await?;

        Ok(Self {
            builder,
            projection: ProjectionMask::all(),
            row_groups: None,
            filter: None,
            limit: None,
            enable_page_filter: false,
        })
    }

    pub async fn new_with_options(
        reader: T,
        options: ArrowReaderOptions,
    ) -> Result<Self, ParquetError> {
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
            enable_page_filter: false,
        })
    }

    pub async fn build(self) -> Result<ParquetRecordBatchStream<T>, ParquetError> {
        let mut builder = self.builder;
        let metadata = builder.metadata();
        let schema = builder.schema();

        // TOOD: using statistics to skip row groups
        if let Some(mut filter) = self.filter {
            // if let Some(row_groups) = self.row_groups.as_ref() {
            //     for _rg in row_groups.iter() {
            //         // rg.stat
            //     }
            // } else {

            // }
            let row_groups = (0..metadata.num_row_groups()).collect::<Vec<usize>>();
            if self.enable_page_filter {
                let mut selection = None;
                for predicate in filter.predicates.iter_mut() {
                    selection = Some(evaluate_predicate(
                        metadata,
                        schema,
                        &row_groups,
                        selection,
                        predicate.as_mut(),
                    )?);
                    dbg!(&selection);
                }
                if let Some(selection) = selection {
                    builder = builder.with_row_selection(selection);
                }
            }

            builder = builder.with_row_filter(filter.into());
        }

        // if let Some(row_groups) = self.row_groups {
        //     builder = builder.with_row_groups(row_groups);
        // }
        if let Some(limit) = self.limit {
            builder = builder.with_limit(limit);
        }

        builder.build()
    }
}

impl<T> ParquetRecordBatchStreamBuilder<T>
where
    T: AsyncFileReader + 'static,
{
    pub fn with_row_groups(self, row_groups: Vec<usize>) -> Self {
        Self {
            row_groups: Some(row_groups),
            ..self
        }
    }

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

    pub fn with_limit(self, limit: usize) -> Self {
        Self {
            limit: Some(limit),
            ..self
        }
    }

    /// use page statistics to filter.
    ///
    /// Note: only simple comparison operations like >, <, >=, <= are supported. Here are some cases
    /// that not supported:
    /// - `a % 100 == 0`
    /// - `a  == 0`
    pub fn with_page_filter(self, enable_page_filter: bool) -> Self {
        Self {
            enable_page_filter,
            ..self
        }
    }

    pub fn metadata(&self) -> &ParquetMetaData {
        self.builder.metadata()
    }

    pub fn parquet_schema(&self) -> &SchemaDescriptor {
        self.builder.parquet_schema()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::{
        array::{ArrayRef, Datum, RecordBatch, StringArray, UInt8Array, UInt64Array},
        compute::kernels::cmp::{gt, lt},
    };
    use arrow_schema::{DataType, Field, Schema};
    use fusio::{DynFs, disk::TokioFs, fs::OpenOptions, path::Path};
    use fusio_parquet::{reader::AsyncReader, writer::AsyncWriter};
    use futures_util::StreamExt;
    use parquet::{
        arrow::{AsyncArrowWriter, ProjectionMask, arrow_reader::ArrowPredicate},
        basic::Compression,
        file::properties::{EnabledStatistics, WriterProperties},
        format::SortingColumn,
        schema::types::ColumnPath,
    };

    use crate::reader::{
        ParquetRecordBatchStreamBuilder,
        filter::{AislePredicate, RowFilter},
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

    // fn get_random_record_batch(record_size: usize) -> RecordBatch {
    //     let mut ids = vec![];
    //     let mut ages = vec![];
    //     let mut names = vec![];
    //     for i in 0..record_size {
    //         ids.push(i as u64);
    //         ages.push((i % 256) as u8);
    //         names.push(format!("{:08}", i));
    //     }
    //     RecordBatch::try_from_iter(vec![
    //         ("id", Arc::new(UInt64Array::from(ids)) as ArrayRef),
    //         ("name", Arc::new(StringArray::from(names)) as ArrayRef),
    //         ("age", Arc::new(UInt8Array::from(ages)) as ArrayRef),
    //     ])
    //     .unwrap()
    // }

    async fn load_ordered_data(
        parquet_path: Path,
        writer_properties: WriterProperties,
        record_batch: RecordBatch,
    ) {
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
        writer.write(&record_batch).await.unwrap();
        writer.close().await.unwrap();
    }

    async fn try_load_data(
        dir: &str,
        filename: &str,
        writer_properties: WriterProperties,
        ordered: bool,
    ) {
        let parquet_path = format!("{}/{}", dir, filename);

        if !std::path::Path::new(&parquet_path).exists() {
            if !std::path::Path::new(dir).exists() {
                std::fs::create_dir_all(dir).unwrap();
            }
            let path = Path::new(dir).unwrap().child(filename);
            if ordered {
                load_ordered_data(
                    path,
                    writer_properties,
                    get_ordered_record_batch(8 * 1024 * 1024),
                )
                .await;
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_read_parquet() {
        let column_paths = ColumnPath::new(vec!["id".to_string()]);
        let sorting_columns = vec![SortingColumn::new(0, true, true)];
        let properties = WriterProperties::builder()
            // .set_max_row_group_size(1024 * 16)
            // .set_data_page_size_limit(1024 * 4)
            .set_compression(Compression::LZ4)
            .set_column_statistics_enabled(column_paths.clone(), EnabledStatistics::Page)
            .set_column_bloom_filter_enabled(column_paths.clone(), true)
            .set_sorting_columns(Some(sorting_columns))
            .set_created_by(concat!("aisle version ", env!("CARGO_PKG_VERSION")).to_owned())
            .build();

        let filename = "data.parquet";
        let dir = "./data";
        try_load_data("./data", filename, properties, true).await;

        let fs = Arc::new(TokioFs);

        let file = fs
            .open_options(
                &Path::new(dir).unwrap().child(filename),
                OpenOptions::default().read(true),
            )
            .await
            .unwrap();
        let size = file.size().await.unwrap();
        let reader = AsyncReader::new(file, size).await.unwrap();
        let builder = ParquetRecordBatchStreamBuilder::new(reader).await.unwrap();
        let parquet_schema = builder.parquet_schema();
        let predicates: Vec<Box<dyn ArrowPredicate>> = vec![
            Box::new(AislePredicate::new(
                ProjectionMask::roots(parquet_schema, vec![0]),
                move |batch| {
                    let datum = Arc::new(UInt64Array::new_scalar(100 * 1024)) as Arc<dyn Datum>;
                    gt(batch.column(0), datum.as_ref())
                },
            )),
            Box::new(AislePredicate::new(
                ProjectionMask::roots(parquet_schema, vec![0]),
                move |batch| {
                    let datum =
                        Arc::new(UInt64Array::new_scalar(100 * 1024 + 100)) as Arc<dyn Datum>;
                    lt(batch.column(0), datum.as_ref())
                },
            )),
        ];
        let start = std::time::Instant::now();
        let mut reader = builder
            .with_row_filter(RowFilter::new(predicates))
            .with_page_filter(true)
            .build()
            .await
            .unwrap();
        while let Some(record) = reader.next().await {
            assert!(record.is_ok());
            let record_batch = record.unwrap();
            dbg!(record_batch.column(0));
            // dbg!(record_batch);
        }
        let time = start.elapsed().as_millis() as f64 / 1000.0;
        println!("----------------------------");
        println!("read parquet: {:.4}", time);
        println!("----------------------------");
    }
}
