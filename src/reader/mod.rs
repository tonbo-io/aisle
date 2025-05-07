use filter::{RowFilter, evaluate_predicate};
pub use parquet::arrow::ProjectionMask;
use parquet::{
    arrow::{
        arrow_reader::ArrowReaderOptions,
        async_reader::{AsyncFileReader, ParquetRecordBatchStream},
    },
    errors::ParquetError,
};
use std::result::Result;

pub mod filter;

pub struct ParquetRecordBatchStreamBuilder<T>
where
    T: AsyncFileReader,
{
    reader: T,
    options: ArrowReaderOptions,
    // pub(crate) schema: SchemaRef,
    projection: ProjectionMask,
    row_groups: Option<Vec<usize>>,
    filter: Option<RowFilter>,
    limit: Option<usize>,
}

impl<T> ParquetRecordBatchStreamBuilder<T>
where
    T: AsyncFileReader + 'static,
{
    pub fn new(reader: T) -> Self {
        Self {
            reader,
            options: ArrowReaderOptions::default(),
            projection: ProjectionMask::all(),
            row_groups: None,
            filter: None,
            limit: None,
        }
    }

    pub fn new_with_options(reader: T, options: ArrowReaderOptions) -> Self {
        Self {
            reader,
            options,
            projection: ProjectionMask::all(),
            row_groups: None,
            filter: None,
            limit: None,
        }
    }

    pub async fn build(self) -> Result<ParquetRecordBatchStream<T>, ParquetError> {
        let mut builder =
            parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new_with_options(
                self.reader,
                self.options,
            )
            .await?
            .with_projection(self.projection);

        let metadata = builder.metadata();
        let schema = builder.schema();

        // TOOD: using statistics to skip row groups
        if let Some(mut filter) = self.filter {
            for predicate in filter.predicates.iter_mut() {
                let _selection = evaluate_predicate(metadata, schema, 0, predicate.as_mut())?;
                // selection.selects_any()
                // selection.intersection(other)
                // p.eval
            }

            builder = builder.with_row_filter(filter.into());
        }

        if let Some(row_groups) = self.row_groups {
            builder = builder.with_row_groups(row_groups);
        }
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
}
