use arrow::array::{BooleanArray, RecordBatch};
use arrow_schema::ArrowError;
use parquet::arrow::{ProjectionMask, arrow_reader::ArrowPredicate};

use super::filter::Filter;

#[derive(Clone)]
pub struct AislePredicate<F> {
    projection: ProjectionMask,
    f: F,
}

impl<F> AislePredicate<F>
where
    F: FnMut(RecordBatch) -> Result<Filter, ArrowError> + Send + 'static,
{
    /// Create a new [`AislePredicate`]. `f` will be passed batches
    /// that contains the columns specified in `projection`
    /// and returns a [`Filter`] that describes which rows should
    /// be passed along
    pub fn new(projection: ProjectionMask, f: F) -> Self {
        Self { projection, f }
    }
}

impl<F> ArrowPredicate for AislePredicate<F>
where
    F: FnMut(RecordBatch) -> Result<Filter, ArrowError> + Send + 'static,
{
    fn projection(&self) -> &ProjectionMask {
        &self.projection
    }

    fn evaluate(
        &mut self,
        batch: arrow::array::RecordBatch,
    ) -> std::result::Result<BooleanArray, arrow_schema::ArrowError> {
        let filter = (self.f)(batch)?;
        Ok(filter.array)
    }
}
