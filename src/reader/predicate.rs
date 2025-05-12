use arrow::array::{BooleanArray, RecordBatch};
use arrow_schema::ArrowError;
use parquet::arrow::{ProjectionMask, arrow_reader::ArrowPredicate};

use super::filter::Filter;

pub trait AislePredicate: Send + 'static {
    fn projection(&self) -> &ProjectionMask;

    fn evaluate(&mut self, batch: RecordBatch) -> Result<Filter, ArrowError>;
}

#[derive(Clone)]
pub struct AislePredicateFn<F> {
    projection: ProjectionMask,
    f: F,
}

impl<F> AislePredicateFn<F>
where
    F: FnMut(RecordBatch) -> Result<Filter, ArrowError> + Send + 'static,
{
    /// Create a new [`AislePredicateFn`]. `f` will be passed batches
    /// that contains the columns specified in `projection`
    /// and returns a [`Filter`] that describes which rows should
    /// be passed along
    pub fn new(projection: ProjectionMask, f: F) -> Self {
        Self { projection, f }
    }
}

impl<F> AislePredicate for AislePredicateFn<F>
where
    F: FnMut(RecordBatch) -> Result<Filter, ArrowError> + Send + 'static,
{
    fn evaluate(&mut self, batch: RecordBatch) -> Result<Filter, ArrowError> {
        (self.f)(batch)
    }

    fn projection(&self) -> &ProjectionMask {
        &self.projection
    }
}

pub(crate) struct AisleArrowPredicate(Box<dyn AislePredicate>);

impl AisleArrowPredicate {
    pub(crate) fn new(predicate: Box<dyn AislePredicate>) -> Self {
        Self(predicate)
    }
}

impl ArrowPredicate for AisleArrowPredicate {
    fn projection(&self) -> &ProjectionMask {
        self.0.projection()
    }

    fn evaluate(&mut self, batch: RecordBatch) -> Result<BooleanArray, ArrowError> {
        let filter = self.0.evaluate(batch)?;
        Ok(filter.array)
    }
}
