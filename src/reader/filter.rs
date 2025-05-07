use std::sync::Arc;

use arrow::array::{BooleanArray, RecordBatch};
use arrow_schema::{ArrowError, Field, Schema};
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::{ArrowPredicate, RowSelection, statistics::StatisticsConverter},
    },
    errors::ParquetError,
    file::metadata::ParquetMetaData,
};

pub trait Predicate: ArrowPredicate + Send + 'static {
    fn col(&self) -> &str;
}

pub struct RowFilter {
    /// A list of [`ArrowPredicate`]
    pub(crate) predicates: Vec<Box<dyn ArrowPredicate>>,
    // pub(crate) predicates: Vec<Box<dyn Predicate>>,
}

impl RowFilter {
    /// Create a new [`RowFilter`] from an array of [`ArrowPredicate`]
    pub fn new(predicates: Vec<Box<dyn ArrowPredicate>>) -> Self {
        Self { predicates }
    }
}

impl From<RowFilter> for parquet::arrow::arrow_reader::RowFilter {
    fn from(filter: RowFilter) -> Self {
        parquet::arrow::arrow_reader::RowFilter::new(filter.predicates)
    }
}

pub struct AislePredicate<F> {
    projection: ProjectionMask,
    // p: ArrowPredicateFn<F>,
    f: F,
}

impl<F> AislePredicate<F>
where
    F: FnMut(RecordBatch) -> Result<BooleanArray, ArrowError> + Send + 'static,
{
    /// Create a new [`ArrowPredicateFn`]. `f` will be passed batches
    /// that contains the columns specified in `projection`
    /// and returns a [`BooleanArray`] that describes which rows should
    /// be passed along
    pub fn new(projection: ProjectionMask, f: F) -> Self {
        // projection.leaf_included(leaf_idx)
        // f(a, b);
        Self { projection, f }
    }

    // pub(crate) fn to_selection(&self) {}
}

// impl<F> Predicate for AislePredicate<F>
// where
//     F: Send + 'static,
// {
//     fn col(&self) -> &str {
//         &self.col
//     }
// }

impl<F> ArrowPredicate for AislePredicate<F>
where
    F: FnMut(RecordBatch) -> Result<BooleanArray, ArrowError> + Send + 'static,
{
    fn projection(&self) -> &ProjectionMask {
        &self.projection
    }

    fn evaluate(
        &mut self,
        batch: arrow::array::RecordBatch,
    ) -> std::result::Result<arrow::array::BooleanArray, arrow_schema::ArrowError> {
        (self.f)(batch)
    }
}

pub(crate) fn evaluate_predicate(
    metadata: &ParquetMetaData,
    arrow_schema: &Schema,
    row_group_idx: usize,
    // input_selection: Option<RowSelection>,
    predicate: &mut dyn ArrowPredicate,
) -> Result<RowSelection, ParquetError> {
    // rg.column(0).

    let projection = predicate.projection();
    let rg = metadata.row_group(row_group_idx);

    let mut p_schema = vec![];
    let mut max_batch = vec![];
    let mut min_batch = vec![];
    for (_idx, col) in rg
        .columns()
        .iter()
        .enumerate()
        .filter(|(idx, _col)| projection.leaf_included(*idx))
    {
        let col_name = col.column_descr().name();
        let convert = StatisticsConverter::try_new(
            col_name,
            arrow_schema,
            metadata.file_metadata().schema_descr(),
        )
        .unwrap();
        let mins = convert
            .data_page_mins(
                metadata.column_index().unwrap(),
                metadata.offset_index().unwrap(),
                [row_group_idx].iter(),
            )
            .unwrap();
        let maxs = convert
            .data_page_maxes(
                metadata.column_index().unwrap(),
                metadata.offset_index().unwrap(),
                [row_group_idx].iter(),
            )
            .unwrap();
        p_schema.push(Field::new(
            col_name,
            mins.data_type().clone(),
            mins.is_nullable(),
        ));
        max_batch.push(maxs);
        min_batch.push(mins);
    }

    let max_record_batch =
        RecordBatch::try_new(Arc::new(Schema::new(p_schema.clone())), max_batch).unwrap();
    let min_record_batch =
        RecordBatch::try_new(Arc::new(Schema::new(p_schema)), min_batch).unwrap();

    let max_array = predicate.evaluate(min_record_batch)?;
    let min_array = predicate.evaluate(max_record_batch)?;

    let filters = BooleanArray::from_iter(
        max_array
            .iter()
            .zip(min_array.iter())
            .map(|(max, min)| Some(max.unwrap_or(true) & min.unwrap_or(true))),
    );
    Ok(RowSelection::from_filters(&[filters]))
}
