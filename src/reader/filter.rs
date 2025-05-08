use std::sync::Arc;

use arrow::array::{BooleanArray, RecordBatch};
use arrow_schema::{ArrowError, Field, Schema};
use parquet::{
    arrow::{
        ProjectionMask,
        arrow_reader::{
            ArrowPredicate, RowSelection, RowSelector, statistics::StatisticsConverter,
        },
    },
    errors::ParquetError,
    file::{
        metadata::{ColumnChunkMetaData, ParquetMetaData, RowGroupMetaData},
        statistics::Statistics,
    },
    format::PageLocation,
};

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
        Self { projection, f }
    }
}

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
    row_group_indices: &[usize],
    input_selection: Option<RowSelection>,
    predicate: &mut dyn ArrowPredicate,
) -> Result<RowSelection, ParquetError> {
    if metadata.row_groups().is_empty() || row_group_indices.is_empty() {
        return Ok(RowSelection::from_iter([]));
    }

    let rg = metadata.row_group(row_group_indices[0]);
    if metadata.column_index().is_none() || metadata.offset_index().is_none() {
        let total_rows = rg.column(0).num_values() as usize;
        return Ok(RowSelection::from_consecutive_ranges(
            [0..total_rows].into_iter(),
            total_rows,
        ));
    }

    let projection = predicate.projection();
    let projected_columns: Vec<(usize, &ColumnChunkMetaData)> = rg
        .columns()
        .iter()
        .enumerate()
        .filter(|(idx, _col)| projection.leaf_included(*idx))
        // .map(|(_, col)| col)
        .collect();

    let mut p_schema = vec![];
    let mut max_batch = vec![];
    let mut min_batch = vec![];
    if projected_columns.len() != 1 {
        // two case:
        //   1. projected_columns.len == 0: predicate does not work
        //   2. projected_columns.len > 1: we do it in the future
        let total_rows = rg.column(1).num_values() as usize;
        return Ok(RowSelection::from_consecutive_ranges(
            [0..total_rows].into_iter(),
            total_rows,
        ));
    }
    for (_, col) in projected_columns.iter() {
        let col_name = col.column_descr().name();
        let convert = StatisticsConverter::try_new(
            col_name,
            arrow_schema,
            metadata.file_metadata().schema_descr(),
        )
        .unwrap();
        let mins = convert.data_page_mins(
            metadata.column_index().unwrap(),
            metadata.offset_index().unwrap(),
            row_group_indices,
        )?;
        let maxs = convert.data_page_maxes(
            metadata.column_index().unwrap(),
            metadata.offset_index().unwrap(),
            row_group_indices,
        )?;
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

    let max_array = predicate.evaluate(min_record_batch).unwrap();
    let min_array = predicate.evaluate(max_record_batch).unwrap();

    let filters = max_array
        .iter()
        .zip(min_array.iter())
        .map(|(max, min)| max.unwrap_or(true) || min.unwrap_or(true))
        .collect::<Vec<bool>>();

    // FIXME: update the column index if multiple columns are supported
    let row_counts = page_row_counts(metadata, row_group_indices, projected_columns[0].0);

    let mut selector = vec![];

    for (selected, row_count) in filters.iter().zip(row_counts) {
        if *selected {
            selector.push(RowSelector::select(row_count));
        } else {
            selector.push(RowSelector::skip(row_count));
        }
    }

    let raw = selector.into();
    Ok(match input_selection {
        Some(selection) => selection.intersection(&raw),
        None => raw,
    })
}

fn page_row_counts(
    metadata: &ParquetMetaData,
    row_group_indices: &[usize],
    col_idx: usize,
) -> Vec<usize> {
    let offset_index = metadata.offset_index().unwrap();

    let mut res = vec![];
    for idx in row_group_indices.iter() {
        let row_group = metadata.row_group(*idx);
        let mut row_counts =
            row_group_page_row_counts(row_group, &offset_index[*idx][col_idx].page_locations);
        res.append(&mut row_counts);
    }
    res
}

fn row_group_page_row_counts(
    row_group: &RowGroupMetaData,
    page_offsets: &[PageLocation],
) -> Vec<usize> {
    if page_offsets.is_empty() {
        return vec![];
    }

    let total_rows = row_group.num_rows() as usize;
    let mut row_counts = Vec::with_capacity(page_offsets.len());
    page_offsets.windows(2).for_each(|pages| {
        let start = pages[0].first_row_index as usize;
        let end = pages[1].first_row_index as usize;
        row_counts.push(end - start);
    });
    row_counts.push(total_rows - page_offsets.last().unwrap().first_row_index as usize);
    row_counts
}
