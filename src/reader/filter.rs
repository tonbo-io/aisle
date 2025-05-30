use std::sync::Arc;

use arrow::array::{BooleanArray, RecordBatch};
use arrow_schema::{Field, Schema};
use parquet::{
    arrow::arrow_reader::{
        ArrowPredicate, RowSelection, RowSelector, statistics::StatisticsConverter,
    },
    errors::ParquetError,
    file::metadata::{ColumnChunkMetaData, ParquetMetaData, RowGroupMetaData},
    format::PageLocation,
};

use super::predicate::{AisleArrowPredicate, AislePredicate};

pub struct RowFilter {
    /// A list of [`ArrowPredicate`]
    pub(crate) predicates: Vec<Box<dyn AislePredicate>>,
}

impl RowFilter {
    // /// Create a new [`RowFilter`] from an array of [`ArrowPredicate`]
    pub fn new(predicates: Vec<Box<dyn AislePredicate>>) -> Self {
        Self { predicates }
    }
}

impl From<RowFilter> for parquet::arrow::arrow_reader::RowFilter {
    fn from(filter: RowFilter) -> Self {
        let mut predicates: Vec<Box<dyn ArrowPredicate>> = vec![];
        for pred in filter.predicates.into_iter() {
            predicates.push(Box::new(AisleArrowPredicate::new(pred)));
        }
        parquet::arrow::arrow_reader::RowFilter::new(predicates)
    }
}

pub struct Filter {
    pub(crate) array: BooleanArray,
}

impl Filter {
    pub(crate) fn new(array: BooleanArray) -> Self {
        Self { array }
    }

    pub(crate) fn boolean_array(&self) -> &BooleanArray {
        &self.array
    }
}

pub(crate) fn filter_row_groups(
    metadata: &ParquetMetaData,
    arrow_schema: &Schema,
    row_group_indices: &[usize],
    predicate: &mut dyn AislePredicate,
) -> Result<Vec<usize>, ParquetError> {
    if metadata.row_groups().is_empty() || row_group_indices.is_empty() {
        return Ok(vec![]);
    }

    let projection = predicate.projection();
    let rg = metadata.row_group(row_group_indices[0]);
    let projected_columns: Vec<(usize, &ColumnChunkMetaData)> = rg
        .columns()
        .iter()
        .enumerate()
        .filter(|(idx, _col)| projection.leaf_included(*idx))
        .collect();

    if projected_columns.len() != 1 {
        // two case:
        //   1. projected_columns.len == 0: predicate does not work
        //   2. projected_columns.len > 1: we will do it in the future
        return Ok(row_group_indices.to_vec());
    }

    let mut schema = vec![];
    let mut row_group_metadatas = Vec::with_capacity(row_group_indices.len());

    for idx in row_group_indices.iter() {
        row_group_metadatas.push(metadata.row_group(*idx));
    }

    let mut max_batch = vec![];
    let mut min_batch = vec![];
    for (col_idx, col) in projected_columns.iter() {
        let field = arrow_schema.field(*col_idx);
        schema.push(field.clone());

        let col_name = col.column_descr().name();
        let convert = StatisticsConverter::try_new(
            col_name,
            arrow_schema,
            metadata.file_metadata().schema_descr(),
        )?;
        let mins = convert.row_group_mins(row_group_metadatas.clone().into_iter())?;
        let maxes = convert.row_group_maxes(row_group_metadatas.clone().into_iter())?;
        max_batch.push(maxes);
        min_batch.push(mins);
    }

    let schema = Arc::new(Schema::new(schema));

    let max_record_batch = RecordBatch::try_new(schema.clone(), max_batch).unwrap();
    let min_record_batch = RecordBatch::try_new(schema, min_batch).unwrap();

    let mut selected_row_group_indices = vec![];
    for (idx, selected) in evaluate_merge(predicate, max_record_batch, min_record_batch)?
        .iter()
        .enumerate()
    {
        let row_group_idx = row_group_indices[idx];
        if *selected {
            selected_row_group_indices.push(row_group_idx);
        }
    }

    Ok(selected_row_group_indices)
}

pub(crate) fn evaluate_predicate(
    metadata: &ParquetMetaData,
    arrow_schema: &Schema,
    row_group_indices: &[usize],
    input_selection: Option<RowSelection>,
    predicate: &mut dyn AislePredicate,
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
        .collect();

    let mut fields = vec![];
    let mut max_batch = vec![];
    let mut min_batch = vec![];
    if projected_columns.len() != 1 {
        // two case:
        //   1. projected_columns.len == 0: predicate does not work
        //   2. projected_columns.len > 1: we do it in the future
        let total_rows = rg.column(0).num_values() as usize;
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
        let maxes = convert.data_page_maxes(
            metadata.column_index().unwrap(),
            metadata.offset_index().unwrap(),
            row_group_indices,
        )?;
        fields.push(Field::new(
            col_name,
            mins.data_type().clone(),
            mins.is_nullable(),
        ));
        max_batch.push(maxes);
        min_batch.push(mins);
    }

    let schema = Arc::new(Schema::new(fields));
    let max_record_batch = RecordBatch::try_new(schema.clone(), max_batch).unwrap();
    let min_record_batch = RecordBatch::try_new(schema.clone(), min_batch).unwrap();

    let filters = evaluate_merge(predicate, max_record_batch, min_record_batch)?;

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

/// return the row counts of pages in the given row groups
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

/// return the row counts of pages in the given row group
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

/// evaluate the predicate with two [`RecordBatch`], and then merge the result.
///
/// For example:
///
/// evaluate batch1 get the result `[true, false, false, true]`
///
/// evaluete batch2 get the result `[true, true, false, false]`
///
/// then the final result is `[true, false, false, false]`
///
/// **Note:**
/// - the schema of two batches should be the same.
/// - the column number of two batches should be the same.
/// - the row number of two batches should be the same.
fn evaluate_merge(
    predicate: &mut dyn AislePredicate,
    batch1: RecordBatch,
    batch2: RecordBatch,
) -> Result<Vec<bool>, ParquetError> {
    debug_assert_eq!(batch1.num_columns(), batch2.num_columns());
    debug_assert_eq!(batch1.num_rows(), batch2.num_rows());

    if batch1.schema() != batch2.schema() {
        return Err(ParquetError::ArrowError("schema should be the same".into()));
    }

    let filter1 = predicate.evaluate(batch1)?;
    let filter2 = predicate.evaluate(batch2)?;

    Ok(filter1
        .boolean_array()
        .iter()
        .zip(filter2.boolean_array().iter())
        .map(|(max, min)| max.unwrap_or(true) || min.unwrap_or(true))
        .collect())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Datum, UInt64Array, record_batch};
    use parquet::arrow::ProjectionMask;

    use super::evaluate_merge;
    use crate::{
        ord::{gt, lt, lt_eq},
        predicate::AislePredicate,
        reader::predicate::AislePredicateFn,
    };

    #[test]
    fn test_evaluate() {
        let batch1 = record_batch!(("id", UInt64, [0, 100, 100, 200])).unwrap();
        let batch2 = record_batch!(("id", UInt64, [100, 100, 101, 300])).unwrap();
        {
            let mut predicate = AislePredicateFn::new(ProjectionMask::all(), |batch| {
                let datum = Arc::new(UInt64Array::new_scalar(100)) as Arc<dyn Datum>;
                gt(batch.column(0), datum.as_ref())
            });

            assert_eq!(
                predicate
                    .evaluate(batch1.clone())
                    .unwrap()
                    .array
                    .iter()
                    .map(|v| v.unwrap())
                    .collect::<Vec<bool>>(),
                vec![false, false, false, true]
            );
            assert_eq!(
                predicate
                    .evaluate(batch2.clone())
                    .unwrap()
                    .array
                    .iter()
                    .map(|v| v.unwrap())
                    .collect::<Vec<bool>>(),
                vec![false, false, true, true]
            );
        }
        {
            let mut predicate = AislePredicateFn::new(ProjectionMask::all(), |batch| {
                let datum = Arc::new(UInt64Array::new_scalar(100)) as Arc<dyn Datum>;
                lt(batch.column(0), datum.as_ref())
            });

            assert_eq!(
                predicate
                    .evaluate(batch1.clone())
                    .unwrap()
                    .array
                    .iter()
                    .map(|v| v.unwrap())
                    .collect::<Vec<bool>>(),
                vec![true, false, false, false]
            );
            assert_eq!(
                predicate
                    .evaluate(batch2.clone())
                    .unwrap()
                    .array
                    .iter()
                    .map(|v| v.unwrap())
                    .collect::<Vec<bool>>(),
                vec![false, false, false, false]
            );
        }
        {
            let mut predicate = AislePredicateFn::new(ProjectionMask::all(), |batch| {
                let datum = Arc::new(UInt64Array::new_scalar(100)) as Arc<dyn Datum>;
                lt_eq(batch.column(0), datum.as_ref())
            });

            assert_eq!(
                predicate
                    .evaluate(batch1.clone())
                    .unwrap()
                    .array
                    .iter()
                    .map(|v| v.unwrap())
                    .collect::<Vec<bool>>(),
                vec![true, true, true, false]
            );
            assert_eq!(
                predicate
                    .evaluate(batch2.clone())
                    .unwrap()
                    .array
                    .iter()
                    .map(|v| v.unwrap())
                    .collect::<Vec<bool>>(),
                vec![true, true, false, false]
            );
        }
    }

    #[test]
    fn test_evaluate_merge() {
        let batch1 = record_batch!(("id", UInt64, [0, 100, 100, 200])).unwrap();
        let batch2 = record_batch!(("id", UInt64, [100, 100, 101, 300])).unwrap();
        {
            let mut predicate = AislePredicateFn::new(ProjectionMask::all(), |batch| {
                let datum = Arc::new(UInt64Array::new_scalar(100)) as Arc<dyn Datum>;
                gt(batch.column(0), datum.as_ref())
            });
            // [false, false, false, true];
            // [false, false, true, true];
            let res = evaluate_merge(&mut predicate, batch1.clone(), batch2.clone()).unwrap();
            assert_eq!(res, vec![false, false, true, true]);
        }
        {
            let mut predicate = AislePredicateFn::new(ProjectionMask::all(), |batch| {
                let datum = Arc::new(UInt64Array::new_scalar(100)) as Arc<dyn Datum>;
                lt(batch.column(0), datum.as_ref())
            });

            // [true, false, false, false];
            // [false, false, false, false];
            let res = evaluate_merge(&mut predicate, batch1.clone(), batch2.clone()).unwrap();
            assert_eq!(res, vec![true, false, false, false]);
        }
    }
}
