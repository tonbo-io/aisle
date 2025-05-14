use arrow::{
    array::{BooleanArray, Datum},
    buffer::BooleanBuffer,
    compute::kernels::cmp,
};
use arrow_schema::ArrowError;

use crate::reader::filter::Filter;

/// Perform `left < right` operation on two [`Datum`].
pub fn lt(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<Filter, ArrowError> {
    Ok(Filter::new(cmp::lt(lhs, rhs)?))
}

/// Perform `left <= right` operation on two [`Datum`].
pub fn lt_eq(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<Filter, ArrowError> {
    Ok(Filter::new(cmp::lt_eq(lhs, rhs)?))
}

/// Perform `left > right` operation on two [`Datum`].
pub fn gt(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<Filter, ArrowError> {
    Ok(Filter::new(cmp::gt(lhs, rhs)?))
}

/// Perform `left >= right` operation on two [`Datum`].
pub fn gt_eq(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<Filter, ArrowError> {
    Ok(Filter::new(cmp::gt_eq(lhs, rhs)?))
}

pub fn inf(datum: &dyn Datum) -> Result<Filter, ArrowError> {
    let len = datum.get().0.len();
    Ok(Filter {
        array: BooleanArray::new(BooleanBuffer::collect_bool(len, |_| true), None),
    })
}
