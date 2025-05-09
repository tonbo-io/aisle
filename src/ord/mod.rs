use arrow::{array::Datum, compute::kernels::cmp};
use arrow_schema::ArrowError;

use crate::BooleanArray;

pub fn lt(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    Ok(BooleanArray::new(cmp::lt(lhs, rhs)?))
}

pub fn lt_eq(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    Ok(BooleanArray::new(cmp::lt_eq(lhs, rhs)?))
}

pub fn gt(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    Ok(BooleanArray::new(cmp::gt(lhs, rhs)?))
}

pub fn gt_eq(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    Ok(BooleanArray::new(cmp::gt_eq(lhs, rhs)?))
}

// pub fn eq(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError> {
//     let ge = cmp::gt_eq(lhs, rhs)?;
//     let le = cmp::lt_eq(lhs, rhs)?;
//     let res = arrow::array::BooleanArray::from_iter(ge.iter().zip(le.iter()).map(|b| match b {
//         (None, _) => None,
//         (_, None) => None,
//         (Some(a), Some(b)) => Some(a & b),
//     }));
//     Ok(BooleanArray::new(res))
// }

pub fn neq(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    let ge = cmp::gt(lhs, rhs)?;
    let le = cmp::lt(lhs, rhs)?;
    let res = arrow::array::BooleanArray::from_iter(ge.iter().zip(le.iter()).map(|b| match b {
        (None, _) => Some(true),
        (_, None) => Some(true),
        (Some(a), Some(b)) => Some(a || b),
    }));
    Ok(BooleanArray::new(res))
}
