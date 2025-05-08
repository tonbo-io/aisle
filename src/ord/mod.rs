use arrow::{
    array::{BooleanArray, Datum},
    compute::kernels::cmp,
};
use arrow_schema::ArrowError;

pub fn lt(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    cmp::lt(lhs, rhs)
}

pub fn lt_eq(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    cmp::lt_eq(lhs, rhs)
}

pub fn gt(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    cmp::gt(lhs, rhs)
}

pub fn gt_eq(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    cmp::gt_eq(lhs, rhs)
}

pub fn eq(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    let ge = cmp::gt_eq(lhs, rhs)?;
    let le = cmp::lt_eq(lhs, rhs)?;
    let res = BooleanArray::from_iter(ge.iter().zip(le.iter()).map(|b| match b {
        (None, _) => None,
        (_, None) => None,
        (Some(a), Some(b)) => Some(a & b),
    }));
    Ok(res)
}

pub fn neq(lhs: &dyn Datum, rhs: &dyn Datum) -> Result<BooleanArray, ArrowError> {
    let ge = cmp::gt(lhs, rhs)?;
    let le = cmp::lt(lhs, rhs)?;
    let res = BooleanArray::from_iter(ge.iter().zip(le.iter()).map(|b| match b {
        (None, _) => Some(true),
        (_, None) => Some(true),
        (Some(a), Some(b)) => Some(a || b),
    }));
    Ok(res)
}
