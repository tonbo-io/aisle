use thiserror::Error;

pub mod ord;
pub mod reader;

#[derive(Debug, Error)]
pub enum Error {
    #[error("IO error: {0}")]
    IO(#[from] fusio::Error),
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
    #[error("invalid argument")]
    InvalidArgumentError(String),
}

#[derive(Clone)]
pub struct BooleanArray {
    array: arrow::array::BooleanArray,
}

impl BooleanArray {
    pub(crate) fn new(array: arrow::array::BooleanArray) -> Self {
        Self { array }
    }
}
