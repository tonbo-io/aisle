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
