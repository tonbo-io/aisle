use std::task::{Context, Poll};
use std::{pin::Pin, sync::Arc};

use arrow::array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use datafusion_common::error::Result;
use datafusion_expr::Expr;
use fusio::{DynFs, path::Path};
use fusio_parquet::reader::AsyncReader;
use futures_util::Stream;
use parquet::arrow::async_reader::ParquetRecordBatchStream;
use parquet::arrow::{
    ParquetRecordBatchStreamBuilder,
    arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions},
    async_reader::AsyncFileReader,
};
use parquet::file::metadata::ParquetMetaData;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum Error {
    #[error("IO error: {0}")]
    IO(#[from] fusio::Error),
    #[error("Parquet error: {0}")]
    Parquet(#[from] parquet::errors::ParquetError),
}

pub struct Aisle {
    path: Path,
    fs: Arc<dyn DynFs>,

    schema: Arc<Schema>,
}

impl std::fmt::Debug for Aisle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Aisle").field("path", &self.path).finish()
    }
}

impl Aisle {
    pub async fn new(path: Path, fs: Arc<dyn DynFs>) -> Result<Self, Error> {
        let schema = {
            let file = fs.open(&path).await?;
            let size = file.size().await?;
            let mut reader = AsyncReader::new(file, size).await?;
            let metadata = reader.get_metadata(None).await?;
            let reader = ArrowReaderMetadata::try_new(metadata, ArrowReaderOptions::new())?;
            reader.schema().clone()
        };
        Ok(Self { path, fs, schema })
    }

    pub async fn scan<'scan>(
        &'scan self,
        project: Option<&'scan [usize]>,
        filter: &'scan [Expr],
        limit: Option<usize>,
    ) -> Result<ScanStream<'scan>, Error> {
        let file = self.fs.open(&self.path).await?;
        let size = file.size().await?;
        let mut reader = AsyncReader::new(file, size).await?;
        let metadata = reader.get_metadata(None).await?;
        let stream = ParquetRecordBatchStreamBuilder::new(reader)
            .await?
            .with_limit(limit.unwrap_or(usize::MAX))
            .build()?;
        Ok(ScanStream {
            project,
            filter,
            limit,
            inner: stream,
            metadata,
        })
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[derive(Debug)]
pub struct ScanStream<'scan> {
    project: Option<&'scan [usize]>,
    filter: &'scan [Expr],
    limit: Option<usize>,
    metadata: Arc<ParquetMetaData>,
    inner: ParquetRecordBatchStream<AsyncReader>,
}

impl Stream for ScanStream<'_> {
    type Item = Result<Vec<RecordBatch>, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let meta = self.metadata.row_group(0);
        println!("meta: {:?}", meta);

        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::{pin::pin, sync::Arc};

    use datafusion_expr::{col, lit};
    use fusio::{disk::TokioFs, path::Path};
    use futures_util::StreamExt;

    use crate::Aisle;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_aisle() {
        let fs = Arc::new(TokioFs);
        let path = Path::from_filesystem_path("../parquet-test/data.parquet").unwrap();
        // println!("path: {:?}", path);
        let aisle = Aisle::new(path.clone(), fs.clone()).await.unwrap();
        // println!("schema: {:?}", aisle.schema());

        let filter = [col("c1").eq(lit(42_i32))];

        let mut stream = pin!(aisle.scan(None, &filter, Some(1)).await.unwrap());
        stream.next().await;
        todo!()
    }
}
