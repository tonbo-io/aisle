use std::sync::Arc;

use aisle::{
    ArrowReaderOptions, ParquetRecordBatchStreamBuilder, ProjectionMask,
    filter::RowFilter,
    ord::{gt, lt},
    predicate::{AislePredicate, AislePredicateFn},
};
use arrow::array::{Datum, UInt64Array};
use fusio::{DynFs, disk::TokioFs, fs::OpenOptions, path::Path};
use fusio_parquet::reader::AsyncReader;
use futures_util::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let fs = TokioFs {};
    let file = fs
        .open_options(
            &Path::new("./data/data.parquet").unwrap(),
            OpenOptions::default().read(true),
        )
        .await?;
    let size = file.size().await.unwrap();
    let reader = AsyncReader::new(file, size).await.unwrap();

    let builder = ParquetRecordBatchStreamBuilder::new_with_options(
        reader,
        ArrowReaderOptions::new().with_page_index(true),
    )
    .await?;
    // Create a predicates for filtering
    let parquet_schema = builder.parquet_schema();
    let filters: Vec<Box<dyn AislePredicate>> = vec![
        Box::new(AislePredicateFn::new(
            ProjectionMask::roots(parquet_schema, vec![0]),
            |batch| {
                let key = Arc::new(UInt64Array::new_scalar(1024)) as Arc<dyn Datum>;
                gt(batch.column(0), key.as_ref())
            },
        )),
        Box::new(AislePredicateFn::new(
            ProjectionMask::roots(parquet_schema, vec![0]),
            |batch| {
                let key = Arc::new(UInt64Array::new_scalar(1050)) as Arc<dyn Datum>;
                lt(batch.column(0), key.as_ref())
            },
        )),
    ];

    let mut stream = builder
        .with_row_filter(RowFilter::new(filters))
        .with_limit(10)
        .with_projection(ProjectionMask::all())
        .build()?;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        println!("batch: {:?}", batch);
        // Process your filtered record batch
    }

    Ok(())
}
