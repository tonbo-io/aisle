# Aisle

Aisle is a high-performance Parquet file reader that implements page-level predicate pushdown for efficient data filtering. By pushing predicates down to individual pages using statistics, Aisle significantly reduces I/O operations and memory usage compared to traditional Parquet readers.

## Features

- **Predicate Pushdown**: Filters data at row group, page, and row levels to minimize I/O.
- **High Performance**: Efficient for selective queries on large datasets.
- **Easy Integration**: Drop-in replacement for [Parquet ParquetRecordBatchStreamBuilder](https://docs.rs/parquet/latest/parquet/arrow/async_reader/type.ParquetRecordBatchStreamBuilder.html) and minimal code changes required to migrate.

## Why We Built Aisle
In developing [Tonbo](https://github.com/tonbo-io/tonbo), we need an efficient Parquet reader that could take advantage of page indexes and filters. But [Parquet](https://github.com/apache/arrow-rs/tree/main/parquet) doesn't utilize page index and only filter data at row level. Aisle address these needs by providing:
- Row group level filters by using row group statistics
- Page level filters by using page statistics
- Row level filters provided by [Parquet ParquetRecordBatchStreamBuilder](https://docs.rs/parquet/latest/parquet/arrow/async_reader/type.ParquetRecordBatchStreamBuilder.html)

Aisle provides the most benefit in the following scenarios:
1. **Selective Queries**: The more selective your predicates, the greater the benefit from page-level filtering.
2. **Sorted or Partially Sorted Data**: When data are generally ordered across row groups or pages, Aisle will become more effective

## Performance Benchmark

Performance comparison on equivalent query  with ordered data (time in milliseconds):

|num records(file size)|      Parquet      |    Aisle    |Parquet + Page index |  Aisle + Page index |
| --- | --- | --- | --- | --- |
|   1M(10M)       |         5.7486    |      5.4973 |	      5.1803	    |	      0.8361	  |
|    2M(20M)      |        11.2995    |      5.9036 |	     10.4619	    |	      0.9492	  |
|    4M(40M)      |        22.4779    |      6.0074 |	     20.5882	    |	      0.9926	  |
|    8M(80M)      |        45.2373    |      5.9407 |	     41.6102	    |	      1.1186	  |
|   16M(160M)     |        86.2993    |      6.0680 |	     80.1837	    |	      1.1701	  |
|   32M(325M)     |       181.5905    |      6.0286 |	    168.7143	    |	      1.4381	  |
|    64M(649M)    |       340.8466    |      5.6705 |	    319.2045	    |	      1.7898	  |
|   128M(1.3G)    |       652.1447    |      5.5786 |	    611.5472	    |	      2.7170	  |

Performance comparison on range scans with ordered data (time in milliseconds):

|num records(file size)|      Parquet      |    Aisle    |Parquet + Page index |  Aisle + Page index |
| --- | --- | --- | --- | --- |
|   1M(10M)       |         6.7037    |      6.4074 |	      6.0093	    |	      3.1389	  |
|    2M(20M)      |        13.5426    |     10.3298 |	     12.7447	    |	      6.2394	  |
|    4M(40M)      |        26.5282    |     16.7606 |	     24.7887	    |	     11.7465	  |
|    8M(80M)      |        48.1467    |     23.2133 |	     45.1333	    |	     18.1333	  |
|   16M(160M)     |       101.0506    |     51.2247 |	     94.8315	    |	     44.3764	  |
|   32M(325M)     |       214.3759    |     96.8014 |	    192.1418	    |	     80.7376	  |
|    64M(649M)    |       375.0408    |    164.0255 |	    351.4541	    |	    151.2296	  |
|   128M(1.3G)    |       740.0068    |    307.3197 |	    691.7279	    |	    287.6667	  |


See benchmark for more [details](./benches/read_bench.rs).

See more benchmark results [here](./docs/benchmark.md)

## Installation

### Requirements
- Rust
- parquet 55.1.0 or later

Add Aisle to your `Cargo.toml`:

```toml
[dependencies]
aisle = { git = "https://github.com/tonbo-io/aisle.git", branch = "main" }
parquet = { version = "55.1.0" }
```

## Usage

The basic usage is similar to  [Parquet ParquetRecordBatchStreamBuilder](https://docs.rs/parquet/latest/parquet/arrow/async_reader/type.ParquetRecordBatchStreamBuilder.html#method.new)
```rust
use aisle::{
    ArrowReaderOptions, ParquetRecordBatchStreamBuilder, ProjectionMask,
    filter::RowFilter,
};
use fusio::{DynFs, disk::TokioFs, fs::OpenOptions, path::Path};
use fusio_parquet::reader::AsyncReader;
use futures_util::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create AsyncFileReader
    let fs = TokioFs {};
    let file = fs
        .open_options(
            &Path::new("./data/data.parquet").unwrap(),
            OpenOptions::default().read(true),
        )
        .await?;
    let size = file.size().await.unwrap();
    let reader = AsyncReader::new(file, size).await.unwrap();

    // Initialize builder with page index enabled
    let builder = ParquetRecordBatchStreamBuilder::new_with_options(
        reader,
        ArrowReaderOptions::new().with_page_index(true),
    )
    .await?;

    // Build and process the filtered stream
    let mut stream = builder
        .with_row_filter(RowFilter::new(filters))
        .with_limit(10)
        .with_projection(ProjectionMask::all())
        .build()?;

    while let Some(batch_result) = stream.next().await {
        let batch = batch_result?;
        // Process your batch
    }

    Ok(())
}
```

### Creating Predicates

```rust
use aisle::{
    ProjectionMask,
    ord::{gt, lt},
    filter::RowFilter,
    predicate::{AislePredicate, AislePredicateFn},
};
use arrow::array::{Datum, UInt64Array};
use std::sync::Arc;

// Create range predicate (100 < x < 1000)
let schema = builder.parquet_schema();
let filters: Vec<Box<dyn AislePredicate>> = vec![
    Box::new(AislePredicateFn::new(
        ProjectionMask::roots(schema, vec![0]),
        |batch| {
            let key = Arc::new(UInt64Array::new_scalar(100)) as Arc<dyn Datum>;
            gt(batch.column(0), key.as_ref())
        },
    )),
    Box::new(AislePredicateFn::new(
        ProjectionMask::roots(schema, vec![0]),
        |batch| {
            let key = Arc::new(UInt64Array::new_scalar(1000)) as Arc<dyn Datum>;
            lt(batch.column(0), key.as_ref())
        },
    )),
];

// Build with row filter
let stream = builder.with_row_filter(RowFilter::new(filters)).build()?;
```

### Migration Guide
Migrating from Parquet `ParquetRecordBatchStreamBuilder` to Aisle is straightforward, just follows these steps:

1. replace Parquet `ParquetRecordBatchStreamBuilder` with Aisle `ParquetRecordBatchStreamBuilder`
2. replace Parquet `RowFilter` with aisle `RowFilter`
3. replace Parquet `ArrowPredicateFn` with `AislePredicateFn`
4. replace Parquet `gt`, `gt_eq`, `lt`, `lt_eq` with Aisle `gt`, `gt_eq`, `lt`, `lt_eq`

## Limitation
Only `>`, `>=`, `<`, `<=` are supported for now. If you want to use `=` or other operations, please use them in combination. For example:

```rust
// x = 100 becomes x >= 100 AND x <= 100
let filters: Vec<Box<dyn AislePredicate>> = vec![
    Box::new(AislePredicateFn::new(
        ProjectionMask::roots(schema, vec![0]),
        |batch| {
            let key = Arc::new(UInt64Array::new_scalar(100)) as Arc<dyn Datum>;
            gt_eq(batch.column(0), key.as_ref())
        },
    )),
    Box::new(AislePredicateFn::new(
        ProjectionMask::roots(schema, vec![0]),
        |batch| {
            let key = Arc::new(UInt64Array::new_scalar(100)) as Arc<dyn Datum>;
            lt_eq(batch.column(0), key.as_ref())
        },
    )),
];
```

## Development

### Building

Build the project:
```bash
cargo build
```

### Testing
Run tests:
```bash
cargo test
```

Run benchmark:
```bash
cargo bench --bench read_bench --features tokio
```
