//! Metadata-driven Parquet pruning for Rust.
//!
//! Aisle evaluates [DataFusion] predicates against Parquet metadata (row-group statistics,
//! page indexes, bloom filters) to determine which data to skip **before reading**, dramatically
//! reducing I/O for selective queries.
//!
//! [DataFusion]: https://docs.rs/datafusion-expr
//!
//! # Why Aisle?
//!
//! **The Problem:** Parquet readers typically apply filters *after* reading data, wasting I/O
//! on irrelevant row groups and pages.
//!
//! **The Solution:** Aisle evaluates predicates against metadata *before* reading:
//! - **Row-group pruning** using min/max statistics
//! - **Page-level pruning** using column/offset indexes
//! - **Bloom filter checks** for definite absence (high-cardinality columns)
//!
//! **The Result:** 70-99% I/O reduction for selective queries without modifying the Parquet format.
//!
//! # Quick Start
//!
//! ```rust,no_run
//! use aisle::PruneRequest;
//! use datafusion_expr::{col, lit};
//! use parquet::file::metadata::ParquetMetaDataReader;
//! use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
//! use arrow_schema::{DataType, Field, Schema};
//! use bytes::Bytes;
//! use std::sync::Arc;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! # let parquet_bytes = Bytes::new();
//! // 1. Load metadata (without reading data)
//! let metadata = ParquetMetaDataReader::new()
//!     .parse_and_finish(&parquet_bytes)?;
//!
//! // 2. Define schema and filter predicate
//! let schema = Arc::new(Schema::new(vec![
//!     Field::new("user_id", DataType::Int64, false),
//!     Field::new("age", DataType::Int64, false),
//! ]));
//!
//! let predicate = col("user_id").gt_eq(lit(1000i64))
//!     .and(col("age").lt(lit(30i64)));
//!
//! // 3. Prune row groups using metadata
//! let result = PruneRequest::new(&metadata, &schema)
//!     .with_predicate(&predicate)
//!     .enable_page_index(false)     // Row-group level only
//!     .enable_bloom_filter(false)   // No bloom filters
//!     .prune();
//!
//! println!("Kept {} of {} row groups",
//!     result.row_groups().len(),
//!     metadata.num_row_groups());
//!
//! // 4. Apply pruning to Parquet reader
//! let reader = ParquetRecordBatchReaderBuilder::try_new(parquet_bytes.clone())?
//!     .with_row_groups(result.row_groups().to_vec())  // Skip irrelevant row groups!
//!     .build()?;
//!
//! // Read only the relevant data (70-99% I/O reduction!)
//! for batch in reader {
//!     // Process matching rows...
//! #   let _ = batch?;
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Key Features
//!
//! - **Row-group pruning** — Skip entire row groups using min/max statistics
//! - **Page-level pruning** — Skip individual pages within row groups
//! - **Bloom filter support** — Definite absence checks for point queries (`=`, `IN`)
//! - **DataFusion expressions** — Use familiar `col("x").eq(lit(42))` syntax
//! - **Conservative evaluation** — Never skips data that might match (safety first)
//! - **Async-first API** — Optimized for remote storage (S3, GCS, Azure)
//! - **Non-invasive** — Works with upstream `parquet` crate, no format changes
//! - **Best-effort compilation** — Uses supported predicates even if some fail
//!
//! # Main API Entry Points
//!
//! ## Synchronous API
//!
//! Use [`PruneRequest`] for the builder-style API:
//!
//! ```rust,no_run
//! # use aisle::PruneRequest;
//! # use datafusion_expr::{col, lit};
//! # use parquet::file::metadata::ParquetMetaData;
//! # use arrow_schema::Schema;
//! # use std::sync::Arc;
//! # fn example(metadata: &ParquetMetaData, schema: &Arc<Schema>) {
//! let result = PruneRequest::new(metadata, schema)
//!     .with_predicate(&col("id").gt(lit(100i64)))
//!     .enable_page_index(true)
//!     .prune();
//!
//! let kept_row_groups = result.row_groups();
//! let page_selection = result.row_selection();
//! # }
//! ```
//!
//! ## Async API with Bloom Filters
//!
//! Use [`PruneRequest::prune_async()`] for async pruning with bloom filter support:
//!
//! ```rust,ignore
//! use aisle::PruneRequest;
//! use datafusion_expr::{col, lit};
//! use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
//! use tokio::fs::File;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let file = File::open("data.parquet").await?;
//! let mut builder = ParquetRecordBatchStreamBuilder::new(file).await?;
//!
//! let predicate = col("user_id").eq(lit(12345i64));
//!
//! let result = PruneRequest::new(builder.metadata(), builder.schema())
//!     .with_predicate(&predicate)
//!     .enable_bloom_filter(true)  // Check bloom filters
//!     .enable_page_index(true)
//!     .prune_async(&mut builder).await;
//!
//! println!("Kept {} row groups", result.row_groups().len());
//! # Ok(())
//! # }
//! ```
//!
//! ## Custom Bloom Filter Provider
//!
//! Implement [`AsyncBloomFilterProvider`] for optimized bloom filter loading:
//!
//! ```rust
//! use aisle::AsyncBloomFilterProvider;
//! use parquet::bloom_filter::Sbbf;
//!
//! struct CachedBloomProvider {
//!     // Your cache/storage implementation
//! }
//!
//! impl AsyncBloomFilterProvider for CachedBloomProvider {
//!     async fn bloom_filter(&mut self, row_group: usize, column: usize)
//!         -> Option<Sbbf>
//!     {
//!         // Load from cache or fetch from storage
//! #       None
//!     }
//! }
//! ```
//!
//! # Supported Predicates
//!
//! Aisle supports a conservative subset of DataFusion expressions:
//!
//! | Type | Example | Row-Group | Page-Level | Bloom Filter |
//! |------|---------|-----------|------------|--------------|
//! | **Equality** | `col("x").eq(lit(42))` | ✓ | ✓ | ✓ |
//! | **Inequality** | `col("x").not_eq(lit(42))` | ✓ | ✓ | ✗ |
//! | **Comparisons** | `col("x").lt(lit(100))` | ✓ | ✓ | ✗ |
//! | **Range** | `col("x").between(lit(10), lit(20))` | ✓ | ✓ | ✗ |
//! | **Set membership** | `col("x").in_list(vec![...])` | ✓ | ✓ | ✓ |
//! | **Null checks** | `col("x").is_null()` | ✓ | ✓ | ✗ |
//! | **String prefix** | `col("name").like(lit("prefix%"))` | ✓ | ✓ | ✗ |
//! | **Logical AND** | `a.and(b)` | ✓ | ✓ (best-effort) | ✓ |
//! | **Logical OR** | `a.or(b)` | ✓ | ✓ (all-or-nothing) | ✓ |
//! | **Logical NOT** | `a.not()` | ✓ | ✓ (exact only) | ✗ |
//! | **Type casting** | `cast(col("x"), DataType::Int64)` | ✓ (no-op only) | ✓ | ✓ |
//!
//! Unsupported predicates are logged in [`CompileResult::errors()`] but don't prevent pruning
//! with supported parts.
//!
//! # Page-Level Pruning
//!
//! Enable page indexes for finer-grained pruning within row groups:
//!
//! ```rust,no_run
//! # use aisle::PruneRequest;
//! # use datafusion_expr::{col, lit};
//! # use parquet::file::metadata::ParquetMetaData;
//! # use arrow_schema::Schema;
//! # use std::sync::Arc;
//! # fn example(metadata: &ParquetMetaData, schema: &Arc<Schema>) -> Result<(), Box<dyn std::error::Error>> {
//! let result = PruneRequest::new(metadata, schema)
//!     .with_predicate(&col("id").gt(lit(100i64)))
//!     .enable_page_index(true)  // Enable page-level pruning
//!     .prune();
//!
//! // Apply both row-group and page-level selections
//! if let Some(row_selection) = result.row_selection() {
//!     // Use with ParquetRecordBatchReaderBuilder
//!     // reader.with_row_groups(...).with_row_selection(row_selection)
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Error Handling
//!
//! Aisle uses best-effort compilation. Unsupported predicates are logged but don't block pruning:
//!
//! ```rust,no_run
//! # use aisle::PruneRequest;
//! # use datafusion_expr::{col, lit};
//! # use parquet::file::metadata::ParquetMetaData;
//! # use arrow_schema::Schema;
//! # use std::sync::Arc;
//! # fn example(metadata: &ParquetMetaData, schema: &Arc<Schema>) {
//! let result = PruneRequest::new(metadata, schema)
//!     .with_predicate(&col("complex_expr").gt(lit(100i64)))
//!     .prune();
//!
//! // Check compilation results
//! let compile_result = result.compile_result();
//! if compile_result.error_count() > 0 {
//!     eprintln!("Warning: {} unsupported predicates", compile_result.error_count());
//!     for error in compile_result.errors() {
//!         eprintln!("  - {}", error);
//!     }
//! }
//!
//! // Still prune using supported predicates!
//! println!("Successfully compiled {} predicates", compile_result.prunable_count());
//! # }
//! ```
//!
//! # Performance
//!
//! Aisle can dramatically reduce I/O for selective queries:
//!
//! | Query Type | Selectivity | I/O Reduction |
//! |------------|-------------|---------------|
//! | Point query (`id = 12345`) | 0.001% | ~99.9% |
//! | Range query (`date BETWEEN ...`) | 2% | ~98% |
//! | Multi-column filter | 10% | ~90% |
//!
//! **Performance Factors:**
//! - Row group size (larger → better statistics granularity)
//! - Predicate selectivity (lower → more pruning)
//! - Column cardinality (bloom filters help high-cardinality)
//! - Page index availability (Parquet 1.12+)
//!
//! **Overhead:** Metadata evaluation is typically <1ms per row group.
//!
//! # When to Use Aisle
//!
//! **Good fit:**
//! - Selective queries (reading <20% of data)
//! - Large Parquet files (>100MB, multiple row groups)
//! - Remote storage (S3, GCS) where I/O is expensive
//! - High-cardinality point queries
//!
//! **Not needed:**
//! - Full table scans (no pruning benefit)
//! - Small files (<10MB, single row group)
//! - Already using a query engine with built-in pruning (DataFusion, DuckDB)
//!
//! # Examples
//!
//! See the [repository examples](https://github.com/your-org/aisle/tree/main/examples):
//! - `basic_usage.rs` — Row-group pruning with metadata
//! - `async_usage.rs` — Async API with bloom filters

mod compile;
mod error;
mod ir;
mod prune;
mod pruner;
mod selection;

pub use compile::CompileResult;
pub use error::CompileError;
pub use prune::{
    AsyncBloomFilterProvider, PruneOptions, PruneOptionsBuilder, PruneRequest, PruneResult,
};
pub use pruner::Pruner;
pub use selection::{roaring_to_row_selection, row_selection_to_roaring};
