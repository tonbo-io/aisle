use arrow_schema::Schema;
use datafusion_expr::Expr;
use parquet::file::metadata::ParquetMetaData;

use super::{
    api::{prune_compiled_with_bloom, prune_compiled_with_bloom_provider},
    options::{PruneOptions, PruneOptionsBuilder},
    provider::AsyncBloomFilterProvider,
    result::PruneResult,
};
use crate::compile::{CompileResult, compile_pruning_ir};

/// Builder for one-shot metadata pruning operations.
///
/// Provides a fluent API for configuring and executing pruning without
/// needing to build [`PruneOptions`] separately.
///
/// # Examples
///
/// ```no_run
/// use aisle::PruneRequest;
/// use arrow_schema::{DataType, Field, Schema};
/// use datafusion_expr::{col, lit};
/// use parquet::file::metadata::ParquetMetaData;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let schema = Schema::new(vec![Field::new("age", DataType::Int32, false)]);
/// # let metadata: ParquetMetaData = todo!();
/// let expr = col("age").gt(lit(18));
///
/// let result = PruneRequest::new(&metadata, &schema)
///     .with_predicate(&expr)
///     .enable_bloom_filter(true)
///     .enable_page_index(false)
///     .prune();
///
/// println!("Keep {} row groups", result.row_groups().len());
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct PruneRequest<'a> {
    metadata: &'a ParquetMetaData,
    schema: &'a Schema,
    expr: Option<&'a Expr>,
    options: PruneOptionsBuilder,
}

impl<'a> PruneRequest<'a> {
    /// Creates a new pruning request for the given metadata and schema.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use aisle::PruneRequest;
    /// use arrow_schema::{DataType, Field, Schema};
    /// use parquet::file::metadata::ParquetMetaData;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    /// # let metadata: ParquetMetaData = todo!();
    /// let request = PruneRequest::new(&metadata, &schema);
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(metadata: &'a ParquetMetaData, schema: &'a Schema) -> Self {
        Self {
            metadata,
            schema,
            expr: None,
            options: PruneOptions::builder(),
        }
    }

    /// Sets the filter predicate to evaluate.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use aisle::PruneRequest;
    /// use arrow_schema::{DataType, Field, Schema};
    /// use datafusion_expr::{col, lit};
    /// use parquet::file::metadata::ParquetMetaData;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let schema = Schema::new(vec![Field::new("age", DataType::Int32, false)]);
    /// # let metadata: ParquetMetaData = todo!();
    /// let expr = col("age").gt(lit(18));
    ///
    /// let request = PruneRequest::new(&metadata, &schema).with_predicate(&expr);
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_predicate(mut self, expr: &'a Expr) -> Self {
        self.expr = Some(expr);
        self
    }

    /// Enables or disables page index pruning.
    ///
    /// When enabled, uses page-level statistics for finer-grained pruning.
    /// Defaults to `false`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use aisle::PruneRequest;
    /// use arrow_schema::{DataType, Field, Schema};
    /// use parquet::file::metadata::ParquetMetaData;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    /// # let metadata: ParquetMetaData = todo!();
    /// let request = PruneRequest::new(&metadata, &schema).enable_page_index(true);
    /// # Ok(())
    /// # }
    /// ```
    pub fn enable_page_index(mut self, enable: bool) -> Self {
        self.options = self.options.enable_page_index(enable);
        self
    }

    /// Enables or disables bloom filter pruning.
    ///
    /// When enabled, uses bloom filters for definite absence checks.
    /// Defaults to `false`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use aisle::PruneRequest;
    /// use arrow_schema::{DataType, Field, Schema};
    /// use parquet::file::metadata::ParquetMetaData;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    /// # let metadata: ParquetMetaData = todo!();
    /// let request = PruneRequest::new(&metadata, &schema).enable_bloom_filter(true);
    /// # Ok(())
    /// # }
    /// ```
    pub fn enable_bloom_filter(mut self, enable: bool) -> Self {
        self.options = self.options.enable_bloom_filter(enable);
        self
    }

    /// Enables or disables Roaring bitmap output format.
    ///
    /// When enabled, page selections are emitted as Roaring bitmaps.
    /// Defaults to `false`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use aisle::PruneRequest;
    /// use arrow_schema::{DataType, Field, Schema};
    /// use parquet::file::metadata::ParquetMetaData;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
    /// # let metadata: ParquetMetaData = todo!();
    /// let request = PruneRequest::new(&metadata, &schema).emit_roaring(true);
    /// # Ok(())
    /// # }
    /// ```
    pub fn emit_roaring(mut self, enable: bool) -> Self {
        self.options = self.options.emit_roaring(enable);
        self
    }

    /// Allows ordering predicates to use truncated byte array statistics.
    ///
    /// When disabled (default), byte array ordering relies only on exact min/max
    /// statistics with type-defined (unsigned) column order.
    pub fn allow_truncated_byte_array_ordering(mut self, enable: bool) -> Self {
        self.options = self.options.allow_truncated_byte_array_ordering(enable);
        self
    }

    /// Executes the pruning operation synchronously (without bloom filters).
    ///
    /// For async pruning with bloom filter support, use [`prune_async()`](Self::prune_async).
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use aisle::PruneRequest;
    /// use arrow_schema::{DataType, Field, Schema};
    /// use datafusion_expr::{col, lit};
    /// use parquet::file::metadata::ParquetMetaData;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let schema = Schema::new(vec![Field::new("age", DataType::Int32, false)]);
    /// # let metadata: ParquetMetaData = todo!();
    /// let expr = col("age").gt(lit(18));
    ///
    /// let result = PruneRequest::new(&metadata, &schema)
    ///     .with_predicate(&expr)
    ///     .prune();
    ///
    /// println!(
    ///     "Keep {} of {} row groups",
    ///     result.row_groups().len(),
    ///     metadata.num_row_groups()
    /// );
    /// # Ok(())
    /// # }
    /// ```
    pub fn prune(self) -> PruneResult {
        let options = self.options.build();
        if let Some(expr) = self.expr {
            let compile = compile_pruning_ir(expr, self.schema);
            prune_compiled_with_bloom(self.metadata, self.schema, compile, &options)
        } else {
            // No predicate = keep all row groups
            let row_groups: Vec<usize> = (0..self.metadata.num_row_groups()).collect();
            PruneResult::new(row_groups, None, None, CompileResult::default())
        }
    }

    /// Executes the pruning operation asynchronously with bloom filter support.
    ///
    /// This method accepts an [`AsyncBloomFilterProvider`] to enable bloom filter pruning.
    /// The provider is typically a `ParquetRecordBatchStreamBuilder` or a custom
    /// implementation optimized for your storage backend.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use aisle::PruneRequest;
    /// use datafusion_expr::{col, lit};
    /// use parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder;
    /// use tokio::fs::File;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let file = File::open("data.parquet").await?;
    /// let mut builder = ParquetRecordBatchStreamBuilder::new(file).await?;
    ///
    /// let predicate = col("user_id").eq(lit(12345i64));
    ///
    /// let result = PruneRequest::new(builder.metadata(), builder.schema())
    ///     .with_predicate(&predicate)
    ///     .enable_bloom_filter(true)  // Enable bloom filter pruning
    ///     .enable_page_index(true)
    ///     .prune_async(&mut builder).await;
    ///
    /// println!("Kept {} row groups", result.row_groups().len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn prune_async<P: AsyncBloomFilterProvider>(self, provider: &mut P) -> PruneResult {
        let options = self.options.build();
        if let Some(expr) = self.expr {
            let compile = compile_pruning_ir(expr, self.schema);
            prune_compiled_with_bloom_provider(
                self.metadata,
                self.schema,
                compile,
                &options,
                provider,
            )
            .await
        } else {
            // No predicate = keep all row groups
            let row_groups: Vec<usize> = (0..self.metadata.num_row_groups()).collect();
            PruneResult::new(row_groups, None, None, CompileResult::default())
        }
    }
}
