use arrow_schema::Schema;
#[cfg(feature = "datafusion")]
use datafusion_expr::Expr as DfExpr;
use parquet::file::metadata::ParquetMetaData;

use super::{
    api::{prune_compiled, prune_compiled_with_bloom_provider},
    options::{PruneOptions, PruneOptionsBuilder},
    provider::AsyncBloomFilterProvider,
    result::PruneResult,
};
use crate::AisleResult;
#[cfg(feature = "datafusion")]
use crate::compile::compile_pruning_ir;
use crate::expr::Expr;

#[derive(Debug)]
enum PredicateRef<'a> {
    #[cfg(feature = "datafusion")]
    Expr(&'a DfExpr),
    Ir(&'a [Expr]),
}

/// Builder for one-shot metadata pruning operations.
///
/// Provides a fluent API for configuring and executing pruning without
/// needing to build [`PruneOptions`] separately.
///
/// # Examples
///
/// ```no_run
/// # #[cfg(feature = "datafusion")]
/// # {
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
///     .with_df_predicate(&expr)
///     .enable_bloom_filter(true)
///     .enable_page_index(false)
///     .prune();
///
/// println!("Keep {} row groups", result.row_groups().len());
/// # Ok(())
/// # }
/// # }
/// ```
#[derive(Debug)]
pub struct PruneRequest<'a> {
    metadata: &'a ParquetMetaData,
    schema: &'a Schema,
    predicate: Option<PredicateRef<'a>>,
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
            predicate: None,
            options: PruneOptions::builder(),
        }
    }

    /// Sets the filter predicate to evaluate.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # #[cfg(feature = "datafusion")]
    /// # {
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
    /// let request = PruneRequest::new(&metadata, &schema).with_df_predicate(&expr);
    /// # Ok(())
    /// # }
    /// # }
    /// ```
    #[cfg(feature = "datafusion")]
    pub fn with_df_predicate(mut self, expr: &'a DfExpr) -> Self {
        self.predicate = Some(PredicateRef::Expr(expr));
        self
    }

    /// Sets the filter predicate using a pre-built IR expression.
    ///
    /// This bypasses DataFusion compilation and uses the IR as-is.
    /// No schema validation is performed; invalid columns or types
    /// will be treated conservatively during pruning.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use aisle::{CmpOp, Expr, PruneRequest};
    /// use datafusion_common::ScalarValue;
    /// use parquet::file::metadata::ParquetMetaData;
    /// use arrow_schema::{Field, Schema};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let schema = Schema::new(Vec::<Field>::new());
    /// # let metadata: ParquetMetaData = todo!();
    /// let ir = Expr::Cmp {
    ///     column: "age".to_string(),
    ///     op: CmpOp::Gt,
    ///     value: ScalarValue::Int32(Some(18)),
    /// };
    ///
    /// let result = PruneRequest::new(&metadata, &schema)
    ///     .with_predicate(&ir)
    ///     .prune();
    /// # Ok(())
    /// # }
    /// ```
    pub fn with_predicate(mut self, expr: &'a Expr) -> Self {
        self.predicate = Some(PredicateRef::Ir(std::slice::from_ref(expr)));
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

    /// Enables or disables dictionary hint pruning.
    ///
    /// Dictionary hints are conservative definite-absence checks for `=` and `IN`
    /// predicates. They are opt-in and require an async provider that supplies
    /// complete per-row-group dictionary evidence.
    ///
    /// Defaults to `false`.
    pub fn enable_dictionary_hints(mut self, enable: bool) -> Self {
        self.options = self.options.enable_dictionary_hints(enable);
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

    /// Allows ordering predicates to use truncated byte array statistics (default: `false`).
    ///
    /// # What This Controls
    ///
    /// When evaluating ordering predicates (`<`, `>`, `<=`, `>=`, `BETWEEN`, `LIKE 'prefix%'`)
    /// on string/binary columns, Aisle needs to compare predicate values against min/max
    /// statistics. These comparisons require proper ordering semantics.
    ///
    /// Parquet writers may **truncate** min/max statistics for byte arrays (e.g., keeping only
    /// the first 32 bytes of long strings). Truncation can change ordering semantics:
    /// - Truncated min might be greater than the actual min
    /// - Truncated max might be less than the actual max
    ///
    /// # Default Mode (Conservative)
    ///
    /// When `false` (default), ordering predicates require **both**:
    /// 1. Column has `TYPE_DEFINED_ORDER(UNSIGNED)` metadata
    /// 2. Statistics are **exact** (not truncated)
    ///
    /// If either condition fails, the predicate returns `None` (keeps all row groups).
    ///
    /// # Aggressive Mode (Opt-in)
    ///
    /// When `true`, ordering predicates accept **truncated** statistics as long as:
    /// 1. Column has `TYPE_DEFINED_ORDER(UNSIGNED)` metadata
    ///
    /// **Trade-off**: May produce false positives (keeping some irrelevant row groups)
    /// if truncation changes ordering semantics for your query range.
    ///
    /// # Which Predicates Are Affected
    ///
    /// **Affected** (ordering-sensitive):
    /// - `col.lt(...)`, `col.gt(...)`, `col.lt_eq(...)`, `col.gt_eq(...)`
    /// - `col.between(low, high)`
    /// - `col.like("prefix%")` (prefix matching)
    ///
    /// **Not affected** (always safe regardless of truncation):
    /// - `col.eq(...)`, `col.not_eq(...)`
    /// - `col.in_list(...)`
    /// - `col.is_null()`, `col.is_not_null()`
    ///
    /// # When to Enable
    ///
    /// Enable aggressive mode when:
    /// - You understand your Parquet writer's truncation behavior
    /// - Truncated statistics still preserve meaningful ordering for your queries
    /// - You want maximum pruning and can tolerate potential false positives
    /// - Your files have `TYPE_DEFINED_ORDER(UNSIGNED)` but statistics are truncated
    ///
    /// # When to Keep Disabled (Default)
    ///
    /// Keep conservative mode when:
    /// - You need guaranteed correctness (no false negatives beyond metadata uncertainty)
    /// - Your Parquet files have exact min/max statistics (most Arrow-based writers)
    /// - You're only using equality predicates (truncation doesn't matter)
    /// - You're unsure about statistics truncation behavior
    ///
    /// # Examples
    ///
    /// ```rust
    /// # #[cfg(feature = "datafusion")]
    /// # {
    /// use aisle::PruneRequest;
    /// use datafusion_expr::{col, lit};
    /// # use arrow_schema::{Schema, Field, DataType};
    /// # use parquet::file::metadata::ParquetMetaData;
    /// # fn example(metadata: &ParquetMetaData, schema: &Schema) {
    ///
    /// // Conservative (default): Only uses exact statistics
    /// let result = PruneRequest::new(metadata, schema)
    ///     .with_df_predicate(&col("name").gt(lit("M")))
    ///     .prune();
    /// // If statistics are truncated, keeps all row groups (safe)
    ///
    /// // Aggressive: Allows truncated statistics
    /// let result = PruneRequest::new(metadata, schema)
    ///     .with_df_predicate(&col("description").lt(lit("zebra")))
    ///     .allow_truncated_byte_array_ordering(true)
    ///     .prune();
    /// // Uses truncated stats (may keep some false positives)
    ///
    /// // Equality predicates: unaffected by this setting
    /// let result = PruneRequest::new(metadata, schema)
    ///     .with_df_predicate(&col("status").eq(lit("active")))
    ///     .prune();
    /// // Works regardless of truncation
    /// # }
    /// # }
    /// ```
    ///
    /// # See Also
    ///
    /// - Run `cargo run --example byte_array_ordering` for comprehensive demonstrations
    /// - Parquet spec: [Byte Array Ordering](https://parquet.apache.org/docs/)
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
    /// # #[cfg(feature = "datafusion")]
    /// # {
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
    ///     .with_df_predicate(&expr)
    ///     .prune();
    ///
    /// println!(
    ///     "Keep {} of {} row groups",
    ///     result.row_groups().len(),
    ///     metadata.num_row_groups()
    /// );
    /// # Ok(())
    /// # }
    /// # }
    /// ```
    pub fn prune(self) -> PruneResult {
        let options = self.options.build();
        match self.predicate {
            #[cfg(feature = "datafusion")]
            Some(PredicateRef::Expr(expr)) => {
                let compile = compile_pruning_ir(expr, self.schema);
                prune_compiled(self.metadata, self.schema, compile, &options)
            }
            Some(PredicateRef::Ir(exprs)) => {
                let compile = AisleResult::from_ir_slice(exprs);
                prune_compiled(self.metadata, self.schema, compile, &options)
            }
            None => {
                // No predicate = keep all row groups
                let row_groups: Vec<usize> = (0..self.metadata.num_row_groups()).collect();
                PruneResult::new(row_groups, None, None, AisleResult::default())
            }
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
    /// # #[cfg(feature = "datafusion")]
    /// # {
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
    ///     .with_df_predicate(&predicate)
    ///     .enable_bloom_filter(true)  // Enable bloom filter pruning
    ///     .enable_page_index(true)
    ///     .prune_async(&mut builder).await;
    ///
    /// println!("Kept {} row groups", result.row_groups().len());
    /// # Ok(())
    /// # }
    /// # }
    /// ```
    pub async fn prune_async<P: AsyncBloomFilterProvider>(self, provider: &mut P) -> PruneResult {
        let options = self.options.build();
        match self.predicate {
            #[cfg(feature = "datafusion")]
            Some(PredicateRef::Expr(expr)) => {
                let compile = compile_pruning_ir(expr, self.schema);
                prune_compiled_with_bloom_provider(
                    self.metadata,
                    self.schema,
                    compile,
                    &options,
                    provider,
                )
                .await
            }
            Some(PredicateRef::Ir(exprs)) => {
                let compile = AisleResult::from_ir_slice(exprs);
                prune_compiled_with_bloom_provider(
                    self.metadata,
                    self.schema,
                    compile,
                    &options,
                    provider,
                )
                .await
            }
            None => {
                // No predicate = keep all row groups
                let row_groups: Vec<usize> = (0..self.metadata.num_row_groups()).collect();
                PruneResult::new(row_groups, None, None, AisleResult::default())
            }
        }
    }
}
