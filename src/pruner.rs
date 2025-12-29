use arrow_schema::{Schema, SchemaRef};
use datafusion_expr::Expr;
use parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder};
use parquet::file::metadata::ParquetMetaData;

use crate::compile::{build_schema_path_index, compile_pruning_ir_with_index, SchemaPathIndex};
use crate::prune::{
    prune_compiled, prune_compiled_with_bloom_provider, AsyncBloomFilterProvider, PruneOptions,
    PruneResult,
};

/// Reusable pruning façade for a fixed schema.
///
/// Caches schema indexing so repeated pruning across many Parquet files
/// with the same Arrow schema avoids rebuilding lookup structures.
///
/// # Thread Safety
///
/// `Pruner` is both `Send` and `Sync`. It can be safely shared across threads
/// via `Arc`, as all methods take `&self` and the internal `schema` uses `Arc<Schema>`.
///
/// # Performance
///
/// Building the schema index has a one-time cost proportional to schema complexity.
/// For workloads pruning multiple Parquet files with the same schema, `Pruner`
/// amortizes this cost by reusing the cached index.
///
/// # Examples
///
/// ## Basic Usage
///
/// ```
/// use aisle::Pruner;
/// use datafusion_expr::{col, lit};
/// use std::sync::Arc;
/// use arrow_schema::{Schema, Field, DataType};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("age", DataType::Int32, false),
///     Field::new("country", DataType::Utf8, false),
/// ]));
///
/// let pruner = Pruner::try_new(schema)?;
///
/// // Reuse for multiple files with same schema
/// # /*
/// for file in ["users1.parquet", "users2.parquet"] {
///     let metadata = load_parquet_metadata(file)?;
///     let expr = col("age").gt(lit(18));
///
///     let result = pruner.prune(&metadata, &expr);
///     println!("Keep {} of {} row groups",
///              result.row_groups().len(),
///              metadata.num_row_groups());
/// }
/// # */
/// # Ok(())
/// # }
/// ```
///
/// ## Custom Options
///
/// ```
/// use aisle::{Pruner, PruneOptions};
/// # use std::sync::Arc;
/// # use arrow_schema::{Schema, Field, DataType};
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let options = PruneOptions::builder()
///     .enable_page_index(true)
///     .emit_roaring(false)
///     .build();
///
/// # let schema = Arc::new(Schema::new(vec![
/// #     Field::new("value", DataType::Int32, false)
/// # ]));
/// let pruner = Pruner::try_with_options(schema, options)?;
/// # Ok(())
/// # }
/// ```
///
/// ## Concurrent Usage
///
/// ```
/// use std::sync::Arc;
/// use std::thread;
/// # use aisle::Pruner;
/// # use arrow_schema::Schema;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let schema = Arc::new(Schema::new(vec![
/// #     arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false)
/// # ]));
/// let pruner = Arc::new(Pruner::try_new(schema)?);
///
/// let handles: Vec<_> = (0..4).map(|_| {
///     let pruner = Arc::clone(&pruner);
///     thread::spawn(move || {
///         // Each thread can safely use the same Pruner
///         # /*
///         pruner.prune(&metadata, &expr)
///         # */
///     })
/// }).collect();
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Pruner {
    schema: SchemaRef,
    schema_index: SchemaPathIndex,
    options: PruneOptions,
}

impl Pruner {
    /// Creates a new `Pruner` with default options.
    ///
    /// This is equivalent to `Pruner::try_with_options(schema, PruneOptions::default())`.
    ///
    /// # Errors
    ///
    /// Returns an error if the schema is empty (has no fields).
    ///
    /// # Examples
    ///
    /// ```
    /// use aisle::Pruner;
    /// use std::sync::Arc;
    /// use arrow_schema::{Schema, Field, DataType};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let schema = Arc::new(Schema::new(vec![
    ///     Field::new("id", DataType::Int64, false),
    /// ]));
    ///
    /// let pruner = Pruner::try_new(schema)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn try_new(schema: SchemaRef) -> Result<Self, String> {
        Self::try_with_options(schema, PruneOptions::default())
    }

    /// Creates a new `Pruner` with explicit options.
    ///
    /// # Errors
    ///
    /// Returns an error if the schema is empty (has no fields).
    ///
    /// # Examples
    ///
    /// ```
    /// use aisle::{Pruner, PruneOptions};
    /// use std::sync::Arc;
    /// use arrow_schema::{Schema, Field, DataType};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let schema = Arc::new(Schema::new(vec![
    ///     Field::new("value", DataType::Float64, true),
    /// ]));
    ///
    /// let options = PruneOptions::builder()
    ///     .enable_page_index(false)
    ///     .build();
    ///
    /// let pruner = Pruner::try_with_options(schema, options)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn try_with_options(schema: SchemaRef, options: PruneOptions) -> Result<Self, String> {
        // Validate schema is not empty
        if schema.fields().is_empty() {
            return Err("Schema must have at least one field".to_string());
        }

        let schema_index = build_schema_path_index(schema.as_ref());
        Ok(Self {
            schema,
            schema_index,
            options,
        })
    }

    /// Returns a reference to the underlying Arrow schema.
    ///
    /// # Examples
    ///
    /// ```
    /// # use aisle::Pruner;
    /// # use std::sync::Arc;
    /// # use arrow_schema::{Schema, Field, DataType};
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    /// let pruner = Pruner::try_new(schema)?;
    ///
    /// println!("Schema has {} fields", pruner.schema().fields().len());
    /// # Ok(())
    /// # }
    /// ```
    pub fn schema(&self) -> &Schema {
        self.schema.as_ref()
    }

    /// Returns a reference to the pruning options.
    ///
    /// # Examples
    ///
    /// ```
    /// # use aisle::{Pruner, PruneOptions};
    /// # use std::sync::Arc;
    /// # use arrow_schema::{Schema, Field, DataType};
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
    /// let options = PruneOptions::builder()
    ///     .enable_page_index(true)
    ///     .build();
    ///
    /// let pruner = Pruner::try_with_options(schema, options)?;
    /// let opts = pruner.options();
    /// # Ok(())
    /// # }
    /// ```
    pub fn options(&self) -> &PruneOptions {
        &self.options
    }

    /// Prunes Parquet metadata using the cached schema index.
    ///
    /// Returns a [`PruneResult`] containing:
    /// - Which row groups to read ([`row_groups()`](PruneResult::row_groups))
    /// - Optional row-level selection ([`row_selection()`](PruneResult::row_selection))
    /// - Compilation result with any errors ([`compile_result()`](PruneResult::compile_result))
    ///
    /// If compilation errors occur, all row groups are conservatively kept
    /// (the predicate falls back to runtime evaluation).
    ///
    /// # Examples
    ///
    /// ```
    /// use datafusion_expr::{col, lit};
    /// # use aisle::Pruner;
    /// # use std::sync::Arc;
    /// # use arrow_schema::{Schema, Field, DataType};
    /// # use parquet::file::metadata::ParquetMetaData;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// # let schema = Arc::new(Schema::new(vec![Field::new("age", DataType::Int32, false)]));
    /// let pruner = Pruner::try_new(schema)?;
    /// let expr = col("age").gt(lit(18));
    ///
    /// # /*
    /// let metadata = load_parquet_metadata("users.parquet")?;
    /// let result = pruner.prune(&metadata, &expr);
    ///
    /// println!("Keep {} of {} row groups",
    ///          result.row_groups().len(),
    ///          metadata.num_row_groups());
    ///
    /// if !result.compile_result().errors().is_empty() {
    ///     eprintln!("Compilation errors: {:?}", result.compile_result().errors());
    /// }
    /// # */
    /// # Ok(())
    /// # }
    /// ```
    pub fn prune(&self, metadata: &ParquetMetaData, expr: &Expr) -> PruneResult {
        let compile =
            compile_pruning_ir_with_index(expr, self.schema.as_ref(), &self.schema_index);
        prune_compiled(metadata, self.schema.as_ref(), compile, &self.options)
    }

    /// Prune Parquet metadata using the cached schema index and bloom filters from the async reader.
    pub async fn prune_with_async_reader<T: AsyncFileReader + 'static>(
        &self,
        builder: &mut ParquetRecordBatchStreamBuilder<T>,
        expr: &Expr,
    ) -> PruneResult {
        let compile =
            compile_pruning_ir_with_index(expr, self.schema.as_ref(), &self.schema_index);
        let metadata = builder.metadata().clone();
        prune_compiled_with_bloom_provider(
            metadata.as_ref(),
            self.schema.as_ref(),
            compile,
            &self.options,
            builder,
        )
        .await
    }

    /// Prune Parquet metadata using the cached schema index and a custom async bloom provider.
    pub async fn prune_with_bloom_provider<P: AsyncBloomFilterProvider>(
        &self,
        metadata: &ParquetMetaData,
        expr: &Expr,
        provider: &mut P,
    ) -> PruneResult {
        let compile =
            compile_pruning_ir_with_index(expr, self.schema.as_ref(), &self.schema_index);
        prune_compiled_with_bloom_provider(
            metadata,
            self.schema.as_ref(),
            compile,
            &self.options,
            provider,
        )
        .await
    }
}
