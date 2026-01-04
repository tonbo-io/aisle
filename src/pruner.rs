use arrow_schema::{Schema, SchemaRef};
#[cfg(feature = "datafusion")]
use datafusion_expr::Expr as DfExpr;
use parquet::{
    arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStreamBuilder},
    file::metadata::ParquetMetaData,
};

#[cfg(feature = "datafusion")]
use crate::AisleError;
#[cfg(feature = "datafusion")]
use crate::compile::{SchemaPathIndex, build_schema_path_index, compile_pruning_ir_with_index};
use crate::{
    AisleResult,
    expr::Expr,
    prune::{
        AsyncBloomFilterProvider, PruneOptions, PruneResult, prune_compiled,
        prune_compiled_with_bloom_provider,
    },
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
/// # #[cfg(feature = "datafusion")]
/// # {
/// use std::sync::Arc;
///
/// use aisle::Pruner;
/// use arrow_schema::{DataType, Field, Schema};
/// use datafusion_expr::{col, lit};
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
///     println!(
///         "Keep {} of {} row groups",
///         result.row_groups().len(),
///         metadata.num_row_groups()
///     );
/// }
/// # */
/// # Ok(())
/// # }
/// # }
/// ```
///
/// ## Custom Options
///
/// ```
/// use aisle::{PruneOptions, Pruner};
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
/// use std::{sync::Arc, thread};
/// # use aisle::Pruner;
/// # use arrow_schema::Schema;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// # let schema = Arc::new(Schema::new(vec![
/// #     arrow_schema::Field::new("id", arrow_schema::DataType::Int64, false)
/// # ]));
/// let pruner = Arc::new(Pruner::try_new(schema)?);
///
/// let handles: Vec<_> = (0..4)
///     .map(|_| {
///         let pruner = Arc::clone(&pruner);
///         thread::spawn(move || {
///             // Each thread can safely use the same Pruner
///         # /*
///             pruner.prune(&metadata, &expr)
///         # */
///         })
///     })
///     .collect();
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct Pruner {
    schema: SchemaRef,
    #[cfg(feature = "datafusion")]
    schema_index: SchemaPathIndex,
    options: PruneOptions,
}

impl Pruner {
    /// Compile a predicate and require full support.
    ///
    /// Returns an error if any part of the predicate cannot be compiled.
    ///
    /// # Examples
    ///
    /// ```
    /// use std::sync::Arc;
    ///
    /// use aisle::Pruner;
    /// use arrow_schema::{DataType, Field, Schema};
    /// use datafusion_expr::{col, lit};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let schema = Arc::new(Schema::new(vec![Field::new("age", DataType::Int32, false)]));
    /// let pruner = Pruner::try_new(schema)?;
    ///
    /// let predicate = col("age").gt(lit(18));
    /// let compiled = pruner.try_compile(&predicate)
    ///     .map_err(|errors| format!("Compilation failed: {} errors", errors.len()))?;
    ///
    /// # /*
    /// let metadata = load_parquet_metadata("users.parquet")?;
    /// let result = compiled.prune(&metadata);
    /// println!("Keep {} row groups", result.row_groups().len());
    /// # */
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "datafusion")]
    pub fn try_compile(&self, expr: &DfExpr) -> Result<CompiledPruner, Vec<AisleError>> {
        let compile = compile_pruning_ir_with_index(expr, self.schema.as_ref(), &self.schema_index);
        if compile.has_errors() {
            Err(compile.errors().to_vec())
        } else {
            Ok(CompiledPruner {
                schema: self.schema.clone(),
                options: self.options.clone(),
                compile,
            })
        }
    }

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
    /// use std::sync::Arc;
    ///
    /// use aisle::Pruner;
    /// use arrow_schema::{DataType, Field, Schema};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
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
    /// use std::sync::Arc;
    ///
    /// use aisle::{PruneOptions, Pruner};
    /// use arrow_schema::{DataType, Field, Schema};
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let schema = Arc::new(Schema::new(vec![Field::new(
    ///     "value",
    ///     DataType::Float64,
    ///     true,
    /// )]));
    ///
    /// let options = PruneOptions::builder().enable_page_index(false).build();
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

        #[cfg(feature = "datafusion")]
        let schema_index = build_schema_path_index(schema.as_ref());

        Ok(Self {
            schema,
            #[cfg(feature = "datafusion")]
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
    /// let options = PruneOptions::builder().enable_page_index(true).build();
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
    /// println!(
    ///     "Keep {} of {} row groups",
    ///     result.row_groups().len(),
    ///     metadata.num_row_groups()
    /// );
    ///
    /// if !result.compile_result().errors().is_empty() {
    ///     eprintln!("Compilation errors: {:?}", result.compile_result().errors());
    /// }
    /// # */
    /// # Ok(())
    /// # }
    /// ```
    #[cfg(feature = "datafusion")]
    pub fn prune(&self, metadata: &ParquetMetaData, expr: &DfExpr) -> PruneResult {
        let compile = compile_pruning_ir_with_index(expr, self.schema.as_ref(), &self.schema_index);
        prune_compiled(metadata, self.schema.as_ref(), compile, &self.options)
    }

    /// Prune Parquet metadata using pre-built IR predicates.
    ///
    /// This bypasses DataFusion compilation and uses the IR as-is.
    pub fn prune_ir(&self, metadata: &ParquetMetaData, predicates: &[Expr]) -> PruneResult {
        let compile = AisleResult::from_ir_slice(predicates);
        prune_compiled(metadata, self.schema.as_ref(), compile, &self.options)
    }

    /// Prune Parquet metadata using the cached schema index and bloom filters from the async
    /// reader.
    #[cfg(feature = "datafusion")]
    pub async fn prune_with_async_reader<T: AsyncFileReader + 'static>(
        &self,
        builder: &mut ParquetRecordBatchStreamBuilder<T>,
        expr: &DfExpr,
    ) -> PruneResult {
        let compile = compile_pruning_ir_with_index(expr, self.schema.as_ref(), &self.schema_index);
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

    /// Prune Parquet metadata using pre-built IR predicates and bloom filters from the async
    /// reader.
    pub async fn prune_ir_with_async_reader<T: AsyncFileReader + 'static>(
        &self,
        builder: &mut ParquetRecordBatchStreamBuilder<T>,
        predicates: &[Expr],
    ) -> PruneResult {
        let compile = AisleResult::from_ir_slice(predicates);
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
    #[cfg(feature = "datafusion")]
    pub async fn prune_with_bloom_provider<P: AsyncBloomFilterProvider>(
        &self,
        metadata: &ParquetMetaData,
        expr: &DfExpr,
        provider: &mut P,
    ) -> PruneResult {
        let compile = compile_pruning_ir_with_index(expr, self.schema.as_ref(), &self.schema_index);
        prune_compiled_with_bloom_provider(
            metadata,
            self.schema.as_ref(),
            compile,
            &self.options,
            provider,
        )
        .await
    }

    /// Prune Parquet metadata using pre-built IR predicates and a custom async bloom provider.
    pub async fn prune_ir_with_bloom_provider<P: AsyncBloomFilterProvider>(
        &self,
        metadata: &ParquetMetaData,
        predicates: &[Expr],
        provider: &mut P,
    ) -> PruneResult {
        let compile = AisleResult::from_ir_slice(predicates);
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

/// Pre-compiled pruning plan for reuse across multiple Parquet files.
///
/// Use [`Pruner::try_compile`] to build this once, then call [`CompiledPruner::prune`]
/// for each file's metadata.
#[derive(Debug, Clone)]
pub struct CompiledPruner {
    schema: SchemaRef,
    options: PruneOptions,
    compile: AisleResult,
}

impl CompiledPruner {
    /// Returns the compilation result for this pruner.
    pub fn compile_result(&self) -> &AisleResult {
        &self.compile
    }

    /// Returns a reference to the pruning options used by this pruner.
    pub fn options(&self) -> &PruneOptions {
        &self.options
    }

    /// Prunes Parquet metadata using the pre-compiled predicate.
    pub fn prune(&self, metadata: &ParquetMetaData) -> PruneResult {
        prune_compiled(
            metadata,
            self.schema.as_ref(),
            self.compile.clone(),
            &self.options,
        )
    }

    /// Prune Parquet metadata using the pre-compiled predicate and bloom filters
    /// from the async reader.
    pub async fn prune_with_async_reader<T: AsyncFileReader + 'static>(
        &self,
        builder: &mut ParquetRecordBatchStreamBuilder<T>,
    ) -> PruneResult {
        let metadata = builder.metadata().clone();
        prune_compiled_with_bloom_provider(
            metadata.as_ref(),
            self.schema.as_ref(),
            self.compile.clone(),
            &self.options,
            builder,
        )
        .await
    }

    /// Prune Parquet metadata using the pre-compiled predicate and a custom
    /// async bloom provider.
    pub async fn prune_with_bloom_provider<P: AsyncBloomFilterProvider>(
        &self,
        metadata: &ParquetMetaData,
        provider: &mut P,
    ) -> PruneResult {
        prune_compiled_with_bloom_provider(
            metadata,
            self.schema.as_ref(),
            self.compile.clone(),
            &self.options,
            provider,
        )
        .await
    }
}
