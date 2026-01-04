use crate::{AisleError, Expr};

/// Result of compiling a DataFusion expression into pruning IR.
///
/// Uses an **error accumulation** strategy: attempts to compile all
/// predicates and returns both successes and failures. This allows
/// partial pruning even when some predicates cannot be compiled.
///
/// # Example
/// ```no_run
/// # #[cfg(feature = "datafusion")]
/// # {
/// use aisle::PruneRequest;
/// use arrow_schema::{DataType, Field, Schema};
/// use datafusion_expr::{BinaryExpr, Expr, Operator, col, lit};
/// use parquet::file::metadata::ParquetMetaData;
///
/// let schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
/// let metadata: ParquetMetaData = todo!();
///
/// // Mix of supported and unsupported predicates
/// let unsupported = Expr::BinaryExpr(BinaryExpr {
///     left: Box::new(col("a")),
///     op: Operator::Plus,
///     right: Box::new(lit(10)),
/// });
/// let expr = col("a").gt(lit(5)).and(unsupported);
///
/// let result = PruneRequest::new(&metadata, &schema)
///     .with_df_predicate(&expr)
///     .prune();
///
/// let compile = result.compile_result();
/// assert_eq!(compile.prunable_count(), 1); // a > 5 compiled
/// assert_eq!(compile.error_count(), 1); // a + 10 failed
/// # }
/// ```
#[derive(Clone, Debug, Default)]
pub struct AisleResult {
    /// Successfully compiled predicates that can be evaluated against metadata
    pub(crate) prunable: Vec<Expr>,
    /// Compilation errors for predicates that could not be compiled
    pub(crate) errors: Vec<AisleError>,
}

impl AisleResult {
    /// Get the successfully compiled predicates (internal)
    pub(crate) fn prunable(&self) -> &[Expr] {
        &self.prunable
    }

    #[cfg(feature = "datafusion")]
    pub(crate) fn push_prunable(&mut self, expr: Expr) {
        self.prunable.push(expr);
    }

    #[cfg(feature = "datafusion")]
    pub(crate) fn push_error(&mut self, err: AisleError) {
        self.errors.push(err);
    }

    /// Get the successfully compiled IR expressions
    ///
    /// Returns a slice of all predicates that were successfully compiled into IR.
    /// These can be used with [`RowFilter`](crate::RowFilter) for row-level filtering.
    ///
    /// # Example
    /// ```no_run
    /// # #[cfg(feature = "datafusion")]
    /// # {
    /// # use aisle::PruneRequest;
    /// # use datafusion_expr::{col, lit};
    /// # use parquet::file::metadata::ParquetMetaData;
    /// # use arrow_schema::Schema;
    /// # use std::sync::Arc;
    /// # let metadata: ParquetMetaData = todo!();
    /// # let schema: Arc<Schema> = todo!();
    /// let predicate = col("id").eq(lit(42i64));
    /// let result = PruneRequest::new(&metadata, &schema)
    ///     .with_df_predicate(&predicate)
    ///     .prune();
    ///
    /// let ir_exprs = result.compile_result().ir_exprs();
    /// println!("Compiled {} predicates", ir_exprs.len());
    /// # }
    /// ```
    pub fn ir_exprs(&self) -> &[Expr] {
        &self.prunable
    }

    /// Build a compile result from already-constructed IR expressions.
    ///
    /// This does not perform any schema validation or type checking. Use this
    /// when you are constructing [`Expr`] manually and want to skip
    /// DataFusion compilation.
    pub fn from_ir_exprs(exprs: Vec<Expr>) -> Self {
        Self {
            prunable: exprs,
            errors: Vec::new(),
        }
    }

    /// Build a compile result from a slice of IR expressions.
    ///
    /// This does not perform any schema validation or type checking. Use this
    /// when you are constructing [`Expr`] manually and want to skip
    /// DataFusion compilation.
    pub fn from_ir_slice(exprs: &[Expr]) -> Self {
        Self {
            prunable: exprs.to_vec(),
            errors: Vec::new(),
        }
    }

    /// Get the compilation errors
    pub fn errors(&self) -> &[AisleError] {
        &self.errors
    }

    /// Check if there are any compilation errors
    pub fn has_errors(&self) -> bool {
        !self.errors.is_empty()
    }

    /// Get the number of successfully compiled predicates
    pub fn prunable_count(&self) -> usize {
        self.prunable.len()
    }

    /// Get the number of compilation errors
    pub fn error_count(&self) -> usize {
        self.errors.len()
    }

}
