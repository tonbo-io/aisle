/// Row-level filtering using compiled IR expressions or DataFusion expressions
///
/// **Feature gates**:
/// - `row_filter`: Enables [`RowFilter`] for Aisle's IR expressions (minimal dependencies)
/// - `datafusion`: Enables [`ExprRowFilter`] for DataFusion expressions (requires datafusion-expr)
///
/// ```toml
/// [dependencies]
/// # For RowFilter only (lightweight)
/// aisle = { version = "0.2", features = ["row_filter"] }
///
/// # For both RowFilter and ExprRowFilter
/// aisle = { version = "0.2", features = ["datafusion"] }
/// ```
///
/// ## Available Implementations
///
/// - [`RowFilter`]: Evaluates Aisle's compiled IR expressions
///   - Available with `row_filter` feature
///   - Best for pre-compiled predicates
///   - Minimal dependencies (no datafusion-expr required)
///
/// - [`ExprRowFilter`]: Evaluates full DataFusion expressions
///   - Available with `datafusion` feature
///   - Exact filtering for supported operators
///   - Returns errors for unsupported expressions
///   - Requires datafusion-expr dependency
use std::collections::HashSet;

use arrow_arith::boolean as arith_boolean;
use arrow_array::{Array, BooleanArray, RecordBatch};
use arrow_ord::cmp as ord_cmp;
use arrow_schema::ArrowError;
use arrow_select::filter::filter_record_batch;
use datafusion_common::ScalarValue;
#[cfg(feature = "datafusion")]
use datafusion_expr::{
    Between, BinaryExpr, Expr as DfExpr, Operator, expr::InList, utils::expr_to_columns,
};
use parquet::{
    arrow::{ProjectionMask, arrow_reader::ArrowPredicate},
    schema::types::SchemaDescriptor,
};

use crate::expr::{CmpOp, Expr as IrExpr};

type Result<T> = std::result::Result<T, ArrowError>;

/// Wrapper for using Expr as a row filter with Parquet's built-in filtering
pub struct RowFilter {
    expr: IrExpr,
    projection: ProjectionMask,
}

impl RowFilter {
    /// Create a new row filter from an IR expression
    ///
    /// # Arguments
    /// * `expr` - The compiled IR expression to use for filtering
    /// * `parquet_schema` - Parquet schema descriptor (needed for projection)
    ///
    /// # Example
    /// ```rust,ignore
    /// use aisle::{RowFilter, compile_pruning_ir};
    /// use datafusion_expr::{col, lit};
    ///
    /// let predicate = col("id").eq(lit(42i64));
    /// let compile_result = compile_pruning_ir(&predicate, &schema);
    /// let ir_expr = compile_result.expr().unwrap();
    ///
    /// let filter = RowFilter::new(
    ///     ir_expr.clone(),
    ///     parquet_metadata.file_metadata().schema_descr(),
    /// );
    ///
    /// // Use with Parquet reader's RowFilter
    /// let row_filter = RowFilter::new(vec![Box::new(filter)]);
    /// let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
    ///     .with_row_filter(row_filter)
    ///     .build()?;
    /// ```
    pub fn new(expr: IrExpr, parquet_schema: &SchemaDescriptor) -> Self {
        let projection = projection_from_ir(&expr, parquet_schema);
        Self { expr, projection }
    }

    /// Evaluate the expression against a record batch
    fn evaluate_expr(&self, expr: &IrExpr, batch: &RecordBatch) -> Result<BooleanArray> {
        match expr {
            IrExpr::True => Ok(BooleanArray::from(vec![true; batch.num_rows()])),
            IrExpr::False => Ok(BooleanArray::from(vec![false; batch.num_rows()])),
            IrExpr::Cmp { column, op, value } => self.evaluate_cmp(batch, column, *op, value),
            IrExpr::Between {
                column,
                low,
                high,
                inclusive,
            } => self.evaluate_between(batch, column, low, high, *inclusive),
            IrExpr::InList { column, values } => self.evaluate_in_list(batch, column, values),
            IrExpr::StartsWith { column, prefix } => {
                self.evaluate_starts_with(batch, column, prefix)
            }
            IrExpr::IsNull { column, negated } => self.evaluate_is_null(batch, column, *negated),
            IrExpr::And(exprs) => self.evaluate_and(batch, exprs),
            IrExpr::Or(exprs) => self.evaluate_or(batch, exprs),
            IrExpr::Not(expr) => {
                let inner = self.evaluate_expr(expr, batch)?;
                Ok(arith_boolean::not(&inner)?)
            }
            // Bloom filter variants are metadata-only, treat as True for row filtering
            IrExpr::BloomFilterEq { .. } | IrExpr::BloomFilterInList { .. } => {
                Ok(BooleanArray::from(vec![true; batch.num_rows()]))
            }
        }
    }

    fn evaluate_cmp(
        &self,
        batch: &RecordBatch,
        column: &str,
        op: CmpOp,
        value: &ScalarValue,
    ) -> Result<BooleanArray> {
        use ord_cmp::{eq, gt, gt_eq, lt, lt_eq, neq};

        let array = batch.column_by_name(column).ok_or_else(|| {
            arrow_schema::ArrowError::SchemaError(format!("Column '{}' not found", column))
        })?;

        let scalar_array = value.to_array_of_size(batch.num_rows())?;

        let array_ref = array.as_ref();
        let scalar_ref = scalar_array.as_ref();
        let result = match op {
            CmpOp::Eq => eq(&array_ref, &scalar_ref)?,
            CmpOp::NotEq => neq(&array_ref, &scalar_ref)?,
            CmpOp::Lt => lt(&array_ref, &scalar_ref)?,
            CmpOp::LtEq => lt_eq(&array_ref, &scalar_ref)?,
            CmpOp::Gt => gt(&array_ref, &scalar_ref)?,
            CmpOp::GtEq => gt_eq(&array_ref, &scalar_ref)?,
        };

        Ok(result)
    }

    fn evaluate_between(
        &self,
        batch: &RecordBatch,
        column: &str,
        low: &ScalarValue,
        high: &ScalarValue,
        inclusive: bool,
    ) -> Result<BooleanArray> {
        use ord_cmp::{gt, gt_eq, lt, lt_eq};

        let array = batch.column_by_name(column).ok_or_else(|| {
            arrow_schema::ArrowError::SchemaError(format!("Column '{}' not found", column))
        })?;

        let low_array = low.to_array_of_size(batch.num_rows())?;
        let high_array = high.to_array_of_size(batch.num_rows())?;

        let array_ref = array.as_ref();
        let low_ref = low_array.as_ref();
        let high_ref = high_array.as_ref();
        let (low_cmp, high_cmp) = if inclusive {
            (gt_eq(&array_ref, &low_ref)?, lt_eq(&array_ref, &high_ref)?)
        } else {
            (gt(&array_ref, &low_ref)?, lt(&array_ref, &high_ref)?)
        };

        Ok(arith_boolean::and(&low_cmp, &high_cmp)?)
    }

    fn evaluate_in_list(
        &self,
        batch: &RecordBatch,
        column: &str,
        values: &[ScalarValue],
    ) -> Result<BooleanArray> {
        use ord_cmp::eq;

        let array = batch.column_by_name(column).ok_or_else(|| {
            arrow_schema::ArrowError::SchemaError(format!("Column '{}' not found", column))
        })?;

        let array_ref = array.as_ref();
        let mut result = BooleanArray::from(vec![false; batch.num_rows()]);

        for value in values {
            let scalar_array = value.to_array_of_size(batch.num_rows())?;
            let scalar_ref = scalar_array.as_ref();
            let matches = eq(&array_ref, &scalar_ref)?;
            result = arith_boolean::or(&result, &matches)?;
        }

        Ok(result)
    }

    fn evaluate_starts_with(
        &self,
        batch: &RecordBatch,
        column: &str,
        prefix: &str,
    ) -> Result<BooleanArray> {
        use arrow_array::{LargeStringArray, StringArray, StringViewArray};

        let array = batch.column_by_name(column).ok_or_else(|| {
            arrow_schema::ArrowError::SchemaError(format!("Column '{}' not found", column))
        })?;

        if let Some(string_array) = array.as_any().downcast_ref::<StringArray>() {
            return Ok(string_array
                .iter()
                .map(|opt_str| opt_str.map(|s| s.starts_with(prefix)))
                .collect());
        }

        if let Some(string_array) = array.as_any().downcast_ref::<LargeStringArray>() {
            return Ok(string_array
                .iter()
                .map(|opt_str| opt_str.map(|s| s.starts_with(prefix)))
                .collect());
        }

        if let Some(string_array) = array.as_any().downcast_ref::<StringViewArray>() {
            return Ok(string_array
                .iter()
                .map(|opt_str| opt_str.map(|s| s.starts_with(prefix)))
                .collect());
        }

        Err(arrow_schema::ArrowError::CastError(format!(
            "Column '{}' is not a string",
            column
        )))
    }

    fn evaluate_is_null(
        &self,
        batch: &RecordBatch,
        column: &str,
        negated: bool,
    ) -> Result<BooleanArray> {
        use arith_boolean::{is_null, not};

        let array = batch.column_by_name(column).ok_or_else(|| {
            arrow_schema::ArrowError::SchemaError(format!("Column '{}' not found", column))
        })?;

        let is_null_result = is_null(array)?;

        if negated {
            Ok(not(&is_null_result)?)
        } else {
            Ok(is_null_result)
        }
    }

    fn evaluate_and(&self, batch: &RecordBatch, exprs: &[IrExpr]) -> Result<BooleanArray> {
        use arith_boolean::and;

        let mut result = BooleanArray::from(vec![true; batch.num_rows()]);

        for expr in exprs {
            let expr_result = self.evaluate_expr(expr, batch)?;
            result = and(&result, &expr_result)?;
        }

        Ok(result)
    }

    fn evaluate_or(&self, batch: &RecordBatch, exprs: &[IrExpr]) -> Result<BooleanArray> {
        use arith_boolean::or;

        let mut result = BooleanArray::from(vec![false; batch.num_rows()]);

        for expr in exprs {
            let expr_result = self.evaluate_expr(expr, batch)?;
            result = or(&result, &expr_result)?;
        }

        Ok(result)
    }
}

impl RowFilter {
    /// Evaluate the filter against a batch and return a boolean mask
    pub fn filter_batch(&self, batch: &RecordBatch) -> Result<BooleanArray> {
        self.evaluate_expr(&self.expr, batch)
    }

    /// Evaluate and apply the filter, returning only matching rows
    pub fn apply_filter(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let mask = self.filter_batch(&batch)?;
        filter_record_batch(&batch, &mask)
    }
}

// Implement ArrowPredicate for integration with Parquet's RowFilter
impl ArrowPredicate for RowFilter {
    fn evaluate(&mut self, batch: RecordBatch) -> Result<BooleanArray> {
        self.filter_batch(&batch)
    }

    fn projection(&self) -> &ProjectionMask {
        &self.projection
    }
}

// ============================================================================
// ExprRowFilter - Exact filtering using full DataFusion expressions
// ============================================================================

#[cfg(feature = "datafusion")]
pub use expr_row_filter_impl::*;

#[cfg(feature = "datafusion")]
mod expr_row_filter_impl {
    use super::*;
    use arrow_array::ArrayRef;
    use arrow_cast::{CastOptions, cast_with_options};

    /// Row filter that evaluates DataFusion expressions for exact filtering
    ///
    /// Unlike [`RowFilter`], this evaluates the expression tree at the row level for the
    /// supported operators. It returns an error if the expression contains unsupported
    /// constructs or types.
    ///
    /// # Example
    /// ```rust,ignore
    /// use aisle::ExprRowFilter;
    /// use datafusion_expr::{col, lit};
    /// use parquet::arrow::arrow_reader::RowFilter;
    ///
    /// let predicate = col("id").eq(lit(42i64)).and(col("name").like(lit("test%")));
    ///
    /// let filter = ExprRowFilter::new(
    ///     predicate,
    ///     parquet_metadata.file_metadata().schema_descr(),
    /// );
    ///
    /// let row_filter = RowFilter::new(vec![Box::new(filter)]);
    /// let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
    ///     .with_row_filter(row_filter)
    ///     .build()?;
    /// ```
    pub struct ExprRowFilter {
        expr: DfExpr,
        projection: ProjectionMask,
    }

    impl ExprRowFilter {
        /// Create a new row filter from a DataFusion expression
        ///
        /// # Arguments
        /// * `expr` - The DataFusion expression to evaluate
        /// * `parquet_schema` - Parquet schema descriptor (needed for projection)
        pub fn new(expr: DfExpr, parquet_schema: &SchemaDescriptor) -> Self {
            let projection = projection_from_expr(&expr, parquet_schema);

            Self { expr, projection }
        }

        /// Evaluate the DataFusion expression against a record batch
        fn evaluate_expr(&self, expr: &DfExpr, batch: &RecordBatch) -> Result<BooleanArray> {
            match expr {
                // Literal boolean
                DfExpr::Literal(ScalarValue::Boolean(Some(b)), _) => {
                    Ok(BooleanArray::from(vec![*b; batch.num_rows()]))
                }
                DfExpr::Literal(ScalarValue::Boolean(None), _) => {
                    Ok(BooleanArray::from(vec![None::<bool>; batch.num_rows()]))
                }

                // Binary expressions (comparisons and logical operators)
                DfExpr::BinaryExpr(BinaryExpr { left, op, right }) => {
                    self.evaluate_binary(batch, left, op, right)
                }

                // Column reference (returns null mask for filtering)
                DfExpr::Column(col) => {
                    let array = batch.column_by_name(&col.name).ok_or_else(|| {
                        ArrowError::SchemaError(format!("Column '{}' not found", col.name))
                    })?;

                    // For boolean columns, return the values directly
                    if let Some(bool_array) = array.as_any().downcast_ref::<BooleanArray>() {
                        Ok(bool_array.clone())
                    } else {
                        Err(ArrowError::CastError(format!(
                            "Column '{}' is not boolean",
                            col.name
                        )))
                    }
                }

                // IS NULL / IS NOT NULL
                DfExpr::IsNull(expr) => {
                    let array = self.evaluate_value(expr, batch)?;
                    Ok(arith_boolean::is_null(array.as_ref())?)
                }

                DfExpr::IsNotNull(expr) => {
                    let array = self.evaluate_value(expr, batch)?;
                    let null_mask = arith_boolean::is_null(array.as_ref())?;
                    Ok(arith_boolean::not(&null_mask)?)
                }

                // NOT
                DfExpr::Not(expr) => {
                    let inner = self.evaluate_expr(expr, batch)?;
                    Ok(arith_boolean::not(&inner)?)
                }

                // BETWEEN
                DfExpr::Between(Between {
                    expr,
                    negated,
                    low,
                    high,
                }) => self.evaluate_between_expr(batch, expr, *negated, low, high),

                // IN list
                DfExpr::InList(InList {
                    expr,
                    list,
                    negated,
                }) => self.evaluate_in_list_expr(batch, expr, list, *negated),

                // LIKE
                DfExpr::Like(like) => {
                    self.evaluate_like(batch, &like.expr, &like.pattern, like.negated)
                }

                // CAST
                DfExpr::Cast(cast) => {
                    self.evaluate_cast_expr(&cast.expr, &cast.data_type, false, batch)
                }
                DfExpr::TryCast(cast) => {
                    self.evaluate_cast_expr(&cast.expr, &cast.data_type, true, batch)
                }

                _ => Err(ArrowError::NotYetImplemented(format!(
                    "Expression evaluation not implemented: {:?}",
                    expr
                ))),
            }
        }

        fn evaluate_cast_expr(
            &self,
            expr: &DfExpr,
            data_type: &arrow_schema::DataType,
            safe: bool,
            batch: &RecordBatch,
        ) -> Result<BooleanArray> {
            let array = self.evaluate_value(expr, batch)?;
            let casted = self.cast_array(&array, data_type, safe)?;
            casted
                .as_any()
                .downcast_ref::<BooleanArray>()
                .cloned()
                .ok_or_else(|| {
                    ArrowError::CastError(format!(
                        "CAST result is not boolean (got {})",
                        casted.data_type()
                    ))
                })
        }

        fn evaluate_binary(
            &self,
            batch: &RecordBatch,
            left: &DfExpr,
            op: &Operator,
            right: &DfExpr,
        ) -> Result<BooleanArray> {
            use ord_cmp::{eq, gt, gt_eq, lt, lt_eq, neq};

            match op {
                // Logical operators
                Operator::And => {
                    let left_result = self.evaluate_expr(left, batch)?;
                    let right_result = self.evaluate_expr(right, batch)?;
                    Ok(arith_boolean::and(&left_result, &right_result)?)
                }
                Operator::Or => {
                    let left_result = self.evaluate_expr(left, batch)?;
                    let right_result = self.evaluate_expr(right, batch)?;
                    Ok(arith_boolean::or(&left_result, &right_result)?)
                }

                // Comparison operators
                Operator::Eq
                | Operator::NotEq
                | Operator::Lt
                | Operator::LtEq
                | Operator::Gt
                | Operator::GtEq => {
                    let left_value = self.evaluate_value(left, batch)?;
                    let right_value = self.evaluate_value(right, batch)?;

                    let left = left_value.as_ref();
                    let right = right_value.as_ref();

                    let result = match op {
                        Operator::Eq => eq(&left, &right)?,
                        Operator::NotEq => neq(&left, &right)?,
                        Operator::Lt => lt(&left, &right)?,
                        Operator::LtEq => lt_eq(&left, &right)?,
                        Operator::Gt => gt(&left, &right)?,
                        Operator::GtEq => gt_eq(&left, &right)?,
                        _ => unreachable!(),
                    };

                    Ok(result)
                }

                _ => Err(ArrowError::NotYetImplemented(format!(
                    "Operator not implemented: {:?}",
                    op
                ))),
            }
        }

        fn evaluate_value(&self, expr: &DfExpr, batch: &RecordBatch) -> Result<ArrayRef> {
            match expr {
                DfExpr::Column(col) => batch.column_by_name(&col.name).cloned().ok_or_else(|| {
                    ArrowError::SchemaError(format!("Column '{}' not found", col.name))
                }),
                DfExpr::Literal(val, _) => Ok(val.to_array_of_size(batch.num_rows())?),
                DfExpr::Cast(cast) => {
                    let array = self.evaluate_value(&cast.expr, batch)?;
                    self.cast_array(&array, &cast.data_type, false)
                }
                DfExpr::TryCast(cast) => {
                    let array = self.evaluate_value(&cast.expr, batch)?;
                    self.cast_array(&array, &cast.data_type, true)
                }
                DfExpr::Alias(alias) => self.evaluate_value(&alias.expr, batch),
                _ => Err(ArrowError::NotYetImplemented(format!(
                    "Expression value evaluation not implemented: {:?}",
                    expr
                ))),
            }
        }

        fn cast_array(
            &self,
            array: &ArrayRef,
            data_type: &arrow_schema::DataType,
            safe: bool,
        ) -> Result<ArrayRef> {
            let options = CastOptions {
                safe,
                ..Default::default()
            };
            cast_with_options(array.as_ref(), data_type, &options)
        }

        fn evaluate_between_expr(
            &self,
            batch: &RecordBatch,
            expr: &DfExpr,
            negated: bool,
            low: &DfExpr,
            high: &DfExpr,
        ) -> Result<BooleanArray> {
            use ord_cmp::{gt_eq, lt_eq};
            let values = self.evaluate_value(expr, batch)?;
            let low_array = self.evaluate_value(low, batch)?;
            let high_array = self.evaluate_value(high, batch)?;

            let values_ref = values.as_ref();
            let low_ref = low_array.as_ref();
            let high_ref = high_array.as_ref();

            let low_cmp = gt_eq(&values_ref, &low_ref)?;
            let high_cmp = lt_eq(&values_ref, &high_ref)?;
            let mut result = arith_boolean::and(&low_cmp, &high_cmp)?;

            if negated {
                result = arith_boolean::not(&result)?;
            }

            Ok(result)
        }

        fn evaluate_in_list_expr(
            &self,
            batch: &RecordBatch,
            expr: &DfExpr,
            list: &[DfExpr],
            negated: bool,
        ) -> Result<BooleanArray> {
            use ord_cmp::eq;
            let values = self.evaluate_value(expr, batch)?;

            let values_ref = values.as_ref();
            let mut result = BooleanArray::from(vec![false; batch.num_rows()]);

            for item in list {
                let item_array = self.evaluate_value(item, batch)?;
                let item_ref = item_array.as_ref();
                let matches = eq(&values_ref, &item_ref)?;
                result = arith_boolean::or(&result, &matches)?;
            }

            if negated {
                result = arith_boolean::not(&result)?;
            }

            Ok(result)
        }

        fn evaluate_like(
            &self,
            batch: &RecordBatch,
            expr: &DfExpr,
            pattern: &DfExpr,
            negated: bool,
        ) -> Result<BooleanArray> {
            use arrow_array::{LargeStringArray, StringArray, StringViewArray};

            enum StringArrayRef<'a> {
                Utf8(&'a StringArray),
                LargeUtf8(&'a LargeStringArray),
                Utf8View(&'a StringViewArray),
            }

            fn as_string_array<'a>(array: &'a ArrayRef) -> Result<StringArrayRef<'a>> {
                if let Some(arr) = array.as_any().downcast_ref::<StringArray>() {
                    return Ok(StringArrayRef::Utf8(arr));
                }
                if let Some(arr) = array.as_any().downcast_ref::<LargeStringArray>() {
                    return Ok(StringArrayRef::LargeUtf8(arr));
                }
                if let Some(arr) = array.as_any().downcast_ref::<StringViewArray>() {
                    return Ok(StringArrayRef::Utf8View(arr));
                }
                Err(ArrowError::CastError(format!(
                    "LIKE requires string arrays (got {})",
                    array.data_type()
                )))
            }

            fn string_at<'a>(array: &StringArrayRef<'a>, row: usize) -> Option<&'a str> {
                match array {
                    StringArrayRef::Utf8(arr) => {
                        if arr.is_null(row) {
                            None
                        } else {
                            Some(arr.value(row))
                        }
                    }
                    StringArrayRef::LargeUtf8(arr) => {
                        if arr.is_null(row) {
                            None
                        } else {
                            Some(arr.value(row))
                        }
                    }
                    StringArrayRef::Utf8View(arr) => {
                        if arr.is_null(row) {
                            None
                        } else {
                            Some(arr.value(row))
                        }
                    }
                }
            }

            let values = self.evaluate_value(expr, batch)?;
            let patterns = self.evaluate_value(pattern, batch)?;

            let values = as_string_array(&values)?;
            let patterns = as_string_array(&patterns)?;

            let mut out: Vec<Option<bool>> = Vec::with_capacity(batch.num_rows());
            for row in 0..batch.num_rows() {
                let value = string_at(&values, row);
                let pattern = string_at(&patterns, row);
                let result = match (value, pattern) {
                    (Some(value), Some(pattern)) => {
                        if !pattern.ends_with('%') || pattern.matches('%').count() != 1 {
                            return Err(ArrowError::NotYetImplemented(
                                "Only prefix LIKE patterns (e.g., 'prefix%') are supported"
                                    .to_string(),
                            ));
                        }
                        let prefix = &pattern[..pattern.len() - 1];
                        Some(value.starts_with(prefix))
                    }
                    _ => None,
                };
                out.push(result);
            }

            let mut result = BooleanArray::from(out);

            if negated {
                result = arith_boolean::not(&result)?;
            }

            Ok(result)
        }

        /// Evaluate the filter against a batch and return a boolean mask
        pub fn filter_batch(&self, batch: &RecordBatch) -> Result<BooleanArray> {
            self.evaluate_expr(&self.expr, batch)
        }

        /// Evaluate and apply the filter, returning only matching rows
        pub fn apply_filter(&self, batch: RecordBatch) -> Result<RecordBatch> {
            let mask = self.filter_batch(&batch)?;
            filter_record_batch(&batch, &mask)
        }
    }

    impl ArrowPredicate for ExprRowFilter {
        fn evaluate(&mut self, batch: RecordBatch) -> Result<BooleanArray> {
            self.filter_batch(&batch)
        }

        fn projection(&self) -> &ProjectionMask {
            &self.projection
        }
    }

    fn projection_from_expr(expr: &DfExpr, parquet_schema: &SchemaDescriptor) -> ProjectionMask {
        let mut columns = HashSet::new();
        let supported = collect_expr_columns(expr, &mut columns);
        if !supported {
            return ProjectionMask::all();
        }
        projection_from_columns(&columns, parquet_schema)
    }

    fn collect_expr_columns(expr: &DfExpr, columns: &mut HashSet<String>) -> bool {
        let mut df_columns = HashSet::new();
        if expr_to_columns(expr, &mut df_columns).is_err() {
            return false;
        }

        for column in df_columns {
            columns.insert(column.name);
        }

        true
    }
} // end mod expr_row_filter_impl

fn projection_from_ir(expr: &IrExpr, parquet_schema: &SchemaDescriptor) -> ProjectionMask {
    let mut columns = HashSet::new();
    collect_ir_columns(expr, &mut columns);
    projection_from_columns(&columns, parquet_schema)
}

fn projection_from_columns(
    columns: &HashSet<String>,
    parquet_schema: &SchemaDescriptor,
) -> ProjectionMask {
    if columns.is_empty() {
        return ProjectionMask::all();
    }
    let names: Vec<&str> = columns.iter().map(|name| name.as_str()).collect();
    ProjectionMask::columns(parquet_schema, names)
}

fn collect_ir_columns(expr: &IrExpr, columns: &mut HashSet<String>) {
    match expr {
        IrExpr::True | IrExpr::False => {}
        IrExpr::Cmp { column, .. }
        | IrExpr::Between { column, .. }
        | IrExpr::InList { column, .. }
        | IrExpr::StartsWith { column, .. }
        | IrExpr::IsNull { column, .. } => {
            columns.insert(column.clone());
        }
        IrExpr::And(exprs) | IrExpr::Or(exprs) => {
            for expr in exprs {
                collect_ir_columns(expr, columns);
            }
        }
        IrExpr::Not(expr) => collect_ir_columns(expr, columns),
        IrExpr::BloomFilterEq { column, .. } | IrExpr::BloomFilterInList { column, .. } => {
            columns.insert(column.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Int64Array, LargeStringArray, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    #[cfg(feature = "datafusion")]
    use datafusion_common::Column;
    use datafusion_common::ScalarValue;
    #[cfg(feature = "datafusion")]
    use datafusion_expr::{
        BinaryExpr, Expr as DfExpr, Operator,
        expr::{InList, Like},
    };
    use parquet::{
        basic::Type as PhysicalType,
        schema::types::{SchemaDescriptor, Type},
    };

    use super::*;

    // Helper to create a simple ParquetSchemaDescriptor for tests
    fn schema_descr(arrow_schema: &Schema) -> SchemaDescriptor {
        // Convert Arrow fields to Parquet fields
        let fields: Vec<_> = arrow_schema
            .fields()
            .iter()
            .map(|field| {
                let physical_type = match field.data_type() {
                    DataType::Int64 => PhysicalType::INT64,
                    DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => {
                        PhysicalType::BYTE_ARRAY
                    }
                    _ => PhysicalType::INT64, // Default for tests
                };

                Arc::new(
                    Type::primitive_type_builder(field.name(), physical_type)
                        .build()
                        .unwrap(),
                )
            })
            .collect();

        let group_type = Type::group_type_builder("schema")
            .with_fields(fields)
            .build()
            .unwrap();

        SchemaDescriptor::new(Arc::new(group_type))
    }

    #[test]
    fn test_equality_filter() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();

        let expr = IrExpr::Cmp {
            column: "id".to_string(),
            op: CmpOp::Eq,
            value: ScalarValue::Int64(Some(3)),
        };

        let mut filter = RowFilter::new(expr, &schema_descr(&schema));
        let result = filter.evaluate(batch).unwrap();

        assert_eq!(result.len(), 5);
        assert!(!result.value(0));
        assert!(!result.value(1));
        assert!(result.value(2)); // id == 3
        assert!(!result.value(3));
        assert!(!result.value(4));
    }

    #[test]
    fn test_range_filter() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![5, 10, 15, 20, 25]))],
        )
        .unwrap();

        let expr = IrExpr::Between {
            column: "value".to_string(),
            low: ScalarValue::Int64(Some(10)),
            high: ScalarValue::Int64(Some(20)),
            inclusive: true,
        };

        let mut filter = RowFilter::new(expr, &schema_descr(&schema));
        let result = filter.evaluate(batch).unwrap();

        assert_eq!(result.len(), 5);
        assert!(!result.value(0)); // 5
        assert!(result.value(1)); // 10
        assert!(result.value(2)); // 15
        assert!(result.value(3)); // 20
        assert!(!result.value(4)); // 25
    }

    #[test]
    fn test_in_list_filter() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();

        let expr = IrExpr::InList {
            column: "id".to_string(),
            values: vec![ScalarValue::Int64(Some(2)), ScalarValue::Int64(Some(4))],
        };

        let mut filter = RowFilter::new(expr, &schema_descr(&schema));
        let result = filter.evaluate(batch).unwrap();

        assert_eq!(result.len(), 5);
        assert!(!result.value(0)); // 1
        assert!(result.value(1)); // 2
        assert!(!result.value(2)); // 3
        assert!(result.value(3)); // 4
        assert!(!result.value(4)); // 5
    }

    #[test]
    fn test_string_starts_with() {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringArray::from(vec![
                "alice", "bob", "admin", "user",
            ]))],
        )
        .unwrap();

        let expr = IrExpr::StartsWith {
            column: "name".to_string(),
            prefix: "a".to_string(),
        };

        let mut filter = RowFilter::new(expr, &schema_descr(&schema));
        let result = filter.evaluate(batch).unwrap();

        assert_eq!(result.len(), 4);
        assert!(result.value(0)); // alice
        assert!(!result.value(1)); // bob
        assert!(result.value(2)); // admin
        assert!(!result.value(3)); // user
    }

    #[test]
    fn test_ir_starts_with_large_utf8() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "name",
            DataType::LargeUtf8,
            false,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(LargeStringArray::from(vec![
                "alice", "bob", "admin", "user",
            ]))],
        )
        .unwrap();

        let expr = IrExpr::StartsWith {
            column: "name".to_string(),
            prefix: "a".to_string(),
        };

        let mut filter = RowFilter::new(expr, &schema_descr(&schema));
        let result = filter.evaluate(batch).unwrap();

        assert_eq!(result.len(), 4);
        assert!(result.value(0)); // alice
        assert!(!result.value(1)); // bob
        assert!(result.value(2)); // admin
        assert!(!result.value(3)); // user
    }

    #[test]
    #[cfg(feature = "datafusion")]
    fn test_expr_like_large_utf8() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "name",
            DataType::LargeUtf8,
            false,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(LargeStringArray::from(vec![
                "alice", "bob", "admin", "user",
            ]))],
        )
        .unwrap();

        let expr = DfExpr::Like(Like::new(
            false,
            Box::new(DfExpr::Column(Column::from_name("name"))),
            Box::new(DfExpr::Literal(
                ScalarValue::LargeUtf8(Some("a%".to_string())),
                None,
            )),
            None,
            false,
        ));

        let mut filter = ExprRowFilter::new(expr, &schema_descr(&schema));
        let result = filter.evaluate(batch).unwrap();

        assert_eq!(result.len(), 4);
        assert!(result.value(0)); // alice
        assert!(!result.value(1)); // bob
        assert!(result.value(2)); // admin
        assert!(!result.value(3)); // user
    }

    #[test]
    #[cfg(feature = "datafusion")]
    fn test_expr_like_utf8_view() {
        use arrow_array::StringViewArray;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "name",
            DataType::Utf8View,
            false,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(StringViewArray::from(vec![
                Some("alice"),
                Some("bob"),
                Some("admin"),
                Some("user"),
            ]))],
        )
        .unwrap();

        let expr = DfExpr::Like(Like::new(
            false,
            Box::new(DfExpr::Column(Column::from_name("name"))),
            Box::new(DfExpr::Literal(
                ScalarValue::Utf8View(Some("a%".to_string())),
                None,
            )),
            None,
            false,
        ));

        let mut filter = ExprRowFilter::new(expr, &schema_descr(&schema));
        let result = filter.evaluate(batch).unwrap();

        assert_eq!(result.len(), 4);
        assert!(result.value(0)); // alice
        assert!(!result.value(1)); // bob
        assert!(result.value(2)); // admin
        assert!(!result.value(3)); // user
    }

    #[test]
    #[cfg(feature = "datafusion")]
    fn test_expr_cast_eq_utf8() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5]))],
        )
        .unwrap();

        let cast_expr = DfExpr::Cast(datafusion_expr::expr::Cast::new(
            Box::new(DfExpr::Column(Column::from_name("id"))),
            DataType::Utf8,
        ));

        let expr = DfExpr::BinaryExpr(BinaryExpr {
            left: Box::new(cast_expr),
            op: Operator::Eq,
            right: Box::new(DfExpr::Literal(
                ScalarValue::Utf8(Some("3".to_string())),
                None,
            )),
        });

        let mut filter = ExprRowFilter::new(expr, &schema_descr(&schema));
        let result = filter.evaluate(batch).unwrap();

        assert_eq!(result.len(), 5);
        assert!(!result.value(0));
        assert!(!result.value(1));
        assert!(result.value(2)); // "3"
        assert!(!result.value(3));
        assert!(!result.value(4));
    }

    #[test]
    #[cfg(feature = "datafusion")]
    fn test_expr_in_list_cast_utf8() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 2, 3, 4]))],
        )
        .unwrap();

        let cast_expr = DfExpr::Cast(datafusion_expr::expr::Cast::new(
            Box::new(DfExpr::Column(Column::from_name("id"))),
            DataType::Utf8,
        ));

        let expr = DfExpr::InList(InList {
            expr: Box::new(cast_expr),
            list: vec![
                DfExpr::Literal(ScalarValue::Utf8(Some("2".to_string())), None),
                DfExpr::Literal(ScalarValue::Utf8(Some("4".to_string())), None),
            ],
            negated: false,
        });

        let mut filter = ExprRowFilter::new(expr, &schema_descr(&schema));
        let result = filter.evaluate(batch).unwrap();

        assert_eq!(result.len(), 4);
        assert!(!result.value(0));
        assert!(result.value(1));
        assert!(!result.value(2));
        assert!(result.value(3));
    }

    #[test]
    #[cfg(feature = "datafusion")]
    fn test_expr_is_null_cast() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, true)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![Some(1), None, Some(2)]))],
        )
        .unwrap();

        let cast_expr = DfExpr::Cast(datafusion_expr::expr::Cast::new(
            Box::new(DfExpr::Column(Column::from_name("id"))),
            DataType::Int32,
        ));

        let expr = DfExpr::IsNull(Box::new(cast_expr));

        let mut filter = ExprRowFilter::new(expr, &schema_descr(&schema));
        let result = filter.evaluate(batch).unwrap();

        assert_eq!(result.len(), 3);
        assert!(!result.value(0));
        assert!(result.value(1));
        assert!(!result.value(2));
    }

    #[test]
    fn test_and_filter() {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1, 5, 10, 15, 20]))],
        )
        .unwrap();

        let expr = IrExpr::And(vec![
            IrExpr::Cmp {
                column: "id".to_string(),
                op: CmpOp::GtEq,
                value: ScalarValue::Int64(Some(5)),
            },
            IrExpr::Cmp {
                column: "id".to_string(),
                op: CmpOp::Lt,
                value: ScalarValue::Int64(Some(15)),
            },
        ]);

        let mut filter = RowFilter::new(expr, &schema_descr(&schema));
        let result = filter.evaluate(batch).unwrap();

        assert_eq!(result.len(), 5);
        assert!(!result.value(0)); // 1
        assert!(result.value(1)); // 5
        assert!(result.value(2)); // 10
        assert!(!result.value(3)); // 15
        assert!(!result.value(4)); // 20
    }
}
