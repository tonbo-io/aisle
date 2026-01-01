use std::collections::HashMap;

use arrow_schema::{DataType, Field, Schema};
use datafusion_common::{Column, ScalarValue};
use datafusion_expr::{
    Between, BinaryExpr, Expr, Operator,
    expr::{InList, Like},
    utils::split_conjunction,
};

use crate::{
    error::CompileError,
    ir::{CmpOp, IrExpr},
};

/// Get a user-friendly name for an expression type
fn expr_type_name(expr: &Expr) -> String {
    match expr {
        Expr::Column(col) => format!("column '{}'", col.name),
        Expr::Literal(val, _) => format!("literal {}", val),
        Expr::BinaryExpr(BinaryExpr { op, .. }) => {
            format!("binary operation '{}'", operator_symbol(op))
        }
        Expr::Like(_) => "LIKE expression".to_string(),
        Expr::ScalarFunction(func) => format!("function '{:?}'", func.func),
        Expr::AggregateFunction(func) => format!("aggregate function '{:?}'", func.func),
        Expr::Case(_) => "CASE expression".to_string(),
        Expr::Cast(cast) => format!("CAST to {}", cast.data_type),
        Expr::Between(_) => "BETWEEN expression".to_string(),
        Expr::InList(_) => "IN expression".to_string(),
        Expr::IsNull(_) => "IS NULL expression".to_string(),
        Expr::IsNotNull(_) => "IS NOT NULL expression".to_string(),
        Expr::Not(_) => "NOT expression".to_string(),
        Expr::Negative(_) => "negation".to_string(),
        Expr::TryCast(cast) => format!("TRY_CAST to {}", cast.data_type),
        Expr::Alias(_) => "aliased expression".to_string(),
        _ => "complex expression".to_string(),
    }
}

/// Get a user-friendly symbol for an operator
fn operator_symbol(op: &Operator) -> &'static str {
    match op {
        Operator::Eq => "=",
        Operator::NotEq => "!=",
        Operator::Lt => "<",
        Operator::LtEq => "<=",
        Operator::Gt => ">",
        Operator::GtEq => ">=",
        Operator::Plus => "+",
        Operator::Minus => "-",
        Operator::Multiply => "*",
        Operator::Divide => "/",
        Operator::Modulo => "%",
        Operator::And => "AND",
        Operator::Or => "OR",
        Operator::LikeMatch => "LIKE",
        Operator::NotLikeMatch => "NOT LIKE",
        Operator::ILikeMatch => "ILIKE",
        Operator::NotILikeMatch => "NOT ILIKE",
        Operator::RegexMatch => "~",
        Operator::RegexIMatch => "~*",
        Operator::RegexNotMatch => "!~",
        Operator::RegexNotIMatch => "!~*",
        Operator::BitwiseAnd => "&",
        Operator::BitwiseOr => "|",
        Operator::BitwiseXor => "^",
        Operator::BitwiseShiftLeft => "<<",
        Operator::BitwiseShiftRight => ">>",
        Operator::StringConcat => "||",
        Operator::AtArrow => "@>",
        Operator::ArrowAt => "<@",
        Operator::IsDistinctFrom => "IS DISTINCT FROM",
        Operator::IsNotDistinctFrom => "IS NOT DISTINCT FROM",
        _ => "?",
    }
}

/// Result of compiling a DataFusion expression into pruning IR.
///
/// Uses an **error accumulation** strategy: attempts to compile all
/// predicates and returns both successes and failures. This allows
/// partial pruning even when some predicates cannot be compiled.
///
/// # Example
/// ```no_run
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
///     .with_predicate(&expr)
///     .prune();
///
/// let compile = result.compile_result();
/// assert_eq!(compile.prunable_count(), 1); // a > 5 compiled
/// assert_eq!(compile.error_count(), 1); // a + 10 failed
/// ```
#[derive(Clone, Debug, Default)]
pub struct CompileResult {
    /// Successfully compiled predicates that can be evaluated against metadata
    prunable: Vec<IrExpr>,
    /// Compilation errors for predicates that could not be compiled
    errors: Vec<CompileError>,
}

impl CompileResult {
    /// Get the successfully compiled predicates (internal)
    pub(crate) fn prunable(&self) -> &[IrExpr] {
        &self.prunable
    }

    /// Get the successfully compiled IR expressions
    ///
    /// Returns a slice of all predicates that were successfully compiled into IR.
    /// These can be used with [`IrRowFilter`](crate::IrRowFilter) for row-level filtering.
    ///
    /// # Example
    /// ```no_run
    /// # use aisle::PruneRequest;
    /// # use datafusion_expr::{col, lit};
    /// # use parquet::file::metadata::ParquetMetaData;
    /// # use arrow_schema::Schema;
    /// # use std::sync::Arc;
    /// # let metadata: ParquetMetaData = todo!();
    /// # let schema: Arc<Schema> = todo!();
    /// let predicate = col("id").eq(lit(42i64));
    /// let result = PruneRequest::new(&metadata, &schema)
    ///     .with_predicate(&predicate)
    ///     .prune();
    ///
    /// let ir_exprs = result.compile_result().ir_exprs();
    /// println!("Compiled {} predicates", ir_exprs.len());
    /// ```
    pub fn ir_exprs(&self) -> &[IrExpr] {
        &self.prunable
    }

    /// Get the compilation errors
    pub fn errors(&self) -> &[CompileError] {
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

pub fn compile_pruning_ir(expr: &Expr, schema: &Schema) -> CompileResult {
    let schema_index = build_schema_path_index(schema);
    compile_pruning_ir_with_index(expr, schema, &schema_index)
}

pub(crate) fn compile_pruning_ir_with_index(
    expr: &Expr,
    schema: &Schema,
    schema_index: &SchemaPathIndex,
) -> CompileResult {
    let mut result = CompileResult::default();
    for predicate in split_conjunction(expr) {
        match compile_expr(predicate, schema, schema_index) {
            Ok(ir) => result.prunable.push(ir),
            Err(err) => result.errors.push(err),
        }
    }
    result
}

/// Schema path index for efficient column resolution
///
/// Maps column names and paths to their types for nested field lookup
#[derive(Debug, Clone)]
pub(crate) struct SchemaPathIndex {
    /// Maps full dotted paths to (DataType, Field)
    /// Example: "a.b.c" -> (Int32, field)
    paths: HashMap<String, (DataType, Field)>,
    /// Maps leaf names to all matching paths
    /// Example: "c" -> ["a.b.c", "x.y.c"] (for ambiguity detection)
    leaf_to_paths: HashMap<String, Vec<String>>,
}

impl SchemaPathIndex {
    /// Resolve an unqualified column name (leaf name) to a canonical path.
    fn resolve_unqualified(&self, name: &str) -> Result<(String, DataType), CompileError> {
        match self.leaf_to_paths.get(name) {
            None => Err(CompileError::ColumnNotFound {
                column_name: name.to_string(),
            }),
            Some(paths) if paths.len() == 1 => {
                let path = &paths[0];
                let (data_type, _) = self.paths.get(path).unwrap();
                Ok((path.clone(), data_type.clone()))
            }
            Some(paths) => Err(CompileError::AmbiguousColumn {
                column_name: name.to_string(),
                candidates: paths.clone(),
            }),
        }
    }

    /// Resolve a column reference to a canonical path and data type
    fn resolve_column(&self, column: &Column) -> Result<(String, DataType), CompileError> {
        match &column.relation {
            None => {
                // Simple name - check for unique leaf name match
                self.resolve_unqualified(&column.name)
            }
            Some(relation) => {
                // Relation-qualified - DataFusion parses multi-segment paths:
                // "a.b.c.d" becomes Column { relation: Full { catalog: "a", schema: "b", table: "c"
                // }, name: "d" } "a.b.c" becomes Column { relation: Partial {
                // schema: "a", table: "b" }, name: "c" } "a.b" becomes Column {
                // relation: Bare { table: "a" }, name: "b" }

                // Reconstruct the full path from relation components
                let dotted = if let Some(catalog) = relation.catalog() {
                    // Four-segment: catalog.schema.table.name
                    format!(
                        "{}.{}.{}.{}",
                        catalog,
                        relation.schema().unwrap(),
                        relation.table(),
                        column.name
                    )
                } else if let Some(schema) = relation.schema() {
                    // Three-segment: schema.table.name
                    format!("{}.{}.{}", schema, relation.table(), column.name)
                } else {
                    // Two-segment: table.name
                    format!("{}.{}", relation.table(), column.name)
                };

                // If the fully qualified path exists, check for potential ambiguity
                if let Some((data_type, _)) = self.paths.get(&dotted) {
                    let is_bare = relation.catalog().is_none() && relation.schema().is_none();
                    if !is_bare {
                        // For schema/catalog-qualified paths, accept the explicit path
                        return Ok((dotted, data_type.clone()));
                    }

                    // Check if the leaf name uniquely resolves to a different path
                    // This handles cases like "a.b" where:
                    // - "a.b" exists (nested field)
                    // - "b" exists (top-level field)
                    match self.resolve_unqualified(&column.name) {
                        Ok((path, _)) if path != dotted => {
                            // Leaf uniquely resolves to a different path - this is ambiguous
                            return Err(CompileError::AmbiguousColumn {
                                column_name: dotted.clone(),
                                candidates: vec![path, dotted],
                            });
                        }
                        Err(CompileError::AmbiguousColumn { candidates, .. }) => {
                            // Leaf is ambiguous - check if any candidate is a suffix of dotted
                            // Example: "a.b" vs "b" (b is suffix) -> ambiguous
                            // Example: "my_map.pairs.value" vs "my_map.key_value.value" (no suffix)
                            // -> not ambiguous
                            let has_suffix_candidate = candidates
                                .iter()
                                .any(|c| c != &dotted && dotted.ends_with(&format!(".{}", c)));
                            if has_suffix_candidate {
                                // True ambiguity: dotted path vs shorter suffix path
                                return Err(CompileError::AmbiguousColumn {
                                    column_name: dotted,
                                    candidates,
                                });
                            }
                            // False ambiguity (Map coercion): different paths with same leaf
                            return Ok((dotted, data_type.clone()));
                        }
                        // In all other cases, accept the qualified path:
                        // - Leaf matches dotted path (same field)
                        // - Leaf not found (dotted path is the only match)
                        _ => return Ok((dotted, data_type.clone())),
                    }
                }

                // Fall back to unqualified resolution (likely table qualification)
                self.resolve_unqualified(&column.name)
            }
        }
    }
}

/// Build a schema path index from an Arrow schema
pub(crate) fn build_schema_path_index(schema: &Schema) -> SchemaPathIndex {
    let mut paths = HashMap::new();
    let mut leaf_to_paths: HashMap<String, Vec<String>> = HashMap::new();

    for field in schema.fields() {
        visit_field(field, String::new(), &mut paths, &mut leaf_to_paths);
    }

    SchemaPathIndex {
        paths,
        leaf_to_paths,
    }
}

/// Recursively visit a field and its nested children
fn visit_field(
    field: &Field,
    prefix: String,
    paths: &mut HashMap<String, (DataType, Field)>,
    leaf_to_paths: &mut HashMap<String, Vec<String>>,
) {
    let path = if prefix.is_empty() {
        field.name().to_string()
    } else {
        format!("{}.{}", prefix, field.name())
    };

    // Record this path
    paths.insert(path.clone(), (field.data_type().clone(), field.clone()));

    // Record leaf name mapping
    leaf_to_paths
        .entry(field.name().to_string())
        .or_insert_with(Vec::new)
        .push(path.clone());

    // Recurse into nested types
    match field.data_type() {
        DataType::Struct(nested_fields) => {
            for nested_field in nested_fields.iter() {
                visit_field(nested_field, path.clone(), paths, leaf_to_paths);
            }
        }
        DataType::List(element_field)
        | DataType::LargeList(element_field)
        | DataType::FixedSizeList(element_field, _) => {
            // Parquet uses 3-level LIST encoding for all list variants: <name>.list.<element_name>
            // The intermediate "list" group is required by the Parquet spec
            // LargeList (i64 offsets) and FixedSizeList use the same physical encoding as List
            let list_path = format!("{}.list", path);
            visit_field(element_field, list_path.clone(), paths, leaf_to_paths);

            // Also register the standard "element" path (for coerce_types = true)
            // if the Arrow field name is different
            if element_field.name() != "element" {
                let standard_path = format!("{}.list.element", path);
                let element_type = element_field.data_type().clone();
                paths.insert(
                    standard_path.clone(),
                    (element_type, element_field.as_ref().clone()),
                );
                leaf_to_paths
                    .entry("element".to_string())
                    .or_insert_with(Vec::new)
                    .push(standard_path);
            }
        }
        DataType::Map(entries_field, _) => {
            // Parquet uses 3-level MAP encoding: <name>.<entries_name>.{key, value}
            // The intermediate group uses the entries field name from Arrow schema
            if let DataType::Struct(kv_fields) = entries_field.data_type() {
                let entries_path = format!("{}.{}", path, entries_field.name());
                for kv_field in kv_fields.iter() {
                    visit_field(
                        kv_field.as_ref(),
                        entries_path.clone(),
                        paths,
                        leaf_to_paths,
                    );
                }

                // Also register standard "key"/"value" leaf names when Arrow uses non-standard
                // names
                if kv_fields.len() >= 2 {
                    let key_field = &kv_fields[0];
                    let value_field = &kv_fields[1];
                    if key_field.name() != "key" {
                        let key_path = format!("{}.key", entries_path);
                        paths.insert(
                            key_path.clone(),
                            (key_field.data_type().clone(), key_field.as_ref().clone()),
                        );
                        leaf_to_paths
                            .entry("key".to_string())
                            .or_insert_with(Vec::new)
                            .push(key_path);
                    }
                    if value_field.name() != "value" {
                        let value_path = format!("{}.value", entries_path);
                        paths.insert(
                            value_path.clone(),
                            (
                                value_field.data_type().clone(),
                                value_field.as_ref().clone(),
                            ),
                        );
                        leaf_to_paths
                            .entry("value".to_string())
                            .or_insert_with(Vec::new)
                            .push(value_path);
                    }
                }

                // Also register the standard "key_value" paths (for coerce_types = true)
                // if the Arrow field name is different
                if entries_field.name() != "key_value" {
                    let standard_path = format!("{}.key_value", path);
                    for kv_field in kv_fields.iter() {
                        let field_path = format!("{}.{}", standard_path, kv_field.name());
                        paths.insert(
                            field_path.clone(),
                            (kv_field.data_type().clone(), kv_field.as_ref().clone()),
                        );
                        leaf_to_paths
                            .entry(kv_field.name().to_string())
                            .or_insert_with(Vec::new)
                            .push(field_path);
                    }

                    // Also register standard "key"/"value" under key_value for coerced files
                    if kv_fields.len() >= 2 {
                        let key_field = &kv_fields[0];
                        let value_field = &kv_fields[1];
                        if key_field.name() != "key" {
                            let key_path = format!("{}.key", standard_path);
                            paths.insert(
                                key_path.clone(),
                                (key_field.data_type().clone(), key_field.as_ref().clone()),
                            );
                            leaf_to_paths
                                .entry("key".to_string())
                                .or_insert_with(Vec::new)
                                .push(key_path);
                        }
                        if value_field.name() != "value" {
                            let value_path = format!("{}.value", standard_path);
                            paths.insert(
                                value_path.clone(),
                                (
                                    value_field.data_type().clone(),
                                    value_field.as_ref().clone(),
                                ),
                            );
                            leaf_to_paths
                                .entry("value".to_string())
                                .or_insert_with(Vec::new)
                                .push(value_path);
                        }
                    }
                }
            }
        }
        _ => {}
    }
}

fn compile_expr(
    expr: &Expr,
    schema: &Schema,
    schema_index: &SchemaPathIndex,
) -> Result<IrExpr, CompileError> {
    match expr {
        Expr::Alias(alias) => compile_expr(&alias.expr, schema, schema_index),
        Expr::Literal(ScalarValue::Boolean(Some(true)), _) => Ok(IrExpr::True),
        Expr::Literal(ScalarValue::Boolean(Some(false)), _) => Ok(IrExpr::False),
        Expr::BinaryExpr(BinaryExpr { left, op, right }) => match op {
            Operator::And => {
                let left_ir = compile_expr(left, schema, schema_index)?;
                let right_ir = compile_expr(right, schema, schema_index)?;
                Ok(IrExpr::And(vec![left_ir, right_ir]))
            }
            Operator::Or => {
                let left_ir = compile_expr(left, schema, schema_index)?;
                let right_ir = compile_expr(right, schema, schema_index)?;
                Ok(IrExpr::Or(vec![left_ir, right_ir]))
            }
            Operator::Eq
            | Operator::NotEq
            | Operator::Lt
            | Operator::LtEq
            | Operator::Gt
            | Operator::GtEq => {
                let (column, value, op) = extract_column_literal(left, right, *op, schema_index)?;
                let rule = IrExpr::Cmp { column, op, value };
                Ok(with_bloom_if_applicable(rule))
            }
            _ => Err(CompileError::UnsupportedOperator {
                operator: operator_symbol(op).to_string(),
            }),
        },
        Expr::Between(Between {
            expr,
            negated,
            low,
            high,
        }) => {
            if *negated {
                return Err(CompileError::NegatedNotSupported {
                    predicate_type: "BETWEEN".to_string(),
                });
            }
            let (column_expr, cast_type) = extract_column_with_cast(expr)?;
            let (column, data_type) = schema_index.resolve_column(&column_expr)?;
            ensure_noop_cast(&column_expr, &data_type, cast_type)?;
            let low_lit = extract_literal(low)?;
            let low = low_lit
                .cast_to(&data_type)
                .map_err(|e| CompileError::TypeCastError {
                    literal_type: low_lit.data_type().clone(),
                    target_type: data_type.clone(),
                    reason: e.to_string(),
                })?;
            let high_lit = extract_literal(high)?;
            let high = high_lit
                .cast_to(&data_type)
                .map_err(|e| CompileError::TypeCastError {
                    literal_type: high_lit.data_type().clone(),
                    target_type: data_type.clone(),
                    reason: e.to_string(),
                })?;
            Ok(IrExpr::Between {
                column,
                low,
                high,
                inclusive: true,
            })
        }
        Expr::InList(InList {
            expr,
            list,
            negated,
        }) => {
            if *negated {
                return Err(CompileError::NegatedNotSupported {
                    predicate_type: "IN".to_string(),
                });
            }
            let (column_expr, cast_type) = extract_column_with_cast(expr)?;
            let (column, data_type) = schema_index.resolve_column(&column_expr)?;
            ensure_noop_cast(&column_expr, &data_type, cast_type)?;
            let mut values = Vec::with_capacity(list.len());
            for item in list {
                let lit_val = extract_literal(item)?;
                let lit = lit_val
                    .cast_to(&data_type)
                    .map_err(|e| CompileError::TypeCastError {
                        literal_type: lit_val.data_type().clone(),
                        target_type: data_type.clone(),
                        reason: e.to_string(),
                    })?;
                values.push(lit);
            }
            let rule = IrExpr::InList { column, values };
            Ok(with_bloom_if_applicable(rule))
        }
        Expr::Like(Like {
            negated,
            expr,
            pattern,
            escape_char,
            case_insensitive,
        }) => {
            if *negated {
                return Err(CompileError::NegatedNotSupported {
                    predicate_type: "LIKE".to_string(),
                });
            }
            if *case_insensitive {
                return Err(CompileError::UnsupportedExpr {
                    expr_type: "ILIKE expressions are not supported".to_string(),
                });
            }
            if escape_char.is_some() {
                return Err(CompileError::UnsupportedExpr {
                    expr_type: "LIKE with escape characters is not supported".to_string(),
                });
            }

            let (column_expr, cast_type) = extract_column_with_cast(expr)?;
            let (column, data_type) = schema_index.resolve_column(&column_expr)?;
            ensure_noop_cast(&column_expr, &data_type, cast_type)?;
            if !matches!(
                data_type,
                DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View
            ) {
                return Err(CompileError::UnsupportedExpr {
                    expr_type: "LIKE is only supported for string columns".to_string(),
                });
            }

            let pattern_lit = extract_literal(pattern)?;
            let pattern = match pattern_lit {
                ScalarValue::Utf8(Some(s))
                | ScalarValue::LargeUtf8(Some(s))
                | ScalarValue::Utf8View(Some(s)) => s,
                _ => {
                    return Err(CompileError::UnsupportedExpr {
                        expr_type: "LIKE pattern must be a string literal".to_string(),
                    });
                }
            };

            match like_pattern_to_rule(&column, &pattern, &data_type) {
                Some(rule) => Ok(with_bloom_if_applicable(rule)),
                None => Err(CompileError::UnsupportedExpr {
                    expr_type: format!("Unsupported LIKE pattern: '{pattern}'"),
                }),
            }
        }
        Expr::IsNull(expr) => {
            let (column_expr, cast_type) = extract_column_with_cast(expr)?;
            let (column, data_type) = schema_index.resolve_column(&column_expr)?;
            ensure_noop_cast(&column_expr, &data_type, cast_type)?;
            Ok(IrExpr::IsNull {
                column,
                negated: false,
            })
        }
        Expr::IsNotNull(expr) => {
            let (column_expr, cast_type) = extract_column_with_cast(expr)?;
            let (column, data_type) = schema_index.resolve_column(&column_expr)?;
            ensure_noop_cast(&column_expr, &data_type, cast_type)?;
            Ok(IrExpr::IsNull {
                column,
                negated: true,
            })
        }
        Expr::Not(inner) => Ok(IrExpr::Not(Box::new(compile_expr(
            inner,
            schema,
            schema_index,
        )?))),
        _ => Err(CompileError::UnsupportedExpr {
            expr_type: expr_type_name(expr),
        }),
    }
}

fn with_bloom_if_applicable(rule: IrExpr) -> IrExpr {
    match rule {
        IrExpr::Cmp {
            column,
            op: CmpOp::Eq,
            value,
        } => {
            let bloom = IrExpr::BloomFilterEq {
                column: column.clone(),
                value: value.clone(),
            };
            IrExpr::And(vec![
                IrExpr::Cmp {
                    column,
                    op: CmpOp::Eq,
                    value,
                },
                bloom,
            ])
        }
        IrExpr::InList { column, values } => {
            let bloom = IrExpr::BloomFilterInList {
                column: column.clone(),
                values: values.clone(),
            };
            IrExpr::And(vec![IrExpr::InList { column, values }, bloom])
        }
        other => other,
    }
}

fn like_pattern_to_rule(column: &str, pattern: &str, data_type: &DataType) -> Option<IrExpr> {
    if pattern.is_empty() {
        return None;
    }

    let bytes = pattern.as_bytes();
    let mut has_wildcard = false;
    let mut wildcard_pos = None;
    for (idx, &b) in bytes.iter().enumerate() {
        if b == b'%' || b == b'_' {
            has_wildcard = true;
            wildcard_pos = Some(idx);
            break;
        }
    }

    if !has_wildcard {
        let value = ScalarValue::Utf8(Some(pattern.to_string()))
            .cast_to(data_type)
            .ok()?;
        return Some(IrExpr::Cmp {
            column: column.to_string(),
            op: CmpOp::Eq,
            value,
        });
    }

    // Only support prefix match: "foo%" (no other wildcards)
    let Some(pos) = wildcard_pos else {
        return None;
    };
    if bytes[pos] != b'%' {
        return None;
    }
    if pos != bytes.len() - 1 {
        return None;
    }
    let prefix = &pattern[..pos];
    if prefix.is_empty() {
        return None;
    }
    Some(IrExpr::StartsWith {
        column: column.to_string(),
        prefix: prefix.to_string(),
    })
}

fn extract_column_with_cast(expr: &Expr) -> Result<(Column, Option<DataType>), CompileError> {
    match expr {
        Expr::Column(col) => Ok((col.clone(), None)),
        Expr::Alias(alias) => extract_column_with_cast(&alias.expr),
        Expr::Cast(cast) => {
            let (column, _) = extract_column_with_cast(&cast.expr)?;
            Ok((column, Some(cast.data_type.clone())))
        }
        Expr::TryCast(cast) => {
            let (column, _) = extract_column_with_cast(&cast.expr)?;
            Ok((column, Some(cast.data_type.clone())))
        }
        _ => Err(CompileError::NotAColumn {
            found: expr_type_name(expr),
        }),
    }
}

fn extract_literal(expr: &Expr) -> Result<ScalarValue, CompileError> {
    match expr {
        Expr::Literal(value, _) => Ok(value.clone()),
        Expr::Alias(alias) => extract_literal(&alias.expr),
        Expr::Cast(cast) => {
            let literal = extract_literal(&cast.expr)?;
            literal
                .cast_to(&cast.data_type)
                .map_err(|e| CompileError::TypeCastError {
                    literal_type: literal.data_type().clone(),
                    target_type: cast.data_type.clone(),
                    reason: e.to_string(),
                })
        }
        Expr::TryCast(cast) => {
            let literal = extract_literal(&cast.expr)?;
            literal
                .cast_to(&cast.data_type)
                .map_err(|e| CompileError::TypeCastError {
                    literal_type: literal.data_type().clone(),
                    target_type: cast.data_type.clone(),
                    reason: e.to_string(),
                })
        }
        _ => Err(CompileError::NotALiteral {
            found: expr_type_name(expr),
        }),
    }
}

fn extract_column_literal(
    left: &Expr,
    right: &Expr,
    op: Operator,
    schema_index: &SchemaPathIndex,
) -> Result<(String, ScalarValue, CmpOp), CompileError> {
    // Try: column op literal
    if let (Ok((column_expr, cast_type)), Ok(literal)) =
        (extract_column_with_cast(left), extract_literal(right))
    {
        let (column, data_type) = schema_index.resolve_column(&column_expr)?;
        ensure_noop_cast(&column_expr, &data_type, cast_type)?;
        let value = literal
            .cast_to(&data_type)
            .map_err(|e| CompileError::TypeCastError {
                literal_type: literal.data_type().clone(),
                target_type: data_type.clone(),
                reason: e.to_string(),
            })?;
        return Ok((column, value, map_op(op)?));
    }
    // Try: literal op column (flip operator)
    if let (Ok((column_expr, cast_type)), Ok(literal)) =
        (extract_column_with_cast(right), extract_literal(left))
    {
        let (column, data_type) = schema_index.resolve_column(&column_expr)?;
        ensure_noop_cast(&column_expr, &data_type, cast_type)?;
        let value = literal
            .cast_to(&data_type)
            .map_err(|e| CompileError::TypeCastError {
                literal_type: literal.data_type().clone(),
                target_type: data_type.clone(),
                reason: e.to_string(),
            })?;
        return Ok((column, value, map_op(op)?.flip()));
    }
    Err(CompileError::UnsupportedExpr {
        expr_type: "column-literal comparison required".to_string(),
    })
}

fn ensure_noop_cast(
    column_expr: &Column,
    data_type: &DataType,
    cast_type: Option<DataType>,
) -> Result<(), CompileError> {
    if let Some(cast_type) = cast_type {
        if cast_type != *data_type {
            return Err(CompileError::UnsupportedExpr {
                expr_type: format!(
                    "CAST on column '{}' to {:?} is not supported",
                    column_expr.name, cast_type
                ),
            });
        }
    }
    Ok(())
}

fn map_op(op: Operator) -> Result<CmpOp, CompileError> {
    match op {
        Operator::Eq => Ok(CmpOp::Eq),
        Operator::NotEq => Ok(CmpOp::NotEq),
        Operator::Lt => Ok(CmpOp::Lt),
        Operator::LtEq => Ok(CmpOp::LtEq),
        Operator::Gt => Ok(CmpOp::Gt),
        Operator::GtEq => Ok(CmpOp::GtEq),
        _ => Err(CompileError::UnsupportedOperator {
            operator: operator_symbol(&op).to_string(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_expr::{
        col,
        expr::{Cast, TryCast},
        lit,
    };

    use super::*;

    fn find_rule<'a>(rule: &'a IrExpr, predicate: &impl Fn(&IrExpr) -> bool) -> Option<&'a IrExpr> {
        if predicate(rule) {
            return Some(rule);
        }
        match rule {
            IrExpr::And(parts) | IrExpr::Or(parts) => {
                parts.iter().find_map(|part| find_rule(part, predicate))
            }
            IrExpr::Not(inner) => find_rule(inner, predicate),
            _ => None,
        }
    }

    fn find_cmp(rule: &IrExpr) -> &IrExpr {
        find_rule(rule, &|candidate| matches!(candidate, IrExpr::Cmp { .. }))
            .expect("Expected Cmp expression")
    }

    fn find_in_list(rule: &IrExpr) -> &IrExpr {
        find_rule(rule, &|candidate| {
            matches!(candidate, IrExpr::InList { .. })
        })
        .expect("Expected InList expression")
    }

    #[test]
    fn compile_conjunction() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]);
        let expr = col("a").eq(lit(5)).and(col("b").gt(lit(10)));
        let result = compile_pruning_ir(&expr, &schema);
        assert_eq!(result.prunable.len(), 2);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn compile_unsupported_operator() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
        // Addition is not supported for pruning
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("a")),
            op: Operator::Plus,
            right: Box::new(lit(5)),
        });
        let result = compile_pruning_ir(&expr, &schema);
        assert_eq!(result.prunable.len(), 0);
        assert_eq!(result.errors.len(), 1);
        assert!(matches!(
            result.errors[0],
            CompileError::UnsupportedOperator { .. }
        ));
    }

    #[test]
    fn compile_column_not_found() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
        let expr = col("nonexistent").eq(lit(5));
        let result = compile_pruning_ir(&expr, &schema);
        assert_eq!(result.prunable.len(), 0);
        assert_eq!(result.errors.len(), 1);
        assert!(matches!(
            result.errors[0],
            CompileError::ColumnNotFound { .. }
        ));
    }

    #[test]
    fn compile_negated_between() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
        let expr = col("a").not_between(lit(1), lit(10));
        let result = compile_pruning_ir(&expr, &schema);
        assert_eq!(result.prunable.len(), 0);
        assert_eq!(result.errors.len(), 1);
        assert!(matches!(
            result.errors[0],
            CompileError::NegatedNotSupported { .. }
        ));
    }

    #[test]
    fn compile_partial_success() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]);
        // First predicate is valid, second is not (addition unsupported)
        let unsupported = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("b")),
            op: Operator::Plus,
            right: Box::new(lit(2)),
        });
        let expr = col("a").eq(lit(5)).and(unsupported);
        let result = compile_pruning_ir(&expr, &schema);
        assert_eq!(result.prunable.len(), 1);
        assert_eq!(result.errors.len(), 1);
        assert!(matches!(
            result.errors[0],
            CompileError::UnsupportedOperator { .. }
        ));
    }

    #[test]
    fn test_error_message_quality() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);

        // Test UnsupportedOperator error message
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("a")),
            op: Operator::Plus,
            right: Box::new(lit(5)),
        });
        let result = compile_pruning_ir(&expr, &schema);
        let error_msg = result.errors[0].to_string();
        assert!(error_msg.contains("Operator '+' is not supported"));

        // Test ColumnNotFound error message
        let expr = col("nonexistent").eq(lit(5));
        let result = compile_pruning_ir(&expr, &schema);
        let error_msg = result.errors[0].to_string();
        assert!(error_msg.contains("Column 'nonexistent' not found"));

        // Test NegatedNotSupported error message
        let expr = col("a").not_between(lit(1), lit(10));
        let result = compile_pruning_ir(&expr, &schema);
        let error_msg = result.errors[0].to_string();
        assert!(error_msg.contains("Negated BETWEEN predicates are not supported"));
    }

    #[test]
    fn test_nested_column_simple() {
        use arrow_schema::Fields;

        // Create schema with nested struct: a { b: Int32, c: Utf8 }
        let inner_fields = Fields::from(vec![
            Field::new("b", DataType::Int32, false),
            Field::new("c", DataType::Utf8, false),
        ]);
        let schema = Schema::new(vec![
            Field::new("a", DataType::Struct(inner_fields), false),
            Field::new("x", DataType::Int64, false),
        ]);

        // Simple top-level column
        let expr = col("x").eq(lit(5i64));
        let result = compile_pruning_ir(&expr, &schema);
        assert_eq!(result.prunable.len(), 1);
        assert!(result.errors.is_empty());
        // Verify the column path is "x"
        match find_cmp(&result.prunable[0]) {
            IrExpr::Cmp { column, .. } => assert_eq!(column, "x"),
            _ => unreachable!("find_cmp should return Cmp"),
        }
    }

    #[test]
    fn test_nested_column_unique_leaf() {
        use arrow_schema::Fields;

        // Schema: a { b: Int32 }, x: Int64
        // Leaf "b" is unique (only appears in a.b)
        let inner_fields = Fields::from(vec![Field::new("b", DataType::Int32, false)]);
        let schema = Schema::new(vec![
            Field::new("a", DataType::Struct(inner_fields), false),
            Field::new("x", DataType::Int64, false),
        ]);

        // Reference "b" without qualification - should resolve to "a.b"
        let expr = col("b").eq(lit(42i32));
        let result = compile_pruning_ir(&expr, &schema);
        assert_eq!(result.prunable.len(), 1);
        assert!(result.errors.is_empty());
        match find_cmp(&result.prunable[0]) {
            IrExpr::Cmp { column, .. } => assert_eq!(column, "a.b"),
            _ => unreachable!("find_cmp should return Cmp"),
        }
    }

    #[test]
    fn test_nested_column_ambiguous() {
        use arrow_schema::Fields;

        // Schema: a { b: Int32 }, b: Int64
        // Leaf "b" is ambiguous (appears as both "a.b" and "b")
        let inner_fields = Fields::from(vec![Field::new("b", DataType::Int32, false)]);
        let schema = Schema::new(vec![
            Field::new("a", DataType::Struct(inner_fields), false),
            Field::new("b", DataType::Int64, false),
        ]);

        // Reference "b" without qualification - should error as ambiguous
        let expr = col("b").eq(lit(42));
        let result = compile_pruning_ir(&expr, &schema);
        assert_eq!(result.prunable.len(), 0);
        assert_eq!(result.errors.len(), 1);
        assert!(matches!(
            &result.errors[0],
            CompileError::AmbiguousColumn { column_name, candidates }
            if column_name == "b" && candidates.len() == 2
        ));
    }

    #[test]
    fn test_nested_column_qualified() {
        use arrow_schema::Fields;

        // Schema: a { b: Int32 }
        let inner_fields = Fields::from(vec![Field::new("b", DataType::Int32, false)]);
        let schema = Schema::new(vec![Field::new("a", DataType::Struct(inner_fields), false)]);

        // Reference "a.b" - DataFusion parses as relation-qualified
        // Should resolve to the nested field "a.b"
        let expr = col("a.b").eq(lit(42i32));
        let result = compile_pruning_ir(&expr, &schema);
        assert_eq!(result.prunable.len(), 1);
        assert!(result.errors.is_empty());
        match find_cmp(&result.prunable[0]) {
            IrExpr::Cmp { column, .. } => assert_eq!(column, "a.b"),
            _ => unreachable!("find_cmp should return Cmp"),
        }
    }

    #[test]
    fn test_nested_column_qualified_ambiguous() {
        use arrow_schema::Fields;

        // Schema: a { b: Int32 }, b: Int64
        let inner_fields = Fields::from(vec![Field::new("b", DataType::Int32, false)]);
        let schema = Schema::new(vec![
            Field::new("a", DataType::Struct(inner_fields), false),
            Field::new("b", DataType::Int64, false),
        ]);

        // Reference "a.b" is ambiguous between nested field and top-level b
        let expr = col("a.b").eq(lit(42i32));
        let result = compile_pruning_ir(&expr, &schema);
        assert_eq!(result.prunable.len(), 0);
        assert_eq!(result.errors.len(), 1);
        assert!(matches!(
            &result.errors[0],
            CompileError::AmbiguousColumn { column_name, .. }
            if column_name == "a.b"
        ));
    }

    #[test]
    fn test_nested_column_deeply_nested() {
        use arrow_schema::Fields;

        // Schema: a { b { c: Int32 } }
        let c_field = Field::new("c", DataType::Int32, false);
        let b_fields = Fields::from(vec![Field::new(
            "b",
            DataType::Struct(Fields::from(vec![c_field])),
            false,
        )]);
        let schema = Schema::new(vec![Field::new("a", DataType::Struct(b_fields), false)]);

        // Reference unique leaf "c" - should resolve to "a.b.c"
        let expr = col("c").eq(lit(100i32));
        let result = compile_pruning_ir(&expr, &schema);
        assert_eq!(result.prunable.len(), 1);
        assert!(result.errors.is_empty());
        match find_cmp(&result.prunable[0]) {
            IrExpr::Cmp { column, .. } => assert_eq!(column, "a.b.c"),
            _ => unreachable!("find_cmp should return Cmp"),
        }
    }

    #[test]
    fn test_four_segment_path() {
        use arrow_schema::Fields;

        // Schema: a { b { c { d: Int32 } } }
        let d_field = Field::new("d", DataType::Int32, false);
        let c_fields = Fields::from(vec![Field::new(
            "c",
            DataType::Struct(Fields::from(vec![d_field])),
            false,
        )]);
        let b_fields = Fields::from(vec![Field::new("b", DataType::Struct(c_fields), false)]);
        let schema = Schema::new(vec![Field::new("a", DataType::Struct(b_fields), false)]);

        // Reference "a.b.c.d" - DataFusion parses as TableReference::Full
        let expr = col("a.b.c.d").eq(lit(42i32));
        let result = compile_pruning_ir(&expr, &schema);
        assert_eq!(result.prunable.len(), 1);
        assert!(result.errors.is_empty());
        match find_cmp(&result.prunable[0]) {
            IrExpr::Cmp { column, .. } => assert_eq!(column, "a.b.c.d"),
            _ => unreachable!("find_cmp should return Cmp"),
        }
    }

    #[test]
    fn test_five_segment_path_limitation() {
        use arrow_schema::Fields;

        // Schema: a { b { c { d { e: Int32 } } } }
        let e_field = Field::new("e", DataType::Int32, false);
        let d_fields = Fields::from(vec![Field::new(
            "d",
            DataType::Struct(Fields::from(vec![e_field])),
            false,
        )]);
        let c_fields = Fields::from(vec![Field::new("c", DataType::Struct(d_fields), false)]);
        let b_fields = Fields::from(vec![Field::new("b", DataType::Struct(c_fields), false)]);
        let schema = Schema::new(vec![Field::new("a", DataType::Struct(b_fields), false)]);

        // DataFusion's TableReference only supports catalog.schema.table (3 components)
        // So col("a.b.c.d.e") gets parsed as TableReference::Full with name="d.e"
        // This means 5+ segment paths are not supported - they would need the name
        // component to contain dots, which our current implementation doesn't handle
        let expr = col("a.b.c.d.e").eq(lit(42i32));
        let result = compile_pruning_ir(&expr, &schema);

        // This is expected to fail - documenting the 4-segment limitation
        assert_eq!(result.prunable.len(), 0);
        assert_eq!(result.errors.len(), 1);
        assert!(matches!(
            &result.errors[0],
            CompileError::ColumnNotFound { column_name }
            if column_name == "a.b.c.d.e"
        ));
    }

    #[test]
    fn test_nested_column_between() {
        use arrow_schema::Fields;

        // Test BETWEEN with nested column
        let inner_fields = Fields::from(vec![Field::new("value", DataType::Int32, false)]);
        let schema = Schema::new(vec![Field::new(
            "stats",
            DataType::Struct(inner_fields),
            false,
        )]);

        let expr = col("value").between(lit(10i32), lit(100i32));
        let result = compile_pruning_ir(&expr, &schema);
        assert_eq!(result.prunable.len(), 1);
        assert!(result.errors.is_empty());
        match &result.prunable[0] {
            IrExpr::Between { column, .. } => assert_eq!(column, "stats.value"),
            _ => panic!("Expected Between expression"),
        }
    }

    #[test]
    fn test_nested_column_in_list() {
        use arrow_schema::Fields;

        // Test IN with nested column
        let inner_fields = Fields::from(vec![Field::new("id", DataType::Int32, false)]);
        let schema = Schema::new(vec![Field::new(
            "metadata",
            DataType::Struct(inner_fields),
            false,
        )]);

        let expr = col("id").in_list(vec![lit(1i32), lit(2i32), lit(3i32)], false);
        let result = compile_pruning_ir(&expr, &schema);
        assert_eq!(result.prunable.len(), 1);
        assert!(result.errors.is_empty());
        match find_in_list(&result.prunable[0]) {
            IrExpr::InList { column, values } => {
                assert_eq!(column, "metadata.id");
                assert_eq!(values.len(), 3);
            }
            _ => unreachable!("find_in_list should return InList"),
        }
    }

    #[test]
    fn test_nested_column_is_null() {
        use arrow_schema::Fields;

        // Test IS NULL with nested column
        let inner_fields = Fields::from(vec![Field::new("optional", DataType::Utf8, true)]);
        let schema = Schema::new(vec![Field::new(
            "data",
            DataType::Struct(inner_fields),
            false,
        )]);

        let expr = col("optional").is_null();
        let result = compile_pruning_ir(&expr, &schema);
        assert_eq!(result.prunable.len(), 1);
        assert!(result.errors.is_empty());
        match &result.prunable[0] {
            IrExpr::IsNull { column, negated } => {
                assert_eq!(column, "data.optional");
                assert!(!negated);
            }
            _ => panic!("Expected IsNull expression"),
        }
    }

    #[test]
    fn test_list_column_paths() {
        use std::sync::Arc;

        // Schema: my_list: List<Int32>
        let schema = Schema::new(vec![Field::new(
            "my_list",
            DataType::List(Arc::new(Field::new("element", DataType::Int32, false))),
            true,
        )]);

        // Reference the list element using Parquet path
        let expr = col("my_list.list.element").gt(lit(10i32));
        let result = compile_pruning_ir(&expr, &schema);
        assert_eq!(result.prunable.len(), 1);
        assert!(result.errors.is_empty());
        match find_cmp(&result.prunable[0]) {
            IrExpr::Cmp { column, .. } => assert_eq!(column, "my_list.list.element"),
            _ => unreachable!("find_cmp should return Cmp"),
        }
    }

    #[test]
    fn test_map_column_paths() {
        use std::sync::Arc;

        // Schema: my_map: Map<Utf8, Int32>
        let schema = Schema::new(vec![Field::new(
            "my_map",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Int32, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        )]);

        // Reference map value using Parquet path (uses Arrow field name "entries")
        let expr = col("my_map.entries.value").gt(lit(100i32));
        let result = compile_pruning_ir(&expr, &schema);
        assert_eq!(result.prunable.len(), 1);
        assert!(result.errors.is_empty());
        match find_cmp(&result.prunable[0]) {
            IrExpr::Cmp { column, .. } => assert_eq!(column, "my_map.entries.value"),
            _ => unreachable!("find_cmp should return Cmp"),
        }
    }

    #[test]
    fn test_list_of_structs() {
        use std::sync::Arc;

        // Schema: items: List<Struct<{id: Int32, name: Utf8}>>
        let schema = Schema::new(vec![Field::new(
            "items",
            DataType::List(Arc::new(Field::new(
                "element",
                DataType::Struct(
                    vec![
                        Field::new("id", DataType::Int32, false),
                        Field::new("name", DataType::Utf8, false),
                    ]
                    .into(),
                ),
                false,
            ))),
            true,
        )]);

        // Reference nested field in list element: items.list.element.id
        let expr = col("items.list.element.id").eq(lit(42i32));
        let result = compile_pruning_ir(&expr, &schema);
        assert_eq!(result.prunable.len(), 1);
        assert!(result.errors.is_empty());
        match find_cmp(&result.prunable[0]) {
            IrExpr::Cmp { column, .. } => assert_eq!(column, "items.list.element.id"),
            _ => unreachable!("find_cmp should return Cmp"),
        }
    }

    #[test]
    fn test_list_coercion_compatibility() {
        use std::sync::Arc;

        // Schema with custom element name (simulates non-coerced write)
        let schema = Schema::new(vec![Field::new(
            "my_list",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
            true,
        )]);

        // Both Arrow name ("item") and standard name ("element") should compile
        // Compiler generates path with Arrow name
        let expr1 = col("my_list.list.item").gt(lit(10i32));
        let result1 = compile_pruning_ir(&expr1, &schema);
        assert_eq!(result1.prunable.len(), 1);
        assert!(result1.errors.is_empty());
        match find_cmp(&result1.prunable[0]) {
            IrExpr::Cmp { column, .. } => assert_eq!(column, "my_list.list.item"),
            _ => unreachable!("find_cmp should return Cmp"),
        }

        // Standard "element" should also work (for coerced Parquet files)
        let expr2 = col("my_list.list.element").gt(lit(10i32));
        let result2 = compile_pruning_ir(&expr2, &schema);
        assert_eq!(result2.prunable.len(), 1);
        assert!(result2.errors.is_empty());
    }

    #[test]
    fn test_map_coercion_compatibility() {
        use std::sync::Arc;

        // Schema with custom entries name (simulates non-coerced write)
        let schema = Schema::new(vec![Field::new(
            "my_map",
            DataType::Map(
                Arc::new(Field::new(
                    "pairs", // Non-standard name
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Int32, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        )]);

        // Both Arrow name ("pairs") and standard name ("key_value") should work
        let expr1 = col("my_map.pairs.value").gt(lit(100i32));
        let result1 = compile_pruning_ir(&expr1, &schema);
        assert_eq!(result1.prunable.len(), 1);
        assert!(result1.errors.is_empty());

        // Standard "key_value" should also work (for coerced Parquet files)
        let expr2 = col("my_map.key_value.value").gt(lit(100i32));
        let result2 = compile_pruning_ir(&expr2, &schema);
        assert_eq!(result2.prunable.len(), 1);
        assert!(result2.errors.is_empty());
    }

    #[test]
    fn test_large_list_support() {
        use std::sync::Arc;

        // Schema: LargeList<Int64> - uses i64 offsets instead of i32
        let schema = Schema::new(vec![Field::new(
            "large_list",
            DataType::LargeList(Arc::new(Field::new("element", DataType::Int64, false))),
            true,
        )]);

        // LargeList uses same Parquet encoding as List
        let expr = col("large_list.list.element").gt(lit(100i64));
        let result = compile_pruning_ir(&expr, &schema);
        assert_eq!(result.prunable.len(), 1);
        assert!(result.errors.is_empty());
        match find_cmp(&result.prunable[0]) {
            IrExpr::Cmp { column, .. } => assert_eq!(column, "large_list.list.element"),
            _ => unreachable!("find_cmp should return Cmp"),
        }
    }

    #[test]
    fn test_fixed_size_list_support() {
        use std::sync::Arc;

        // Schema: FixedSizeList<Float32, 10> - arrays of exactly 10 elements
        let schema = Schema::new(vec![Field::new(
            "fixed_list",
            DataType::FixedSizeList(
                Arc::new(Field::new("element", DataType::Float32, false)),
                10,
            ),
            true,
        )]);

        // FixedSizeList uses same Parquet encoding as List
        let expr = col("fixed_list.list.element").gt(lit(0.5f32));
        let result = compile_pruning_ir(&expr, &schema);
        assert_eq!(result.prunable.len(), 1);
        assert!(result.errors.is_empty());
        match find_cmp(&result.prunable[0]) {
            IrExpr::Cmp { column, .. } => assert_eq!(column, "fixed_list.list.element"),
            _ => unreachable!("find_cmp should return Cmp"),
        }
    }

    #[test]
    fn test_large_list_custom_element_name() {
        use std::sync::Arc;

        // LargeList with custom element name (non-coerced)
        let schema = Schema::new(vec![Field::new(
            "large_list",
            DataType::LargeList(Arc::new(Field::new("item", DataType::Int64, false))),
            true,
        )]);

        // Both "item" (Arrow name) and "element" (standard) should work
        let expr1 = col("large_list.list.item").gt(lit(100i64));
        let result1 = compile_pruning_ir(&expr1, &schema);
        assert_eq!(result1.prunable.len(), 1);
        assert!(result1.errors.is_empty());

        let expr2 = col("large_list.list.element").gt(lit(100i64));
        let result2 = compile_pruning_ir(&expr2, &schema);
        assert_eq!(result2.prunable.len(), 1);
        assert!(result2.errors.is_empty());
    }

    #[test]
    fn compile_like_prefix_to_starts_with() {
        let schema = Schema::new(vec![Field::new("s", DataType::Utf8, false)]);
        let expr = Expr::Like(Like::new(
            false,
            Box::new(col("s")),
            Box::new(lit("foo%")),
            None,
            false,
        ));
        let result = compile_pruning_ir(&expr, &schema);
        assert!(result.errors.is_empty());
        assert_eq!(result.prunable.len(), 1);
        assert!(matches!(
            result.prunable[0],
            IrExpr::StartsWith { ref column, ref prefix }
                if column == "s" && prefix == "foo"
        ));
    }

    #[test]
    fn compile_like_exact_to_eq() {
        let schema = Schema::new(vec![Field::new("s", DataType::Utf8, false)]);
        let expr = Expr::Like(Like::new(
            false,
            Box::new(col("s")),
            Box::new(lit("bar")),
            None,
            false,
        ));
        let result = compile_pruning_ir(&expr, &schema);
        assert!(result.errors.is_empty());
        assert_eq!(result.prunable.len(), 1);
        match find_cmp(&result.prunable[0]) {
            IrExpr::Cmp {
                column,
                op: CmpOp::Eq,
                ..
            } => assert_eq!(column, "s"),
            _ => unreachable!("find_cmp should return Cmp"),
        }
    }

    #[test]
    fn compile_like_unsupported_pattern() {
        let schema = Schema::new(vec![Field::new("s", DataType::Utf8, false)]);
        let expr = Expr::Like(Like::new(
            false,
            Box::new(col("s")),
            Box::new(lit("f%o")),
            None,
            false,
        ));
        let result = compile_pruning_ir(&expr, &schema);
        assert_eq!(result.prunable.len(), 0);
        assert_eq!(result.errors.len(), 1);
    }

    #[test]
    fn compile_cast_literal_in_comparison() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("a")),
            op: Operator::Eq,
            right: Box::new(Expr::Cast(Cast::new(Box::new(lit("42")), DataType::Int64))),
        });
        let result = compile_pruning_ir(&expr, &schema);
        assert_eq!(result.prunable.len(), 1);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn compile_try_cast_literal_in_comparison() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("a")),
            op: Operator::Eq,
            right: Box::new(Expr::TryCast(TryCast::new(
                Box::new(lit("7")),
                DataType::Int64,
            ))),
        });
        let result = compile_pruning_ir(&expr, &schema);
        assert_eq!(result.prunable.len(), 1);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn compile_try_cast_literal_invalid() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(col("a")),
            op: Operator::Eq,
            right: Box::new(Expr::TryCast(TryCast::new(
                Box::new(lit("not-a-number")),
                DataType::Int64,
            ))),
        });
        let result = compile_pruning_ir(&expr, &schema);
        assert_eq!(result.prunable.len(), 0);
        assert_eq!(result.errors.len(), 1);
    }

    #[test]
    fn compile_cast_column_noop() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Cast(Cast::new(Box::new(col("a")), DataType::Int64))),
            op: Operator::Gt,
            right: Box::new(lit(10)),
        });
        let result = compile_pruning_ir(&expr, &schema);
        assert_eq!(result.prunable.len(), 1);
        assert!(result.errors.is_empty());
    }

    #[test]
    fn compile_cast_column_type_change_unsupported() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int32, false)]);
        let expr = Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Cast(Cast::new(Box::new(col("a")), DataType::Int64))),
            op: Operator::Gt,
            right: Box::new(lit(10i64)),
        });
        let result = compile_pruning_ir(&expr, &schema);
        assert_eq!(result.prunable.len(), 0);
        assert_eq!(result.errors.len(), 1);
    }
}

#[cfg(test)]
mod debug_tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema};

    use super::*;

    #[test]
    fn debug_map_paths() {
        let schema = Schema::new(vec![Field::new(
            "my_map",
            DataType::Map(
                Arc::new(Field::new(
                    "entries",
                    DataType::Struct(
                        vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Int32, true),
                        ]
                        .into(),
                    ),
                    false,
                )),
                false,
            ),
            true,
        )]);

        let index = build_schema_path_index(&schema);
        println!("\nGenerated paths:");
        for (path, (dtype, _)) in &index.paths {
            println!("  {} -> {:?}", path, dtype);
        }

        println!("\nLeaf mappings:");
        for (leaf, paths) in &index.leaf_to_paths {
            println!("  {} -> {:?}", leaf, paths);
        }
    }
}
