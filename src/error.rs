use arrow_schema::DataType;
use thiserror::Error;

/// Errors that can occur during predicate compilation
///
/// These errors indicate which parts of a predicate cannot be evaluated
/// against Parquet metadata for pruning purposes.
#[derive(Debug, Error, Clone)]
pub enum AisleError {
    /// The expression type is not supported for metadata pruning
    #[error("Unsupported expression: {expr_type}")]
    UnsupportedExpr {
        /// User-friendly description of the expression type
        expr_type: String,
    },

    /// Column reference could not be extracted from expression
    #[error("Expected column reference, found {found}")]
    NotAColumn {
        /// What was found instead of a column
        found: String,
    },

    /// Literal value could not be extracted from expression
    #[error("Expected literal value, found {found}")]
    NotALiteral {
        /// What was found instead of a literal
        found: String,
    },

    /// Column not found in the provided schema
    #[error("Column '{column_name}' not found in schema")]
    ColumnNotFound {
        /// Name of the column that was not found
        column_name: String,
    },

    /// Type mismatch when casting literal to column type
    ///
    /// Uses typed DataType fields for programmatic inspection and better error messages.
    /// Users can pattern match on specific type mismatches if needed.
    #[error("Cannot cast {literal_type:?} to {target_type:?}: {reason}")]
    TypeCastError {
        /// Type of the literal value (typed for programmatic access)
        literal_type: DataType,
        /// Target column type (typed for programmatic access)
        target_type: DataType,
        /// Reason for the cast failure (from Arrow's cast implementation)
        reason: String,
    },

    /// Negated predicates are not supported (e.g., NOT BETWEEN, NOT IN)
    #[error("Negated {predicate_type} predicates are not supported")]
    NegatedNotSupported {
        /// Type of predicate that was negated
        predicate_type: String,
    },

    /// Binary operator is not supported for metadata pruning
    #[error("Operator '{operator}' is not supported for metadata pruning")]
    UnsupportedOperator {
        /// The operator symbol or name
        operator: String,
    },

    /// Column name is ambiguous (multiple fields with the same leaf name)
    #[error("Column '{column_name}' is ambiguous. Candidates: {}", candidates.join(", "))]
    AmbiguousColumn {
        /// Name of the ambiguous column
        column_name: String,
        /// List of qualified paths that match this name
        candidates: Vec<String>,
    },
}
