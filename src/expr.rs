use std::ops::{BitAnd, BitOr, Not};

use datafusion_common::ScalarValue;

pub(crate) mod rewrite;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TriState {
    True,
    False,
    Unknown,
}

impl TriState {
    pub(crate) fn and(self, other: Self) -> Self {
        match (self, other) {
            (TriState::False, _) | (_, TriState::False) => TriState::False,
            (TriState::True, TriState::True) => TriState::True,
            _ => TriState::Unknown,
        }
    }

    pub(crate) fn or(self, other: Self) -> Self {
        match (self, other) {
            (TriState::True, _) | (_, TriState::True) => TriState::True,
            (TriState::False, TriState::False) => TriState::False,
            _ => TriState::Unknown,
        }
    }

    pub(crate) fn not(self) -> Self {
        match self {
            TriState::True => TriState::False,
            TriState::False => TriState::True,
            TriState::Unknown => TriState::Unknown,
        }
    }
}

// Operator trait implementations for ergonomic usage
impl BitAnd for TriState {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        self.and(rhs)
    }
}

impl BitOr for TriState {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        self.or(rhs)
    }
}

impl Not for TriState {
    type Output = Self;

    fn not(self) -> Self::Output {
        TriState::not(self)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CmpOp {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,
}

impl CmpOp {
    #[cfg(feature = "datafusion")]
    pub(crate) fn flip(self) -> Self {
        match self {
            CmpOp::Eq => CmpOp::Eq,
            CmpOp::NotEq => CmpOp::NotEq,
            CmpOp::Lt => CmpOp::Gt,
            CmpOp::LtEq => CmpOp::GtEq,
            CmpOp::Gt => CmpOp::Lt,
            CmpOp::GtEq => CmpOp::LtEq,
        }
    }
}

/// Expression tree for Parquet metadata pruning.
///
/// Note: This enum includes internal variants for bloom filter optimization
/// (marked with `#[doc(hidden)]`). These are added automatically during compilation
/// and should not be constructed manually.
#[derive(Clone, Debug, PartialEq)]
#[non_exhaustive]
pub enum Expr {
    True,
    False,
    Cmp {
        column: String,
        op: CmpOp,
        value: ScalarValue,
    },
    Between {
        column: String,
        low: ScalarValue,
        high: ScalarValue,
        inclusive: bool,
    },
    InList {
        column: String,
        values: Vec<ScalarValue>,
    },
    /// Internal: Bloom filter equality check (added automatically during compilation).
    ///
    /// This variant is an implementation detail for bloom filter optimization and
    /// should not be constructed manually. Use [`Expr::eq`] instead, and bloom
    /// filter variants will be added automatically during compilation when appropriate.
    #[doc(hidden)]
    BloomFilterEq {
        column: String,
        value: ScalarValue,
    },
    /// Internal: Bloom filter IN list check (added automatically during compilation).
    ///
    /// This variant is an implementation detail for bloom filter optimization and
    /// should not be constructed manually. Use [`Expr::in_list`] instead, and bloom
    /// filter variants will be added automatically during compilation when appropriate.
    #[doc(hidden)]
    BloomFilterInList {
        column: String,
        values: Vec<ScalarValue>,
    },
    /// Internal: Dictionary hint equality check (added automatically during compilation).
    ///
    /// This variant is an implementation detail for dictionary-hint optimization and
    /// should not be constructed manually. Use [`Expr::eq`] instead, and dictionary hint
    /// variants will be added automatically during pruning when enabled.
    #[doc(hidden)]
    DictionaryHintEq {
        column: String,
        value: ScalarValue,
    },
    /// Internal: Dictionary hint IN list check (added automatically during compilation).
    ///
    /// This variant is an implementation detail for dictionary-hint optimization and
    /// should not be constructed manually. Use [`Expr::in_list`] instead, and dictionary hint
    /// variants will be added automatically during pruning when enabled.
    #[doc(hidden)]
    DictionaryHintInList {
        column: String,
        values: Vec<ScalarValue>,
    },
    StartsWith {
        column: String,
        prefix: String,
    },
    IsNull {
        column: String,
        negated: bool,
    },
    And(Vec<Expr>),
    Or(Vec<Expr>),
    Not(Box<Expr>),
}

impl Expr {
    /// Build a comparison expression with an explicit operator.
    pub fn cmp(column: impl Into<String>, op: CmpOp, value: ScalarValue) -> Self {
        Expr::Cmp {
            column: column.into(),
            op,
            value,
        }
    }

    /// Build an equality expression (`=`).
    pub fn eq(column: impl Into<String>, value: ScalarValue) -> Self {
        Self::cmp(column, CmpOp::Eq, value)
    }

    /// Build a not-equal expression (`!=`).
    pub fn not_eq(column: impl Into<String>, value: ScalarValue) -> Self {
        Self::cmp(column, CmpOp::NotEq, value)
    }

    /// Build a less-than expression (`<`).
    pub fn lt(column: impl Into<String>, value: ScalarValue) -> Self {
        Self::cmp(column, CmpOp::Lt, value)
    }

    /// Build a less-than-or-equal expression (`<=`).
    pub fn lt_eq(column: impl Into<String>, value: ScalarValue) -> Self {
        Self::cmp(column, CmpOp::LtEq, value)
    }

    /// Build a greater-than expression (`>`).
    pub fn gt(column: impl Into<String>, value: ScalarValue) -> Self {
        Self::cmp(column, CmpOp::Gt, value)
    }

    /// Build a greater-than-or-equal expression (`>=`).
    pub fn gt_eq(column: impl Into<String>, value: ScalarValue) -> Self {
        Self::cmp(column, CmpOp::GtEq, value)
    }

    /// Build a BETWEEN expression.
    pub fn between(
        column: impl Into<String>,
        low: ScalarValue,
        high: ScalarValue,
        inclusive: bool,
    ) -> Self {
        Expr::Between {
            column: column.into(),
            low,
            high,
            inclusive,
        }
    }

    /// Build an IN (...) expression.
    pub fn in_list(column: impl Into<String>, values: Vec<ScalarValue>) -> Self {
        Expr::InList {
            column: column.into(),
            values,
        }
    }

    /// Build a prefix match expression (`LIKE 'prefix%'`).
    pub fn starts_with(column: impl Into<String>, prefix: impl Into<String>) -> Self {
        Expr::StartsWith {
            column: column.into(),
            prefix: prefix.into(),
        }
    }

    /// Build an IS NULL expression.
    pub fn is_null(column: impl Into<String>) -> Self {
        Expr::IsNull {
            column: column.into(),
            negated: false,
        }
    }

    /// Build an IS NOT NULL expression.
    pub fn is_not_null(column: impl Into<String>) -> Self {
        Expr::IsNull {
            column: column.into(),
            negated: true,
        }
    }

    /// Build an AND expression.
    pub fn and(parts: Vec<Expr>) -> Self {
        Expr::And(parts)
    }

    /// Build an OR expression.
    pub fn or(parts: Vec<Expr>) -> Self {
        Expr::Or(parts)
    }

    /// Build a NOT expression.
    pub fn not(expr: Expr) -> Self {
        Expr::Not(Box::new(expr))
    }
}

impl std::fmt::Display for Expr {
    /// User-friendly display that hides internal bloom filter details.
    ///
    /// Bloom filter variants are displayed as `<bloom filter>` to avoid noise
    /// in user-facing output. Use `Debug` formatting (`{:?}`) to see the full
    /// internal representation.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Expr::True => write!(f, "TRUE"),
            Expr::False => write!(f, "FALSE"),
            Expr::Cmp { column, op, value } => {
                let op_str = match op {
                    CmpOp::Eq => "=",
                    CmpOp::NotEq => "!=",
                    CmpOp::Lt => "<",
                    CmpOp::LtEq => "<=",
                    CmpOp::Gt => ">",
                    CmpOp::GtEq => ">=",
                };
                write!(f, "{} {} {:?}", column, op_str, value)
            }
            Expr::Between {
                column,
                low,
                high,
                inclusive,
            } => {
                if *inclusive {
                    write!(f, "{} BETWEEN {:?} AND {:?}", column, low, high)
                } else {
                    write!(f, "{} > {:?} AND {} < {:?}", column, low, column, high)
                }
            }
            Expr::InList { column, values } => {
                write!(f, "{} IN (", column)?;
                for (i, v) in values.iter().enumerate() {
                    if i > 0 {
                        write!(f, ", ")?;
                    }
                    write!(f, "{:?}", v)?;
                }
                write!(f, ")")
            }
            // Hide bloom filter variants from user-facing output
            Expr::BloomFilterEq { .. }
            | Expr::BloomFilterInList { .. }
            | Expr::DictionaryHintEq { .. }
            | Expr::DictionaryHintInList { .. } => write!(f, "<bloom filter>"),
            Expr::StartsWith { column, prefix } => {
                write!(f, "{} LIKE '{}%'", column, prefix)
            }
            Expr::IsNull { column, negated } => {
                if *negated {
                    write!(f, "{} IS NOT NULL", column)
                } else {
                    write!(f, "{} IS NULL", column)
                }
            }
            Expr::And(parts) => {
                if parts.is_empty() {
                    write!(f, "TRUE")
                } else if parts.len() == 1 {
                    write!(f, "{}", parts[0])
                } else {
                    write!(f, "(")?;
                    for (i, part) in parts.iter().enumerate() {
                        if i > 0 {
                            write!(f, " AND ")?;
                        }
                        write!(f, "{}", part)?;
                    }
                    write!(f, ")")
                }
            }
            Expr::Or(parts) => {
                if parts.is_empty() {
                    write!(f, "FALSE")
                } else if parts.len() == 1 {
                    write!(f, "{}", parts[0])
                } else {
                    write!(f, "(")?;
                    for (i, part) in parts.iter().enumerate() {
                        if i > 0 {
                            write!(f, " OR ")?;
                        }
                        write!(f, "{}", part)?;
                    }
                    write!(f, ")")
                }
            }
            Expr::Not(inner) => write!(f, "NOT ({})", inner),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_display_hides_bloom_filters() {
        // Simple equality - clean output
        let expr = Expr::eq("id", ScalarValue::Int64(Some(42)));
        assert_eq!(expr.to_string(), "id = Int64(42)");

        // Bloom filter variant - hidden in Display
        let bloom = Expr::BloomFilterEq {
            column: "id".to_string(),
            value: ScalarValue::Int64(Some(42)),
        };
        assert_eq!(bloom.to_string(), "<bloom filter>");

        // AND with bloom filter - bloom filter hidden
        let compiled = Expr::and(vec![
            Expr::eq("id", ScalarValue::Int64(Some(42))),
            Expr::BloomFilterEq {
                column: "id".to_string(),
                value: ScalarValue::Int64(Some(42)),
            },
        ]);
        assert_eq!(compiled.to_string(), "(id = Int64(42) AND <bloom filter>)");
    }

    #[test]
    fn test_display_formatting() {
        assert_eq!(Expr::True.to_string(), "TRUE");
        assert_eq!(Expr::False.to_string(), "FALSE");

        assert_eq!(
            Expr::gt("age", ScalarValue::Int32(Some(18))).to_string(),
            "age > Int32(18)"
        );

        assert_eq!(
            Expr::in_list(
                "status",
                vec![
                    ScalarValue::Utf8(Some("active".to_string())),
                    ScalarValue::Utf8(Some("pending".to_string())),
                ]
            )
            .to_string(),
            "status IN (Utf8(\"active\"), Utf8(\"pending\"))"
        );

        assert_eq!(
            Expr::is_null("deleted_at").to_string(),
            "deleted_at IS NULL"
        );

        assert_eq!(
            Expr::starts_with("name", "John").to_string(),
            "name LIKE 'John%'"
        );
    }
}
