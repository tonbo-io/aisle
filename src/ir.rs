use std::ops::{BitAnd, BitOr, Not};

use datafusion_common::ScalarValue;

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

#[derive(Clone, Debug, PartialEq)]
pub enum IrExpr {
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
    BloomFilterEq {
        column: String,
        value: ScalarValue,
    },
    BloomFilterInList {
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
    And(Vec<IrExpr>),
    Or(Vec<IrExpr>),
    Not(Box<IrExpr>),
}
