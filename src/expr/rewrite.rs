use super::Expr;
use datafusion_common::ScalarValue;

// Internal rewrite passes to make expressions bloom-aware.
///
/// Bloom filters are only injected in positive (non-negated) polarity.
#[allow(dead_code)]
pub fn inject_bloom_filters(expr: Expr) -> Expr {
    inject_metadata_hints(
        expr,
        MetadataHintConfig {
            bloom: true,
            dictionary: false,
        },
    )
}

/// Apply bloom injection across a list of predicates.
#[allow(dead_code)]
pub fn inject_bloom_filters_all(predicates: &[Expr]) -> Vec<Expr> {
    inject_metadata_hints_all(
        predicates,
        MetadataHintConfig {
            bloom: true,
            dictionary: false,
        },
    )
}

#[derive(Debug, Clone, Copy, Default)]
pub struct MetadataHintConfig {
    pub bloom: bool,
    pub dictionary: bool,
}

/// Inject metadata hints (bloom and/or dictionary) into a single expression.
///
/// Hints are injected only in positive (non-negated) polarity.
pub fn inject_metadata_hints(expr: Expr, config: MetadataHintConfig) -> Expr {
    inject_metadata_hints_inner(expr, true, config)
}

/// Inject metadata hints (bloom and/or dictionary) into multiple predicates.
pub fn inject_metadata_hints_all(predicates: &[Expr], config: MetadataHintConfig) -> Vec<Expr> {
    predicates
        .iter()
        .cloned()
        .map(|expr| inject_metadata_hints(expr, config))
        .collect()
}

fn inject_metadata_hints_inner(expr: Expr, allow_hints: bool, config: MetadataHintConfig) -> Expr {
    match expr {
        Expr::Cmp { .. } | Expr::InList { .. } if allow_hints => {
            with_metadata_hints_if_applicable(expr, config)
        }
        Expr::And(parts) => Expr::And(
            parts
                .into_iter()
                .map(|part| inject_metadata_hints_inner(part, allow_hints, config))
                .collect(),
        ),
        Expr::Or(parts) => Expr::Or(
            parts
                .into_iter()
                .map(|part| inject_metadata_hints_inner(part, allow_hints, config))
                .collect(),
        ),
        Expr::Not(inner) => Expr::Not(Box::new(inject_metadata_hints_inner(
            *inner,
            !allow_hints,
            config,
        ))),
        other => other,
    }
}

fn with_metadata_hints_if_applicable(rule: Expr, config: MetadataHintConfig) -> Expr {
    match rule {
        Expr::Cmp { column, op, value } if matches!(op, super::CmpOp::Eq) => {
            let mut parts = vec![Expr::Cmp {
                column: column.clone(),
                op: super::CmpOp::Eq,
                value: value.clone(),
            }];
            if config.bloom {
                parts.push(Expr::BloomFilterEq {
                    column: column.clone(),
                    value: value.clone(),
                });
            }
            if config.dictionary && is_dictionary_supported_scalar(&value) {
                parts.push(Expr::DictionaryHintEq {
                    column: column.clone(),
                    value: value.clone(),
                });
            }
            if parts.len() == 1 {
                parts.into_iter().next().expect("one part")
            } else {
                Expr::And(parts)
            }
        }
        Expr::InList { column, values } => {
            let mut parts = vec![Expr::InList {
                column: column.clone(),
                values: values.clone(),
            }];
            if config.bloom {
                parts.push(Expr::BloomFilterInList {
                    column: column.clone(),
                    values: values.clone(),
                });
            }
            if config.dictionary && is_dictionary_supported_in_list(&values) {
                parts.push(Expr::DictionaryHintInList {
                    column: column.clone(),
                    values: values.clone(),
                });
            }
            if parts.len() == 1 {
                parts.into_iter().next().expect("one part")
            } else {
                Expr::And(parts)
            }
        }
        other => other,
    }
}

fn is_dictionary_supported_scalar(value: &ScalarValue) -> bool {
    matches!(
        value,
        ScalarValue::Utf8(Some(_))
            | ScalarValue::LargeUtf8(Some(_))
            | ScalarValue::Utf8View(Some(_))
            | ScalarValue::Binary(Some(_))
            | ScalarValue::LargeBinary(Some(_))
            | ScalarValue::BinaryView(Some(_))
            | ScalarValue::FixedSizeBinary(_, Some(_))
    )
}

fn is_dictionary_supported_in_list(values: &[ScalarValue]) -> bool {
    !values.is_empty() && values.iter().all(is_dictionary_supported_scalar)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bloom_injection_respects_negation_polarity() {
        let expr = Expr::eq("id", ScalarValue::Int64(Some(42)));
        let injected = inject_bloom_filters(expr);
        match injected {
            Expr::And(parts) => {
                assert!(matches!(parts[0], Expr::Cmp { .. }));
                assert!(matches!(parts[1], Expr::BloomFilterEq { .. }));
            }
            _ => panic!("expected And for positive context"),
        }

        let neg = Expr::not(Expr::eq("id", ScalarValue::Int64(Some(42))));
        let neg_injected = inject_bloom_filters(neg);
        match neg_injected {
            Expr::Not(inner) => assert!(matches!(*inner, Expr::Cmp { .. })),
            _ => panic!("expected Not(Cmp) for negative context"),
        }

        let double_neg = Expr::not(Expr::not(Expr::eq("id", ScalarValue::Int64(Some(42)))));
        let double_injected = inject_bloom_filters(double_neg);
        match double_injected {
            Expr::Not(inner) => match inner.as_ref() {
                Expr::Not(double_inner) => match double_inner.as_ref() {
                    Expr::And(parts) => {
                        assert!(matches!(parts[0], Expr::Cmp { .. }));
                        assert!(matches!(parts[1], Expr::BloomFilterEq { .. }));
                    }
                    _ => panic!("expected And inside double negation"),
                },
                _ => panic!("expected Not inside double negation"),
            },
            _ => panic!("expected Not(Not(...)) for double negation"),
        }
    }

    #[test]
    fn bloom_injection_for_in_list() {
        let expr = Expr::in_list(
            "status",
            vec![
                ScalarValue::Utf8(Some("active".to_string())),
                ScalarValue::Utf8(Some("pending".to_string())),
            ],
        );
        let injected = inject_bloom_filters(expr);
        match injected {
            Expr::And(parts) => {
                assert_eq!(parts.len(), 2);
                assert!(matches!(parts[0], Expr::InList { .. }));
                assert!(matches!(parts[1], Expr::BloomFilterInList { .. }));
            }
            _ => panic!("expected And([InList, BloomFilterInList])"),
        }
    }

    #[test]
    fn bloom_not_injected_for_non_eq_comparisons() {
        // Only Eq should get bloom filters, not Lt/Gt/etc
        let lt_expr = Expr::lt("age", ScalarValue::Int32(Some(18)));
        let injected = inject_bloom_filters(lt_expr);
        assert!(matches!(injected, Expr::Cmp { .. }));

        let gt_expr = Expr::gt("age", ScalarValue::Int32(Some(65)));
        let injected = inject_bloom_filters(gt_expr);
        assert!(matches!(injected, Expr::Cmp { .. }));

        let not_eq_expr = Expr::not_eq("status", ScalarValue::Utf8(Some("deleted".to_string())));
        let injected = inject_bloom_filters(not_eq_expr);
        assert!(matches!(injected, Expr::Cmp { .. }));
    }

    #[test]
    fn bloom_injection_in_and_expression() {
        let expr = Expr::and(vec![
            Expr::eq("id", ScalarValue::Int64(Some(42))),
            Expr::eq("user_id", ScalarValue::Int64(Some(100))),
        ]);
        let injected = inject_bloom_filters(expr);
        match injected {
            Expr::And(parts) => {
                assert_eq!(parts.len(), 2);
                // Each predicate should be wrapped in And([Cmp, Bloom])
                assert!(matches!(parts[0], Expr::And(_)));
                assert!(matches!(parts[1], Expr::And(_)));
            }
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn bloom_injection_in_or_expression() {
        let expr = Expr::or(vec![
            Expr::eq("id", ScalarValue::Int64(Some(42))),
            Expr::eq("id", ScalarValue::Int64(Some(43))),
        ]);
        let injected = inject_bloom_filters(expr);
        match injected {
            Expr::Or(parts) => {
                assert_eq!(parts.len(), 2);
                // Each branch should get bloom filters
                assert!(matches!(parts[0], Expr::And(_)));
                assert!(matches!(parts[1], Expr::And(_)));
            }
            _ => panic!("expected Or"),
        }
    }

    #[test]
    fn bloom_injection_respects_negation_in_complex_expr() {
        // And([eq, Not(eq)]) -> And([And([Cmp, Bloom]), Not(Cmp)])
        let expr = Expr::and(vec![
            Expr::eq("id", ScalarValue::Int64(Some(42))),
            Expr::not(Expr::eq("deleted", ScalarValue::Boolean(Some(true)))),
        ]);
        let injected = inject_bloom_filters(expr);
        match injected {
            Expr::And(parts) => {
                assert_eq!(parts.len(), 2);
                // First: positive context, should have bloom
                assert!(matches!(parts[0], Expr::And(_)));
                // Second: negative context, should NOT have bloom
                match &parts[1] {
                    Expr::Not(inner) => assert!(matches!(**inner, Expr::Cmp { .. })),
                    _ => panic!("expected Not(Cmp)"),
                }
            }
            _ => panic!("expected And"),
        }
    }

    #[test]
    fn bloom_not_injected_for_other_predicates() {
        // Between, StartsWith, IsNull, etc. should pass through unchanged
        let between = Expr::between(
            "age",
            ScalarValue::Int32(Some(18)),
            ScalarValue::Int32(Some(65)),
            true,
        );
        let injected = inject_bloom_filters(between.clone());
        assert_eq!(format!("{:?}", injected), format!("{:?}", between));

        let starts_with = Expr::starts_with("name", "John");
        let injected = inject_bloom_filters(starts_with.clone());
        assert_eq!(format!("{:?}", injected), format!("{:?}", starts_with));

        let is_null = Expr::is_null("deleted_at");
        let injected = inject_bloom_filters(is_null.clone());
        assert_eq!(format!("{:?}", injected), format!("{:?}", is_null));

        let is_not_null = Expr::is_not_null("created_at");
        let injected = inject_bloom_filters(is_not_null.clone());
        assert_eq!(format!("{:?}", injected), format!("{:?}", is_not_null));
    }

    #[test]
    fn bloom_injection_all_applies_to_multiple_predicates() {
        let predicates = vec![
            Expr::eq("id", ScalarValue::Int64(Some(42))),
            Expr::eq("user_id", ScalarValue::Int64(Some(100))),
            Expr::lt("age", ScalarValue::Int32(Some(18))),
        ];
        let injected = inject_bloom_filters_all(&predicates);
        assert_eq!(injected.len(), 3);
        // First two should have bloom filters
        assert!(matches!(injected[0], Expr::And(_)));
        assert!(matches!(injected[1], Expr::And(_)));
        // Third (Lt) should not
        assert!(matches!(injected[2], Expr::Cmp { .. }));
    }

    #[test]
    fn dictionary_injection_enabled_for_positive_eq() {
        let expr = Expr::eq("s", ScalarValue::Utf8(Some("foo".to_string())));
        let injected = inject_metadata_hints(
            expr,
            MetadataHintConfig {
                bloom: false,
                dictionary: true,
            },
        );
        match injected {
            Expr::And(parts) => {
                assert_eq!(parts.len(), 2);
                assert!(matches!(parts[0], Expr::Cmp { .. }));
                assert!(matches!(parts[1], Expr::DictionaryHintEq { .. }));
            }
            _ => panic!("expected And([Cmp, DictionaryHintEq])"),
        }
    }

    #[test]
    fn dictionary_injection_respects_negation_polarity() {
        let expr = Expr::not(Expr::eq("s", ScalarValue::Utf8(Some("foo".to_string()))));
        let injected = inject_metadata_hints(
            expr,
            MetadataHintConfig {
                bloom: false,
                dictionary: true,
            },
        );
        match injected {
            Expr::Not(inner) => assert!(matches!(*inner, Expr::Cmp { .. })),
            _ => panic!("expected Not(Cmp)"),
        }
    }

    #[test]
    fn metadata_injection_no_hints_keeps_expression() {
        let expr = Expr::eq("id", ScalarValue::Int64(Some(42)));
        let injected = inject_metadata_hints(
            expr.clone(),
            MetadataHintConfig {
                bloom: false,
                dictionary: false,
            },
        );
        assert_eq!(format!("{:?}", injected), format!("{:?}", expr));
    }

    #[test]
    fn dictionary_not_injected_for_unsupported_cmp_literal() {
        let expr = Expr::eq("id", ScalarValue::Int64(Some(42)));
        let injected = inject_metadata_hints(
            expr,
            MetadataHintConfig {
                bloom: false,
                dictionary: true,
            },
        );
        assert!(matches!(injected, Expr::Cmp { .. }));
    }

    #[test]
    fn dictionary_not_injected_for_mixed_or_empty_in_list() {
        let mixed = Expr::in_list(
            "s",
            vec![
                ScalarValue::Utf8(Some("alpha".to_string())),
                ScalarValue::Int32(Some(7)),
            ],
        );
        let mixed_injected = inject_metadata_hints(
            mixed,
            MetadataHintConfig {
                bloom: false,
                dictionary: true,
            },
        );
        assert!(matches!(mixed_injected, Expr::InList { .. }));

        let empty = Expr::in_list("s", vec![]);
        let empty_injected = inject_metadata_hints(
            empty,
            MetadataHintConfig {
                bloom: false,
                dictionary: true,
            },
        );
        assert!(matches!(empty_injected, Expr::InList { .. }));
    }
}
