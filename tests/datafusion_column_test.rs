/// Test to verify how DataFusion represents nested column access
///
/// KEY FINDINGS:
/// 1. col("a.b") -> Column { relation: Some("a"), name: "b" } (table-qualified, NOT nested)
/// 2. col("a.b.c") -> Column { relation: Partial {schema: "a", table: "b"}, name: "c" }
///    (schema-qualified)
/// 3. Arrow Schema field_with_name("a.b") -> Error (dotted paths not supported)
/// 4. Nested struct access in DataFusion uses GetIndexedField expression (not Column)
///
/// IMPLICATION FOR AISLE:
/// - Column names are NOT nested paths by default
/// - For Parquet metadata pruning, we care about physical column paths in the file
/// - Parquet stores structs as nested columns with dotted paths (e.g., "a.b")
/// - Need to map Column expressions to Parquet column paths
use arrow_schema::{DataType, Field, Fields, Schema};
use datafusion_expr::{Expr, col};

#[test]
fn test_nested_field_representation() {
    // Create a schema with nested struct
    let inner_fields = Fields::from(vec![
        Field::new("b", DataType::Int32, false),
        Field::new("c", DataType::Utf8, false),
    ]);

    let _schema = Schema::new(vec![
        Field::new("a", DataType::Struct(inner_fields.clone()), false),
        Field::new("x", DataType::Int64, false),
    ]);

    // Test 1: Simple column
    let simple_col = col("x");
    println!("Simple column: {:?}", simple_col);
    if let Expr::Column(c) = &simple_col {
        println!("  name: {:?}", c.name);
        println!("  relation: {:?}", c.relation);
    }

    // Test 2: Dotted name - this becomes relation-qualified, NOT nested access
    let dotted_col = col("a.b");
    println!("\nDotted column col('a.b'): {:?}", dotted_col);
    if let Expr::Column(c) = &dotted_col {
        println!("  name: {:?}", c.name);
        println!("  relation: {:?}", c.relation);
        println!("  This is TABLE-qualified (relation='a', name='b'), not nested field access!");
    }

    // Test 3: Check for GetIndexedField - this is how nested fields are actually accessed
    println!("\n=== GetIndexedField Note ===");
    println!("DataFusion uses Expr::GetIndexedField for nested struct access");
    println!("But compile.rs receives Expr, so we need to handle GetIndexedField variants");
}

#[test]
fn test_schema_field_lookup() {
    // Test how to look up nested fields in Arrow Schema
    let inner_fields = Fields::from(vec![
        Field::new("b", DataType::Int32, false),
        Field::new("c", DataType::Utf8, false),
    ]);

    let schema = Schema::new(vec![
        Field::new("a", DataType::Struct(inner_fields.clone()), false),
        Field::new("b", DataType::Int64, false), // Ambiguous leaf name!
    ]);

    // Direct lookup works for top-level
    assert!(schema.field_with_name("a").is_ok());
    assert!(schema.field_with_name("b").is_ok()); // Gets the top-level b

    // Dotted path lookup - does NOT work
    let dotted_result = schema.field_with_name("a.b");
    println!("Dotted lookup 'a.b': {:?}", dotted_result);
    assert!(
        dotted_result.is_err(),
        "Arrow Schema does not support dotted path lookup"
    );

    // Manual traversal is required
    if let Ok(field_a) = schema.field_with_name("a") {
        if let DataType::Struct(fields) = field_a.data_type() {
            println!("\nStruct 'a' has fields:");
            for field in fields.iter() {
                println!("  - {}: {:?}", field.name(), field.data_type());
            }

            // Can we find nested b?
            let nested_b = fields.iter().find(|f| f.name() == "b");
            println!("\nNested 'a.b': {:?}", nested_b);
            assert!(
                nested_b.is_some(),
                "Should find nested field 'b' inside struct 'a'"
            );
        }
    }
}

#[test]
fn test_column_name_variants() {
    // Test different ways columns might be represented

    // 1. Simple name
    let c1 = col("simple");
    if let Expr::Column(c) = c1 {
        println!("Simple: name='{}', relation={:?}", c.name, c.relation);
        assert_eq!(c.name, "simple");
        assert!(c.relation.is_none());
    }

    // 2. Qualified (table.column)
    let c2 = col("table.column");
    if let Expr::Column(c) = c2 {
        println!("Qualified: name='{}', relation={:?}", c.name, c.relation);
        assert_eq!(c.name, "column");
        assert!(c.relation.is_some());
    }

    // 3. Multi-level dotted (a.b.c) - how is this interpreted?
    let c3 = col("a.b.c");
    if let Expr::Column(c) = c3 {
        println!("Multi-dot: name='{}', relation={:?}", c.name, c.relation);
        // Result: relation=Partial {schema: "a", table: "b"}, name="c"
    }
}

#[test]
fn test_parquet_column_path_mapping() {
    // In Parquet, nested struct fields are stored with dotted paths
    // For example, a struct field "a.b" refers to field "b" inside struct "a"

    use std::sync::Arc;

    use parquet::{
        basic::{Repetition, Type as PhysicalType},
        schema::types::Type,
    };

    // Build a Parquet schema with nested struct
    let b_field = Type::primitive_type_builder("b", PhysicalType::INT32)
        .with_repetition(Repetition::REQUIRED)
        .build()
        .unwrap();

    let c_field = Type::primitive_type_builder("c", PhysicalType::BYTE_ARRAY)
        .with_repetition(Repetition::REQUIRED)
        .build()
        .unwrap();

    let a_struct = Type::group_type_builder("a")
        .with_repetition(Repetition::REQUIRED)
        .with_fields(vec![Arc::new(b_field), Arc::new(c_field)])
        .build()
        .unwrap();

    let schema = Type::group_type_builder("schema")
        .with_fields(vec![Arc::new(a_struct)])
        .build()
        .unwrap();

    let schema_descr = parquet::schema::types::SchemaDescriptor::new(Arc::new(schema));

    // Check column paths
    println!("\nParquet column paths:");
    for (idx, column) in schema_descr.columns().iter().enumerate() {
        let path = column.path().string();
        let name = column.name();
        println!("  [{}] path='{}', name='{}'", idx, path, name);
    }

    // Expected:
    // [0] path='a.b', name='b'
    // [1] path='a.c', name='c'

    assert_eq!(schema_descr.columns()[0].path().string(), "a.b");
    assert_eq!(schema_descr.columns()[0].name(), "b");
}
