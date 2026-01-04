//! Parquet column path mapping for nested structs.

use std::sync::Arc;

use parquet::{
    basic::{Repetition, Type as PhysicalType},
    schema::types::Type,
};

#[test]
fn test_parquet_column_path_mapping() {
    // In Parquet, nested struct fields are stored with dotted paths
    // For example, a struct field "a.b" refers to field "b" inside struct "a"

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
    // Expected:
    // [0] path='a.b', name='b'
    // [1] path='a.c', name='c'
    assert_eq!(schema_descr.columns()[0].path().string(), "a.b");
    assert_eq!(schema_descr.columns()[0].name(), "b");
    assert_eq!(schema_descr.columns()[1].path().string(), "a.c");
    assert_eq!(schema_descr.columns()[1].name(), "c");
}
