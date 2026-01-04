use std::sync::Arc;

use aisle::{Expr, PruneRequest};
use arrow_array::{
    ArrayRef, Int32Array, ListArray, MapArray, RecordBatch, StringArray, StructArray,
};
use arrow_schema::{DataType, Field, Schema};
use bytes::Bytes;
use datafusion_common::ScalarValue;
use parquet::{
    arrow::ArrowWriter,
    file::{
        metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader},
        properties::{EnabledStatistics, WriterProperties},
    },
};

fn write_parquet(batches: &[RecordBatch], props: WriterProperties) -> Vec<u8> {
    let mut buffer: Vec<u8> = Vec::new();
    let schema = batches[0].schema();
    let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props)).unwrap();
    for batch in batches {
        writer.write(batch).unwrap();
    }
    writer.close().unwrap();
    buffer
}


fn i32_val(value: i32) -> ScalarValue {
    ScalarValue::Int32(Some(value))
}
fn load_metadata(bytes: &[u8]) -> ParquetMetaData {
    let bytes = Bytes::copy_from_slice(bytes);
    ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .parse_and_finish(&bytes)
        .unwrap()
}

#[test]
fn prunes_row_groups_with_list_column() {
    // Create schema with List<Int32>
    let schema = Schema::new(vec![Field::new(
        "my_list",
        DataType::List(Arc::new(Field::new("element", DataType::Int32, false))),
        true,
    )]);

    // Create two batches with different value ranges
    let batch1 = {
        let values = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let offsets = arrow_buffer::OffsetBuffer::new(vec![0, 5].into());
        let list_array = ListArray::new(
            Arc::new(Field::new("element", DataType::Int32, false)),
            offsets,
            Arc::new(values),
            None,
        );
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(list_array) as ArrayRef],
        )
        .unwrap()
    };

    let batch2 = {
        let values = Int32Array::from(vec![10, 11, 12, 13, 14]);
        let offsets = arrow_buffer::OffsetBuffer::new(vec![0, 5].into());
        let list_array = ListArray::new(
            Arc::new(Field::new("element", DataType::Int32, false)),
            offsets,
            Arc::new(values),
            None,
        );
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(list_array) as ArrayRef],
        )
        .unwrap()
    };

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_max_row_group_size(1)
        .build();

    let bytes = write_parquet(&[batch1, batch2], props);
    let metadata = load_metadata(&bytes);

    // Filter: list elements > 9
    // Should keep only row group 1 (values 10-14)
    let expr = Expr::gt("my_list.list.element", i32_val(9));
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .emit_roaring(false)
        .prune();
    assert_eq!(result.row_groups(), &[1]);
}

#[test]
fn prunes_row_groups_with_list_column_coerced_names() {
    // Create schema with List<Int32> but non-standard element name "item"
    let schema = Schema::new(vec![Field::new(
        "my_list",
        DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
        true,
    )]);

    // Create two batches with different value ranges
    let batch1 = {
        let values = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let offsets = arrow_buffer::OffsetBuffer::new(vec![0, 5].into());
        let list_array = ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, false)),
            offsets,
            Arc::new(values),
            None,
        );
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(list_array) as ArrayRef],
        )
        .unwrap()
    };

    let batch2 = {
        let values = Int32Array::from(vec![10, 11, 12, 13, 14]);
        let offsets = arrow_buffer::OffsetBuffer::new(vec![0, 5].into());
        let list_array = ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, false)),
            offsets,
            Arc::new(values),
            None,
        );
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(list_array) as ArrayRef],
        )
        .unwrap()
    };

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_max_row_group_size(1)
        .set_coerce_types(true)
        .build();

    let bytes = write_parquet(&[batch1, batch2], props);
    let metadata = load_metadata(&bytes);

    // Filter using standard Parquet path after coercion
    let expr = Expr::gt("my_list.list.element", i32_val(9));
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .emit_roaring(false)
        .prune();
    assert_eq!(result.row_groups(), &[1]);
}

#[test]
fn prunes_row_groups_with_map_column() {
    // Create schema with Map<Utf8, Int32>
    let schema = Schema::new(vec![Field::new(
        "my_map",
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Int32, false),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        ),
        true,
    )]);

    // Create two batches with different value ranges
    let batch1 = {
        let keys = StringArray::from(vec!["a", "b"]);
        let values = Int32Array::from(vec![1, 2]);
        let entries = StructArray::from(vec![
            (
                Arc::new(Field::new("key", DataType::Utf8, false)),
                Arc::new(keys) as ArrayRef,
            ),
            (
                Arc::new(Field::new("value", DataType::Int32, false)),
                Arc::new(values) as ArrayRef,
            ),
        ]);
        let offsets = arrow_buffer::OffsetBuffer::new(vec![0, 2].into());
        let map_array = MapArray::new(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Int32, false),
                    ]
                    .into(),
                ),
                false,
            )),
            offsets,
            entries,
            None,
            false,
        );
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(map_array) as ArrayRef],
        )
        .unwrap()
    };

    let batch2 = {
        let keys = StringArray::from(vec!["c", "d"]);
        let values = Int32Array::from(vec![10, 11]);
        let entries = StructArray::from(vec![
            (
                Arc::new(Field::new("key", DataType::Utf8, false)),
                Arc::new(keys) as ArrayRef,
            ),
            (
                Arc::new(Field::new("value", DataType::Int32, false)),
                Arc::new(values) as ArrayRef,
            ),
        ]);
        let offsets = arrow_buffer::OffsetBuffer::new(vec![0, 2].into());
        let map_array = MapArray::new(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("key", DataType::Utf8, false),
                        Field::new("value", DataType::Int32, false),
                    ]
                    .into(),
                ),
                false,
            )),
            offsets,
            entries,
            None,
            false,
        );
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(map_array) as ArrayRef],
        )
        .unwrap()
    };

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_max_row_group_size(1)
        .build();

    let bytes = write_parquet(&[batch1, batch2], props);
    let metadata = load_metadata(&bytes);

    // Filter: map values > 9
    // Should keep only row group 1 (values 10, 11)
    // Note: Parquet uses the Arrow field name "entries", not "key_value"
    let expr = Expr::gt("my_map.entries.value", i32_val(9));
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .emit_roaring(false)
        .prune();
    assert_eq!(result.row_groups(), &[1]);
}

#[test]
fn prunes_row_groups_with_map_column_coerced_names() {
    // Create schema with Map<Utf8, Int32> but non-standard names
    let schema = Schema::new(vec![Field::new(
        "my_map",
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("keys", DataType::Utf8, false),
                        Field::new("values", DataType::Int32, false),
                    ]
                    .into(),
                ),
                false,
            )),
            false,
        ),
        true,
    )]);

    // Create two batches with different value ranges
    let batch1 = {
        let keys = StringArray::from(vec!["a", "b"]);
        let values = Int32Array::from(vec![1, 2]);
        let entries = StructArray::from(vec![
            (
                Arc::new(Field::new("keys", DataType::Utf8, false)),
                Arc::new(keys) as ArrayRef,
            ),
            (
                Arc::new(Field::new("values", DataType::Int32, false)),
                Arc::new(values) as ArrayRef,
            ),
        ]);
        let offsets = arrow_buffer::OffsetBuffer::new(vec![0, 2].into());
        let map_array = MapArray::new(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("keys", DataType::Utf8, false),
                        Field::new("values", DataType::Int32, false),
                    ]
                    .into(),
                ),
                false,
            )),
            offsets,
            entries,
            None,
            false,
        );
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(map_array) as ArrayRef],
        )
        .unwrap()
    };

    let batch2 = {
        let keys = StringArray::from(vec!["c", "d"]);
        let values = Int32Array::from(vec![10, 11]);
        let entries = StructArray::from(vec![
            (
                Arc::new(Field::new("keys", DataType::Utf8, false)),
                Arc::new(keys) as ArrayRef,
            ),
            (
                Arc::new(Field::new("values", DataType::Int32, false)),
                Arc::new(values) as ArrayRef,
            ),
        ]);
        let offsets = arrow_buffer::OffsetBuffer::new(vec![0, 2].into());
        let map_array = MapArray::new(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(
                    vec![
                        Field::new("keys", DataType::Utf8, false),
                        Field::new("values", DataType::Int32, false),
                    ]
                    .into(),
                ),
                false,
            )),
            offsets,
            entries,
            None,
            false,
        );
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(map_array) as ArrayRef],
        )
        .unwrap()
    };

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_max_row_group_size(1)
        .set_coerce_types(true)
        .build();

    let bytes = write_parquet(&[batch1, batch2], props);
    let metadata = load_metadata(&bytes);

    // Filter using standard Parquet path after coercion
    let expr = Expr::gt("my_map.key_value.value", i32_val(9));
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .emit_roaring(false)
        .prune();
    assert_eq!(result.row_groups(), &[1]);
}

#[test]
fn prunes_row_groups_with_list_of_structs() {
    // Create schema with List<Struct<{id: Int32, name: Utf8}>>
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

    // Create two batches with different ID ranges
    let batch1 = {
        let ids = Int32Array::from(vec![1, 2]);
        let names = StringArray::from(vec!["a", "b"]);
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("id", DataType::Int32, false)),
                Arc::new(ids) as ArrayRef,
            ),
            (
                Arc::new(Field::new("name", DataType::Utf8, false)),
                Arc::new(names) as ArrayRef,
            ),
        ]);
        let offsets = arrow_buffer::OffsetBuffer::new(vec![0, 2].into());
        let list_array = ListArray::new(
            Arc::new(Field::new(
                "element",
                DataType::Struct(
                    vec![
                        Field::new("id", DataType::Int32, false),
                        Field::new("name", DataType::Utf8, false),
                    ]
                    .into(),
                ),
                false,
            )),
            offsets,
            Arc::new(struct_array),
            None,
        );
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(list_array) as ArrayRef],
        )
        .unwrap()
    };

    let batch2 = {
        let ids = Int32Array::from(vec![10, 11]);
        let names = StringArray::from(vec!["c", "d"]);
        let struct_array = StructArray::from(vec![
            (
                Arc::new(Field::new("id", DataType::Int32, false)),
                Arc::new(ids) as ArrayRef,
            ),
            (
                Arc::new(Field::new("name", DataType::Utf8, false)),
                Arc::new(names) as ArrayRef,
            ),
        ]);
        let offsets = arrow_buffer::OffsetBuffer::new(vec![0, 2].into());
        let list_array = ListArray::new(
            Arc::new(Field::new(
                "element",
                DataType::Struct(
                    vec![
                        Field::new("id", DataType::Int32, false),
                        Field::new("name", DataType::Utf8, false),
                    ]
                    .into(),
                ),
                false,
            )),
            offsets,
            Arc::new(struct_array),
            None,
        );
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![Arc::new(list_array) as ArrayRef],
        )
        .unwrap()
    };

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .set_max_row_group_size(1)
        .build();

    let bytes = write_parquet(&[batch1, batch2], props);
    let metadata = load_metadata(&bytes);

    // Filter: item.id > 9
    // Should keep only row group 1 (IDs 10, 11)
    let expr = Expr::gt("items.list.element.id", i32_val(9));
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .emit_roaring(false)
        .prune();
    assert_eq!(result.row_groups(), &[1]);
}

#[test]
fn test_list_custom_element_name_shows_bug() {
    // This test demonstrates the bug: element field name mismatch
    use std::sync::Arc;

    let schema = Schema::new(vec![Field::new(
        "my_list",
        DataType::List(Arc::new(Field::new("item", DataType::Int32, false))),
        true,
    )]);

    let values = Int32Array::from(vec![1, 2, 3]);
    let offsets = arrow_buffer::OffsetBuffer::new(vec![0, 3].into());
    let list_array = ListArray::new(
        Arc::new(Field::new("item", DataType::Int32, false)),
        offsets,
        Arc::new(values),
        None,
    );
    let batch = RecordBatch::try_new(
        Arc::new(schema.clone()),
        vec![Arc::new(list_array) as ArrayRef],
    )
    .unwrap();

    let props = WriterProperties::builder()
        .set_statistics_enabled(EnabledStatistics::Page)
        .build();

    let bytes = write_parquet(&[batch], props);
    let metadata = load_metadata(&bytes);

    // Check what Parquet actually wrote
    println!("\nParquet column path:");
    for col in metadata.file_metadata().schema_descr().columns() {
        println!("  {}", col.path().string());
    }

    // Try to prune - this will fail because data_type_for_path expects "element"
    let expr = Expr::gt("my_list.list.item", i32_val(0));
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&expr)
        .enable_page_index(false)
        .emit_roaring(false)
        .prune();
    println!("Row groups kept: {:?}", result.row_groups());

    // Bug: If Parquet uses "item", this should work but won't
}
