/// Example demonstrating row-level filtering with Aisle's IR.
///
/// This example shows how to:
/// 1. Use metadata pruning to skip entire row groups
/// 2. Apply exact row-level filtering with IR expressions
/// 3. Combine both for optimal query performance
use aisle::{Expr, PruneRequest, RowFilter as AisleRowFilter};
use datafusion_common::ScalarValue;
use parquet::arrow::arrow_reader::RowFilter as ParquetRowFilter;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Step 1: Create a Parquet file with 3 row groups
    // Sample data structure:
    //   Row group 0: id=[1,2,3,4],       name=["a","b","c","d"]
    //   Row group 1: id=[41,42,43,44],   name=["e","f","g","h"]
    //   Row group 2: id=[101,102,103,104], name=["i","j","k","l"]
    let (parquet_bytes, schema, parquet_schema) = helpers::create_sample_parquet()?;
    let metadata = helpers::load_metadata(&parquet_bytes)?;

    // Step 2: Define a filter predicate using Aisle's IR
    // Filter: id > 42
    // Expected to match: Row group 1 (id 43, 44) and Row group 2 (id 101-104)
    let predicate = Expr::gt("id", ScalarValue::Int64(Some(42)));

    // Step 3: Use metadata-based pruning to skip irrelevant row groups
    // This evaluates the predicate against row group statistics (min/max values)
    let prune_result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&predicate)
        .enable_page_index(false) // Could enable for page-level pruning
        .prune();

    println!("Metadata Pruning Results:");
    println!("  Total row groups: 3");
    println!("  Kept row groups: {:?}", prune_result.row_groups());
    println!(
        "  Pruned: {} row groups (skipped I/O)\n",
        3 - prune_result.row_groups().len()
    );

    // Step 4: Create exact row filter using the IR predicate
    // This will filter out rows within the kept row groups that don't match
    println!("IR expression for exact filtering: {:?}\n", predicate);

    let row_filter = ParquetRowFilter::new(vec![Box::new(AisleRowFilter::new(
        predicate,
        &parquet_schema,
    ))]);

    // Step 5: Read with BOTH metadata pruning AND exact filtering
    // This is the optimal approach:
    //   1. Metadata pruning skips entire row groups (I/O reduction)
    //   2. Exact filtering removes non-matching rows from remaining groups (correctness)
    let filtered_rows = helpers::read_with_row_filter(
        &parquet_bytes,
        &prune_result,
        row_filter,
    )?;

    println!("Final Results (after both pruning and filtering):");
    for (id, name) in &filtered_rows {
        println!("  id={}, name={}", id, name);
    }

    println!("\nSummary:");
    println!("  Original data: 12 rows in 3 row groups");
    println!(
        "  After metadata pruning: {} row groups kept",
        prune_result.row_groups().len()
    );
    println!(
        "  After exact filtering: {} rows returned (expected: 6)",
        filtered_rows.len()
    );
    println!(
        "\nBenefit: Skipped {} row groups entirely (I/O reduction)",
        3 - prune_result.row_groups().len()
    );

    Ok(())
}

// ============================================================================
// Helper functions (Parquet setup - not Aisle-specific)
// ============================================================================

mod helpers {
    use std::sync::Arc;

    use aisle::PruneResult;
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use bytes::Bytes;
    use parquet::{
        arrow::ArrowWriter,
        file::{
            metadata::{ParquetMetaData, ParquetMetaDataReader},
            properties::{EnabledStatistics, WriterProperties},
        },
        schema::types::SchemaDescriptor,
    };

    pub fn create_sample_parquet(
    ) -> Result<(Bytes, Arc<Schema>, SchemaDescriptor), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));

        let batches = vec![
            create_batch(schema.clone(), &[1, 2, 3, 4], &["a", "b", "c", "d"]),
            create_batch(schema.clone(), &[41, 42, 43, 44], &["e", "f", "g", "h"]),
            create_batch(
                schema.clone(),
                &[101, 102, 103, 104],
                &["i", "j", "k", "l"],
            ),
        ];

        let mut buffer = Vec::new();
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_max_row_group_size(4) // Force separate row groups
            .build();

        let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), Some(props))?;
        for batch in &batches {
            writer.write(batch)?;
        }
        writer.close()?;

        // Load metadata to extract Parquet schema descriptor
        let bytes = Bytes::from(buffer);
        let metadata = ParquetMetaDataReader::new().parse_and_finish(&bytes)?;
        let parquet_schema = metadata.file_metadata().schema_descr().clone();

        Ok((bytes, schema, parquet_schema))
    }

    pub fn load_metadata(bytes: &Bytes) -> Result<ParquetMetaData, Box<dyn std::error::Error>> {
        Ok(ParquetMetaDataReader::new().parse_and_finish(bytes)?)
    }

    pub fn read_with_row_filter(
        bytes: &Bytes,
        result: &PruneResult,
        row_filter: parquet::arrow::arrow_reader::RowFilter,
    ) -> Result<Vec<(i64, String)>, Box<dyn std::error::Error>> {
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())?
            .with_row_groups(result.row_groups().to_vec())
            .with_row_filter(row_filter)
            .build()?;

        let mut rows = Vec::new();
        for batch in reader {
            let batch = batch?;
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            for i in 0..batch.num_rows() {
                rows.push((ids.value(i), names.value(i).to_string()));
            }
        }

        Ok(rows)
    }

    fn create_batch(schema: Arc<Schema>, ids: &[i64], names: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }
}
