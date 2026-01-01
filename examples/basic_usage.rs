/// Basic example demonstrating Aisle's metadata-based filter pushdown.
///
/// This example shows how to:
/// 1. Prune row groups using metadata and predicates
/// 2. Apply pruning results to a Parquet reader
/// 3. Measure I/O reduction from pruning
use aisle::PruneRequest;
use datafusion_expr::{col, lit};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Step 1: Create a Parquet file with 3 row groups
    // Sample data structure:
    //   Row group 0: id=[1,2,3],       age=[25,30,35]
    //   Row group 1: id=[100,101,102], age=[40,45,50]
    //   Row group 2: id=[200,201,202], age=[55,60,65]
    let (parquet_bytes, schema) = helpers::create_sample_parquet()?;
    let metadata = helpers::load_metadata(&parquet_bytes)?;

    // Step 2: Define a filter predicate using DataFusion expressions
    // This predicate will select rows where: id >= 100 AND age < 50
    // Expected match: Only row group 1 (rows: [100,101,102] with ages [40,45,50])
    let predicate = col("id").gt_eq(lit(100i64)).and(col("age").lt(lit(50i64)));

    // Step 3: Use Aisle to determine which row groups to read
    // This is the core metadata-based pruning operation
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&predicate)
        .enable_page_index(false) // Row-group level pruning only
        .enable_bloom_filter(false) // No bloom filters in this example
        .prune();

    // Show pruning results
    println!("Filter: id >= 100 AND age < 50\n");
    println!("Pruning result:");
    println!("  ✓ Kept row groups: {:?}", result.row_groups());
    println!(
        "  ✗ Pruned: {} of 3 row groups ({}% I/O reduction)\n",
        3 - result.row_groups().len(),
        ((3 - result.row_groups().len()) as f64 / 3.0 * 100.0) as i32
    );

    // Step 4: Apply pruning to Parquet reader
    // Compare row counts with and without pruning
    let rows_without_pruning = helpers::count_rows_without_pruning(&parquet_bytes)?;
    let rows_with_pruning = helpers::read_with_pruning(&parquet_bytes, &result)?;

    println!("Performance comparison:");
    println!(
        "  Without pruning: {} rows from 3 row groups",
        rows_without_pruning
    );
    println!(
        "  With pruning:    {} rows from {} row group(s)",
        rows_with_pruning,
        result.row_groups().len()
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
        arrow::{ArrowWriter, arrow_reader::ParquetRecordBatchReaderBuilder},
        file::{
            metadata::{ParquetMetaData, ParquetMetaDataReader},
            properties::{EnabledStatistics, WriterProperties},
        },
    };

    pub fn create_sample_parquet() -> Result<(Bytes, Arc<Schema>), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("age", DataType::Int64, false),
        ]));

        let batches = vec![
            create_batch(
                schema.clone(),
                &[1, 2, 3],
                &["Alice", "Bob", "Carol"],
                &[25, 30, 35],
            ),
            create_batch(
                schema.clone(),
                &[100, 101, 102],
                &["Dave", "Eve", "Frank"],
                &[40, 45, 50],
            ),
            create_batch(
                schema.clone(),
                &[200, 201, 202],
                &["Grace", "Henry", "Iris"],
                &[55, 60, 65],
            ),
        ];

        let mut buffer = Vec::new();
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_max_row_group_size(3)
            .build();

        let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), Some(props))?;
        for batch in &batches {
            writer.write(batch)?;
        }
        writer.close()?;

        Ok((Bytes::from(buffer), schema))
    }

    pub fn load_metadata(bytes: &Bytes) -> Result<ParquetMetaData, Box<dyn std::error::Error>> {
        Ok(ParquetMetaDataReader::new().parse_and_finish(bytes)?)
    }

    pub fn count_rows_without_pruning(bytes: &Bytes) -> Result<usize, Box<dyn std::error::Error>> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())?.build()?;
        let mut total = 0;
        for batch in reader {
            total += batch?.num_rows();
        }
        Ok(total)
    }

    pub fn read_with_pruning(
        bytes: &Bytes,
        result: &PruneResult,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes.clone())?
            .with_row_groups(result.row_groups().to_vec())
            .build()?;

        let mut total = 0;
        for batch in reader {
            total += batch?.num_rows();
        }
        Ok(total)
    }

    fn create_batch(schema: Arc<Schema>, ids: &[i64], names: &[&str], ages: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
                Arc::new(Int64Array::from(ages.to_vec())),
            ],
        )
        .unwrap()
    }
}
