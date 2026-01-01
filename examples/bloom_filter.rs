/// Async example demonstrating Aisle with bloom filter pruning.
///
/// This example shows:
/// 1. Async Parquet reading with bloom filters
/// 2. Using bloom filters for point queries (= and IN predicates)
/// 3. Combining statistics + bloom filters for aggressive pruning
use aisle::PruneRequest;
use datafusion_expr::{col, lit};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Step 1: Create a Parquet file with bloom filters enabled
    // Sample data structure:
    //   Row group 0: user_id=[1,2,3]
    //   Row group 1: user_id=[1000,1001,1002]
    //   Row group 2: user_id=[5000,5001,5002]
    let (parquet_bytes, _schema) = helpers::create_parquet_with_bloom_filters().await?;

    // Step 2: Define a point query predicate
    // This type of query (equality on high-cardinality column) benefits most from bloom filters
    let predicate = col("user_id").eq(lit(1000i64));

    // Step 3: Open async Parquet reader
    let cursor = std::io::Cursor::new(parquet_bytes);
    let mut builder =
        parquet::arrow::async_reader::ParquetRecordBatchStreamBuilder::new(cursor).await?;

    // Step 4: Prune using bloom filters with unified API
    // Bloom filters provide definite ABSENCE checks:
    //   - Row group 0: stats=[1,3]       -> Pruned by statistics
    //   - Row group 1: stats=[1000,1002] -> Bloom filter confirms 1000 exists -> Keep
    //   - Row group 2: stats=[5000,5002] -> Pruned by statistics
    let metadata = builder.metadata().clone();
    let schema = builder.schema().clone();
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&predicate)
        .enable_page_index(false)
        .enable_bloom_filter(true) // Enable bloom filter pruning for point queries
        .emit_roaring(false)
        .prune_async(&mut builder)
        .await;

    // Show pruning results
    println!("Filter: user_id = 1000 (point query)\n");
    println!("Pruning result:");
    println!("  ✓ Kept row groups: {:?}", result.row_groups());
    println!(
        "  ✗ Pruned: {} of 3 row groups ({}% I/O reduction)\n",
        3 - result.row_groups().len(),
        ((3 - result.row_groups().len()) as f64 / 3.0 * 100.0) as i32
    );

    // Why bloom filters matter:
    //   • Statistics alone: Can't prove a value EXISTS within a range, only bounds
    //   • Bloom filters: Provide definite ABSENCE checks (no false negatives)
    //   • Best for: High-cardinality columns (user IDs, SKUs, transaction IDs)
    //   • Ideal queries: = and IN predicates with specific values

    Ok(())
}

// ============================================================================
// Helper functions (Parquet setup - not Aisle-specific)
// ============================================================================

mod helpers {
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use parquet::{
        arrow::AsyncArrowWriter,
        file::properties::{EnabledStatistics, WriterProperties},
    };

    pub async fn create_parquet_with_bloom_filters()
    -> Result<(Vec<u8>, Arc<Schema>), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("user_id", DataType::Int64, false),
            Field::new("username", DataType::Utf8, false),
            Field::new("score", DataType::Int64, false),
        ]));

        let batches = vec![
            create_batch(
                schema.clone(),
                &[1, 2, 3],
                &["alice", "bob", "charlie"],
                &[100, 200, 150],
            ),
            create_batch(
                schema.clone(),
                &[1000, 1001, 1002],
                &["dave", "eve", "frank"],
                &[300, 250, 400],
            ),
            create_batch(
                schema.clone(),
                &[5000, 5001, 5002],
                &["grace", "henry", "iris"],
                &[500, 450, 350],
            ),
        ];

        let mut buffer = Vec::new();
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_bloom_filter_enabled(true)
            .set_max_row_group_size(3)
            .build();

        let mut writer = AsyncArrowWriter::try_new(&mut buffer, schema.clone(), Some(props))?;
        for batch in &batches {
            writer.write(batch).await?;
        }
        writer.close().await?;

        Ok((buffer, schema))
    }

    fn create_batch(
        schema: Arc<Schema>,
        ids: &[i64],
        names: &[&str],
        scores: &[i64],
    ) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
                Arc::new(Int64Array::from(scores.to_vec())),
            ],
        )
        .unwrap()
    }
}
