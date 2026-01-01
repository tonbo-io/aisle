/// Byte array ordering example demonstrating Aisle's ordering-aware pruning.
///
/// This example shows:
/// 1. Equality predicates (always safe, no ordering concerns)
/// 2. Ordering predicates with exact statistics (safe by default)
/// 3. Ordering predicates with truncated statistics (requires aggressive mode)
/// 4. Prefix matching with LIKE
/// 5. Binary column ordering
use aisle::PruneRequest;
use datafusion_expr::{col, lit};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // ========================================================================
    // SCENARIO 1: Equality Predicates (Always Safe)
    // ========================================================================
    // Equality predicates (=, !=, IN) work with any statistics, even truncated ones.
    // They don't rely on ordering, so column order metadata is not required.

    let (bytes, schema) = helpers::create_string_parquet()?;
    let metadata = helpers::load_metadata(&bytes)?;

    // Sample data: 3 row groups
    //   Row group 0: name=['Alice', 'Bob', 'Carol']
    //   Row group 1: name=['Dave', 'Eve', 'Frank']
    //   Row group 2: name=['Grace', 'Henry', 'Iris']

    // Test 1: Exact match - works with any statistics
    let predicate = col("name").eq(lit("Eve"));
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&predicate)
        .prune();

    println!("Scenario 1: Equality Predicates\n");
    println!("Filter: name = 'Eve'");
    println!(
        "✓ Kept row groups: {:?} (equality works regardless of truncation)\n",
        result.row_groups()
    );

    // Test 2: IN list - also equality-based
    let predicate = col("name").in_list(vec![lit("Alice"), lit("Grace")], false);
    let result = PruneRequest::new(&metadata, &schema)
        .with_predicate(&predicate)
        .prune();

    println!("Filter: name IN ('Alice', 'Grace')");
    println!(
        "✓ Kept row groups: {:?} (IN list is equality-based, always safe)\n",
        result.row_groups()
    );

    // ========================================================================
    // SCENARIO 2: Ordering Predicates with Exact Statistics (Default Safe)
    // ========================================================================
    // Ordering predicates (<, >, <=, >=, BETWEEN) require TYPE_DEFINED_ORDER metadata.
    // When statistics are exact (not truncated), pruning is safe by default.

    let (bytes_exact, schema_exact) = helpers::create_string_parquet()?;
    let metadata_exact = helpers::load_metadata(&bytes_exact)?;

    // File written with TYPE_DEFINED_ORDER(UNSIGNED) and exact min/max:
    //   Row group 0: min='Alice',  max='Carol'
    //   Row group 1: min='Dave',   max='Frank'
    //   Row group 2: min='Grace',  max='Iris'

    // Test 1: Greater than - uses ordering
    let predicate = col("name").gt(lit("Eve"));
    let result = PruneRequest::new(&metadata_exact, &schema_exact)
        .with_predicate(&predicate)
        .prune();

    println!("Scenario 2: Ordering Predicates with Exact Statistics\n");
    println!("Filter: name > 'Eve'");
    println!(
        "✓ Kept row groups: {:?} (exact stats + default mode = safe)\n",
        result.row_groups()
    );

    // Test 2: Range query
    let predicate = col("name")
        .gt_eq(lit("Dave"))
        .and(col("name").lt(lit("Henry")));
    let result = PruneRequest::new(&metadata_exact, &schema_exact)
        .with_predicate(&predicate)
        .prune();

    println!("Filter: name >= 'Dave' AND name < 'Henry'");
    println!(
        "✓ Kept row groups: {:?} (range pruning works)\n",
        result.row_groups()
    );

    // ========================================================================
    // SCENARIO 3: Prefix Matching with LIKE
    // ========================================================================
    // LIKE 'prefix%' is converted to range query: >= 'prefix' AND < 'prefiy'
    // This also requires column order metadata for safe pruning.

    let (bytes_prefix, schema_prefix) = helpers::create_email_parquet()?;
    let metadata_prefix = helpers::load_metadata(&bytes_prefix)?;

    // Email data: 3 row groups
    //   Row group 0: email=['alice@example.com', 'bob@example.com']
    //   Row group 1: email=['admin@corp.com', 'admin@test.com']
    //   Row group 2: email=['user@example.com', 'zed@example.com']

    let predicate = col("email").like(lit("admin%"));
    let result = PruneRequest::new(&metadata_prefix, &schema_prefix)
        .with_predicate(&predicate)
        .prune();

    println!("Scenario 3: Prefix Matching (LIKE 'prefix%')\n");
    println!("Filter: email LIKE 'admin%'");
    println!(
        "✓ Kept row groups: {:?} (prefix matching uses ordering)\n",
        result.row_groups()
    );

    // ========================================================================
    // SCENARIO 4: Truncated Statistics (Aggressive Mode)
    // ========================================================================
    // When min/max statistics are truncated, ordering predicates become unsafe.
    // Use .allow_truncated_byte_array_ordering(true) to opt-in to aggressive pruning.

    let (bytes_trunc, schema_trunc) = helpers::create_truncated_stats_parquet()?;
    let metadata_trunc = helpers::load_metadata(&bytes_trunc)?;

    // File with long strings that might trigger truncation in some writers:
    //   Row group 0: min='AAA...', max='BBB...'
    //   Row group 1: min='MMM...', max='NNN...'
    //   Row group 2: min='XXX...', max='ZZZ...'

    let predicate = col("description").gt(lit("N"));

    println!("Scenario 4: Truncated Statistics (Aggressive Mode)\n");
    println!("Filter: description > 'N'\n");

    // Conservative mode (default) - may not use truncated stats
    let result_conservative = PruneRequest::new(&metadata_trunc, &schema_trunc)
        .with_predicate(&predicate)
        .prune();

    println!("Conservative mode (default):");
    println!("  Result: {:?}", result_conservative.row_groups());
    if result_conservative.compile_result().errors().is_empty() {
        println!("  Note: Exact statistics available, pruning works");
    } else {
        println!("  Note: Would keep all row groups if stats were truncated");
    }

    // Aggressive mode - allows truncated stats
    let result_aggressive = PruneRequest::new(&metadata_trunc, &schema_trunc)
        .with_predicate(&predicate)
        .allow_truncated_byte_array_ordering(true)
        .prune();

    println!("\nAggressive mode (.allow_truncated_byte_array_ordering(true)):");
    println!("  Result: {:?}", result_aggressive.row_groups());
    println!("  ✓ Uses truncated statistics for pruning\n");

    // ========================================================================
    // SCENARIO 5: Binary Column Ordering
    // ========================================================================
    // Binary columns follow the same rules as string columns.
    // Equality is always safe, ordering predicates require TYPE_DEFINED_ORDER metadata.

    let (bytes_binary, schema_binary) = helpers::create_binary_parquet()?;
    let metadata_binary = helpers::load_metadata(&bytes_binary)?;

    // Binary hash column: 3 row groups
    //   Row group 0: hash in range [0x00..., 0x3F...]
    //   Row group 1: hash in range [0x40..., 0x7F...]
    //   Row group 2: hash in range [0x80..., 0xFF...]

    // Test 1: Binary equality - always safe
    let predicate = col("hash").eq(lit(&[0x50, 0xAA, 0xBB][..]));
    let result = PruneRequest::new(&metadata_binary, &schema_binary)
        .with_predicate(&predicate)
        .prune();

    println!("Scenario 5: Binary Column Ordering\n");
    println!("Filter: hash = 0x50AABB (equality)");
    println!(
        "✓ Kept row groups: {:?} (binary equality always safe)\n",
        result.row_groups()
    );

    // Test 2: Binary ordering - requires column order
    let predicate = col("hash").gt(lit(&[0x80][..]));
    let result = PruneRequest::new(&metadata_binary, &schema_binary)
        .with_predicate(&predicate)
        .prune();

    println!("Filter: hash > 0x80 (ordering predicate)");
    println!(
        "✓ Kept row groups: {:?} (binary ordering requires TYPE_DEFINED_ORDER)\n",
        result.row_groups()
    );

    // ========================================================================
    // Summary
    // ========================================================================
    // Key takeaways:
    //   ✓ Equality predicates (=, !=, IN): Always safe, no ordering concerns
    //   ✓ Ordering predicates (<, >, <=, >=, BETWEEN, LIKE): Require column order metadata
    //   ✓ Exact statistics: Safe by default
    //   ✓ Truncated statistics: Use .allow_truncated_byte_array_ordering(true) to opt-in
    //   ✓ Binary columns: Same rules apply as string columns

    Ok(())
}

// ============================================================================
// Helper functions
// ============================================================================

mod helpers {
    use std::sync::Arc;

    use arrow_array::{BinaryArray, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use bytes::Bytes;
    use parquet::{
        arrow::ArrowWriter,
        file::{
            metadata::{ParquetMetaData, ParquetMetaDataReader},
            properties::{EnabledStatistics, WriterProperties},
        },
    };

    pub fn create_string_parquet() -> Result<(Bytes, Arc<Schema>), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![Field::new("name", DataType::Utf8, false)]));

        let batches = vec![
            create_string_batch(schema.clone(), &["Alice", "Bob", "Carol"]),
            create_string_batch(schema.clone(), &["Dave", "Eve", "Frank"]),
            create_string_batch(schema.clone(), &["Grace", "Henry", "Iris"]),
        ];

        write_parquet(schema, batches)
    }

    pub fn create_email_parquet() -> Result<(Bytes, Arc<Schema>), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "email",
            DataType::Utf8,
            false,
        )]));

        let batches = vec![
            create_string_batch(schema.clone(), &["alice@example.com", "bob@example.com"]),
            create_string_batch(schema.clone(), &["admin@corp.com", "admin@test.com"]),
            create_string_batch(schema.clone(), &["user@example.com", "zed@example.com"]),
        ];

        write_parquet(schema, batches)
    }

    pub fn create_truncated_stats_parquet()
    -> Result<(Bytes, Arc<Schema>), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "description",
            DataType::Utf8,
            false,
        )]));

        // Use long strings that might trigger truncation in some writers
        // (though Arrow's writer produces exact stats by default)
        let batches = vec![
            create_string_batch(
                schema.clone(),
                &["AAA-long-string-here", "BBB-long-string-here"],
            ),
            create_string_batch(
                schema.clone(),
                &["MMM-long-string-here", "NNN-long-string-here"],
            ),
            create_string_batch(
                schema.clone(),
                &["XXX-long-string-here", "ZZZ-long-string-here"],
            ),
        ];

        write_parquet(schema, batches)
    }

    pub fn create_binary_parquet() -> Result<(Bytes, Arc<Schema>), Box<dyn std::error::Error>> {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "hash",
            DataType::Binary,
            false,
        )]));

        let batches = vec![
            create_binary_batch(schema.clone(), &[&[0x00, 0x11], &[0x3F, 0xFF]]),
            create_binary_batch(schema.clone(), &[&[0x40, 0x00], &[0x7F, 0xFF]]),
            create_binary_batch(schema.clone(), &[&[0x80, 0x00], &[0xFF, 0xFF]]),
        ];

        write_parquet(schema, batches)
    }

    pub fn load_metadata(bytes: &Bytes) -> Result<ParquetMetaData, Box<dyn std::error::Error>> {
        Ok(ParquetMetaDataReader::new().parse_and_finish(bytes)?)
    }

    fn write_parquet(
        schema: Arc<Schema>,
        batches: Vec<RecordBatch>,
    ) -> Result<(Bytes, Arc<Schema>), Box<dyn std::error::Error>> {
        let mut buffer = Vec::new();
        let props = WriterProperties::builder()
            .set_statistics_enabled(EnabledStatistics::Chunk)
            .set_max_row_group_size(2)
            .build();

        let mut writer = ArrowWriter::try_new(&mut buffer, schema.clone(), Some(props))?;
        for batch in &batches {
            writer.write(batch)?;
        }
        writer.close()?;

        Ok((Bytes::from(buffer), schema))
    }

    fn create_string_batch(schema: Arc<Schema>, values: &[&str]) -> RecordBatch {
        RecordBatch::try_new(schema, vec![Arc::new(StringArray::from(values.to_vec()))]).unwrap()
    }

    fn create_binary_batch(schema: Arc<Schema>, values: &[&[u8]]) -> RecordBatch {
        RecordBatch::try_new(schema, vec![Arc::new(BinaryArray::from(values.to_vec()))]).unwrap()
    }
}
