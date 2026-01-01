use std::io::{Seek, SeekFrom};
use std::sync::Arc;

use aisle::{IrRowFilter, compile_pruning_ir};
use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion_expr::{col, lit};
use parquet::arrow::{
    ArrowWriter,
    arrow_reader::{ParquetRecordBatchReaderBuilder, RowFilter},
};
use tempfile::tempfile;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Step 1: Create a schema with two columns: id (Int64) and name (Utf8)
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("name", DataType::Utf8, false),
    ]));

    // Step 2: Create sample data with 4 rows
    // Original data: [(1, "a"), (2, "b"), (43, "c"), (44, "d")]
    // Our filter (id > 42) should keep only the last 2 rows: (43, "c") and (44, "d")
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(vec![1, 2, 43, 44])),
            Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
        ],
    )?;

    // Step 3: Write the data to a temporary Parquet file
    let mut file = tempfile()?;
    let mut writer = ArrowWriter::try_new(&mut file, schema.clone(), None)?;
    writer.write(&batch)?;
    writer.close()?;
    file.seek(SeekFrom::Start(0))?;

    // Step 4: Open the Parquet file for reading
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)?;

    // Step 5: Define a filter predicate using DataFusion expressions
    // This predicate will filter rows where id > 42
    let predicate = col("id").gt(lit(42i64));

    // Step 6: Compile the DataFusion expression into Aisle's pruning IR
    // The IR is a simplified representation that can be evaluated row-by-row
    // during Parquet decode, before materializing the full columns
    let compile = compile_pruning_ir(&predicate, builder.schema().as_ref());

    let ir_expr = compile
        .ir_exprs()
        .first()
        .cloned()
        .ok_or("Predicate not supported for row filtering. \
                Aisle supports: comparisons (=, !=, <, <=, >, >=), BETWEEN, IN, IS NULL, LIKE 'prefix%'")?;

    // Show what was compiled (useful for understanding the IR representation)
    println!("Compiled IR: {:?}\n", ir_expr);

    // Step 7: Create an IrRowFilter that will evaluate the IR during Parquet decode
    // This is more efficient than reading all data first, then filtering
    let filter = IrRowFilter::new(ir_expr, builder.parquet_schema());
    let row_filter = RowFilter::new(vec![Box::new(filter)]);

    // Step 8: Build the reader with the row filter attached
    // Only rows matching the filter will be decoded and returned
    let mut reader = builder.with_row_filter(row_filter).build()?;

    // Step 9: Read and display the filtered results
    println!("Filtered results (id > 42):");
    let mut total_rows = 0usize;
    for batch in &mut reader {
        let batch = batch?;
        total_rows += batch.num_rows();

        // Display the actual filtered data
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
            println!("  id={}, name={}", ids.value(i), names.value(i));
        }
    }

    println!("\nTotal rows returned: {} (expected: 2)", total_rows);
    Ok(())
}
