# Aisle

<p align="left">
  <a href="https://crates.io/crates/aisle/"><img src="https://img.shields.io/crates/v/aisle.svg"></a>
  <a href="https://docs.rs/aisle"><img src="https://img.shields.io/docsrs/aisle"></a>
  <a href="https://github.com/your-org/aisle/blob/main/LICENSE"><img src="https://img.shields.io/crates/l/aisle"></a>
</p>

**Metadata-driven Parquet pruning for Rust: Skip irrelevant data before reading**

Aisle evaluates DataFusion predicates against Parquet metadata (row-group statistics, page indexes, bloom filters) to determine which data to skip, dramatically reducing I/O for selective queries without modifying the upstream `parquet` crate.

📖 **[Read the full documentation on docs.rs](https://docs.rs/aisle)**

## Why Aisle?

Parquet readers typically apply filters *after* reading data, wasting I/O on irrelevant row groups and pages.

Aisle evaluates your predicates against metadata *before* reading:
- **Row-group pruning** using min/max statistics
- **Page-level pruning** using column/offset indexes
- **Bloom filter checks** for definite absence (high-cardinality columns)

It effectively reduces I/O for selective queries with zero changes to the Parquet format.

## Quick Start

```rust
use aisle::{Expr, PruneRequest};
use datafusion_common::ScalarValue;
use parquet::file::metadata::ParquetMetaDataReader;
use parquet::arrow::ParquetRecordBatchReaderBuilder;

// 1. Load metadata (without reading data)
let metadata = ParquetMetaDataReader::new()
    .parse_and_finish(&parquet_bytes)?;

// 2. Define your filter using Aisle expressions
let predicate = Expr::and(vec![
    Expr::gt_eq("user_id", ScalarValue::Int64(Some(1000))),
    Expr::lt("age", ScalarValue::Int64(Some(30))),
]);

// 3. Prune row groups
let result = PruneRequest::new(&metadata, &schema)
    .with_predicate(&predicate)
    .enable_page_index(false)     // Row-group level only
    .enable_bloom_filter(false)   // No bloom filters
    .prune();

println!("Pruned {} of {} row groups ({}% I/O reduction)",
    metadata.num_row_groups() - result.row_groups().len(),
    metadata.num_row_groups(),
    ((metadata.num_row_groups() - result.row_groups().len()) * 100
        / metadata.num_row_groups())
);

// 4. Apply pruning to Parquet reader
let reader = ParquetRecordBatchReaderBuilder::try_new(parquet_bytes)?
    .with_row_groups(result.row_groups().to_vec())  // Skip irrelevant row groups!
    .build()?;

// Read only the relevant data
for batch in reader {
    // Process matching rows...
}
```

Add to your `Cargo.toml`:

```toml
[dependencies]
aisle = "0.2"
datafusion-common = "51"
parquet = "57"
arrow-schema = "57"
```

### Optional Features

**Row Filtering** (requires `arrow-arith`, `arrow-ord`, `arrow-select`, and `arrow-cast`):
```toml
[dependencies]
aisle = { version = "0.2", features = ["row_filter"] }
```

Enables `RowFilter` for exact row-level filtering using Parquet's built-in `RowFilter`. Most users only need metadata pruning (default), but this feature allows using the same IR expression for both metadata pruning and exact row filtering.

## When to Use Aisle

**Good fit:**
- Selective queries (reading <20% of data)
- Large Parquet files (>100MB, multiple row groups)
- Remote storage (S3, GCS) where I/O is expensive
- High-cardinality point queries (user IDs, transaction IDs)
- Time-series with range queries

**Not needed:**
- Full table scans (no pruning benefit)
- Small files (<10MB, single row group)
- Already using a query engine with built-in pruning

**Tips:** Combine Aisle with proper Parquet configuration:
- Sort data by frequently-filtered columns
- Use reasonable row group sizes (64-256MB)
- Enable bloom filters for high-cardinality columns
- Write page indexes (Parquet 1.12+)

## Key Features

- **Row-group pruning**: Skip entire row groups using min/max statistics
- **Page-level pruning**: Skip individual pages within row groups
- **Bloom filter support**: Definite absence checks for point queries (`=`, `IN`)
- **Aisle expressions**: Build metadata-safe predicates with `Expr::...` (optional DataFusion compilation via `with_df_predicate` + `datafusion` feature)
- **Conservative evaluation**: Never skips data that might match (safety first)
- **Async-first API**: Optimized for remote storage (S3, GCS, Azure)
- **Non-invasive**: Works with upstream `parquet` crate, no format changes
- **Best-effort compilation**: Uses supported predicates even if some fail

## How It Works

```
┌─────────────────────────────────────────────────────┐
│                  Your Query                         │
│   WHERE user_id >= 1000 AND age < 30                │
└─────────────────────┬───────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────┐
│              Aisle Compiler                         │
│   Converts DataFusion Expr -> Pruning IR            │
│   (supports =, !=, <, >, <=, >=, BETWEEN, IN,       │
│    IS NULL, LIKE 'prefix%', AND, OR, NOT, CAST)     │
└─────────────────────┬───────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────┐
│          Metadata Evaluation                        │
│  • Row-group statistics (min/max, null_count)       │
│  • Page indexes (page-level min/max)                │
│  • Bloom filters (definite absence checks)          │
│  • Tri-state logic (True/False/Unknown)             │
└─────────────────────┬───────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────┐
│              Pruning Result                         │
│   row_groups: [2, 5, 7]  <- Only these needed!      │
│   row_selection: Some(...) <- Page-level selection  │
│   compile_result: Unsupported predicates logged     │
└─────────────────────┬───────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────┐
│          Parquet Reader                             │
│   .with_row_groups([2, 5, 7])                       │
│   .with_row_selection(...)                          │
│   I/O reduced! ⚡                                    │
└─────────────────────────────────────────────────────┘
```

## What's Supported

### Predicates

| Type | Example | Row-Group | Page-Level | Bloom Filter |
|------|---------|-----------|------------|--------------|
| **Equality** | `col("x").eq(lit(42))` | ✓ | ✓ | ✓ |
| **Inequality** | `col("x").not_eq(lit(42))` | ✓ | ✓ | ✗ |
| **Comparisons** | `col("x").lt(lit(100))` | ✓ | ✓ | ✗ |
| **Range** | `col("x").between(lit(10), lit(20))` | ✓ | ✓ | ✗ |
| **Set membership** | `col("x").in_list(vec![lit(1), lit(2)])` | ✓ | ✓ | ✓ |
| **Null checks** | `col("x").is_null()` | ✓ | ✓ | ✗ |
| **String prefix** | `col("name").like(lit("prefix%"))` | ✓ | ✓ | ✗ |
| **Logical AND** | `col("x").gt(lit(10)).and(col("y").lt(lit(5)))` | ✓ | ✓ (best-effort) | ✓ |
| **Logical OR** | `col("x").eq(lit(1)).or(col("x").eq(lit(2)))` | ✓ | ✓ (all-or-nothing) | ✓ |
| **Logical NOT** | `col("x").gt(lit(50)).not()` | ✓ | ✓ (exact only) | ✗ |
| **Type casting** | `cast(col("x"), DataType::Int64).eq(lit(100))` | ✓ (no-op casts only) | ✓ | ✓ |

### Data Types

Current supported **leaf** types for statistics-based pruning:
- **Integers**: Int8/16/32/64, UInt8/16/32/64
- **Floats**: Float32/Float64
- **Boolean**
- **Strings**: Utf8, LargeUtf8, Utf8View
- **Binary**: Binary, LargeBinary, BinaryView, FixedSizeBinary

Not yet supported (treated conservatively as "unknown"):
- Temporal logical types (Date32/Date64, Timestamp)
- Decimals (Decimal128/Decimal256)
- Interval/Duration and other complex logical types

### Metadata Sources

| Source | Row-Group | Page-Level | Point Queries |
|--------|-----------|------------|---------------|
| **Statistics** (min/max) | ✓ Always | ✓ Via page index | Range queries |
| **Null count** | ✓ Always | ✓ Via page index | IS NULL checks |
| **Bloom filters** | ✓ Optional | ✗ Not applicable | `=` and `IN` |

## Known Limitations

- **Type coverage is partial**: Only the leaf types listed above are supported for stats-based pruning; temporal/logical types and decimals are currently conservative.

- **Byte array ordering requires column metadata**: For ordering predicates (`<`, `>`, `<=`, `>=`) on Binary/Utf8 columns:
  - **Default (conservative)**: Requires `TYPE_DEFINED_ORDER(UNSIGNED)` column order AND exact (non-truncated) min/max statistics
  - **Opt-in (aggressive)**: Use `.allow_truncated_byte_array_ordering(true)` to allow truncated statistics, but be aware this may cause false negatives if truncation changes ordering semantics
  - **Equality predicates** (`=`, `!=`, `IN`) always work regardless of truncation

- **No non-trivial column casts**: Only no-op column casts are allowed; literal casts happen at compile time.

- **Page-level NOT is conservative**: NOT is only inverted when the inner page selection is exact; otherwise it falls back to row-group evaluation.

- **OR requires full support**: If any OR branch is unsupported at page level, page pruning is disabled for the whole OR.

- **LIKE support is limited**: Only prefix patterns (`'prefix%'`) are pushed down.

## Usage Examples

**Page-level pruning:**
```rust
let result = PruneRequest::new(&metadata, &schema)
    .with_predicate(&predicate)
    .enable_page_index(true)
    .prune();
```

**Async with bloom filters:**
```rust
let metadata = builder.metadata().clone();
let schema = builder.schema().clone();
let result = PruneRequest::new(&metadata, &schema)
    .with_predicate(&predicate)
    .enable_bloom_filter(true)
    .prune_async(&mut builder).await;
```

**Custom bloom provider:**
```rust
impl AsyncBloomFilterProvider for MyProvider {
    async fn bloom_filter(&mut self, rg: usize, col: usize) -> Option<Sbbf> {
        // Your optimized loading logic
    }
}

let result = PruneRequest::new(&metadata, &schema)
    .with_predicate(&predicate)
    .prune_async(&mut my_provider).await;
```

**Byte array ordering (advanced):**
```rust
// Conservative (default): Requires exact min/max for ordering predicates
let result = PruneRequest::new(&metadata, &schema)
    .with_predicate(&Expr::gt("name", ScalarValue::Utf8(Some("prefix".to_string()))))
    .prune();

// Aggressive: Allow truncated byte array statistics (may have false negatives)
let result = PruneRequest::new(&metadata, &schema)
    .with_predicate(&Expr::gt("name", ScalarValue::Utf8(Some("prefix".to_string()))))
    .allow_truncated_byte_array_ordering(true)
    .prune();
```

## Performance

Reasonable expectations (actual results depend on file layout and metadata quality):

| Query Type | Expected I/O Reduction | Notes |
|------------|------------------------|-------|
| Point query (`id = 12345`) | High (often substantial) | Best with bloom filters + accurate stats |
| Range query (`date BETWEEN ...`) | Moderate to high | Depends on row-group size and data distribution |
| Multi-column filter | Moderate | AND helps; OR can reduce page-level pruning |
| High-cardinality IN (`sku IN (...)`) | Moderate to high | Bloom filters help when present |

These are **guidance only** until benchmarks land.

**Performance Factors:**
- **Row group size**: Larger row groups -> better statistics granularity
- **Predicate selectivity**: Lower selectivity -> more pruning opportunities
- **Column cardinality**: Bloom filters shine for high-cardinality columns
- **Page index availability**: Enables page-level pruning (Parquet 1.12+)

**Overhead:** Metadata evaluation is typically small relative to I/O, but varies with predicate complexity and metadata availability.

## Examples

Run the included examples to see end-to-end usage:

- **`basic_usage`**: Row-group pruning with metadata and predicates
- **`bloom_filter`**: Async API with bloom filter support
- **`byte_array_ordering`**: String/binary ordering awareness with exact/truncated statistics

```bash
# Row-group pruning example
cargo run --example basic_usage

# Async + bloom filters
cargo run --example bloom_filter

# Byte array ordering scenarios
cargo run --example byte_array_ordering
```

## Architecture

See detailed documentation:
- **[Architecture](docs/architecture.md)**: Internal design and IR compilation
- **[CLAUDE.md](CLAUDE.md)**: Project overview and design philosophy
- **[Development Plan](docs/development_plan.md)**: Implementation roadmap
- **[Deep Dive: DataFusion Parquet vs Aisle](docs/deep_dive_datafusion_parquet_vs_aisle.md)**: Design comparison and non-goals

## Testing

Aisle has comprehensive test coverage (111 tests):

```bash
# Run all tests
cargo test

# Run specific test suites
cargo test --test best_effort_pruning  # NOT pushdown edge cases
cargo test --test null_count_edge_cases  # Null handling
cargo test --test async_bloom  # Bloom filter integration
```

## License

Licensed under the MIT License. See [LICENSE](LICENSE) for details.

---

**Built with:** [arrow-rs](https://github.com/apache/arrow-rs) • [DataFusion](https://github.com/apache/datafusion) • [Parquet](https://parquet.apache.org/)
