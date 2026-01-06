# Aisle
<img width="1138" height="177" alt="aisle banner (1)" src="https://github.com/user-attachments/assets/f5f873b3-018e-4735-8166-0b945b61dfbd" />

<p align="left">
  <a href="https://crates.io/crates/aisle/"><img src="https://img.shields.io/crates/v/aisle.svg"></a>
  <a href="https://docs.rs/aisle"><img src="https://img.shields.io/docsrs/aisle"></a>
  <a href="https://github.com/your-org/aisle/blob/main/LICENSE"><img src="https://img.shields.io/crates/l/aisle"></a>
</p>

Metadata-driven Parquet pruning for Rust: Skip irrelevant data before reading

Aisle evaluates pruning predicates against Parquet metadata (row-group statistics, page indexes, bloom filters) to determine which data to skip, dramatically reducing I/O for selective queries without modifying the upstream `parquet` crate.

📖 [Read the full documentation on docs.rs](https://docs.rs/aisle)

## Why Aisle?

Parquet readers typically apply filters *after* reading data, wasting I/O on irrelevant row groups and pages.

Aisle evaluates your predicates against metadata *before* reading:
- Row-group pruning: using min/max statistics
- Page-level pruning: using column/offset indexes
- Bloom filter checks: for definite absence (high-cardinality columns)

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

Good fit:
- Selective queries (reading <20% of data)
- Large Parquet files (>100MB, multiple row groups)
- Remote storage (S3, GCS) where I/O is expensive
- High-cardinality point queries (user IDs, transaction IDs)
- Time-series with range queries

Not needed:
- Full table scans (no pruning benefit)
- Small files (<10MB, single row group)
- Already using a query engine with built-in pruning

Tips: Combine Aisle with proper Parquet configuration:
- Sort data by frequently-filtered columns
- Use reasonable row group sizes (64-256MB)
- Enable bloom filters for high-cardinality columns
- Write page indexes (Parquet 1.12+)

## Key Features

- Row-group pruning: Skip entire row groups using min/max statistics
- Page-level pruning: Skip individual pages within row groups
- Bloom filter support: Definite absence checks for point queries (`=`, `IN`)
- Aisle expressions: Build metadata-safe predicates with `Expr::...` (optional DataFusion compilation via `with_df_predicate` + `datafusion` feature)
- Conservative evaluation: Never skips data that might match (safety first)
- Async-first API: Optimized for remote storage (S3, GCS, Azure)
- Non-invasive: Works with upstream `parquet` crate, no format changes
- Best-effort compilation: Uses supported predicates even if some fail

## How It Works

```
┌─────────────────────────────────────────────────────┐
│                  Your Predicate                     │
│   WHERE user_id >= 1000 AND age < 30                │
└────────────┬────────────────────────────────────────┘
             │
             │  Two entry points:
             │
             ├─────────────┐
             │             │
             ▼             ▼
┌────────────────────┐  ┌──────────────────────────────┐
│  Aisle Native Expr │  │  DataFusion Expr (optional)  │
│  (direct usage)    │  │  (with "datafusion" feature) │
└────────┬───────────┘  └────────┬─────────────────────┘
         │                       │
         │                       ▼
         │              ┌───────────────────┐
         │              │  Aisle Compiler   │
         │              │  DF Expr -> Aisle │
         │              └────────┬──────────┘
         │                       │
         └───────────┬───────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────┐
│          Metadata Evaluation                        │
│  • Row-group statistics (min/max, null_count)       │
│  • Page indexes (page-level min/max)                │
│  • Bloom filters (definite absence checks)          │
│  • Tri-state logic (True/False/Unknown)             │
│                                                     │
│  Supports: =, !=, <, >, <=, >=, BETWEEN, IN,        │
│            IS NULL, LIKE 'prefix%', AND, OR, NOT    │
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

Current supported leaf types for statistics-based pruning:
- Integers: Int8/16/32/64, UInt8/16/32/64
- Floats: Float32/Float64
- Boolean
- Strings: Utf8, LargeUtf8, Utf8View
- Binary: Binary, LargeBinary, BinaryView, FixedSizeBinary

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

- Type coverage is partial: Only the leaf types listed above are supported for stats-based pruning; temporal/logical types and decimals are currently conservative.

- Byte array ordering requires column metadata: For ordering predicates (`<`, `>`, `<=`, `>=`) on Binary/Utf8 columns:
  - Default (conservative): Requires `TYPE_DEFINED_ORDER(UNSIGNED)` column order AND exact (non-truncated) min/max statistics
  - Opt-in (aggressive): Use `.allow_truncated_byte_array_ordering(true)` to allow truncated statistics, but be aware this may cause false negatives if truncation changes ordering semantics
  - Equality predicates (`=`, `!=`, `IN`) always work regardless of truncation

- No non-trivial column casts: Only no-op column casts are allowed; literal casts happen at compile time.

- Page-level NOT is conservative: NOT is only inverted when the inner page selection is exact; otherwise it falls back to row-group evaluation.

- OR requires full support: If any OR branch is unsupported at page level, page pruning is disabled for the whole OR.

- LIKE support is limited: Only prefix patterns (`'prefix%'`) are pushed down.

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

Benchmark results from our test suite (100 row groups, 100K rows total):

### Metadata Evaluation Overhead

Extremely low overhead - metadata evaluation completes in microseconds:

| Operation | Time | Notes |
|-----------|------|-------|
| Row-group pruning (stats only) | ~38 µs | Evaluating 100 row groups |
| With bloom filters | ~10 ms | Includes async I/O for bloom filter loading |

Metadata overhead is negligible compared to I/O costs, especially for S3 storage (50-100ms per file read).

### I/O Reduction

Real-world effectiveness on point queries (`id = value` on high-cardinality column):

| Scenario | Row Groups Kept | I/O Reduction | Speedup | Notes |
|----------|-----------------|---------------|---------|-------|
| Stats only | 25 / 100 | 75% | Moderate | When stats overlap |
| Bloom filters | 1 / 100 | 96% | ~750x | Definite absence checks |

Bloom filter impact: When statistics can't prune (overlapping ranges), bloom filters achieve dramatic reductions for point queries.

### Query Performance Comparison

Point query performance (DataFusion baseline, 100 row groups):

| Configuration | Time | Row Groups Matched | Relative |
|---------------|------|-------------------|----------|
| Row-group stats only | 4.9 ms | 80 / 100 | Baseline |
| + Page index | 3.6 ms | 80 / 100 (with page pruning) | 1.4x faster |
| + Bloom filter | 8.1 ms | 5 / 100 | More I/O reduction but bloom overhead |
| + Both | 8.1 ms | 5 / 100 (with page pruning) | Best selectivity |

Trade-off: Bloom filters add I/O overhead but dramatically reduce row groups scanned. Best for:
- High-cardinality point queries
- Remote storage (S3) where avoiding file reads matters most
- Workloads where pruning > bloom I/O cost

### Performance Factors

Data layout (most important):
- Sorted data: Best pruning (non-overlapping ranges)
- Random data: Moderate pruning (overlapping ranges, bloom filters help)
- Clustered data: Good pruning within clusters

Query selectivity:
- Point queries (`=`, `IN`): Excellent with bloom filters (90-99% reduction)
- Range queries (`BETWEEN`, `<`, `>`): Good with sorted data (50-90% reduction)
- Multi-column AND: Good (intersect pruned sets)
- Multi-column OR: Conservative (union pruned sets)

Metadata availability:
- Row-group stats: Always available
- Page indexes: Parquet 1.12+ (enables page-level pruning)
- Bloom filters: Optional, must be written explicitly

### S3/Object Storage Impact

For S3-based storage where each file read costs 50-100ms:

Without pruning (1000 files):
- Total latency: 50-100 seconds
- Full data transfer cost

With Aisle (99% pruning):
- Metadata evaluation: ~1-5 ms
- Read 10 files: ~1 second
- 50-100x total speedup
- 99% cost reduction

This makes Aisle critical for S3-based LSM engines where metadata pruning isn't optional—it's survival.

### Run Benchmarks

```bash
# Full benchmark suite
cd benches/df_compare
cargo bench

# Specific benchmarks
cargo bench --bench metadata_eval
cargo bench --bench point_query
cargo bench --bench bloom_filter
```

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
