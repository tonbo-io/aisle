# Aisle: Metadata-Driven Parquet Pruning

## What is this project?

Aisle is a Rust library that provides **logical-level metadata pruning** for Parquet files. It accepts logical expressions (DataFusion `Expr` or native `aisle::Expr`), evaluates them against Parquet metadata (row-group stats, page indexes, bloom filters), and returns row-group and row-level selections to guide the Parquet reader.

**Positioning**: Aisle operates at the **storage layer** (logical expressions → metadata pruning), while DataFusion's `datafusion-pruning` operates at the **execution layer** (physical expressions → metadata pruning). They're complementary, not competing.

## Why do we need it?

### The Ecosystem Gap

**DataFusion pruning exists** (`datafusion-pruning` v51.0+), but only at the physical layer:
- Requires `PhysicalExpr` (needs full DataFusion physical planning)
- Dependencies: 569 crates (includes physical-expr, physical-plan, execution)
- Binary size: ~15-20 MB
- Use case: For DataFusion users with physical expressions

**Aisle fills the logical-layer gap**:
- Accepts logical `Expr` (no physical planning required)
- Dependencies: 295 crates (parquet, arrow, datafusion-common only)
- Binary size: ~0.8 MB
- Use case: For storage engines without execution machinery

### Critical for S3-Based Storage Engines

**Local disk** (RocksDB, LevelDB):
- Read latency: ~100 µs
- Sequential reads are cheap
- Metadata pruning is a **nice optimization**

**S3/Object storage** (Tonbo, cloud-native LSM):
- Read latency: 50-100 ms (500-1000x slower!)
- Each HTTP request has overhead
- Metadata pruning is **critical for viability**

Example: Query across 1000 SST files on S3
- Without pruning: 1000 × 100ms = 100 seconds
- With Aisle (99% pruning): ~1 second metadata + 10 files × 100ms = ~2 seconds
- **50x speedup** by avoiding unnecessary S3 reads

**Result**: Enables metadata-driven I/O reduction for storage engines that can't use DataFusion's execution layer

## How does it work?

High-level implementation flow:

1. **Input**: DataFusion `Expr` + Arrow schema
2. **Normalize**: Simplify, split conjunctions, and canonicalize predicates
3. **Compile**: Translate supported parts into a pruning IR (small, conservative subset)
4. **Evaluate**: Use Parquet metadata to decide:
   - `row_groups: Vec<usize>` to keep
   - `RowSelection` or bitmap for page-level pruning (if page indexes exist)
5. **Apply**: Pass `.with_row_groups(...)` and `.with_row_selection(...)` to Parquet readers

## Architecture

### Design Philosophy

1. **Conservative evaluation**: Never skip data that might match (safety first)
2. **Best-effort compilation**: Use supported predicates even if some parts fail
3. **Non-invasive**: Works with upstream `parquet` crate, no format changes
4. **Async-first**: Optimized for remote storage (S3, GCS, Azure)

### Data Flow

```
DataFusion Expr + Arrow Schema
    ↓
Normalize & Compile -> Pruning IR
    ↓
Evaluate against Parquet metadata
    ↓
PruneResult { row_groups, row_selection, roaring_bitmap }
    ↓
Apply to ParquetRecordBatchReader
```

### Module Structure

- **`compile`**: DataFusion Expr -> Pruning IR compilation
- **`expr`**: Internal representation for metadata-evaluable predicates
- **`prune`**: Core pruning logic (row-group + page-level)
  - `request`: Builder API (`PruneRequest`)
  - `result`: Pruning results
  - `options`: Configuration
  - `api`: Internal pruning functions
  - `eval`: Expression evaluation (dispatch)
  - `cmp`, `between`, `in_list`, etc.: Predicate evaluators
  - `provider`: Async bloom filter trait
- **`selection`**: RowSelection ↔ RoaringBitmap conversion
- **`pruner`**: Stateful pruner for reuse across multiple predicates
- **`error`**: Compilation errors

## API Design

### Unified Builder Pattern

**Sync API** (no bloom filters):
```rust
let result = PruneRequest::new(&metadata, &schema)
    .with_df_predicate(&predicate)
    .enable_page_index(true)
    .prune();
```

**Async API** (with bloom filters):
```rust
let metadata = builder.metadata().clone();
let schema = builder.schema().clone();
let result = PruneRequest::new(&metadata, &schema)
    .with_df_predicate(&predicate)
    .enable_bloom_filter(true)
    .prune_async(&mut builder).await;
```

### Key Types

- **`PruneRequest`**: Builder for one-shot pruning operations
- **`PruneResult`**: Contains `row_groups`, `row_selection`, `roaring_bitmap`, `compile_result`
- **`PruneOptions`**: Configuration (page_index, bloom_filter, emit_roaring)
- **`Pruner`**: Stateful pruner for reusing compiled metadata across predicates
- **`AsyncBloomFilterProvider`**: Trait for custom bloom filter loading

## Supported Features

### Predicates

- **Comparisons**: `=`, `!=`, `<`, `<=`, `>`, `>=`
- **Range**: `BETWEEN`
- **Set membership**: `IN`
- **Null checks**: `IS NULL`, `IS NOT NULL`
- **String prefix**: `LIKE 'prefix%'`
- **Logical**: `AND`, `OR`, `NOT`
- **Type casting**: `CAST` (no-op column casts, compile-time literal casts)

### Data Types

**Supported** (statistics-based pruning):
- Integers: Int8/16/32/64, UInt8/16/32/64
- Floats: Float32/Float64
- Boolean
- Strings: Utf8, LargeUtf8, Utf8View
- Binary: Binary, LargeBinary, BinaryView, FixedSizeBinary

**Not yet supported** (conservative - keeps all data):
- Temporal: Date32/Date64, Timestamp
- Decimals: Decimal128/Decimal256
- Complex types: Duration, Interval, etc.

### Metadata Sources

- **Row-group statistics**: Always available (min/max, null_count)
- **Page indexes**: Optional, enables page-level pruning
- **Bloom filters**: Optional, definite absence checks for `=` and `IN`

## Implementation Details

### Tri-State Logic

Evaluation uses three-valued logic:
- **True**: Definitely matches (keep data)
- **False**: Definitely doesn't match (prune data)
- **Unknown**: Might match (keep data, conservative)

### Page-Level Pruning Rules

- **AND**: Best-effort - skip unsupported parts, intersect supported ones
- **OR**: All-or-nothing - return None if any part unsupported
- **NOT**: Only invert when inner selection is exact (no unknown pages)

### Type Casting

**Column casts**: Only no-op casts allowed (same type)
- Rationale: Non-trivial casts would require transforming statistics

**Literal casts**: Performed at compile time
- Rationale: Safe to cast literals upfront without affecting statistics

### Selection Formats

**RowSelection**: Parquet-native format (ranges of skip/select)
**RoaringBitmap**: Compact bitmap (limited to u32::MAX rows)

## Testing Strategy

**Coverage**: 117 tests across multiple suites

**Test Organization**:
- `src/*/tests.rs`: Unit tests for each module
- `tests/async_bloom*.rs`: Async bloom filter integration
- `tests/best_effort_pruning.rs`: NOT pushdown edge cases
- `tests/cast_support.rs`: Type casting coverage
- `tests/null_count_edge_cases.rs`: Null handling
- `tests/prune_*.rs`: Integration tests
- `tests/large_dataset_test.rs`: u32::MAX boundary handling

**Doc Tests**: 24 passing (all public APIs documented)

## Development Guidelines

### Adding New Predicates

1. Add IR variant in `src/expr.rs`
2. Add compilation in `src/compile.rs`
3. Add row-group evaluator in `src/prune/<predicate>.rs`
4. Add page-level evaluator in same file
5. Wire up in `src/prune/eval.rs`
6. Add tests in `tests/`

### Adding New Data Types

1. Update statistics extraction in `src/prune/stats.rs`
2. Add comparison logic in `src/prune/cmp.rs`
3. Add tests covering all predicate types

### Performance Characteristics

**Metadata evaluation overhead**: Very low (~38 µs for 100 row groups)

**Compared to DataFusion** (apples-to-apples, same features):

| Configuration | DataFusion | Aisle | Difference |
|---------------|-----------|-------|------------|
| Page index only | 3.6 ms | 18.2 ms | 5x slower |
| Bloom filter only | 8.1 ms | 13.7 ms | 1.7x slower |
| Both (page + bloom) | 8.1 ms | 13.0 ms | 1.6x slower |

**Optimization opportunities**:
- Page-level pruning: Avoid full-page decoding when indexes enable pruning
- Bloom filters: Batch loading when possible (see `AsyncBloomFilterProvider::bloom_filters_batch`)
- Reuse `Pruner` across queries to amortize compilation cost

## Known Limitations

1. **Type coverage is partial**: Only leaf types listed above; temporal/logical types conservative
2. **No non-trivial column casts**: Only no-op casts allowed
3. **Page-level NOT is conservative**: Only exact selections inverted
4. **OR requires full support**: Any unsupported branch disables page pruning
5. **LIKE support is limited**: Only prefix patterns (`'prefix%'`)

## Trade-offs & Design Decisions

### Conservative vs. Aggressive Pruning

**Decision**: Conservative (never skip potential matches)
**Rationale**: Correctness over performance; users trust results

### Logical Layer vs Physical Layer

**Decision**: Accept logical expressions (`Expr`), not physical (`PhysicalExpr`)
**Rationale**:
- Storage engines don't have physical planning machinery
- DataFusion already provides physical-layer pruning (`datafusion-pruning`)
- Aisle fills the gap for logical-layer pruning (storage layer)
- Different layers, different tools—complementary not competing

### Non-Invasive Design

**Decision**: External library, not fork of `parquet` crate
**Rationale**: Lower maintenance burden, easier adoption

### Best-Effort Compilation

**Decision**: Use supported predicates even if some fail
**Rationale**: Partial pruning better than none; unsupported parts evaluated at read time

### Async API Design

**Decision**: Separate `prune()` and `prune_async()` methods
**Rationale**: Bloom filters require async I/O; separate methods clearer than type-level async

## References

- [Architecture doc](docs/architecture.md): Detailed IR compilation and evaluation
- [Development plan](docs/development_plan.md): Implementation checklist and status
- [README](README.md): User-facing documentation
- [Parquet format spec](https://parquet.apache.org/docs/): Metadata structure
- [DataFusion expressions](https://docs.rs/datafusion-expr): Input predicate format

## Future Enhancements (Post-v0.2.0)

- Temporal type support (Date, Timestamp)
- Decimal type support
- Dictionary encoding hints
- Projection pushdown
- Benchmarking suite
- Performance profiling
