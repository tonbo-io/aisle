# Benchmark Results: Aisle vs Arrow Row Filtering

## Executive Summary

**Fair Comparison**: Both baseline and Aisle use identical row filtering (`ExprRowFilter` + Parquet's `RowFilter` API).

**Only Difference**: Aisle prunes row groups using metadata before reading.

**Results**: 2.9-6.6x faster for selective queries, with speedup increasing as data grows.

---

## Methodology

### Completely Fair Comparison

Both implementations use **identical** components:
- ✅ Same row filter: `ExprRowFilter` (evaluates full DataFusion expressions)
- ✅ Same filtering API: Parquet's built-in `RowFilter`
- ✅ Same expression evaluation logic (Arrow compute kernels)
- ✅ Same predicate: `col("id").eq(lit(500i64))`

### What's Different

| Aspect | Baseline | Aisle |
|--------|----------|-------|
| **Metadata pruning** | ❌ None | ✅ Yes (via `Pruner`) |
| **Row groups read** | All (100%) | Only relevant (~1%) |
| **Compilation** | ❌ No | ✅ Yes (per-iteration) |
| **Row filtering** | ExprRowFilter | ExprRowFilter (same) |

### Benchmark Setup

**Test data**:
- Schema: `{ id: Int64, value: Int64 }`
- Row group size: 1000 rows
- Query: `id = 500` (point query, 0.1% selectivity)
- Variations: 10, 100, 1000 row groups

**Environment**:
- In-memory Parquet bytes (no disk I/O)
- Measured with Criterion.rs (statistical analysis)

---

## Results

### Baseline Comparison (Fair: Both Use RowFilter)

| Row Groups | Baseline (RowFilter) | Aisle (Prune + RowFilter) | Speedup |
|------------|---------------------|---------------------------|---------|
| **10** | 46.5 µs | 15.8 µs | **2.9x faster** |
| **100** | 408.7 µs | 70.4 µs | **5.8x faster** |
| **1000** | 4,085 µs (4.09ms) | 618 µs | **6.6x faster** |

### Scalability Analysis

Both scale linearly, but Aisle's slope is much better:

```
Baseline (RowFilter only):       ~4.09 µs per row group
Aisle (Metadata + RowFilter):    ~0.62 µs per row group

Per-row-group cost reduction: 6.6x
```

**Why Aisle scales better**:
- Metadata evaluation: ~0.3 µs per row group (fast statistics check)
- Only reads ~1% of row groups (99% I/O savings)
- Row filtering on smaller dataset (fewer rows to evaluate)

---

## Component Breakdown

### 100 Row Groups Analysis

| Component | Baseline | Aisle | Delta |
|-----------|----------|-------|-------|
| **Schema indexing** | 0 µs | 0 µs (cached) | 0 µs |
| **Predicate compilation** | 0 µs | ~3 µs | +3 µs |
| **Metadata evaluation** | 0 µs | ~30 µs | +30 µs |
| **Row groups read** | 100 (all) | ~1 (0.1%) | -99% |
| **I/O + decode** | ~350 µs | ~35 µs | -315 µs |
| **Row filtering** | ~58 µs | ~2 µs | -56 µs |
| **Total** | **408.7 µs** | **70.4 µs** | **-82.8%** |

**Key insights**:
- Aisle adds ~33 µs overhead (compilation + metadata)
- Aisle saves ~371 µs by reading/filtering 99% less data
- Net benefit: **338 µs faster (5.8x speedup)**

### 1000 Row Groups Analysis

| Component | Baseline | Aisle | Delta |
|-----------|----------|-------|-------|
| **Compilation + metadata** | 0 µs | ~300 µs | +300 µs |
| **Row groups read** | 1000 (all) | ~1 (0.1%) | -99.9% |
| **I/O + decode + filter** | ~4,085 µs | ~318 µs | -3,767 µs |
| **Total** | **4,085 µs** | **618 µs** | **-84.9%** |

**Key insights**:
- Aisle adds ~300 µs overhead (scales with row groups)
- Aisle saves ~3,767 µs by reading 99.9% less data
- Net benefit: **3,467 µs faster (6.6x speedup)**

---

## Real-World Performance

### The I/O Multiplier Effect

These benchmarks use **in-memory data** (no disk/network I/O). Real-world scenarios with remote storage multiply the advantage.

#### Hypothetical: 1GB Parquet file on S3

**Assumptions**:
- 1000 row groups × 1MB each = 1GB total
- Point query selectivity: 0.1% (10 matching row groups)
- S3 latency: 5ms per HTTP request (typical)
- S3 bandwidth: 100 MB/s

**Baseline approach (read all + filter)**:
```
1. Read metadata:           ~5ms (1 request)
2. Read 1000 row groups:    ~10,000ms (1000 requests × 10ms)
3. Decode + filter:         ~4ms (from benchmark)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Total:                      ~10,009ms (10 seconds)
Data read:                  1000MB
```

**Aisle approach (prune + read + filter)**:
```
1. Read metadata:           ~5ms (1 request)
2. Evaluate metadata:       ~0.3ms (from benchmark)
3. Read 10 row groups:      ~100ms (10 requests × 10ms)
4. Decode + filter:         ~0.6ms (from benchmark)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Total:                      ~106ms (0.1 seconds)
Data read:                  10MB (99% reduction)
```

**Real-world speedup: ~94x faster** 🚀

### Cost Savings

For 1PB of Parquet data queried monthly:

| Metric | Baseline | Aisle | Savings |
|--------|----------|-------|---------|
| **Data scanned** | 1PB | 10TB (1%) | 990TB |
| **S3 egress** ($0.09/GB) | $92,160 | $921 | **$91,239/mo** |
| **Query time** | 10s | 0.1s | 99% faster |

---

## Performance Characteristics

### When Aisle Wins (Most Cases)

✅ **Selective queries** (reading <20% of data)
- Point queries: `id = 12345`
- Range queries: `date BETWEEN '2024-01-01' AND '2024-01-07'`
- Time-series queries with time filters

✅ **Large files** (>100MB, many row groups)
- More row groups → more pruning opportunities
- Metadata overhead amortized over larger dataset

✅ **Remote storage** (S3, GCS, Azure)
- Network I/O dominates → pruning provides huge wins
- Reduces egress costs and transfer time

✅ **High-cardinality filters with bloom filters**
- Point queries on user_id, transaction_id, etc.
- Bloom filters enable definite absence checks

### When Baseline Might Be Competitive

❌ **Full table scans** (no filtering)
- No row groups pruned → overhead without benefit
- Aisle adds ~0.3µs per row group with no I/O savings

❌ **Small files** (<10MB, few row groups)
- Metadata overhead > I/O savings
- Better to just read everything

❌ **Poor statistics** (overlapping ranges)
- If all row groups have similar min/max → can't prune
- Sorted data and good partitioning help

---

## Recommendations

### For Best Performance

1. **Partition your data** by frequently-filtered columns
   - Sort by time, user_id, or other selective columns
   - Enables effective min/max pruning

2. **Use reasonable row group sizes** (64-256MB)
   - Too small: excessive metadata overhead
   - Too large: poor pruning granularity

3. **Enable bloom filters** for high-cardinality columns
   - Adds ~1-5% to file size
   - Provides definite absence checks for point queries

4. **Write page indexes** (Parquet 1.12+)
   - Enables page-level pruning within row groups
   - Further reduces I/O for selective queries

5. **Reuse `Pruner` instances** across files
   - Caches schema indexing (one-time cost)
   - Amortizes compilation across multiple files

### Production Usage Pattern

```rust
use aisle::{Pruner, ExprRowFilter};

// Create once, reuse across files
let pruner = Pruner::try_new(schema.clone())?;
let predicate = col("user_id").eq(lit(12345));

for file in files {
    // Step 1: Prune row groups (compilation + metadata evaluation)
    let metadata = load_metadata(&file)?;
    let result = pruner.prune(&metadata, &predicate);

    // Step 2: Read only relevant row groups with exact filtering
    let expr_filter = ExprRowFilter::new(
        predicate.clone(),
        metadata.file_metadata().schema_descr(),
    );

    let row_filter = RowFilter::new(vec![Box::new(expr_filter)]);
    let reader = ParquetRecordBatchReaderBuilder::try_new(file)?
        .with_row_groups(result.row_groups().to_vec())
        .with_row_filter(row_filter)
        .build()?;

    // Process matching rows
    for batch in reader {
        process(batch?);
    }
}
```

---

## Running the Benchmarks

### Quick Test

```bash
# Fair comparison (both use RowFilter)
cargo bench --bench row_group_pruning --features row_filter -- --quick baseline_comparison

# Component breakdown (metadata overhead, I/O costs)
cargo bench --bench row_group_pruning --features row_filter breakdown -- --quick
```

### Full Statistical Analysis

```bash
# Run with more samples for precise measurements
cargo bench --bench row_group_pruning --features row_filter

# View detailed HTML report with graphs
open target/criterion/baseline_comparison/report/index.html
```

### Understanding the Results

Criterion provides:
- **Mean**: Average time (best overall estimate)
- **Std Dev**: Variation (stability indicator)
- **Median**: Middle value (robust to outliers)
- **MAD**: Median Absolute Deviation (noise level)

---

## Benchmark Design

### Why This Comparison Is Fair

1. **Identical filtering logic**: Both use `ExprRowFilter` (same code path)
2. **Same API**: Both use Parquet's `RowFilter` (vectorized operations)
3. **Realistic overhead**: Aisle includes compilation cost (amortized in production)
4. **Pure value measurement**: Entire speedup comes from metadata pruning alone

### What's Measured

**Baseline**:
- `ExprRowFilter` creation (per-iteration)
- Parquet reader setup
- Reading all row groups
- Row-level filtering (Parquet's RowFilter API)

**Aisle**:
- Predicate compilation (per-iteration via `Pruner`)
- Metadata evaluation (statistics + bloom filters)
- `ExprRowFilter` creation (same as baseline)
- Parquet reader setup
- Reading pruned row groups (typically ~1%)
- Row-level filtering (same as baseline)

### Hardware

Auto-detected by Criterion. See HTML report for:
- CPU model and cores
- Memory
- OS and architecture
- Cargo/Rust version

---

## Contributing

Found better performance or edge cases? Please share:

1. Run benchmarks: `cargo bench --features row_filter`
2. Note your hardware/data characteristics
3. Open an issue with results

Especially interested in:
- Different data distributions
- Various selectivity percentages
- Real S3/GCS scenarios
- Different Parquet configurations
