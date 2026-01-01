# Aisle Benchmark Design

## Overview

Benchmarks measure the **overhead** of metadata evaluation vs. the **benefit** of I/O reduction. The goal is to demonstrate that metadata overhead is negligible compared to I/O savings.

## Benchmark Categories

### 1. Micro-benchmarks (Criterion)

Measure isolated component performance:

#### A. Compilation Benchmarks
**What:** DataFusion `Expr` → Pruning IR compilation time

**Scenarios:**
- Simple predicates: `col("x").eq(lit(42))`
- Complex AND chains: `col("a").gt(lit(1)).and(col("b").lt(lit(10))).and(...)`
- Deep OR trees: `col("x").eq(lit(1)).or(col("x").eq(lit(2))).or(...)`
- Mixed AND/OR: Realistic query patterns
- Unsupported predicates: Measure best-effort compilation overhead

**Variables:**
- Predicate complexity (1, 5, 10, 20 leaf nodes)
- Nesting depth (1-5 levels)

**Expected results:** <100μs for typical predicates

---

#### B. Row-Group Pruning Benchmarks
**What:** Evaluate predicate against row group statistics

**Scenarios by predicate type:**
- **Equality**: `col("x").eq(lit(value))`
- **Inequality**: `col("x").not_eq(lit(value))`
- **Comparisons**: `<`, `>`, `<=`, `>=`
- **Range**: `col("x").between(low, high)`
- **IN list**: `col("x").in_list([1, 2, ..., N])` (vary N: 10, 100, 1000)
- **IS NULL**: `col("x").is_null()`
- **LIKE prefix**: `col("name").like("prefix%")`
- **AND chains**: `col("a").gt(1).and(col("b").lt(10))`
- **OR branches**: `col("x").eq(1).or(col("x").eq(2))`
- **NOT inversion**: `col("x").gt(50).not()`

**Scenarios by data type:**
- Integers: Int32, Int64, UInt32, UInt64
- Floats: Float32, Float64
- Strings: Utf8, LargeUtf8, Utf8View
- Binary: Binary, FixedSizeBinary
- Byte array ordering: exact vs truncated stats

**Variables:**
- Number of row groups: 10, 100, 1000, 10000
- Selectivity: 1%, 10%, 50%, 90% (how many row groups match)
- Statistics quality: tight bounds vs wide bounds

**Expected results:** <10μs per row group for simple predicates, <1ms for 1000 row groups

---

#### C. Page-Level Pruning Benchmarks
**What:** Evaluate predicate against page index statistics

**Scenarios:**
- Same predicate types as row-group benchmarks
- AND (best-effort): Some columns have page index, some don't
- OR (all-or-nothing): All columns must have page index
- NOT (exact only): Only exact page selections inverted

**Variables:**
- Number of pages per row group: 10, 100, 1000
- Total row groups: 10, 100
- Page index availability: all columns, some columns, no columns

**Expected results:** <100μs per row group for typical page counts

---

#### D. Bloom Filter Benchmarks
**What:** Bloom filter check overhead

**Scenarios:**
- Single equality check: `col("x").eq(lit(value))`
- IN list with bloom: `col("x").in_list([...])` with varying list sizes
- AND with multiple bloom filters
- Bloom filter hit vs miss (value present vs absent)

**Variables:**
- Number of row groups: 10, 100, 1000
- IN list size: 10, 100, 1000
- Bloom filter false positive rate: 0.01, 0.05, 0.1

**Expected results:** <50μs per bloom filter check

---

#### E. Selection Format Conversion
**What:** RowSelection ↔ RoaringBitmap conversion overhead

**Scenarios:**
- Sparse selection: 1% of rows selected
- Dense selection: 90% of rows selected
- Fragmented selection: Many small ranges
- Large selection: Near u32::MAX rows

**Variables:**
- Total rows: 1K, 100K, 10M, u32::MAX
- Selection density: 1%, 10%, 50%, 90%

**Expected results:** <1ms for typical conversions

---

### 2. Integration Benchmarks (Criterion + Custom Metrics)

Measure end-to-end performance with real Parquet files:

#### A. Real-World Query Patterns

**Setup:**
- Generate realistic Parquet files (1GB, 10GB, 100GB simulated)
- Various data distributions (uniform, skewed, sorted)
- Different row group sizes (64MB, 128MB, 256MB)

**Query patterns:**
- **Point query**: `id = 12345` (high selectivity, bloom filter benefit)
- **Range query**: `date BETWEEN '2024-01-01' AND '2024-01-07'`
- **Multi-column filter**: `user_id > 1000 AND age < 30`
- **High-cardinality IN**: `sku IN (...)` with 1000 values
- **String prefix**: `email LIKE 'admin%'`

**Metrics:**
- Metadata evaluation time
- Row groups pruned (count and percentage)
- Estimated I/O reduction (bytes)
- Actual I/O reduction (measure bytes read)

**Expected results:** 70-99% I/O reduction with <1ms overhead for metadata

---

#### B. Scalability Benchmarks

**What:** Performance scaling with file size

**Variables:**
- Row groups: 10, 100, 1000, 10000
- Columns: 10, 50, 100
- Predicate selectivity: 0.1%, 1%, 10%

**Metrics:**
- Pruning time vs row group count (should be linear)
- Memory usage vs metadata size

**Expected results:** O(n) scaling with row group count, <10ms for 10K row groups

---

### 3. Comparison Benchmarks

Compare Aisle against baselines:

#### A. No Pruning Baseline
- Read entire file without pruning
- Measure total I/O and time

#### B. Arrow Parquet Reader Default
- Use arrow-rs reader without Aisle
- Compare I/O and time

#### C. DataFusion Pruning
- If applicable, compare against DataFusion's built-in pruning
- Show that Aisle provides similar benefits for standalone usage

---

## Benchmark Implementation Plan

### Phase 1: Micro-benchmarks (Week 1)
- [ ] Set up Criterion framework
- [ ] Compilation benchmarks
- [ ] Row-group pruning benchmarks (basic predicates)
- [ ] Basic reporting

### Phase 2: Extended Micro-benchmarks (Week 2)
- [ ] Page-level pruning benchmarks
- [ ] Bloom filter benchmarks
- [ ] Selection format conversion
- [ ] All data types coverage

### Phase 3: Integration Benchmarks (Week 3)
- [ ] Real Parquet file generation
- [ ] End-to-end query patterns
- [ ] I/O measurement infrastructure
- [ ] Scalability tests

### Phase 4: Analysis & Optimization (Week 4)
- [ ] Identify bottlenecks
- [ ] Optimize hot paths
- [ ] Re-run benchmarks
- [ ] Document findings

---

## Benchmark Organization

```
benches/
├── criterion/               # Criterion micro-benchmarks
│   ├── compilation.rs       # IR compilation benchmarks
│   ├── row_group_pruning.rs # Row-group statistics evaluation
│   ├── page_pruning.rs      # Page-level pruning
│   ├── bloom_filter.rs      # Bloom filter checks
│   ├── selection.rs         # RowSelection ↔ Roaring conversion
│   └── data_types.rs        # Type-specific benchmarks
├── integration/             # End-to-end benchmarks
│   ├── real_world.rs        # Realistic query patterns
│   ├── scalability.rs       # Scaling tests
│   └── comparison.rs        # Baseline comparisons
└── helpers/                 # Shared utilities
    ├── parquet_gen.rs       # Test file generation
    ├── metrics.rs           # Custom metrics
    └── mod.rs

Cargo.toml:
[dev-dependencies]
criterion = { version = "0.5", features = ["html_reports"] }

[[bench]]
name = "compilation"
harness = false

[[bench]]
name = "row_group_pruning"
harness = false
# ... etc
```

---

## Key Metrics to Track

### Performance Metrics
1. **Overhead**: Metadata evaluation time (μs/ms)
2. **Throughput**: Row groups evaluated per second
3. **Latency**: P50, P95, P99 for pruning operations
4. **Scalability**: Time vs row group count (confirm O(n))

### Effectiveness Metrics
1. **Pruning rate**: % of row groups skipped
2. **I/O reduction**: % of bytes not read
3. **False positive rate**: Row groups kept but don't match
4. **Predicate coverage**: % of predicate successfully compiled

### Regression Detection
1. Track all metrics across commits
2. Alert on >10% performance regression
3. Celebrate >10% improvement

---

## Expected Benchmark Results (Targets)

| Operation | Target | Notes |
|-----------|--------|-------|
| Simple predicate compilation | <50μs | `col("x").eq(lit(42))` |
| Complex predicate compilation | <200μs | 10+ leaf nodes with AND/OR |
| Row-group eval (1 RG) | <10μs | Simple equality predicate |
| Row-group eval (1000 RG) | <1ms | Linear scaling |
| Page-level eval (100 pages) | <100μs | Per row group |
| Bloom filter check | <50μs | Single check |
| RowSelection → Roaring | <1ms | 10M rows |
| End-to-end pruning | <10ms | 1000 row groups, complex predicate |

**I/O Reduction Targets:**
- Point queries: 90-99% (with bloom filters)
- Range queries: 50-90% (depends on distribution)
- Multi-column: 40-80% (depends on selectivity)

---

## Benchmark Reporting

### Automated Reports
- Criterion HTML reports (graphs, statistical analysis)
- JSON export for CI/CD tracking
- Markdown summary for release notes

### Dashboard (Future)
- Track metrics over time
- Compare branches
- Identify performance trends

---

## Notes

- All benchmarks should use realistic data distributions (not just sequential integers)
- Include both "happy path" (good pruning) and "worst case" (no pruning) scenarios
- Measure memory usage alongside time
- Consider async overhead for bloom filter benchmarks
- Document hardware specs with benchmark results
