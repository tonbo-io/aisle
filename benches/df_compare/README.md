# Aisle Benchmark

Comprehensive benchmarks comparing Aisle's metadata-driven pruning against DataFusion-Parquet's built-in pruning.

## TL;DR - Benchmark Focus

**Run context (2026-01-01):** local filesystem (`tempfile`), warm OS cache; end‑to‑end timings are local file reads (no remote I/O). DataFusion uses `target_partitions=1` for single‑thread parity.

**What these benchmarks cover:**
- **End‑to‑end point query:** ParquetExec (DataFusion) vs Aisle, with row‑group/page‑index/bloom options.
- **Metadata evaluation:** Aisle‑only (DataFusion pruning removed to avoid apples‑to‑oranges).
- **Bloom filter effectiveness:** How many row groups are eliminated and the metadata overhead.

## Benchmark Results

### Metadata Evaluation (`metadata_eval.rs`, Aisle-only)

**Dataset:** 100 row groups × 1,000 rows, predicate: `id = 500`

| Method | Time | Notes |
|--------|------|-------|
| Aisle row‑group only | **26.53 µs** | stats only |
| Aisle row‑group + page index | 27.09 µs | page‑level pruning enabled |
| Aisle row‑group + page index + bloom | 9.43 ms | includes bloom filter I/O |

**Key Insight:** Page index adds ~0.6 µs overhead at 100 row groups; bloom filters are dominated by load time in local‑fs runs.

### End-to-End Point Query (`compare.rs`)

**Dataset:** 100 row groups × 10,000 rows, predicate: `id = 51000`

**Note:** Numbers below are from the latest `compare.rs` Criterion run on this dataset. Aisle uses the async stream reader (same style as DataFusion ParquetExec) for the read path.

**End-to-End Performance:**

| Method | Time | Rows Read | I/O Reduction |
|--------|------|-----------|---------------|
| DataFusion (row-group only) | 4.56 ms | 800,000 | baseline |
| DataFusion (page index) | 3.40 ms | 163,840 | **79.5%** |
| DataFusion (bloom filter) | 11.69 ms | 50,000 | **93.8%** |
| DataFusion (page + bloom) | 9.27 ms | 10,240 | **98.7%** |
| Aisle (row-group only) | 4.75 ms | 800,000 | baseline |
| Aisle (page index) | 5.36 ms | 163,840 | **79.5%** |
| Aisle (bloom filter) | 9.45 ms | 50,000 | **93.8%** |
| Aisle (both) | 10.52 ms | 10,240 | **98.7%** |

**Rows Read / I/O Reduction:** Aisle values are from the same dataset using `point_query.rs` (verbose read stats). DataFusion rows read are estimated from ParquetExec metrics (row-group + page index + bloom).

**How They Work Together:**
- **Bloom filter**: Eliminates row groups (80 → 5 row groups, 93.8% I/O reduction)
- **Page index**: Refines within row groups (reads 20.5% of rows in kept groups)
- **Combined**: Bloom filter does coarse elimination, page index refines within those 5 row groups (50,000 → 10,240 rows)

**Why bloom filter paths appear slower?** This is a **local fs** benchmark with data already in memory. Bloom filter loading (≈8.8 ms) dominates, so wall‑clock time is slower despite far less I/O. Page-index‑only can still win locally because it reduces decode work. On remote storage (S3/GCS/Azure), the I/O reduction is often the primary win.

### Bloom Filter Effectiveness (`bloom_filter.rs`)

**Dataset:** 100 row groups with wide (but not identical) stats, alternating EVEN/ODD data

**Query for ODD value:**
- Stats only: 25 row groups (stats prune some, but still many)
- Bloom filter: 1 row group (definite absence eliminates the rest)
- **96% row group reduction** in this run

**Metadata overhead:** 8.8 ms (includes file I/O for loading bloom filters)

## When to Use Each

### Use DataFusion-Parquet When:
- ✅ You need mature DataFusion integration
- ✅ Low selectivity queries (> 50% of data matches)
- ✅ Row-group level pruning is sufficient
- ✅ Minimal metadata overhead is critical

### Use Aisle When:
- ✅ **Highly selective queries** (< 30% selectivity)
- ✅ **Point queries** (equality predicates like `id = X`)
- ✅ **Overlapping row group ranges**
- ✅ **Remote storage** (S3, GCS, Azure) where I/O is expensive
- ✅ **Cost optimization** - Reduce cloud data transfer charges
- ✅ **Metadata caching** - Multiple queries on same data

## Running the Benchmarks

```bash
# All benchmarks
cargo bench --manifest-path benches/df_compare/Cargo.toml

# Specific benchmarks
cargo bench --manifest-path benches/df_compare/Cargo.toml --bench compare
cargo bench --manifest-path benches/df_compare/Cargo.toml --bench metadata_eval
cargo bench --manifest-path benches/df_compare/Cargo.toml --bench point_query
cargo bench --manifest-path benches/df_compare/Cargo.toml --bench bloom_filter
cargo bench --manifest-path benches/df_compare/Cargo.toml --bench projection

# View HTML reports
open benches/df_compare/target/criterion/report/index.html
```

## Benchmark Design

### `metadata_eval.rs` - Metadata Evaluation
- **Purpose:** Fair comparison of pure pruning performance
- **Tests:** Fairness, data distributions, predicate types, selectivity, scaling
- **Key Metric:** Metadata evaluation time

### `compare.rs` - End-to-End (Prune → Read → Filter)
- **Purpose:** Demonstrate end-to-end impact of pruning on actual reads
- **Data:** Overlapping row group ranges with page indexes + bloom filters
- **Key Metrics:** Wall-clock time (DataFusion via ParquetExec), rows read, rows matched

### `point_query.rs` - End-to-End with I/O (verbose)
- **Purpose:** Demonstrate page-level + bloom filter advantages with detailed prints
- **Data:** Overlapping row group ranges (realistic scenario)
- **Tests:** Row-group only, page index, bloom filter, both combined
- **Key Metrics:** I/O reduction, actual data read

### `bloom_filter.rs` - Definite Absence
- **Purpose:** Demonstrate bloom filter effectiveness
- **Data:** Alternating EVEN/ODD row groups with wide (but not identical) stats
- **Key Metric:** Row group elimination rate

### `projection.rs` - Wide Schema Projection
- **Purpose:** Measure projection impact on Aisle read path under wide schemas
- **Data:** 40 row groups × 4,096 rows with 50 columns (`group_mod` filter + 48 payload cols)
- **Key Metrics:** Wall-clock latency, rows read, decoded/output payload proxy
- **Latest results:** `benches/df_compare/results/projection_pushdown.md`

## References

- [Aisle Documentation](../../README.md)
- [DataFusion Pruning](https://docs.rs/datafusion-pruning)
- [Parquet Format Spec](https://parquet.apache.org/docs/)
