# Aisle Benchmarks

This directory contains Criterion-based benchmarks for measuring Aisle's performance.

## Running Benchmarks

### Run all benchmarks
```bash
cargo bench
```

### Run specific benchmark
```bash
cargo bench --bench row_group_pruning
```

### Run specific benchmark group
```bash
# Only equality predicate benchmarks
cargo bench --bench row_group_pruning equality_predicate

# Only scalability benchmarks
cargo bench --bench row_group_pruning scalability
```

### Run with baseline comparison
```bash
# Save current performance as baseline
cargo bench --bench row_group_pruning -- --save-baseline main

# Make changes, then compare
cargo bench --bench row_group_pruning -- --baseline main
```

## Benchmark Structure

### `row_group_pruning.rs`
Tests row-group level pruning performance:

**Predicate Types:**
- `equality_predicate`: Simple `col("x").eq(lit(value))`
- `range_predicate`: `col("x").gt_eq(...).and(col("x").lt(...))`
- `in_list_predicate`: `col("x").in_list([...])` with varying list sizes
- `string_prefix`: `col("name").like("prefix%")`

**Logical Operators:**
- `and_chains`: AND predicates with 2, 5, 10 conditions
- `or_branches`: OR predicates with 2, 5, 10 branches

**Scalability:**
- `scalability`: Tests with 10, 100, 1000, 10000 row groups

## Viewing Results

### Terminal Output
Results appear in terminal with statistical analysis (mean, median, std dev).

### HTML Reports
Open `target/criterion/report/index.html` in browser for:
- Performance graphs
- Distribution plots
- Comparison charts
- Historical trends

### JSON Export
Results saved in `target/criterion/<benchmark>/base/estimates.json` for programmatic analysis.

## Interpreting Results

### Key Metrics

**Time**: Total execution time (μs, ms)
- **Target**: <10μs per row group for simple predicates

**Throughput**: Row groups evaluated per second
- **Target**: >100K row groups/second for simple predicates

**Scalability**: Time vs row group count
- **Expected**: O(n) linear scaling

### Example Output

```
equality_predicate/10   time:   [8.234 µs 8.312 µs 8.395 µs]
                        thrpt:  [1.1914 Melem/s 1.2032 Melem/s 1.2145 Melem/s]

equality_predicate/100  time:   [82.15 µs 82.89 µs 83.71 µs]
                        thrpt:  [1.1947 Melem/s 1.2064 Melem/s 1.2172 Melem/s]

equality_predicate/1000 time:   [821.3 µs 828.4 µs 836.1 µs]
                        thrpt:  [1.1960 Melem/s 1.2073 Melem/s 1.2179 Melem/s]
```

**Analysis**: Linear scaling (~8.3μs/RG), throughput constant across scales.

## Performance Targets

| Benchmark | Target | Notes |
|-----------|--------|-------|
| equality_predicate (1 RG) | <10μs | Simple point query |
| equality_predicate (1000 RG) | <1ms | Should scale linearly |
| range_predicate (100 RG) | <100μs | Two-sided range |
| in_list (100 values) | <200μs | List membership |
| string_prefix (100 RG) | <150μs | LIKE pattern |
| and_chains (5 conditions) | <50μs | Conjunctive predicates |
| or_branches (5 branches) | <100μs | Disjunctive predicates |

## Regression Detection

Performance regressions >10% will be flagged by Criterion:

```
equality_predicate/100  time:   [92.15 µs 93.12 µs 94.21 µs]
                        change: [+11.2% +12.4% +13.6%] (p = 0.00 < 0.05)
                        Performance has regressed.
```

## Adding New Benchmarks

### 1. Create new benchmark file

```rust
// benches/my_benchmark.rs
use criterion::{criterion_group, criterion_main, Criterion};

fn my_bench(c: &mut Criterion) {
    c.bench_function("my_test", |b| {
        b.iter(|| {
            // Code to benchmark
        });
    });
}

criterion_group!(benches, my_bench);
criterion_main!(benches);
```

### 2. Add to Cargo.toml

```toml
[[bench]]
name = "my_benchmark"
harness = false
```

### 3. Run

```bash
cargo bench --bench my_benchmark
```

## CI Integration

For CI/CD, run benchmarks without HTML reports:

```bash
# Quick benchmark (fewer samples)
cargo bench --bench row_group_pruning -- --quick

# Export results only
cargo bench --bench row_group_pruning -- --noplot
```

## Troubleshooting

### Benchmark takes too long
```bash
# Reduce sample size
cargo bench -- --sample-size 10

# Reduce measurement time
cargo bench -- --measurement-time 5
```

### High variance in results
```bash
# Run more iterations
cargo bench -- --sample-size 1000

# Check system load (other processes interfering)
```

## See Also

- [Benchmark Design](../docs/benchmark_design.md) - Comprehensive benchmark plan
- [Criterion Docs](https://bheisler.github.io/criterion.rs/book/) - Criterion user guide
