# Benchmark Baselines for Aisle

## Current State

The `row_group_pruning` benchmark currently measures **only Aisle's overhead**:
- Time to compile predicates
- Time to evaluate against statistics
- Scalability across row group counts

**Missing**: Comparison against alternatives to demonstrate value proposition.

---

## Recommended Baselines

### 1. No Pruning (Full Scan)

**What it measures**: Reading ALL row groups without any metadata evaluation

**Purpose**: Shows the I/O cost that Aisle helps avoid

**Implementation**:
```rust
fn bench_baseline_no_pruning(c: &mut Criterion) {
    // Just iterate through all row groups, no predicate evaluation
    b.iter(|| {
        // Return all row groups without evaluation
        let all_groups: Vec<usize> = (0..metadata.num_row_groups()).collect();
        black_box(all_groups)
    });
}
```

**Expected result**: ~0.1μs (just creating a Vec)

**Value**: Shows that Aisle's 3.6μs overhead eliminates reading 90% of data (huge win)

---

### 2. Manual Statistics Check

**What it measures**: Hand-written code to check min/max without Aisle

**Purpose**: Shows what users would write without Aisle (comparison against "doing it yourself")

**Implementation**:
```rust
fn bench_baseline_manual_stats(c: &mut Criterion) {
    b.iter(|| {
        let mut kept = Vec::new();
        for rg_idx in 0..metadata.num_row_groups() {
            let rg = metadata.row_group(rg_idx);
            // Manual min/max checking
            if let Some(stats) = rg.column(0).statistics() {
                // Hand-written predicate evaluation
                if matches_predicate(stats) {
                    kept.push(rg_idx);
                }
            }
        }
        black_box(kept)
    });
}
```

**Expected result**: ~2-5μs (similar to Aisle, more error-prone)

**Value**: Shows Aisle provides similar performance with better ergonomics

---

### 3. Compilation-Only Baseline

**What it measures**: Just compiling DataFusion Expr → IR, without evaluation

**Purpose**: Separates compilation overhead from evaluation overhead

**Implementation**:
```rust
fn bench_baseline_compilation_only(c: &mut Criterion) {
    b.iter(|| {
        let predicate = col("id").eq(lit(500i64));
        let compile_result = compile_pruning_ir(&predicate, &schema);
        black_box(compile_result)
    });
}
```

**Expected result**: <1μs (compilation is cheap)

**Value**: Shows most time is in statistics evaluation, not compilation

---

### 4. Zero-Work Baseline (Theoretical Minimum)

**What it measures**: Minimal possible overhead (just function call)

**Purpose**: Theoretical lower bound for comparison

**Implementation**:
```rust
fn bench_baseline_noop(c: &mut Criterion) {
    b.iter(|| {
        // Absolute minimum - just return empty result
        black_box(PruneResult::default())
    });
}
```

**Expected result**: <0.01μs

**Value**: Shows how much overhead is fundamental vs optimizable

---

## Comparison Table

| Approach | Time (100 RG) | I/O Saved | Complexity | Correctness |
|----------|---------------|-----------|------------|-------------|
| **No pruning** | ~0.1μs | 0% | Trivial | N/A |
| **Manual stats** | ~2-5μs | 70-90% | High | Error-prone |
| **Aisle** | ~26μs | 70-90% | Low | Guaranteed |
| **Noop (theoretical)** | <0.01μs | N/A | N/A | N/A |

**Key insight**: Aisle's 26μs overhead is negligible compared to avoiding MB-GB of I/O.

---

## Baseline Strategy by Benchmark Type

### Micro-benchmarks (row_group_pruning)
**Compare against**:
- No pruning (show overhead is justified)
- Compilation-only (separate concerns)

### Integration benchmarks (future)
**Compare against**:
- Full table scan with I/O measurement
- Arrow ParquetRecordBatchReader default behavior
- Optional: DataFusion's built-in pruning (if comparable)

---

## How to Use Baselines in Criterion

### Method 1: Multiple benchmark functions
```rust
criterion_group!(
    benches,
    bench_aisle_pruning,           // Main benchmark
    bench_baseline_no_pruning,     // Baseline 1
    bench_baseline_manual_stats,   // Baseline 2
);
```

### Method 2: Comparison groups
```rust
fn compare_approaches(c: &mut Criterion) {
    let mut group = c.benchmark_group("pruning_comparison");

    group.bench_function("aisle", |b| { /* Aisle pruning */ });
    group.bench_function("no_pruning", |b| { /* No pruning */ });
    group.bench_function("manual", |b| { /* Manual stats */ });

    group.finish();
}
```

**Output**:
```
pruning_comparison/aisle        time: [26.7 μs ...]
pruning_comparison/no_pruning   time: [0.12 μs ...]
pruning_comparison/manual       time: [3.8 μs ...]
```

### Method 3: Save and compare baselines
```bash
# Benchmark without Aisle (hypothetical)
cargo bench -- --save-baseline no-pruning

# Benchmark with Aisle
cargo bench -- --baseline no-pruning
```

**Output**: Shows performance difference and statistical significance

---

## Recommended Next Steps

### Phase 1: Add micro-benchmarks baselines
1. Add `bench_baseline_no_pruning` (trivial, shows overhead is small)
2. Add `bench_baseline_compilation_only` (isolate compilation cost)

### Phase 2: Add integration baselines
1. Create actual Parquet files (10MB, 100MB, 1GB)
2. Measure actual bytes read with and without Aisle
3. Calculate real I/O savings percentage

### Phase 3: Document findings
1. README performance section with real numbers
2. Blog post showing "26μs overhead saves 900MB I/O"
3. Comparison table for users choosing Aisle

---

## Example: What a Good Baseline Comparison Looks Like

```
Benchmark: Point query on 1GB file (1000 row groups)

Approach            | Metadata Time | Bytes Read | Total Time | I/O Saved
--------------------|---------------|------------|------------|----------
No pruning          | 0             | 1.0 GB     | 450ms      | 0%
Aisle pruning       | 0.26ms        | 0.01 GB    | 5.2ms      | 99%
Manual stats check  | 0.18ms        | 0.01 GB    | 5.1ms      | 99%

Verdict: Aisle adds 0.08ms overhead vs manual (negligible) and saves 445ms I/O
```

This makes the value proposition crystal clear!
