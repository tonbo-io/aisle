# Projection Pushdown Benchmark Results

## Baseline (Before Implementation)

- Captured at (UTC): `2026-02-11T14:27:55Z`
- Git commit: `2b66de4`
- Branch: `feat/projection-pushdown`
- Command:
  - `cargo bench --manifest-path benches/df_compare/Cargo.toml --bench projection -- --save-baseline projection_before`
- Criterion baseline ID: `projection_before`
- Baseline files:
  - `benches/df_compare/target/criterion/projection_pushdown_wide_scan/aisle_projection_pipeline/projection_before/estimates.json`
  - `benches/df_compare/target/criterion/projection_pushdown_wide_scan/aisle_projection_manual_reference/projection_before/estimates.json`

### Environment

- OS/Kernel: `Linux 6.14.0-37-generic #37~24.04.1-Ubuntu`
- CPU: `Intel(R) Core(TM) Ultra 9 185H`
- Logical CPUs: `22` (threads/core: `2`)
- Rust: `rustc 1.92.0 (ded5c06cf 2025-12-08)`
- Cargo: `cargo 1.92.0 (344c4567c 2025-10-21)`

### Workload

- Benchmark group: `projection_pushdown_wide_scan`
- Dataset: `40` row groups × `4,096` rows/group
- Total schema width: `50` columns (`key`, `group_mod`, 48 payload columns)
- Predicate: `group_mod = 3` (keeps all row groups; selective at row level)

### Latency Baseline (Criterion mean point estimate)

| Benchmark ID | Mean latency | 95% CI |
|---|---:|---:|
| `aisle_projection_pipeline` | `48.880 ms` | `[48.068 ms, 49.755 ms]` |
| `aisle_projection_manual_reference` | `2.591 ms` | `[2.545 ms, 2.642 ms]` |

### Rows/Payload Baseline (benchmark-emitted metrics)

| Scenario | Kept row groups | Rows read | Rows emitted | Output columns | Decoded cell proxy | Output bytes proxy |
|---|---:|---:|---:|---:|---:|---:|
| `aisle_projection_pipeline` | `40` | `163,840` | `16,400` | `50` | `820,000` | `6,641,600` |
| `aisle_projection_manual_reference` | `40` | `163,840` | `16,400` | `1` | `16,400` | `132,832` |

Notes:
- `Rows read` is computed from kept row groups (`sum(row_group.num_rows)`).
- `Decoded cell proxy` is `rows_emitted × output_columns`.
- `Output bytes proxy` is Arrow in-memory batch footprint from `get_array_memory_size()`.
