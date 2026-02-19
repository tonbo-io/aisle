# Dictionary Hints Provider Optimization (Batch + Cache)

- Date (UTC): `2026-02-18T16:45:31Z`
- Git commit: `0f8c7a7`
- Branch: `feat/dictionary-hints-provider-optimization`
- OS: `Linux 6.17.0-14-generic x86_64 GNU/Linux`
- Rust: `rustc 1.92.0 (ded5c06cf 2025-12-08)`
- Cargo: `cargo 1.92.0 (344c4567c 2025-10-21)`

## Commands

```bash
# Before baseline
cargo bench --manifest-path benches/df_compare/Cargo.toml --bench dictionary_hints -- --save-baseline dict_hints_provider_before --sample-size 10

# After comparison
cargo bench --manifest-path benches/df_compare/Cargo.toml --bench dictionary_hints -- --baseline dict_hints_provider_before --sample-size 10
```

## Strategy Modes

- `no cache`
  - IDs: `aisle_metadata_only_candidate`, `aisle_prune_plus_scan_candidate`
- `batch only`
  - IDs: `aisle_metadata_only_batch_only`, `aisle_prune_plus_scan_batch_only`
- `batch + cache`
  - IDs: `aisle_metadata_only_batch_plus_cache`, `aisle_prune_plus_scan_batch_plus_cache`
- `batch + cache (cold)`
  - IDs: `aisle_metadata_only_remote_batch_plus_cache_cold`, `aisle_prune_plus_scan_remote_batch_plus_cache_cold`

## Scenario-Level Pruning/Read Indicators (all modes)

Query: `key = 'target_07'`

- Row groups kept: `4 / 64`
- Pages kept: `8 / 128`
- Rows read: `8192`
- Rows matched: `1024`
- Rows read proxy (`rows_read / total_rows`): `6.25%`

## Before/After Latency (Criterion Mean)

| Mode | Benchmark ID | Before | After | Delta |
|---|---|---:|---:|---:|
| no cache | `aisle_metadata_only_candidate` | `93.890 µs` | `93.657 µs` | `-0.25%` |
| no cache | `aisle_prune_plus_scan_candidate` | `684.668 µs` | `672.847 µs` | `-1.73%` |
| batch only | `aisle_metadata_only_batch_only` | `99.182 µs` | `99.466 µs` | `+0.29%` |
| batch only | `aisle_prune_plus_scan_batch_only` | `658.592 µs` | `703.286 µs` | `+6.79%` |
| batch + cache | `aisle_metadata_only_batch_plus_cache` | `96.267 µs` | `94.972 µs` | `-1.35%` |
| batch + cache | `aisle_prune_plus_scan_batch_plus_cache` | `772.774 µs` | `726.060 µs` | `-6.05%` |

## Interpretation

- Metadata-only overhead did **not** show a broad, significant improvement across all modes.
- `no cache`: no statistically significant change (`p > 0.05`).
- `batch only`: metadata-only unchanged; prune+scan regressed (`p < 0.05`).
- `batch + cache`: metadata-only slight improvement (small), prune+scan improved directionally but not significant (`p > 0.05`).

## Recommendation

- Use `batch + cache` as the default provider strategy for repeated query workloads over the same file/metadata.
- For one-shot queries, `no cache` and `batch only` remain comparable in metadata-only latency.
- Keep conservative pruning semantics unchanged: only `DictionaryHintEvidence::Exact` drives pruning.

## Remote Simulation Benchmark (Strategy Visibility)

To model object-store-like costs, benchmark now includes a remote simulation mode with latency:

- `single lookup`: `base_rtt + per_item_cost`
- `batch lookup`: `base_rtt + per_item_cost * batch_size`
- Config used: `base_rtt=200µs`, `per_item=30µs`
- Workload: `24` row groups, `8` dictionary-hinted key columns, `AND` equality across all key columns.

Measured (same run):

| Mode | Metadata-only latency | Prune+scan latency |
|---|---:|---:|
| `no cache` | `[264.35 ms, 265.90 ms, 267.48 ms]` | `[266.81 ms, 268.54 ms, 270.58 ms]` |
| `batch only` | `[39.554 ms, 39.664 ms, 39.823 ms]` | `[44.627 ms, 45.199 ms, 45.755 ms]` |
| `batch + cache` (warm) | `[246.64 µs, 247.65 µs, 249.24 µs]` | `[1.5979 ms, 1.6509 ms, 1.7187 ms]` |
| `batch + cache` (cold) | `[46.316 ms, 46.706 ms, 47.284 ms]` | `[50.364 ms, 51.168 ms, 51.885 ms]` |

Remote-sim takeaway:

- `batch only` shows a clear reduction vs `no cache` when each row-group request includes many hinted columns.
- Warm `batch + cache` collapses metadata lookup cost to near in-memory overhead.
- Cold `batch + cache` remains near the batch-only range (expected: first query still has remote-like fetch cost).
