# Dictionary Hints Baseline (Before MVP)

- Date (UTC): 2026-02-13 14:52:11 UTC
- Git commit: `8e74334`
- Branch: `feat/dictionary-hints-mvp`
- OS: `Linux 6.17.0-14-generic x86_64 GNU/Linux`
- CPU: `Intel(R) Core(TM) Ultra 9 185H` (22 logical CPUs)
- Rust: `rustc 1.92.0 (ded5c06cf 2025-12-08)`
- Cargo: `cargo 1.92.0 (344c4567c 2025-10-21)`

## Command

```bash
cargo bench --manifest-path benches/df_compare/Cargo.toml --bench dictionary_hints -- --save-baseline dict_hints_before
```

## Benchmark IDs (stable)

- `dictionary_hints_candidate/aisle_metadata_only_candidate`
- `dictionary_hints_candidate/aisle_prune_plus_scan_candidate`

## Scenario-level pruning/read indicators

From benchmark scenario printout (`key = 'target_07'`):

- Row groups kept: `64 / 64`
- Pages kept: `128 / 128`
- Rows read: `131072`
- Rows matched: `1024`
- Decode proxy (`rows_read / total_rows`): `100.0%`

## Baseline latency

- `dictionary_hints_candidate/aisle_metadata_only_candidate`
  - Time: `[18.218 µs, 19.680 µs, 21.266 µs]`
- `dictionary_hints_candidate/aisle_prune_plus_scan_candidate`
  - Time: `[17.150 ms, 19.493 ms, 22.167 ms]`

## Criterion baseline artifacts

Saved under:

- `benches/df_compare/target/criterion/dictionary_hints_candidate/aisle_metadata_only_candidate/dict_hints_before/`
- `benches/df_compare/target/criterion/dictionary_hints_candidate/aisle_prune_plus_scan_candidate/dict_hints_before/`
