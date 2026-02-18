# Dictionary Hints Baseline (Updated Dictionary-Contract Semantics)

- Date (UTC): 2026-02-17
- Git commit: `172f55a` + local review fixes
- Branch: `feat/dictionary-hints-mvp`
- OS: `Linux 6.17.0-14-generic x86_64 GNU/Linux`
- CPU: `Intel(R) Core(TM) Ultra 9 185H` (22 logical CPUs)
- Rust: `rustc 1.92.0 (ded5c06cf 2025-12-08)`
- Cargo: `cargo 1.92.0 (344c4567c 2025-10-21)`

## Command

```bash
cargo bench --manifest-path benches/df_compare/Cargo.toml --bench dictionary_hints -- --save-baseline dict_hints_after_contract --sample-size 10
```

## Benchmark IDs (stable)

- `dictionary_hints_candidate/aisle_metadata_only_candidate`
- `dictionary_hints_candidate/aisle_prune_plus_scan_candidate`

## Scenario-level pruning/read indicators

From benchmark scenario printout (`key = 'target_07'`):

- Row groups kept: `4 / 64`
- Pages kept: `8 / 128`
- Rows read: `8192`
- Rows matched: `1024`
- Decode proxy (`rows_read / total_rows`): `6.25%`

## Baseline latency

- `dictionary_hints_candidate/aisle_metadata_only_candidate`
  - Time: `[95.083 µs, 95.425 µs, 96.209 µs]`
- `dictionary_hints_candidate/aisle_prune_plus_scan_candidate`
  - Time: `[754.73 µs, 770.99 µs, 792.24 µs]`

## Criterion baseline artifacts

Saved under:

- `benches/df_compare/target/criterion/dictionary_hints_candidate/aisle_metadata_only_candidate/dict_hints_after_contract/`
- `benches/df_compare/target/criterion/dictionary_hints_candidate/aisle_prune_plus_scan_candidate/dict_hints_after_contract/`
