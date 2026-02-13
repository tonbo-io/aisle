# Dictionary Hints MVP - Measured Impact

## Run Context

- Date (UTC): 2026-02-13 15:36:10 UTC
- Git commit: `bbb377f`
- Branch: `feat/dictionary-hints-mvp`
- Baseline ID: `dict_hints_before`
- Benchmark command:

```bash
cargo bench --manifest-path benches/df_compare/Cargo.toml --bench dictionary_hints -- --baseline dict_hints_before
```

## PR-Ready Measured Impact

### Latency

- `dictionary_hints_candidate/aisle_metadata_only_candidate`
  - Before: `[18.218 µs, 19.680 µs, 21.266 µs]`
  - After: `[14.961 µs, 14.976 µs, 14.995 µs]`
  - Delta (midpoint): `-23.90%`
  - Speedup: `1.31x`
  - Criterion significance: `p = 0.00 < 0.05` (performance improved)

- `dictionary_hints_candidate/aisle_prune_plus_scan_candidate`
  - Before: `[17.150 ms, 19.493 ms, 22.167 ms]`
  - After: `[653.36 µs, 667.35 µs, 683.03 µs]`
  - Delta (midpoint): `-96.58%`
  - Speedup: `29.21x`
  - Criterion significance: `p = 0.00 < 0.05` (performance improved)

### Pruning / Read / Decode Indicators

Scenario: `key = 'target_07'`

- Row groups kept: `64 / 64` -> `4 / 64` (`-93.75%`)
- Pages kept: `128 / 128` -> `8 / 128` (`-93.75%`)
- Rows read: `131072` -> `8192` (`-93.75%`)
- Rows matched: `1024` -> `1024` (unchanged)
- Decode proxy (`rows_read / total_rows`): `100.0%` -> `6.25%` (`-93.75%`)

## Notes

- This benchmark compares against the saved baseline `dict_hints_before` created prior to dictionary-hints implementation.
- Dictionary hints are opt-in (`enable_dictionary_hints(true)`) and conservative: missing/ambiguous hints fall back to `Unknown` (keep data).
- Confidence intervals are benchmark-time ranges reported by Criterion for each run.
