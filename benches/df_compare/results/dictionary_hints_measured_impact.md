# Dictionary Hints MVP - Measured Impact

## Run Context

- Date (UTC): 2026-02-17
- Git commit: `172f55a` + local review fixes
- Branch: `feat/dictionary-hints-mvp`
- Baseline ID: `dict_hints_before`
- Benchmark command:

```bash
cargo bench --manifest-path benches/df_compare/Cargo.toml --bench dictionary_hints -- --sample-size 10
```

## PR-Ready Measured Impact

### Latency

- `dictionary_hints_candidate/aisle_metadata_only_candidate`
  - Before: `[18.218 µs, 19.680 µs, 21.266 µs]`
  - After: `[97.859 µs, 98.446 µs, 99.220 µs]`
  - Delta (midpoint): `+400.23%`
  - Speedup: `0.20x`
  - Criterion significance: `p = 0.00 < 0.05` (performance regressed)

- `dictionary_hints_candidate/aisle_prune_plus_scan_candidate`
  - Before: `[17.150 ms, 19.493 ms, 22.167 ms]`
  - After: `[604.26 µs, 624.81 µs, 641.68 µs]`
  - Delta (midpoint): `-96.79%`
  - Speedup: `31.20x`
  - Criterion significance: `p = 0.00 < 0.05` (performance improved)

### Pruning / Read / Decode Indicators

Scenario: `key = 'target_07'`

- Row groups kept: `64 / 64` -> `4 / 64` (`-93.75%`)
- Pages kept: `128 / 128` -> `8 / 128` (`-93.75%`)
- Rows read: `131072` -> `8192` (`-93.75%`)
- Rows matched: `1024` -> `1024` (unchanged)
- Decode proxy (`rows_read / total_rows`): `100.0%` -> `6.25%` (`-93.75%`)

## Notes

- The metadata-only benchmark now uses async pruning with dictionary evidence enabled (`enable_dictionary_hints(true)`), so it includes provider lookup overhead by design.
- Dictionary hints are opt-in (`enable_dictionary_hints(true)`) and conservative: only exact provider evidence is used for pruning; missing/ambiguous evidence falls back to `Unknown` (keep data).
- Confidence intervals are benchmark-time ranges reported by Criterion for each run.
