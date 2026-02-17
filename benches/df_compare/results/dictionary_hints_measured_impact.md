# Dictionary Hints MVP - Measured Impact

## Run Context

- Date (UTC): 2026-02-17
- Git commit: `172f55a` + local review fixes
- Branch: `feat/dictionary-hints-mvp`
- Historical baseline ID (non-comparable): `dict_hints_before`
- Contract-correct baseline ID (comparable): `dict_hints_after_contract`

Benchmark commands used:

```bash
# Comparable (current semantics)
cargo bench --manifest-path benches/df_compare/Cargo.toml --bench dictionary_hints -- --baseline dict_hints_after_contract --sample-size 10
```

## Methodology / Comparability

- `dictionary_hints_candidate/aisle_metadata_only_candidate` changed from sync `prune()` to async `prune_async()` with `.enable_dictionary_hints(true)`.
- This means dictionary evidence lookup and provider overhead are now part of the benchmark path by design.
- Therefore, `dict_hints_before` vs current metadata-only timing is historical context, not an apples-to-apples performance trend.
- For ongoing tracking, use the contract-correct baseline `dict_hints_after_contract`.

## Comparable Latency (Use This for Tracking)

- `dictionary_hints_candidate/aisle_metadata_only_candidate`
  - Before (`dict_hints_after_contract`): `[95.083 µs, 95.425 µs, 96.209 µs]`
  - After: `[95.815 µs, 104.98 µs, 111.42 µs]`
  - Criterion change: `[-2.5065% +3.4316% +9.6942%]`
  - Significance: `p = 0.36 > 0.05` (no statistically significant change)

- `dictionary_hints_candidate/aisle_prune_plus_scan_candidate`
  - Before (`dict_hints_after_contract`): `[754.73 µs, 770.99 µs, 792.24 µs]`
  - After: `[690.91 µs, 732.65 µs, 782.26 µs]`
  - Criterion change: `[-8.2510% +1.6683% +14.609%]`
  - Significance: `p = 0.82 > 0.05` (no statistically significant change)

## Historical Context (Non-Comparable)

- `dictionary_hints_candidate/aisle_metadata_only_candidate`
  - Historical before (`dict_hints_before`): `[18.218 µs, 19.680 µs, 21.266 µs]`
  - Current (new semantics): `[97.859 µs, 98.446 µs, 99.220 µs]`
  - This reflects benchmark-path expansion (dictionary async lookup path included), not a direct same-method regression.

## Pruning / Read / Decode Indicators

Scenario: `key = 'target_07'`

- Row groups kept: `64 / 64` -> `4 / 64` (`-93.75%`)
- Pages kept: `128 / 128` -> `8 / 128` (`-93.75%`)
- Rows read: `131072` -> `8192` (`-93.75%`)
- Rows matched: `1024` -> `1024` (unchanged)
- Decode proxy (`rows_read / total_rows`): `100.0%` -> `6.25%` (`-93.75%`)

## Notes

- Dictionary hints are opt-in (`enable_dictionary_hints(true)`).
- Pruning uses dictionary evidence only when provider returns `DictionaryHintEvidence::Exact`.
- Missing/inexact evidence falls back to `Unknown` (keep data).
