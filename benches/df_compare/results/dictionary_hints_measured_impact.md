# Dictionary Hints MVP - Measured Impact

> Provider strategy follow-up (`no cache` / `batch only` / `batch + cache`) is tracked in
> `benches/df_compare/results/dictionary_hints_provider_optimization.md`.

## Run Context

- Date (UTC): 2026-02-17
- Git commit: `172f55a` + local review fixes
- Branch: `feat/dictionary-hints-mvp`
- Historical baseline ID (non-comparable): `dict_hints_before`
- Contract-correct baseline ID (comparable): `dict_hints_after_contract`

## Dictionary Hints: What the numbers actually mean

### 1) Same-method performance (comparable baseline)

Use `dict_hints_after_contract` only.

- `dictionary_hints_candidate/aisle_metadata_only_candidate`
  - Before: `[95.083 µs, 95.425 µs, 96.209 µs]`
  - After: `[95.815 µs, 104.98 µs, 111.42 µs]`
  - `p = 0.36` -> no statistically significant change

- `dictionary_hints_candidate/aisle_prune_plus_scan_candidate`
  - Before: `[754.73 µs, 770.99 µs, 792.24 µs]`
  - After: `[690.91 µs, 732.65 µs, 782.26 µs]`
  - `p = 0.82` -> no statistically significant change

### 2) Practical value (I/O reduction)

Scenario: `key = 'target_07'`

- Row groups kept: `64 -> 4` (`-93.75%`)
- Rows read: `131072 -> 8192` (`-93.75%`)
- Rows matched: `1024 -> 1024` (unchanged)

This is the core benefit: same results, much less data read.

### 3) Why the “+400% regression” appeared

That came from a non-comparable comparison:

- Old path: sync `prune()` (no dictionary async lookup path)
- New path: async `prune_async()` + dictionary evidence lookup

So it measured a changed benchmark method, not a like-for-like regression.
