# Projection Pushdown API Checkpoint

This document captures the public API changes introduced for projection pushdown.

## Goals

- Keep pruning correctness unchanged (no false pruning).
- Preserve backward compatibility for existing `PruneRequest` / `PruneResult` usage.
- Make projection pushdown explicit and opt-in for readers.

## Public API Additions

### `PruneRequest`

- `with_output_projection(...)`
  - Records requested output columns in the pruning result.
  - Does not change row-group/page pruning behavior.
- `with_projection(...)`
  - Alias for `with_output_projection(...)`.

### `PruneResult`

- `output_projection() -> Option<&[String]>`
- `predicate_columns() -> &[String]`
- `required_columns() -> &[String]`
- `output_projection_mask(&SchemaDescriptor) -> Option<ProjectionMask>`
- `required_projection_mask(&SchemaDescriptor) -> ProjectionMask`
- `into_parts_with_projection(...)` (new extended tuple-returning variant)

## Compatibility Notes

- Existing APIs are preserved:
  - `PruneRequest::prune()` and `prune_async()` signatures unchanged.
  - `PruneResult::into_parts()` signature unchanged.
- Projection is opt-in:
  - If not configured, behavior matches previous releases.
  - If configured but not applied to reader, row/column behavior remains unchanged.

## Integration Pattern

1. Build pruning request with predicate and output projection.
2. Run `prune()`/`prune_async()`.
3. Apply `row_groups`/`row_selection` as before.
4. Apply `output_projection_mask(...)` to Parquet reader builder.
