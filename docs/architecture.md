# Aisle Technical Architecture

## Goals
- Provide metadata-based Parquet filter pushdown without modifying upstream `parquet`
- Accept DataFusion logical expressions as input
- Produce row-group pruning and row-level selection (RowSelection / RoaringBitmap)
- Be conservative: never prune rows that might match

## Non-Goals
- Full SQL expression evaluation at pruning time
- Replacing runtime filters (physical expression evaluation)
- Changing Parquet file layout or metadata format

## Inputs / Outputs

**Inputs**:
- DataFusion `Expr` + Arrow schema
- Parquet metadata (row-group stats, page indexes, bloom filters)
- Configuration (`PruneOptions`)

**Outputs** (`PruneResult`):
- `row_groups: Vec<usize>` — Row groups to read
- `row_selection: Option<RowSelection>` — Row-level selection (Parquet-native)
- `roaring_bitmap: Option<RoaringBitmap>` — Compact bitmap (optional)
- `compile_result: CompileResult` — Compilation diagnostics

## Architecture Overview

```
DataFusion Expr + Arrow Schema
    ↓
Normalize & Compile → Pruning IR (best-effort)
    ↓
Evaluate IR against Parquet Metadata
    ├─ Row-group pruning (statistics + bloom filters)
    └─ Page-level pruning (page indexes)
         ↓
    PruneResult { row_groups, row_selection, roaring_bitmap }
```

## Module Structure (Actual Implementation)

### Core Modules

- **`compile`** (`src/compile.rs`)
  - Compiles DataFusion `Expr` to pruning IR
  - Best-effort: unsupported predicates logged but don't fail
  - Returns `CompileResult` with prunable IR and errors

- **`ir`** (`src/ir.rs`)
  - Pruning IR types: `IrExpr`, `CmpOp`, `TriState`
  - Minimal, metadata-evaluable subset of DataFusion expressions
  - Tri-state logic for conservative evaluation

- **`prune`** (`src/prune/`)
  - **`request.rs`**: `PruneRequest` builder API
  - **`result.rs`**: `PruneResult` output type
  - **`options.rs`**: `PruneOptions` configuration
  - **`api.rs`**: Internal pruning functions
  - **`eval.rs`**: Expression evaluation dispatcher
  - **`cmp.rs`**: Comparison operators (`=`, `!=`, `<`, `>`, etc.)
  - **`between.rs`**: BETWEEN range predicates
  - **`in_list.rs`**: IN set membership
  - **`is_null.rs`**: IS NULL / IS NOT NULL
  - **`starts_with.rs`**: LIKE 'prefix%' via StartsWith
  - **`bloom.rs`**: Bloom filter evaluation
  - **`page.rs`**: Page-level pruning logic
  - **`stats.rs`**: Statistics extraction from Parquet metadata
  - **`context.rs`**: Evaluation context (row group, column lookup)
  - **`provider.rs`**: `AsyncBloomFilterProvider` trait

- **`selection`** (`src/selection.rs`)
  - Bidirectional conversion: RowSelection ↔ RoaringBitmap
  - Handles u32::MAX boundary (RoaringBitmap limitation)

- **`pruner`** (`src/pruner.rs`)
  - Stateful pruner for reusing compiled predicates
  - Pre-computes column mappings for efficiency

- **`error`** (`src/error.rs`)
  - `CompileError` enum for unsupported predicates

## Pruning IR

### Supported Predicates

```rust
pub enum IrExpr {
    True,
    False,
    Cmp { column: String, op: CmpOp, value: ScalarValue },
    Between { column: String, low: ScalarValue, high: ScalarValue, inclusive: bool },
    InList { column: String, values: Vec<ScalarValue> },
    StartsWith { column: String, prefix: String },
    IsNull { column: String, negated: bool },
    BloomFilterEq { column: String, value: ScalarValue },
    BloomFilterInList { column: String, values: Vec<ScalarValue> },
    And(Vec<IrExpr>),
    Or(Vec<IrExpr>),
    Not(Box<IrExpr>),
}
```

### Tri-State Semantics

Each predicate evaluates to `{True, False, Unknown}`:
- **True**: Definitely matches → Keep data
- **False**: Definitely doesn't match → Prune data
- **Unknown**: Might match → Keep data (conservative)

This ensures pruning is safe (no false negatives).

## Compilation Strategy

1. **Normalize**: Simplify DataFusion `Expr`, constant fold
2. **Split conjunctions**: Break `AND` into individual predicates
3. **Compile to IR**: Best-effort translation
   - Supported predicates → `IrExpr`
   - Unsupported predicates → logged in `CompileResult::errors`
4. **Type checking**: Validate column types against schema
5. **Cast handling**:
   - **Column casts**: Only no-op casts allowed (same type)
   - **Literal casts**: Performed at compile time

## Metadata Sources

### Row-Group Statistics (Always Available)
- **min/max**: Column value ranges
- **null_count**: Number of null values
- **distinct_count**: Cardinality (optional)

### Page Indexes (Optional, Parquet 1.12+)
- **Column index**: Per-page min/max statistics
- **Offset index**: Page boundaries and row counts
- Enables page-level pruning within row groups

### Bloom Filters (Optional)
- Definite absence checks for `=` and `IN` predicates
- High-cardinality columns (user IDs, SKUs, transaction IDs)
- Loaded asynchronously via `AsyncBloomFilterProvider`

## Evaluation

### Row-Group Pruning

For each row group:
1. Evaluate IR against row-group statistics
2. If predicate is **False** → Drop row group
3. If predicate is **True** or **Unknown** → Keep row group

### Page-Level Pruning (Optional)

If page indexes exist:
1. Evaluate IR against page statistics (per-column)
2. Build page selection using tri-state logic
3. Apply pruning rules:
   - **AND**: Best-effort (skip unsupported, intersect supported)
   - **OR**: All-or-nothing (require all branches supported)
   - **NOT**: Conservative (only exact selections inverted)
4. Convert page selections to `RowSelection`

### Bloom Filter Pruning

For `=` and `IN` predicates with bloom filters:
1. Check bloom filter for definite absence
2. If **not present** → Prune row group
3. If **might be present** → Keep row group (false positives possible)

## RowSelection Alignment

The `RowSelection` must correspond **only** to included row groups:

1. Build `row_groups: Vec<usize>` (sorted)
2. For each included group, build selection relative to that group
3. Concatenate selections in row-group order
4. Pass both `.with_row_groups(row_groups)` and `.with_row_selection(selection)` to Parquet reader

## Type Casting

### Column Casts (Conservative)
Only **no-op casts** allowed (cast to same type):
- ✅ `CAST(id AS INT64) = 100` where `id` is INT64
- ❌ `CAST(id AS STRING) = '100'` where `id` is INT64 (rejected)

**Rationale**: Non-trivial casts would require transforming statistics, risking incorrect pruning.

### Literal Casts (Compile-Time)
Performed at **compile time**:
- ✅ `id = CAST('100' AS INT64)` → compiled to `id = 100`
- ✅ `id IN (CAST('1' AS INT64), CAST('2' AS INT64))` → `id IN (1, 2)`

**Rationale**: Safe to cast literals upfront without affecting statistics.

## RoaringBitmap Support

**Use Case**: Compact serialization for remote workers

**Limitation**: u32::MAX rows (~4.2 billion)

**Behavior**:
- If `total_rows > u32::MAX` → Skip RoaringBitmap, use RowSelection only
- Otherwise → Generate both formats (configurable via `emit_roaring`)

## Error Handling

### Compilation Errors
- **Unsupported predicates**: Logged in `CompileResult::errors()`
- **Type mismatches**: Rejected with clear error messages
- **Non-literal arguments**: Rejected (metadata can't evaluate runtime values)

### Runtime Degradation
- **Missing statistics**: Conservative evaluation (keep data)
- **Missing page indexes**: Fall back to row-group pruning only
- **Missing bloom filters**: Skip bloom filter checks

## API Design

### Unified Builder Pattern

**Sync API** (no bloom filters):
```rust
let result = PruneRequest::new(&metadata, &schema)
    .with_predicate(&predicate)
    .enable_page_index(true)
    .prune();
```

**Async API** (with bloom filters):
```rust
let metadata = builder.metadata().clone();
let schema = builder.schema().clone();
let result = PruneRequest::new(&metadata, &schema)
    .with_predicate(&predicate)
    .enable_bloom_filter(true)
    .prune_async(&mut builder).await;
```

### Key Types

- **`PruneRequest`**: One-shot pruning builder
- **`Pruner`**: Stateful pruner (reuses compiled predicates)
- **`PruneResult`**: Output with row_groups, row_selection, roaring_bitmap
- **`PruneOptions`**: Configuration (page_index, bloom_filter, emit_roaring)
- **`AsyncBloomFilterProvider`**: Trait for custom bloom filter loading

## Performance Considerations

- **Metadata overhead**: Typically <1ms per row group
- **Page-level pruning**: Avoids full-page decoding when indexes enable pruning
- **Bloom filters**: Batch loading via `bloom_filters_batch()` for efficiency
- **Selection construction**: Minimal allocations via iterators

## Alternatives Considered

### Modify `parquet` Crate
**Pros**: Deeper integration, single-pass evaluation
**Cons**: High maintenance burden, upstream dependency
**Decision**: External library for easier adoption

### Physical Plan Traits as IR
**Pros**: Reuse DataFusion infrastructure
**Cons**: Runtime-coupled, heavy dependencies, complex
**Decision**: Custom IR for simplicity and safety

### Direct Evaluation (No IR)
**Pros**: Simpler implementation
**Cons**: Hard to maintain tri-state semantics, error-prone
**Decision**: IR provides clear safety guarantees

## References

- [Development Plan](development_plan.md): Implementation status and roadmap
- [CLAUDE.md](../CLAUDE.md): Development guidelines and design decisions
- [Parquet Format Spec](https://parquet.apache.org/docs/): Metadata structure
- [DataFusion Expressions](https://docs.rs/datafusion-expr): Input predicate format
- [Arrow Schema](https://docs.rs/arrow-schema): Type system
