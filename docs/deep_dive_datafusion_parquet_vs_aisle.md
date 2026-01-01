# Deep Dive: DataFusion Parquet vs Aisle

This report explains how DataFusion's Parquet pruning differs from Aisle, why a direct
end-to-end comparison is often unfair, and what we plan to do (and not do) going forward.

## TL;DR

- DataFusion's Parquet datasource is a full execution component with access to **physical plan context**,
  **partition/file metadata**, and **runtime filters**.
- Aisle is a **standalone pruning library** that only uses **Parquet metadata** plus a logical predicate.
- DataFusion is broader and more integrated; Aisle is more conservative and explicit about metadata semantics.

## Shared Ground

Both systems:
- Use Parquet metadata (row-group stats, page index, bloom filters) to skip I/O.
- Must be conservative: never skip rows that might match.
- Can emit row-group selections and (when available) page-level selections.

## What DataFusion Parquet Does

DataFusion's Parquet datasource is built for a full query engine pipeline. Key capabilities:

- **Partition / file pruning** based on catalog or file listing metadata.
- **Dynamic (runtime) filtering** from joins and other execution-time signals.
- **Row-group pruning** using `PruningPredicate` (wide predicate support).
- **Bloom filter pruning** for equality and IN predicates when available.
- **Page pruning** (single-column predicates only).
- **Access plan coordination** (scan range pruning, row-group order optimization).

Important details:
- Row-group pruning currently **ignores ColumnOrder** in Parquet metadata.
- Min/max **exactness (truncation) is not modeled** in the pruning decision path.
- Page pruning is **single-column** and uses `PruningPredicate` on page stats.

## What Aisle Does

Aisle is a standalone library that focuses on correctness and explicit metadata semantics:

- **Compiles DataFusion logical Expr into a small IR** (conservative subset).
- **Tri-state evaluation** (True / False / Unknown) to avoid unsafe pruning.
- **Row-group pruning + optional page pruning**, based on Parquet metadata only.
- **Bloom filters** integrated into IR when equality/IN are present.
- **Column order + min/max exactness checks** for byte-array ordering predicates.
- Optional **row-level filtering** via `IrRowFilter` when the `row_filter` feature is enabled.

## Why Direct Comparisons Are Often Unfair

DataFusion can prune more because it sees more:

- Partition values (directory structure).
- File-level statistics (external catalogs).
- Runtime filters derived from intermediate results.
- Scan-range and ordering constraints from the physical plan.

Aisle intentionally does **not** depend on those inputs. It only sees:

- Parquet metadata
- A logical predicate expression
- Optional bloom/page index data embedded in the file

## Fair Comparison Guidelines

To compare Aisle and DataFusion pruning fairly:

1) Use a **single Parquet file** (no partition pruning).
2) Disable dynamic filters and runtime pruning features.
3) Use identical predicates.
4) Compare **row-group selections** and **page selections** only.
5) Measure pruning latency separately from scan latency.

## Strengths and Trade-offs

**DataFusion Parquet (datasource)**
- Strengths: Broad predicate coverage, deep integration, runtime pruning.
- Trade-offs: Must generalize across data sources; some metadata semantics are not modeled
  (column order, min/max exactness).

**Aisle**
- Strengths: Conservative, explicit metadata semantics; decoupled from execution engine;
  easier to reason about correctness on metadata-only inputs.
- Trade-offs: Narrower predicate/type coverage and less access to execution-time context.

## What We Plan to Do Next (Aisle)

Near-term:
- Expand **type coverage** (more integer families, temporal types, decimals where safe).
- Improve **ordering safety** (unsigned types + ColumnOrder handling).
- Broaden **page-level pruning** where metadata is trustworthy.
- Publish **transparent benchmarks** that isolate metadata-only pruning performance.

Longer-term:
- Provide an **optional adapter** for DataFusion to reuse Aisle pruning results where it
  makes sense (metadata-only stage).
- Improve documentation on **safe vs best-effort pruning** semantics.

## What We Will Not Do

- **Modify the Parquet crate or Parquet format.**
- **Depend on DataFusion runtime context** (dynamic filters, partition pruning, scan ranges)
  inside Aisle core. Those are engine responsibilities.
- **Perform unsafe pruning** when metadata is missing, ambiguous, or inexact.
- **Support non-trivial column casts** that would require transforming statistics.

---

If you want a head-to-head benchmark, we will focus on **metadata-only pruning** so the
comparison is honest and reproducible.
