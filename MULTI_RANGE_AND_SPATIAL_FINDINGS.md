# Multi-Range / Skip-Scan & Spatial Query Investigation — Findings

Status: investigation + prototype complete. Prototype gated behind the `unstable` feature.
Audience: SpacetimeDB engine maintainers.

This document records what we built, what we measured, and what we concluded while
exploring how to make multi-dimensional / spatial queries (the motivating case: a game's
"entities in a 3x3 chunk neighborhood" / AOE-radius lookup) cheaper and faster.

The single most important takeaway is at the bottom: **the lever that actually reduces the
billed cost of these queries is host-side predicate evaluation (predicate pushdown), not any
index or data-structure change.**

---

## 1. The cost model that governs everything

Reducers run in WASM (wasmtime). The billed unit is **energy ≈ wasmtime fuel ≈ WASM
instructions executed** (`crates/client-api-messages/src/energy.rs`: *"1:1 to wasmtime fuel …
represents a CPU instruction"*; wasmtime is configured with `consume_fuel(true)` in
`crates/core/src/host/wasmtime/mod.rs`).

Crucial consequence: **host (native Rust) work is NOT fuel-metered.** Index descents, row
fetches, distance math done host-side cost the user nothing in billed energy. Only WASM-side
work is billed: argument serialization, the syscall/boundary crossing, decoding returned rows,
and any logic the reducer runs in WASM.

Therefore, for a query that returns `K` rows, the billed-cost floor is:

```
billed ≈ (boundary crossings) + (rows decoded in WASM) + (WASM-side logic)
       ≥ 1 crossing + K decodes
```

Two distinct cost axes, often conflated:
- **Billed cost (fuel)** — what the end user pays. Driven by WASM instructions.
- **Wall-clock (host CPU)** — what the operator pays in capacity. Driven by host-side work.

A change can help one and not the other. Most of this investigation found changes that help
wall-clock slightly (or not at all) and do nothing for billed cost.

---

## 2. What we built: multi-range ("skip scan") `filter`, behind `unstable`

Goal: let `filter((0..=2, 0..=2))` express a grid/box query in one call instead of N calls
(one `(eq, range)` scan per leading value). The composite B-tree only supports a single
contiguous range (equality prefix + one trailing range), so a true box is not a contiguous
slice of the key order; it requires a **loose index scan / skip scan** that, for each distinct
leading value present in the index, performs a sub-scan.

Implemented (all gated by `#[cfg(feature = "unstable")]` on the module-facing side; host side
always present):

| Layer | File | What |
|---|---|---|
| Engine iterator | `crates/table/src/table.rs` | `TableAndIndex::seek_multi_range` — explicit-stack DFS skip scan, no boxing/recursion/per-row alloc; reads only the leading column per probe |
| Bounds decode | `crates/table/src/table_index/mod.rs` | `TableIndex::multi_bounds_from_bsatn` — per-column `Bound` decode |
| Datastore | `crates/datastore/src/locking_tx_datastore/mut_tx.rs` | `index_scan_multi_range`, `IndexScanMultiRanged` (commit+tx skip scans chained via existing `ScanMutTx::combine`, tx-delete filtering) |
| Host ABI | `crates/core/.../wasm_common.rs`, `wasmtime/wasm_instance_env.rs`, `instance_env.rs`, `mod.rs` | new `spacetime_10.6` syscalls `datastore_index_scan_multi_range_bsatn` (+ delete variant); `IMPLEMENTED_ABI` bumped `(10,5)→(10,6)` |
| sys bindings | `crates/bindings-sys/src/lib.rs` | gated `spacetime_10.6` externs + safe wrappers |
| bindings API | `crates/bindings/src/table.rs`, `crates/lib/src/filterable_value.rs` | `filter`/`delete` overloaded via a uniform `IndexScanRangeBoundsTerminator` macro, `cfg`-mutually-exclusive with the stable macro (no trait-resolution ambiguity; default builds byte-for-byte unchanged) |
| Tests / bench | `crates/table/src/table.rs` (5 unit/proptests), `modules/perf-test`, `crates/bench/benches/index_scan_gate.rs` | correctness across AV + `BytesKey` paths, and a fuel+wall benchmark |

Non-breaking: the new syscalls follow the documented additive-minor-version convention
(`VersionTuple::supports` keeps every `≤10.5` module working). Rust-only; no C#/C++/TS changes.
Correctness validated by a brute-force property test plus deterministic grid / 3-column /
excluded-unbounded / `BTreeAV`-path tests.

---

## 3. What we measured

Benchmark: `crates/bench/benches/index_scan_gate.rs` against `modules/perf-test`, on a
`(zone, chunk_x, chunk_z)` index (the real-world shape: leading equality + skip column + range).
Reported per query, 1000 queries/run; **fuel is deterministic**, wall-clock is the median.

```
DENSE 3x3 neighborhood  (every chunk_x in band populated)
  baseline (3 calls)   fuel = 10,904 / query    wall = 1.42 ms / 1k
  skip scan (1 call)   fuel =  9,771 / query    wall = 1.91 ms / 1k
    ENERGY (fuel): 1.12x cheaper      WALL: 0.74x (skip ~1.35x slower)

SPARSE wide band, radius 8  (17 chunk_x, ~5 populated)
  baseline (17 calls)  fuel = 25,926 / query    wall = 5.41 ms / 1k
  skip scan (1 call)   fuel = 15,747 / query    wall = 3.02 ms / 1k
    ENERGY (fuel): 1.65x cheaper      WALL: 1.79x FASTER
```

Derived constants and model:
- Each eliminated boundary crossing ≈ **566 fuel** (serialize args + syscall + iterator setup/teardown).
- **Billed saving ≈ `(N − 1) × 566` fuel**, where `N` = crossings collapsed. It is *fixed*
  regardless of how many rows are returned, because per-row decode is identical either way.
- Per-row decode ≈ ~250 fuel/row. So the saving's *fraction* shrinks as result size grows:

| Result rows | Fuel saving (3-wide band) |
|---|---|
| ~10 | ~30% |
| ~40 | ~10% |
| ~250 | ~1.8% |
| ~1000 | ~0.45% |

Wall-clock crossover: a manual loop is *told* the leading values (1 B-tree descent each); skip
must *discover* them (~2 descents per group — discover + scan). For a dense contiguous band the
manual loop is descent-optimal, so skip is ~1.35x slower wall-clock (not removable: the extra
descents are fundamental). For a sparse/wide band the manual loop wastes a call + descent per
absent value, so skip wins both.

---

## 4. Verdict on multi-range / skip scan

- **Always wins billed cost** (1 crossing vs N), but the win is **negligible for dense grids
  with many entities** (e.g. 3x3 with 1000 entities ≈ 0.45% fuel) and comes with a wall-clock
  regression there.
- **Wins both metrics for sparse/wide bands** and for **small result sets with many crossings**,
  and is the only practical option when leading values are **unknown / non-contiguous** (you
  can't write the manual loop without first scanning to find them).
- For the user's stated dense 3x3 spatial case specifically, it is mostly **ergonomic sugar**.

Maintenance cost to weigh: a permanent (additive) ABI surface, a parallel bindings macro that
must track the stable one, the host iterator, and the multi-bounds decoder. Reasonable to keep
behind `unstable` for the sparse/unknown-leading-value cases; not justified by the dense case alone.

---

## 5. Spatial indexes and other structures — all rejected for this use case

We evaluated whether a different index/structure could beat the B-tree for box/radius queries.
The decisive fact: **every index lives host-side, so none of them changes the billed cost**
(`1 crossing + K decodes`). They only affect host CPU and write cost.

| Approach | Billed cost | Wall-clock (dense, 1000 rows) | Why rejected |
|---|---|---|---|
| Multi-range skip scan | floor | ~1.35x slower | extra discovery descents; negligible fuel win at scale |
| R-tree / R*-tree | **0% better** | small net (decode dominates) | host-side → no billed change; **worse write churn** on moving entities; heavier storage; needs SQL/MM spatial extension to be SQL-accessible |
| Morton / Hilbert curve on B-tree | **0% better** | modest | same row set + 1 crossing; only tighter 2D clustering server-side |
| Finger search (nightly cursor / custom B-tree) | **0% better** | <1% at 1000 rows (descents are <5% of wall) | accelerates resume-iteration, not the O(log n) *jumps* that dominate; nightly toolchain cost |
| Intra-query parallelism | **0% better** | marginal-to-negative | host-side; bottleneck is the single-threaded WASM consumer; steals cores from the server's inter-query parallelism; determinism concerns |

Continuous-coordinate note: a composite B-tree on continuous `(f32, f32)` degenerates to an
`O(slab)` scan (the leading column is ~unique per entity, so a box query scans the whole vertical
strip). That is *why* games discretize into integer chunks — chunking restores a low-cardinality
leading column. An R-tree would fix the read shape (`O(box)`), but at a write-churn cost that is
disqualifying for per-tick-moving entities (see §6), and still 0% billed benefit.

Floats and indexing (per `https://spacetimedb.com/docs/tables/indexes/`): `f32/f64` are not
valid B-tree index keys (NaN has no total order; equality is a footgun). The documented
workaround is to scale to integers (e.g. `×1000` → `i32`). An R-tree *could* index floats
(it uses a total order + distance, never equality), but the write-churn problem remains.

---

## 6. The dominant cost for moving entities is writes, not reads

SpacetimeDB updates are **delete + re-insert of the whole row**, not in-place
(`crates/datastore/.../mut_tx.rs::update`; `Table::insert_into_indices` / `delete_from_indices`
iterate **every** index). So:

> Each row update re-indexes that row in **every** index on the table
> (`delete(old) + insert(new)`, ~`2 × O(log n)` per index), per updated entity, per tick.

- Index granularity (`×1000` vs chunk) is only a **second-order** effect: coarse keys make an
  in-chunk move re-insert the *same* key (same node, no rebalancing — cheaper) vs `×1000`'s key
  *relocation*. But the whole-row delete+insert happens regardless because the row bytes changed.
- The real lever for write cost is **data modeling**: keep fast-changing position in an
  *unindexed* row/table, keep the indexed *bucket* in a row that only changes on chunk-crossing,
  and minimize indexes on hot tables. Not an index-structure choice.
- This is another reason R-trees are wrong here: their insert/delete (MBR propagation,
  splits/merges, R* reinsertion) is more expensive than a B-tree's, amplifying per-tick churn.

---

## 7. The actual lever: host-side predicate pushdown (NOT an index)

For AOE/radius, the current pattern is: index/scan → return **candidate** rows (the bounding
box) to WASM → decode each + compute distance in WASM → keep the matches. The waste is:
extra crossings, decoding the box-minus-circle overshoot, and per-row distance math — **all
fuel-metered**.

If the host evaluated the predicate and returned **only matches**, the billed cost drops to
`1 crossing + K_matches decodes`, eliminating the overshoot decodes and the distance math from
fuel. For selective predicates generally (not just spatial), this is a large billed win
(e.g. 100 of 10,000 rows match → ~99% fewer billed decodes).

Findings on feasibility:
- **Not available to reducers today.** The reducer ABI exposes only full scan and index
  point/range/multi-range scan. No predicate-filter syscall; no reducer-side SQL.
- **The host already has the evaluator.** `crates/datastore/.../state_view.rs` has a
  `RowFilter` trait (`RangeOnColumn`, `EqOnColumn`) + `ApplyFilter` used for no-index column
  filters, and `crates/physical-plan/src/plan.rs` has `PhysicalExpr::eval_bool_with_params(row,
  params) -> bool` (Field / Value / Param / BinOp comparisons / LogOp And/Or) used by the
  SQL/subscription engine to filter rows host-side. The gap is **exposure**, not capability.
- Caveats: the predicate must be **host-evaluable** (an expression language), not an arbitrary
  WASM closure; and exact-circle radius needs an extension because `PhysicalExpr::BinOp` is
  comparison-only (no arithmetic) — the **box** is expressible today, the **circle** needs
  arithmetic ops or a built-in `within_distance`.

This is general (predicate pushdown for selective reads), it aligns with the team's SQL
direction (it is WHERE-clause pushdown), and it is the only thing examined that reduces the
**billed** cost below the multi-range floor. See `HOST_SIDE_PREDICATE_PLAN.md`.

---

## 8. Recommendations

1. **Keep multi-range/skip scan behind `unstable`** for the sparse/wide and unknown-leading-value
   cases. Do not invest further (e.g. eager enumeration) purely for the dense 3x3 case.
2. **Do not build a spatial index (R-tree/quadtree/Morton) for dynamic entity positions.** It is
   0% billed benefit, worse write churn, heavier storage, and adds non-core SQL surface. The
   chunk-grid pattern is already the right tool for moving entities.
3. **For write cost, recommend data modeling** (separate hot position from the indexed bucket;
   fewer indexes on hot tables) rather than any structural change.
4. **Pursue host-side predicate pushdown** as the real performance/cost lever — general, aligned
   with SQL compliance, and reuses the existing host evaluator. Detailed plan in
   `HOST_SIDE_PREDICATE_PLAN.md`.
