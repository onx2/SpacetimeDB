# Plan: Host-Side Query Execution for Spatial / Selective Reads

Status: proposal. Companion to `MULTI_RANGE_AND_SPATIAL_FINDINGS.md`.
Revision 2: re-centered on the **query-builder + views** as the primary surface (was: a bespoke
reducer predicate ABI, now demoted to an in-reducer complement).
Revision 3: verified the view refresh/read-set path in code. **Open Question #1 is resolved YES**
(body imperative reads ARE tracked); §5.1's "main missing piece" framing was wrong and is
corrected; added §4A on the mixed body+`Query` semantics.

## 1. Objective

Let a declarative query run **host-side** so only matching rows cross the WASM boundary, instead
of pulling candidate rows into a reducer/view body and filtering them in fuel-metered WASM. The
motivating case is a spatial neighborhood ("entities in my 3x3 chunk grid" / AOE radius), but the
mechanism is general (predicate pushdown / `WHERE`-clause execution) and aligns with the team's
SQL-compliance direction.

Cost context (see findings doc §1): billed energy = wasmtime fuel = WASM instructions; host-native
work is unmetered. The win is moving filtering off the WASM/fuel path and crossing fewer rows.

## 2. Primary surface: query-builder + views (NOT a new reducer predicate type)

The query-builder (`crates/query-builder/`) builds a **SQL string** (`Query::into_sql()`), which
the SQL engine parses → `crates/physical-plan` → `crates/execution` executes host-side using
`PhysicalExpr::eval_bool_with_params`. A `#[spacetimedb::view]` returns a `Query<T>` that the host
evaluates and (for subscriptions) maintains incrementally. So:

> "Host-side predicate" == "query-builder `where`" == the SQL engine's row filter. The
> query-builder is the user-facing surface; views/subscriptions are the host-side execution path.

This subsumes the previously-proposed bespoke `pred!` macro for the view/subscription case. A
reducer-callable filtered read remains a *complement* for in-reducer imperative use (§7).

## 3. Current query-builder capability (from `expr.rs`, `join.rs`, `lib.rs`)

Supported:
- `from(table)`, chained `where`/`filter`: `BoolExpr` = `Eq/Ne/Gt/Lt/Gte/Lte/And/Or/Not`, operands
  are Column or Literal; column-vs-column and column-vs-literal; multiple `where` AND together.
- Range via `c.x.gte(lo).and(c.x.lte(hi))`.
- `left_semijoin` / `right_semijoin`: **equi-join only** (`IxJoinEq` = `lhs_col = rhs_col`), on
  **indexed** columns, **two tables**, returns one side; plus per-side `where`.

Not supported (relevant gaps):
- Arithmetic in expressions (`chunk_x - 1`): operands are only Column/Literal.
- Non-equi / band joins (`other.x BETWEEN me.x - 1 AND me.x + 1`).
- Self-joins, table aliases, >2-table join chains.
- Query parameters (inject `ctx.sender()`); projection (`SELECT col` — currently `SELECT *`/`tbl.*`).

## 4. The motivating query and the two approaches

Query: for the caller, find entities in the 3x3 chunk neighborhood of the caller's online
character.
```
sender identity ─▶ online_character WHERE identity = sender ─▶ actor_id
actor_id        ─▶ spatial (me) WHERE actor_id = …          ─▶ zone, chunk_x, chunk_z
(zone, x±1, z±1)─▶ spatial (others) in the 3x3 box
```

### Approach A (recommended): resolve scalars in the view body, return a declarative box query
The view body (runs once per evaluation, with caller context) does the two point lookups
imperatively and computes integer bounds, then returns a comparisons-only box query with the
bounds **baked as literals**. Handle the offline/not-placed case with an **unsatisfiable box**
(`x_lo > x_hi`), NOT an early `return` (the macro rewrites the body into a single
`Query::into_sql({ body })` expression, so early returns of non-`RawQuery` types don't typecheck):
```rust
#[view]
fn spatial_neighbors(ctx: &ViewContext) -> impl Query<Spatial> {
    // body: cheap point lookups, runs once (not per result row); recorded as view deps
    let me = ctx.db.online_character().identity().find(ctx.sender())
        .and_then(|c| ctx.db.spatial_tbl().actor_id().find(c.actor_id));
    let (zone, x_lo, x_hi, z_lo, z_hi) = match me {
        Some(s) => (s.zone,
            s.chunk_x.saturating_sub(1), s.chunk_x.saturating_add(1),
            s.chunk_z.saturating_sub(1), s.chunk_z.saturating_add(1)),
        None => (Zone::NONE, 1, 0, 1, 0), // unsatisfiable -> empty result
    };
    ctx.from.spatial_tbl().r#where(move |c|
        c.zone.eq(zone)
         .and(c.chunk_x.gte(x_lo)).and(c.chunk_x.lte(x_hi))
         .and(c.chunk_z.gte(z_lo)).and(c.chunk_z.lte(z_hi)))
}
```
The box uses only `where` comparisons + `and` — **expressible with today's builder**. No joins,
arithmetic, or SQL params required. The host evaluates the box host-side and returns only matches.
**Projection is not available**: the view must return whole `Spatial` rows (`run_query_for_view`
rejects a return-table row type != the view's expected row type, and the builder emits `SELECT *`),
so read `.actor_id` client-side; `impl Query<ActorId>` is not expressible.

### Approach A semantics — combining a procedural body with a `Query` return (VERIFIED)
When a view body does imperative `ctx.db` reads AND returns a `Query`, both halves write into ONE
shared read set keyed by `ViewCallInfo { view_id, sender }`:
- The body runs under `func_type = View(...)` (`WasmtimeInstance::call_view` →
  `start_funcall(.., op.call_type())`), so its imperative reads are recorded as view deps:
  **point lookups precisely** (`record_index_scan_point` → `(table, cols, key)`), **range scans
  coarsely** (`record_index_scan_range` with `point=None` degrades to full-table).
- `run_query_for_view` then records the returned query's reads, **coarsely** (full-table
  `record_table_scan`) for every table the plan touches.

Refresh = **full re-run of the whole view function** (`call_view_inner` invokes the WASM export =
body + return), so **baked bounds are recomputed every refresh — never frozen across refreshes**.
Correctness for the box view (no staleness):
- player moves → me's `spatial` row del+ins matches the body's precise PK read → re-run →
  bounds recomputed → new baked SQL;
- entity enters/leaves box → any `spatial` write hits the query's coarse table read → re-run;
- player comes online → `online_character[sender]` insert matches the body's precise sender read
  (this table is not read by the returned query, so the body read is what catches it).
The coarse table read from the returned query is also a correctness backstop for any path where the
body short-circuits before reading a row. **The earlier worry about "two freshness levels / frozen
bounds" does not apply**, because the body re-executes on every refresh.

Cost/granularity implication: per (re-)eval the **billed WASM** work is only the body's 2 point
seeks + SQL-string build; the box scan/filter/materialize is host-side (unmetered). The price is
**over-refresh**: the query's coarse table read re-runs the view on *any* `spatial` write, even
outside the box. This over-refresh is **identical for a fully declarative query** (same
`run_query_for_view` coarse recording), so Approach B does not reduce it — it would only move the 2
point seeks host-side, at the cost of the engine features in §6.

### Approach B: one fully declarative correlated query
Express the whole thing as a single SQL query (3-table join incl. a `spatial`-to-`spatial`
self-join, a band join `other.chunk_x BETWEEN me.chunk_x-1 AND me.chunk_x+1`, arithmetic, a
`:sender` parameter, projection). Almost none of this exists in the builder or SQL engine. Large
effort; not required if A works.

## 5. What's remaining (Approach A — the pragmatic path)

1. **Caller-parameterized views — already supported; body-read tracking already works (VERIFIED).**
   `ViewContext` already injects `sender()` (`crates/bindings/src/lib.rs`), and the body's
   imperative reads are already recorded as view deps because the body runs under
   `func_type = View(...)` (see §4A). So the prior "must cover body reads / main missing piece for
   correctness" claim was wrong: re-running the body on refresh recomputes the baked bounds, and
   me-moves are caught by the precise PK read. **No new framework work is required for Approach A
   correctness.** The only residual is granularity (over-refresh, §4A) — a performance, not
   correctness, concern, and no worse than fully declarative.
2. **Incremental view maintenance (IVM) of the box range-scan** as *other* entities move in/out of
   the grid. This is the **dominant cost/complexity and the go/no-go gate** — efficient spatial
   change propagation (which subscriptions overlap a moved entity's chunk), not a naive re-scan.
3. *(Optional optimization)* planner integration so a two-range box uses the multi-range/skip-scan
   engine primitive prototyped in `crates/table/src/table.rs` (`seek_multi_range`) rather than
   `(zone, chunk_x range)` + residual `chunk_z` filter. The engine primitive already exists.
4. *(Optional, separate)* **exact radius** (`(x-px)²+(z-pz)² < r²`): add arithmetic ops or a
   `within_distance` builtin through query-builder expr → SQL grammar → `PhysicalExpr` (currently
   comparison-only) → executor. Recommend a dedicated `within_distance` builtin over general
   arithmetic (smaller, deterministic, sqrt-free via squared distance).

## 6. What Approach B additionally requires (if fully declarative is ever desired)
- Parameters in queries (bind `ctx.sender()`), projection (`SELECT col`).
- Self-joins + table aliases; >2-table join chaining.
- Non-equi / band joins, and a planner strategy to execute them as a correlated index range scan
  (the multi-range/skip scan per driving row).
- Arithmetic in the expression language end-to-end.

## 7. Complement: in-reducer host-side filtered read
Views serve subscriptions/continuous queries. A **reducer** that must filter host-side (e.g. apply
AOE damage in-reducer, not stream to clients) is not served by views. Options:
- Let reducers execute a query-builder `Query` and iterate its (host-filtered) results, or
- A reducer ABI that carries a serialized predicate (subset of `PhysicalExpr`: Field/Value/
  Compare/And/Or/Not), evaluated host-side via the existing `ApplyFilter` + `eval_bool_with_params`
  (`crates/datastore/.../state_view.rs`, `crates/physical-plan/src/plan.rs`).
Both reuse the existing host evaluator; keep behind `unstable`. The predicate must be
host-evaluable (an expression), never an arbitrary WASM closure — see §9.

## 8. Phasing
- **P1 — box neighborhood as a (parameterized) view.** Box query + procedural body work with
  today's builder/framework (§4A: caller injection and body read-set deps already exist). Delivers
  host-side execution for the neighborhood with no framework changes for a *one-shot* view. Gate:
  §5.2 IVM for a usable *live subscription*.
- **P2 — multi-range planner integration (§5.3).** Use `seek_multi_range` for the box.
- **P3 — exact radius (§5.4).** `within_distance` builtin through the stack.
- **P-complement — reducer-callable host-side filter (§7)** for in-reducer use.

## 9. Constraints / determinism / safety
- **Host-evaluable only.** Predicates/queries are declarative (SQL / `PhysicalExpr`), never WASM
  callbacks. An arbitrary Rust closure is WASM and cannot run host-side without a per-row crossing
  + row decode (strictly worse). A closure-shaped surface must be a compile-time-lowered expression
  (restricted), not arbitrary code.
- **Determinism (replication).** `AlgebraicValue` comparisons are deterministic; any added
  arithmetic/distance must use IEEE-754 (no fma/fast-math), squared distance, defined NaN behavior.
- **IVM correctness** is the dominant risk: a parameterized spatial subscription must stay fresh as
  both the subscriber and other entities move, without re-scanning the world per move.

## 10. Effort (rough)
- §5.1 caller-parameterized views + body dep tracking: **already implemented** (verified §4A); zero
  for one-shot correctness. Optional: finer-grained read-set tracking to cut over-refresh (medium).
- §5.2 IVM for spatial range subscriptions: large / research-adjacent — **the real cost**.
- §5.3 multi-range planner integration: small-medium (primitive exists).
- §5.4 `within_distance` builtin: medium (grammar + expr + executor + builder + tests).
- §7 reducer complement: ~1.5–2.5 weeks (ABI + predicate codec + bindings ergonomics).

## 11. Testing & measurement
- Correctness: declarative result == imperative result (proptest), across commit+tx state + deletes.
- Determinism: identical inputs → identical ordered results across runs/replicas.
- Cost: extend `crates/bench/benches/index_scan_gate.rs` (reports **fuel** + wall). Compare the
  imperative WASM-side neighborhood (today) vs the host-side view/query for fuel (expect large
  reduction from not crossing/decoding non-matches) and wall. Add an IVM-churn benchmark (entities
  moving) — that, not the one-shot query, is the metric that decides viability.

## 12. Open questions
1. ~~Can a view body do imperative `ctx.db` reads AND return a `Query`, with those body reads
   tracked as re-evaluation dependencies?~~ **RESOLVED: yes** (verified, see §4A). Body runs under
   `func_type = View`, point reads tracked precisely, range/query reads tracked coarsely; refresh
   re-runs the whole body so baked bounds recompute. No new work needed for correctness.
2. Is the cost target a live **subscription** (IVM required) or one-shot evaluation (much cheaper)?
3. Radius: dedicated `within_distance` builtin (recommended) vs general arithmetic in the expr stack?
4. In-reducer need (§7): execute a query-builder `Query` from a reducer, or a separate predicate ABI?
