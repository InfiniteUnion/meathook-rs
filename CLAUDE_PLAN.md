# Generic tiered memory: `Store<R>` trait + one `Tier` layer (v0.2.0)

> Execution note: first step after approval is to copy this plan to `CLAUDE_PLAN.md` at the repo root (user preference; plan mode forbids writing it there now).

## Context

Today the buffering tiers are fixed: `Buffered` hardcodes an in-memory `Vec<R>` and `DiskSpool` hardcodes JSONL-on-filesystem, and each re-implements its own windowing/flush/replay logic (~300 lines each). The user wants arbitrary, generic memory tiers between source and terminal sink — e.g. SQLite instead of JSONL — including zero tiers. Chaining is *already* arbitrary (sinks nest tower-style), so the refactor extracts the storage backend into a small `Store<R>` trait with **GAT segment handles**, and replaces both layers with one generic `Tier<R, St: Store<R>, S: Sink<R>>` that owns windowing, `FlushPolicy` firing, replay-on-startup, and retain-on-failure exactly once.

Decisions settled with the user:
- **Architecture**: `Store` trait + single `Tier` layer (not backend-per-layer, not dyn-erased).
- **Store trait**: GAT `type Segment<'a>` handles with `commit(self)` delete-after-downstream-success.
- **Backends shipping now**: `MemStore` (replaces `Buffered` internals) + `JsonlStore` (port of `DiskSpool`). `SqliteStore` deferred, but the trait must stay implementable for it without changes.
- **Compat**: clean break — delete `Buffered`, `DiskSpool`, `SpoolError`, `SinkExt::{buffered,spooled}`; bump to **0.2.0**.

Constraints: Rust 2024 / MSRV 1.88, RPITIT `+ Send` futures, concrete thiserror enums (never `Box<dyn Error>`), CI gate `clippy -D warnings -D clippy::pedantic -D clippy::perf` (docs + `#[must_use]` on all new public items).

## New public API

### `src/store.rs` — traits (new)

```rust
pub trait Store<R>: Send {
    type Error: error::Error + Send + Sync + 'static;
    /// Handle to the oldest stored window. Must be Send: the tier holds it
    /// across downstream ingest awaits.
    type Segment<'a>: Segment<R, Error = Self::Error> + Send where Self: 'a;

    /// Add records to `window` (aligned unix secs). Durable stores must be
    /// write-ahead: don't return until the records would survive a crash.
    fn append(&mut self, window: i64, records: Vec<R>)
        -> impl Future<Output = Result<(), Self::Error>> + Send;
    /// Open the oldest stored window, or None when empty.
    fn oldest(&mut self)
        -> impl Future<Output = Result<Option<Self::Segment<'_>>, Self::Error>> + Send;
    /// Pipeline name for windows replayed before any live WindowMeta.
    fn pipeline_hint(&self) -> Option<&str> { None }
}

pub trait Segment<R> {
    type Error;
    fn window(&self) -> i64;
    /// Store must retain data until commit — dropping uncommitted loses nothing.
    fn records(&mut self) -> impl Future<Output = Result<Vec<R>, Self::Error>> + Send;
    /// Remove the window; call only after downstream accepted the records.
    fn commit(self) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
```

### `src/store/mem.rs` — `MemStore<R>` (new)

`BTreeMap<i64, Vec<R>>`. `Store` impl requires `R: Clone + Send + 'static` (`Sink::ingest` consumes its `Vec<R>`, so a failing downstream swallows it; memory has no backing copy to re-read — same cost as today's `Buffered`, document in type docs). `Error = Infallible`. `MemSegment<'a, R>` holds `&'a mut MemStore<R>` + window; `records()` clones out of the map, `commit()` removes the entry, dropping uncommitted is naturally a no-op. Manual `Default` impl (avoid `R: Default` bound).

### `src/store/jsonl.rs` — `JsonlStore<R>` (new, port of disk.rs)

Layout unchanged: `{dir}/{window}.jsonl`. `JsonlStoreError` thiserror enum = old `SpoolError`'s `Io {path, source}` + `Serialize` variants (the `Downstream` variant moves to `TierError`). Fields: `dir`, `pipeline` (dir-derived, `with_pipeline_name` override — port disk.rs:80-103), `initialized` (lazy `create_dir_all`). `Store` impl requires `R: Serialize + DeserializeOwned + Send + 'static` — serde bounds live **only** here, never on `Store`/`Tier`. Port verbatim from `src/layer/disk.rs`:
- `list_segments()` oldest-first + "ignoring unrecognized file" warn (disk.rs:160-181)
- `append`: serialize lines → write_all → `sync_all` file → fsync dir if new segment (disk.rs:185-207), **minus** the `active_window/active_count` bookkeeping (moves to Tier)
- `JsonlSegment<R>` owns `{window, path, PhantomData}` — no borrow of the store needed (`type Segment<'a> = JsonlSegment<R>`); `records()` = read + parse with torn-final-line / corrupt-line warns (disk.rs:223-249); `commit()` = `remove_file`
- Sync `std::fs` inside async fns, same as today (documented as fine at target rates)

### `src/layer/tier.rs` — `Tier<R, St, S>` + `TierError` (new, replaces disk.rs + Buffered)

```rust
#[derive(Debug, thiserror::Error)]
pub enum TierError<St, S>
where St: error::Error + Send + Sync + 'static,
      S:  error::Error + Send + Sync + 'static,
{
    #[error("store error: {0}")]            Store(#[source] St),
    #[error("downstream sink error: {0}")]  Downstream(#[source] S),
}
```

`Tier` fields: `store, policy, inner, pipeline: Option<String>, initialized: bool, active_window: Option<i64>, active_count: usize, PhantomData<fn() -> R>`. Builders: `new(store, policy, inner)`, `with_pipeline_name`, `inner()` accessor. `window_secs()`/`align()` ported verbatim from disk.rs:105-113.

Unified semantics (= today's DiskSpool, now for all backends):
- **Window key** = `align(meta.end.unix_timestamp())` — uses the batch handover time from `Pipeline::tick`, not `now_utc()`, making time-based tests deterministic via crafted metas.
- **ingest**: cache first-seen `meta.pipeline`; `ensure_init` (first-use replay, errors logged not propagated — mirrors disk.rs:135-157); append non-empty batches; then drain **all** windows if `active_count >= policy.max_records`, else drain **closed** windows only.
- **flush**: set `initialized = true`, `drain(true)` propagating errors, then `inner.flush()`.
- **drain loop** (the GAT/borrow-check heart — hoist `pipeline_name()` and `window_secs()` into locals *before* the loop so no `&self` method call overlaps the live segment):

```rust
async fn drain(&mut self, include_active: bool) -> Result<(), TierError<St::Error, S::Error>> {
    let pipeline = self.pipeline_name();          // hoisted: no &self calls while seg lives
    let window_secs = self.window_secs();
    loop {                                        // loop+let-else, not while-let (2024 temporaries)
        let Some(mut seg) = self.store.oldest().await.map_err(TierError::Store)? else {
            return Ok(());
        };
        let window = seg.window();
        if !include_active && Some(window) == self.active_window { return Ok(()); }
        let records = seg.records().await.map_err(TierError::Store)?;
        if !records.is_empty() {
            let start = OffsetDateTime::from_unix_timestamp(window)
                .unwrap_or(OffsetDateTime::UNIX_EPOCH);
            let meta = WindowMeta { pipeline: pipeline.clone(), start,
                                    end: start + time::Duration::seconds(window_secs) };
            self.inner.ingest(&meta, records).await.map_err(TierError::Downstream)?;
        }
        seg.commit().await.map_err(TierError::Store)?;
        if Some(window) == self.active_window { self.active_window = None; self.active_count = 0; }
    }
}
```

`seg` borrows the field `self.store`; `self.inner` / `self.active_window` are disjoint fields — split borrows are fine. Pipeline-name resolution: explicit override → first-seen `meta.pipeline` → `store.pipeline_hint()` → `"unknown"`.

### `src/layer.rs` edits

- Delete `Buffered` + `Window` + `mod disk` / disk re-exports; add `mod tier; pub use tier::{Tier, TierError};`.
- `SinkExt`: remove `buffered`/`spooled`; add `fn tier<St: Store<R>>(self, store: St, policy: FlushPolicy) -> Tier<R, St, Self>`; keep `tee`. Update module header docs (stack example → `Tier(MemStore) → Tier(JsonlStore) → HfSink`) and `FlushPolicy` docs (`every` = aligned window width).
- `FlushPolicy` itself unchanged.

### `src/lib.rs`

Add `pub mod store;`. Re-exports: `pub use layer::{FlushPolicy, SinkExt, Tee, TeeError, Tier, TierError};` + `pub use store::{JsonlStore, JsonlStoreError, MemStore, Segment, Store};`. Update crate docs (lines 18-22) to describe `Tier` + stores.

## Files

| Action | Path |
|---|---|
| create | `src/store.rs`, `src/store/mem.rs`, `src/store/jsonl.rs`, `src/layer/tier.rs` |
| delete | `src/layer/disk.rs` (after porting) |
| modify | `src/layer.rs`, `src/lib.rs`, `src/test_util.rs`, `src/sink/huggingface.rs` (doc links :156-161), `examples/nea_weather.rs`, `README.md`, `CHANGELOG.md`, `Cargo.toml` (→ 0.2.0) |

## Implementation order

1. **Add-alongside**: create `store.rs` + `mem.rs` + `jsonl.rs`, wire `pub mod store;` — old layers still compile. `cargo build --all-features`.
2. **Tier**: create `layer/tier.rs`, add `SinkExt::tier` + re-exports. This build is the GAT/Send gate — if it fails, apply the pre-decided fallback (below).
3. **Tests**: add `meta_at(pipeline, unix)` helper to `test_util.rs` (WindowMeta with start=end=given unix ts, steers alignment deterministically); port tests per the map below. `cargo test`.
4. **Delete old API**: remove `Buffered` + its tests from `layer.rs`, delete `layer/disk.rs`, purge `Buffered/DiskSpool/SpoolError` from lib.rs:42, fix huggingface.rs doc links.
5. **Example**: `examples/nea_weather.rs` — header doc (line 4), imports (line 18: `DiskSpool` → `JsonlStore, Tier`), `Ctx::spooled_hf` (line 144) → `tiered_hf` returning `Tier<R, JsonlStore<R>, HfSink<R>>` using `.tier(JsonlStore::new(self.spool_dir.join(pipeline)), self.policy)`, 3 call sites (261/280/299).
6. **Docs & release**: README (diagram :41-43, quickstart :89-90, Durability :110-114, HF :132-134, config comment :157), lib.rs crate docs, CHANGELOG entry (breaking removals + behavior changes below), Cargo.toml 0.2.0.

## Test porting map

| Old | New |
|---|---|
| layer.rs `buffered_holds_until_max_records` | tier.rs `mem_tier_holds_until_max_records` (`.tier(MemStore::new(), ...)`) |
| layer.rs `buffered_fires_on_elapsed_window` (paused clock) | tier.rs `mem_tier_drains_closed_window_on_next_ingest` — `meta_at("p", 0)` then `meta_at("p", 301)` under `every(300s)`; assert `[1]` shipped with window-0 meta, `[2]` retained |
| layer.rs `buffered_retains_records_across_failing_downstream` | tier.rs `mem_tier_retains_records_across_failing_downstream` |
| layer.rs `flush_drains_the_whole_stack` | tier.rs same name; stack = `Tier(MemStore)` over `Tier(MemStore)` — proves Tier-over-Tier composes |
| layer.rs `tee_*` ×2 | unchanged |
| disk.rs `ingest_is_write_ahead`, `replays_leftover_segments_on_first_use` (asserts `"weather"` via `pipeline_hint`), `retains_segments_across_failing_downstream`, `max_records_drains_active_segment` | jsonl.rs, tier-level via `.tier(JsonlStore::new(dir), ...)` |
| disk.rs `tolerates_torn_final_line` | jsonl.rs store-level (`oldest()`→`records()`) + tier-level flush variant |
| new | mem.rs store-contract tests (ordering, retain-until-commit, drop-uncommitted); jsonl.rs unit tests (ignores non-jsonl files, commit deletes, oldest-first) |

## Behavior changes (intentional — CHANGELOG)

1. Mem tier windows are now wall-clock **aligned** (old Buffered: elapsed-since-first-record). A record landing at :59 of an hourly window flushes ~1 min later, not 1 h.
2. Drained `WindowMeta` is derived from the window key for **all** tiers (old Buffered used first-tick start / now-utc end). No existing test asserts the old meta.
3. Drain now **stops at the first error** instead of attempting newer segments past a failed older one (disk.rs:282-298) — preserves oldest-first delivery order; what a transactional store needs.
4. Live batches from the jsonl tier carry first-seen `meta.pipeline` instead of always the dir-derived name (replay still uses the hint) — identical in practice.

## Risk & pre-decided fallback

**GAT + RPITIT Send inference**: `Tier`'s futures must be Send while holding `St::Segment<'_>` across `inner.ingest().await`. The `type Segment<'a>: … + Send` item bound should satisfy rustc ≥ 1.88, but "implementation of Send is not general enough" is a known rough edge with generic `St`. **Fallback** (Tier logic unchanged, only the handle disappears): replace the GAT with `fn peek_oldest(&mut self) -> … Option<(i64, Vec<R>)>` + `fn commit(&mut self, window: i64)`. Still correct for SQLite (commit = keyed DELETE; Tier's `&mut` exclusivity prevents read-then-delete races).

Note for future `SqliteStore`: `rusqlite::Connection` is `!Sync`, so a segment literally holding a `Transaction<'a>` won't be `Send`; segment-as-window-id + keyed DELETE works fine under either trait shape.

## Verification

- `cargo test --all-features` and `cargo test --no-default-features`
- `cargo clippy --all-targets --all-features -- -D warnings -D clippy::pedantic -D clippy::perf` (CI gate)
- `cargo doc --no-deps`; `cargo build --example nea_weather`
- Stragglers: `grep -rn "Buffered\|DiskSpool\|SpoolError\|spooled\|\.buffered(" src examples README.md docs website`
- End-to-end smoke: existing tier/jsonl tests cover ingest→spool→flush; `tests/hf_integration.rs` untouched (ignored, needs HF_TOKEN)
