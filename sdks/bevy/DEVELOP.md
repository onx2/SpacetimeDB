# Notes for maintainers

## Why this crate is outside the workspace

`sdks/bevy` is listed under `exclude` in the repository's root `Cargo.toml`. Bevy 0.20 declares
`rust-version = "1.96.0"`, and the repository's `rust-toolchain.toml` pins an older toolchain, so
the crate cannot be a workspace member until the repository's toolchain moves. It has its own
`rust-toolchain.toml`, its own `Cargo.lock`, and reaches the rest of the repository through path
dependencies on `crates/lib`, `crates/sats`, `crates/client-api-messages` and
`crates/query-builder`.

Consequences:

- `cargo test --all` at the root does not build it. `cargo ci test` and `cargo ci lint` run it
  from its own directory; see `tools/ci/commands/{test,lint}/src/main.rs`.
- Dependencies are spelled with versions here rather than inherited with `.workspace = true`.
- When the root toolchain catches up with Bevy's, move the crate into `members`, drop the
  toolchain file and the lockfile, and switch the dependencies to workspace inheritance.

## Layout

| Path | What it is |
| --- | --- |
| `src/protocol/` | The part with no Bevy in it: the websocket transport (`connection/`), the traits generated bindings implement (`module.rs`), and the refcounting that works out what a message changes on net (`row_set.rs`). Generated bindings reach it as `spacetimedb_bevy::__codegen::core`, so its exported names are a compatibility surface. |
| `src/connection.rs` | `StdbConnection<M>`, `StdbState<M>`, the one exclusive system that applies every message, reconnecting. |
| `src/pairing.rs` | Working out a message's net change on the parsing task; every row named by a ticket the storages key on. |
| `src/apply.rs`, `src/store.rs`, `src/entity.rs`, `src/unique.rs` | Applying a transaction, the two storage kinds, and unique-column indexes. |
| `src/subscription.rs`, `src/call.rs`, `src/messages.rs` | Subscription and call entities, and the row messages. |
| `src/reflect.rs` | Opaque `Reflect` stand-ins for SpacetimeDB's own types, which generated rows name. |
| `tests/` | The suites. `*_bindings/` directories are generated; see below. `live.rs` needs a server and is ignored by default. |
| `examples/quickstart-chat/` | A package of its own, so that `cargo test` here does not build the whole engine. |

## The codegen backend

`crates/codegen/src/bevy.rs` is the `Lang` implementation behind `spacetime generate --lang bevy`.
It builds and is tested with the repository's toolchain like the other backends; only the runtime
crate needs the newer one. Its snapshot is `crates/codegen/tests/snapshots/codegen__codegen_bevy.snap`.

## Regenerating the test bindings

From the repository root, with a built CLI (the schema extractor is the standalone binary beside
it, or whatever `SPACETIMEDB_SCHEMA_EXTRACTOR` names):

```sh
cargo run -p spacetimedb-cli -- generate --lang bevy -y \
    -p templates/chat-console-rs/spacetimedb -o sdks/bevy/tests/chat_bindings
cargo run -p spacetimedb-cli -- generate --lang bevy -y \
    -p modules/sdk-test-view -o sdks/bevy/tests/view_bindings
cargo run -p spacetimedb-cli -- generate --lang bevy -y \
    -p templates/chat-console-rs/spacetimedb -o sdks/bevy/examples/quickstart-chat/src/module_bindings
```

## Running the tests

```sh
cd sdks/bevy
cargo test                                              # everything that needs no server
cargo test --test live --test reconnect -- --ignored    # with `spacetime start` and the chat module published; see tests/live.rs
```

`tests/reconnect.rs` puts a TCP proxy it controls between the client and the server, cuts or
stalls it, and checks what the app is told, including that a reconnect sends one `SubscribeBatch`
and reports only the rows that changed while the client was away.
