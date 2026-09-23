# SpacetimeDB Bevy SDK

A Bevy-native client for [SpacetimeDB](https://spacetimedb.com). The client cache lives in the
Bevy `World`: tables are read with system params and queries, row changes arrive as messages,
reducers are called from systems, subscriptions and calls can be entities, and the connection is
a Bevy state. Nothing hands an app a callback that runs on another thread, and no row is cloned
to be read.

| Bevy | Rust | Where |
| --- | --- | --- |
| `0.20.0-rc.1` | 1.96.0 or newer | `sdks/bevy`, with its own toolchain; see [DEVELOP.md](DEVELOP.md) |

## Using it

1. Generate bindings for your module:

   ```sh
   spacetime generate --lang bevy -p path/to/your/module -o src/module_bindings
   ```

   The crate holding the bindings must depend on `bevy` or on `bevy_reflect`: generated rows
   derive `Reflect`, and the derive finds the crate through your `Cargo.toml`. A Bevy game already
   does.

2. Add the plugin, subscribe, and write ordinary systems:

   ```rust,ignore
   mod module_bindings;
   use module_bindings::{Player, RemoteModule, RemoteReducers};
   use spacetimedb_bevy::*;

   App::new()
       .add_plugins(DefaultPlugins)
       .add_plugins(
           StdbPlugin::<RemoteModule>::new()
               .entity_table::<Player>()
               .connect_to(ConnectOptions::new("http://127.0.0.1:3000", "my-database")),
       )
       .add_systems(Startup, |mut commands: Commands| {
           commands.subscribe::<RemoteModule, _>(["SELECT * FROM player"]);
       })
       .add_systems(Update, walk.run_if(connected::<RemoteModule>()))
       .run();

   fn walk(reducers: Reducers<RemoteModule>, players: Query<&Row<Player>>) {
       reducers.move_to(1.0, 2.0); // generated: one method per reducer
   }
   ```

[`examples/quickstart-chat`](examples/quickstart-chat) is the shortest complete client: connect,
subscribe, read a table, call a reducer, handle the answer, in a window with a text field.

### The pieces

| You want to | Use |
| --- | --- |
| Read a table | `Rows<T>` system param: `iter`, `get(&pk)`, `find(Column, &value)` for a unique column, `len`, `iter_changed`. Works for every table, however it is stored. |
| Have rows as entities | `StdbPlugin::entity_table::<T>()`, then `Query<&Row<T>>`. See below. |
| React to changes | `MessageReader<RowInserted<T>>`, `RowUpdated<T>` (has `old` and `new`), `RowDeleted<T>`, and `RowEvent<T>` for event tables. |
| Subscribe | `commands.subscribe_to::<M, _>(query::player().filter(\|p\| p.score.gt(10)))` with a typed query from the generated `query` module, or `commands.subscribe::<M, _>([sql, ..])` with SQL. Either spawns a `Subscription<M>` entity: it waits for a connection, is sent again after a reconnect, and despawning it unsubscribes. Observe `SubscriptionApplied` or `SubscriptionFailed` on it. |
| Call a reducer | `Reducers<M>` param, then read `MessageReader<ReducerResult<R>>`. Or spawn a `ReducerCall<R>` entity with your own context components and observe `ReducerFinished` on it. `Procedures<M>` and `ProcedureCall<P>` are the same for procedures. |
| Know the connection | `State<StdbState<M>>`, holding a `ConnectionState`; the `StdbIdentity<M>` resource while connected; and the `Connected<M>`, `ConnectionLost<M>` and `Disconnected<M>` events. Gate a system with `run_if(connected::<M>())`. |
| Talk to two databases | Add an `StdbPlugin` per module. Everything above names its module, so the two connections keep their own state, identity and reconnect policy. |
| Connect later, or disconnect | `commands.connect::<M>(options)`, `commands.disconnect::<M>()`, or the `StdbConnection<M>` resource. |
| Reconnect | `StdbPlugin::with_reconnect(ReconnectPolicy::Backoff { .. })`; the policy is a resource and can change at run time. |

### Where rows live

Every table is readable through `Rows<T>`, and is one of two things, chosen per table on the plugin:

- **A store** (the default): the rows are in a `TableStore<T>` resource, a dense `Vec` with an
  index by primary key, built the first time a system calls `Rows::get` on the table. Fastest to
  apply and to look up. Right for data: chat, inventory, configuration.
- **Entities**, with `entity_table::<T>()`: each row is one entity, and the row is an immutable
  `Row<T>` component on it. An update replaces the component on the same entity, so
  `Changed<Row<T>>` sees it and whatever the app put on the entity stays. When the row leaves, the
  entity is despawned, or kept and marked `RowLeft<T>` with `keep_entities_of::<T>()`. Right for
  rows that are things in the game. `entity_table_in::<T, G>()` puts several tables that share a
  primary key on the same entity.

`without_row_messages::<T>()` skips the row messages for a large table the app only ever reads
through `Rows`, saving a clone per row when a snapshot lands.

### When your systems see what

Transactions are applied one at a time, in the order the server sent them, and each is finished
before the next begins. A system in the `StdbTransaction` schedule runs once per server message,
after that message's rows are in place and before the next is applied: if it reads
`RowInserted<Message>` and looks the sender up in `Rows<User>`, the user is there, as the server's
ordering promises. The same reader in `Update` gets every message of the frame in order, but
`Rows` already shows the state after the last of them. Use `Update` when the message itself is
all you need, `StdbTransaction` when a handler looks at other rows.

With two modules, a handler that works per transaction rather than per message says which one it
is about: `snapshot_scores.run_if(from_module::<Arena>())`.

### Losing the connection

A requested `disconnect` empties the tables. A dropped connection keeps them readable while the
client retries under its `ReconnectPolicy`, and calls in flight fail with `ConnectionLost`. Once
back, every live subscription goes out as one `SubscribeBatch`, the server answers with one
snapshot, and that snapshot is reconciled against the rows that were kept: the app hears
`RowUpdated` for what moved and nothing for what did not, and an entity-stored row keeps its
entity.

## How it works

The IO task pool parses a message and works out what it changes on net, without touching the
`World`; a long row list is decoded in parts over the async compute pool. One exclusive system on
the main thread applies that to the tables, writes the row messages and runs the `StdbTransaction`
schedule. [DEVELOP.md](DEVELOP.md) says what each file owns and how to work on the crate.

## Features

| Feature | Default | What it adds |
| --- | --- | --- |
| `tls` | yes | `wss://` and `https://` hosts. A browser build never uses it. |
