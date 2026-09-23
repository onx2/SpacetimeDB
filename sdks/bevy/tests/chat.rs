//! The apply step, row messages and `Rows` against hand-written chat bindings. No network.

mod chat_bindings;

use bevy_app::{App, Update};
use bevy_ecs::prelude::*;
use chat_bindings::{Message, RemoteModule, RemoteUpdate, User};
use spacetimedb_bevy::__codegen::core::TableUpdate;
use spacetimedb_bevy::__codegen::lib::{Identity, Timestamp};
use spacetimedb_bevy::{
    apply_update, RowDeleted, RowInserted, RowUpdated, Rows, StdbPlugin, StdbTransaction, TransactionSeq,
};

fn identity(byte: u8) -> Identity {
    Identity::from_byte_array([byte; 32])
}

fn user(byte: u8, name: &str, online: bool) -> User {
    User {
        identity: identity(byte),
        name: Some(name.into()),
        online,
    }
}

fn users(inserts: Vec<User>, deletes: Vec<User>) -> RemoteUpdate {
    RemoteUpdate {
        user: TableUpdate::from_rows(inserts, deletes),
        ..Default::default()
    }
}

#[derive(Resource, Default)]
struct Log(Vec<String>);

fn names(users: &Rows<User>) -> Vec<String> {
    let mut names: Vec<_> = users.iter().filter_map(|user| user.name.clone()).collect();
    names.sort();
    names
}

/// Handlers in `StdbTransaction` see each transaction's changes against that transaction's state;
/// the same messages read in `Update` arrive in order against the latest state.
#[test]
fn strict_and_relaxed_delivery() {
    fn record(mut inserted: MessageReader<RowInserted<User>>, users: Rows<User>, mut log: ResMut<Log>) {
        for message in inserted.read() {
            let name = message.row.name.as_deref().unwrap();
            log.0
                .push(format!("tx {}: +{name}, resident {:?}", message.seq, names(&users)));
        }
    }

    let mut strict = App::new();
    strict
        .add_plugins(StdbPlugin::<RemoteModule>::new())
        .init_resource::<Log>()
        .add_systems(StdbTransaction, record);
    let mut relaxed = App::new();
    relaxed
        .add_plugins(StdbPlugin::<RemoteModule>::new())
        .init_resource::<Log>()
        .add_systems(Update, record);

    for app in [&mut strict, &mut relaxed] {
        // Two transactions arrive within one frame.
        apply_update::<RemoteModule>(app.world_mut(), users(vec![user(1, "ada", true)], vec![])).unwrap();
        apply_update::<RemoteModule>(app.world_mut(), users(vec![user(2, "bo", true)], vec![])).unwrap();
        app.update();
    }

    assert_eq!(
        strict.world().resource::<Log>().0,
        [
            r#"tx 1: +ada, resident ["ada"]"#,
            r#"tx 2: +bo, resident ["ada", "bo"]"#
        ]
    );
    assert_eq!(
        relaxed.world().resource::<Log>().0,
        [
            r#"tx 1: +ada, resident ["ada", "bo"]"#,
            r#"tx 2: +bo, resident ["ada", "bo"]"#
        ]
    );
}

#[test]
fn update_and_delete_reach_rows_and_messages() {
    fn record(
        mut updated: MessageReader<RowUpdated<User>>,
        mut deleted: MessageReader<RowDeleted<User>>,
        users: Rows<User>,
        mut log: ResMut<Log>,
    ) {
        for message in updated.read() {
            log.0.push(format!("{:?} -> {:?}", message.old.name, message.new.name));
        }
        for message in deleted.read() {
            log.0.push(format!("-{:?}", message.row.name));
        }
        log.0.push(format!(
            "changed {:?}",
            users.iter_changed().map(|u| &u.name).collect::<Vec<_>>()
        ));
    }

    let mut app = App::new();
    app.add_plugins(StdbPlugin::<RemoteModule>::new())
        .init_resource::<Log>()
        .add_systems(StdbTransaction, record);
    let world = app.world_mut();

    apply_update::<RemoteModule>(world, users(vec![user(1, "ada", true), user(2, "bo", true)], vec![])).unwrap();
    // `set_name`: same identity, new name. Then user 2 leaves the subscription.
    apply_update::<RemoteModule>(world, users(vec![user(1, "ada l.", true)], vec![user(1, "ada", true)])).unwrap();
    apply_update::<RemoteModule>(world, users(vec![], vec![user(2, "bo", true)])).unwrap();

    let log = &world.resource::<Log>().0;
    assert_eq!(log[1], r#"Some("ada") -> Some("ada l.")"#);
    assert_eq!(log[2], r#"changed [Some("ada l.")]"#);
    assert_eq!(log[3], r#"-Some("bo")"#);
    assert_eq!(log[4], "changed []");
    assert_eq!(world.resource::<TransactionSeq<RemoteModule>>().0, 3);

    fn lookup(users: Rows<User>) {
        assert_eq!(users.len(), 1);
        assert_eq!(users.get(&identity(1)).unwrap().name.as_deref(), Some("ada l."));
        assert!(users.get(&identity(2)).is_none());
    }
    world.run_system_cached(lookup).unwrap();
}

/// A table without a primary key still stores, iterates and deletes rows.
#[test]
fn table_without_primary_key() {
    let mut app = App::new();
    app.add_plugins(StdbPlugin::<RemoteModule>::new());
    let world = app.world_mut();

    let hello = Message {
        sender: identity(1),
        sent: Timestamp::UNIX_EPOCH,
        text: "hello".into(),
    };
    let update = RemoteUpdate {
        message: TableUpdate::from_rows([hello.clone()], []),
        ..Default::default()
    };
    apply_update::<RemoteModule>(world, update).unwrap();

    fn one_message(messages: Rows<Message>) {
        assert_eq!(messages.iter().map(|m| m.text.as_str()).collect::<Vec<_>>(), ["hello"]);
    }
    world.run_system_cached(one_message).unwrap();

    let update = RemoteUpdate {
        message: TableUpdate::from_rows([], [hello]),
        ..Default::default()
    };
    apply_update::<RemoteModule>(world, update).unwrap();

    fn no_messages(messages: Rows<Message>) {
        assert!(messages.is_empty());
    }
    world.run_system_cached(no_messages).unwrap();
}

/// Random inserts, updates and deletes, checked against a `HashMap` after every transaction.
/// Exercises slot reuse and the `swap_remove` fix-up in the dense store.
#[test]
fn dense_store_matches_a_model() {
    use std::collections::HashMap;

    #[derive(Resource, Default)]
    struct Model(HashMap<u8, User>);

    fn check(users: Rows<User>, model: Res<Model>) {
        assert_eq!(users.len(), model.0.len());
        for (id, expected) in &model.0 {
            assert_eq!(users.get(&identity(*id)), Some(expected));
        }
        for user in users.iter() {
            assert!(model.0.values().any(|expected| expected == user));
        }
    }

    let mut app = App::new();
    app.add_plugins(StdbPlugin::<RemoteModule>::new())
        .init_resource::<Model>();
    let world = app.world_mut();

    let mut state = 0x2545_f491_4f6c_dd1d_u64;
    let mut random = move |below: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % below
    };

    for round in 0..500 {
        let (mut inserts, mut deletes) = (Vec::new(), Vec::new());
        let mut model = world.resource::<Model>().0.clone();
        for _ in 0..random(8) {
            let id = random(24) as u8;
            let new = user(id, &format!("u{id}r{round}"), random(2) == 0);
            match random(3) {
                // Delete if present; otherwise insert, or update if present.
                // An id picked twice in a round yields an insert and delete of a row that never
                // becomes resident, which the server can also produce.
                0 => deletes.extend(model.remove(&id)),
                _ => {
                    deletes.extend(model.insert(id, new.clone()));
                    inserts.push(new);
                }
            }
        }
        world.resource_mut::<Model>().0 = model;
        apply_update::<RemoteModule>(world, users(inserts, deletes)).unwrap();
        world.run_system_cached(check).unwrap();
    }
}

/// A table set up `without_row_messages` stores and changes rows as usual, in either storage,
/// and says nothing about it.
#[test]
fn a_table_without_row_messages() {
    fn heard(
        inserted: MessageReader<RowInserted<User>>,
        updated: MessageReader<RowUpdated<User>>,
        deleted: MessageReader<RowDeleted<User>>,
    ) -> usize {
        inserted.len() + updated.len() + deleted.len()
    }

    for entities in [false, true] {
        let mut plugin = StdbPlugin::<RemoteModule>::new().without_row_messages::<User>();
        if entities {
            plugin = plugin.entity_table::<User>();
        }
        let mut app = App::new();
        app.add_plugins(plugin);
        let world = app.world_mut();

        apply_update::<RemoteModule>(world, users(vec![user(1, "ada", true), user(2, "bo", true)], vec![])).unwrap();
        apply_update::<RemoteModule>(
            world,
            users(
                vec![user(1, "ada l.", true)],
                vec![user(1, "ada", true), user(2, "bo", true)],
            ),
        )
        .unwrap();

        assert_eq!(world.run_system_cached(heard).unwrap(), 0);
        let resident = world.run_system_cached(|users: Rows<User>| names(&users)).unwrap();
        assert_eq!(resident, ["ada l."]);
        // Change detection does not depend on the messages.
        let changed = world
            .run_system_cached(|users: Rows<User>| users.iter_changed().count())
            .unwrap();
        assert_eq!(changed, 1);
    }
}

/// The store indexes its rows by key when a row is first looked up, from the rows there are by
/// then, and keeps the index in step afterwards: through inserts, updates, and deletes that move
/// other rows about.
#[test]
fn rows_are_found_by_key_however_late_the_first_lookup() {
    fn found(world: &mut World, byte: u8) -> Option<String> {
        world
            .run_system_cached_with(
                |byte: In<u8>, users: Rows<User>| users.get(&identity(*byte)).and_then(|user| user.name.clone()),
                byte,
            )
            .unwrap()
    }

    let mut app = App::new();
    app.add_plugins(StdbPlugin::<RemoteModule>::new());
    let world = app.world_mut();

    // Rows arrive, change and leave before anything is looked up.
    let first = vec![user(1, "ada", true), user(2, "bob", true), user(3, "cy", true)];
    apply_update::<RemoteModule>(world, users(first, vec![])).unwrap();
    let change = users(
        vec![user(2, "rob", true)],
        vec![user(2, "bob", true), user(1, "ada", true)],
    );
    apply_update::<RemoteModule>(world, change).unwrap();
    assert_eq!(found(world, 1), None);
    assert_eq!(found(world, 2).as_deref(), Some("rob"));
    assert_eq!(found(world, 3).as_deref(), Some("cy"));

    // From here the index exists, and has to follow.
    let change = users(
        vec![user(4, "di", true), user(3, "cyd", true)],
        vec![user(3, "cy", true), user(2, "rob", true)],
    );
    apply_update::<RemoteModule>(world, change).unwrap();
    assert_eq!(found(world, 2), None);
    assert_eq!(found(world, 3).as_deref(), Some("cyd"));
    assert_eq!(found(world, 4).as_deref(), Some("di"));
    // The slot that row 2 left is taken again.
    apply_update::<RemoteModule>(world, users(vec![user(5, "eve", true)], vec![])).unwrap();
    assert_eq!(found(world, 5).as_deref(), Some("eve"));
    assert_eq!(found(world, 4).as_deref(), Some("di"));
}

// `a_second_module_is_refused` stood here until 2026-09-21. It asserted that an app could hold
// one `StdbPlugin`, because the state, identity and reconnect policy were the app's rather than
// each module's. Phase 9 made all three name their module, so a second one is no longer refused;
// `tests/two_modules.rs` now asserts the opposite, and that neither module speaks for the other.
