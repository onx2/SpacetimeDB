//! Entity storage: what a table stored as entities gives beyond one stored in a `TableStore`.
//! No network. For the refcounting semantics, the whole parity suite also runs in this mode:
//! `STDB_BEVY_STORAGE=entity cargo test -- --ignored`.

mod chat_bindings;

use std::collections::HashMap;

use bevy_app::App;
use bevy_ecs::prelude::*;
use chat_bindings::{Message, RemoteModule, RemoteUpdate, User};
use spacetimedb_bevy::__codegen::core::TableUpdate;
use spacetimedb_bevy::__codegen::lib::Identity;
use spacetimedb_bevy::{apply_update, Row, RowDeleted, RowEntities, RowUpdated, Rows, StdbPlugin, TableStore};

fn identity(byte: u8) -> Identity {
    Identity::from_byte_array([byte; 32])
}

fn user(byte: u8, name: &str) -> User {
    User {
        identity: identity(byte),
        name: Some(name.into()),
        online: true,
    }
}

fn users(inserts: Vec<User>, deletes: Vec<User>) -> RemoteUpdate {
    RemoteUpdate {
        user: TableUpdate::from_rows(inserts, deletes),
        ..Default::default()
    }
}

/// `user` as entities, `message` left in its store: the choice is per table.
fn app() -> App {
    let mut app = App::new();
    app.add_plugins(StdbPlugin::<RemoteModule>::new().entity_table::<User>());
    app
}

/// What the game hangs on a user's entity.
#[derive(Component, Debug, PartialEq)]
struct Nameplate(&'static str);

#[test]
fn storage_is_chosen_per_table() {
    let app = app();
    assert!(app.world().contains_resource::<RowEntities<User>>());
    assert!(!app.world().contains_resource::<TableStore<User>>());
    assert!(app.world().contains_resource::<TableStore<Message>>());
    assert!(!app.world().contains_resource::<RowEntities<Message>>());
}

/// An update keeps the entity, so what the app attached to it survives, and `Changed` sees it.
#[test]
fn an_update_keeps_the_entity_and_what_the_app_put_on_it() {
    let mut app = app();
    let world = app.world_mut();
    apply_update::<RemoteModule>(world, users(vec![user(1, "ada"), user(2, "bo")], vec![])).unwrap();

    let ada = world
        .resource::<RowEntities<User>>()
        .get(&identity(1))
        .expect("ada has an entity");
    world.entity_mut(ada).insert(Nameplate("over ada's head"));

    fn changed_names(changed: Query<&Row<User>, Changed<Row<User>>>) -> Vec<String> {
        let mut names: Vec<_> = changed.iter().filter_map(|user| user.name.clone()).collect();
        names.sort();
        names
    }
    assert_eq!(world.run_system_cached(changed_names).unwrap(), ["ada", "bo"]);

    // `set_name`: same identity, new name.
    apply_update::<RemoteModule>(world, users(vec![user(1, "ada l.")], vec![user(1, "ada")])).unwrap();

    assert_eq!(
        world.resource::<RowEntities<User>>().get(&identity(1)),
        Some(ada),
        "the row moved to another entity"
    );
    assert_eq!(world.get::<Row<User>>(ada).unwrap().name.as_deref(), Some("ada l."));
    assert_eq!(world.get::<Nameplate>(ada), Some(&Nameplate("over ada's head")));
    // Only the updated row counts as changed since the system last ran.
    assert_eq!(world.run_system_cached(changed_names).unwrap(), ["ada l."]);
}

/// A remote row and a local component in one query: the join a wrapper cannot offer.
#[test]
fn rows_join_with_local_components() {
    let mut app = app();
    let world = app.world_mut();
    apply_update::<RemoteModule>(world, users(vec![user(1, "ada"), user(2, "bo")], vec![])).unwrap();
    let bo = world.resource::<RowEntities<User>>().get(&identity(2)).unwrap();
    world.entity_mut(bo).insert(Nameplate("bo's"));

    fn labelled(users: Query<(&Row<User>, &Nameplate)>) -> Vec<(String, &'static str)> {
        users
            .iter()
            .map(|(user, plate)| (user.name.clone().unwrap(), plate.0))
            .collect()
    }
    assert_eq!(world.run_system_cached(labelled).unwrap(), [("bo".to_owned(), "bo's")]);
}

/// A row that leaves takes its entity, and everything on it, with it.
#[test]
fn a_delete_despawns_the_entity() {
    let mut app = app();
    let world = app.world_mut();
    apply_update::<RemoteModule>(world, users(vec![user(1, "ada")], vec![])).unwrap();
    let ada = world.resource::<RowEntities<User>>().get(&identity(1)).unwrap();
    world.entity_mut(ada).insert(Nameplate("ada's"));

    apply_update::<RemoteModule>(world, users(vec![], vec![user(1, "ada")])).unwrap();
    assert!(world.get_entity(ada).is_err());
    assert!(world.resource::<RowEntities<User>>().is_empty());
    assert_eq!(world.resource::<RowEntities<User>>().get(&identity(1)), None);
}

/// `Rows` reads an entity-backed table like any other.
#[test]
fn rows_reads_entity_tables_too() {
    let mut app = app();
    let world = app.world_mut();
    apply_update::<RemoteModule>(world, users(vec![user(1, "ada"), user(2, "bo")], vec![])).unwrap();

    fn read(users: Rows<User>, index: Res<RowEntities<User>>) {
        assert_eq!(users.len(), 2);
        assert!(users.as_slice().is_none(), "rows on entities are not one slice");
        assert_eq!(users.get(&identity(2)).unwrap().name.as_deref(), Some("bo"));
        assert!(users.get(&identity(3)).is_none());
        assert_eq!(users.entity(&identity(1)), index.get(&identity(1)));
        let mut names: Vec<_> = users.iter().filter_map(|user| user.name.clone()).collect();
        names.sort();
        assert_eq!(names, ["ada", "bo"]);
    }
    world.run_system_cached(read).unwrap();
}

/// The app may despawn a row's entity itself. The row's later update and delete must not panic.
#[test]
fn an_entity_the_app_despawned_is_tolerated() {
    let mut app = app();
    let world = app.world_mut();
    apply_update::<RemoteModule>(world, users(vec![user(1, "ada")], vec![])).unwrap();
    let ada = world.resource::<RowEntities<User>>().get(&identity(1)).unwrap();
    world.despawn(ada);

    apply_update::<RemoteModule>(world, users(vec![], vec![user(1, "ada")])).unwrap();
    assert!(world.resource::<RowEntities<User>>().is_empty());
}

/// What the server does next to a row whose entity the app despawned is still told to the app,
/// as it is for a row in a store: an app that follows the table through its messages has to
/// learn that the row changed, and that it left.
#[test]
fn a_row_the_app_despawned_still_has_its_messages() {
    fn names(
        mut updated: MessageReader<RowUpdated<User>>,
        mut deleted: MessageReader<RowDeleted<User>>,
    ) -> Vec<String> {
        let name = |user: &User| user.name.clone().unwrap_or_default();
        let updated = updated
            .read()
            .map(|user| format!("{} -> {}", name(&user.old), name(&user.new)));
        let deleted = deleted.read().map(|user| format!("{} left", name(&user.row)));
        updated.chain(deleted).collect()
    }

    let mut app = app();
    let world = app.world_mut();
    apply_update::<RemoteModule>(world, users(vec![user(1, "ada"), user(2, "bo")], vec![])).unwrap();
    let ada = world.resource::<RowEntities<User>>().get(&identity(1)).unwrap();
    world.despawn(ada);
    assert_eq!(world.resource::<RowEntities<User>>().get(&identity(1)), None);

    apply_update::<RemoteModule>(world, users(vec![user(1, "ada l.")], vec![user(1, "ada")])).unwrap();
    assert_eq!(world.run_system_cached(names).unwrap(), ["ada -> ada l."]);
    // Still no entity for it: the app took that away, and only a new row brings one back.
    assert_eq!(world.resource::<RowEntities<User>>().get(&identity(1)), None);

    apply_update::<RemoteModule>(world, users(vec![], vec![user(1, "ada l.")])).unwrap();
    assert_eq!(world.run_system_cached(names).unwrap(), ["ada l. left"]);
    assert_eq!(world.resource::<RowEntities<User>>().len(), 1);

    // The key is as good as new: a row that arrives under it is found.
    apply_update::<RemoteModule>(world, users(vec![user(1, "ada again")], vec![])).unwrap();
    assert!(world.resource::<RowEntities<User>>().get(&identity(1)).is_some());
}

/// Random inserts, updates and deletes, checked against a `HashMap` after every transaction.
#[test]
fn entity_storage_matches_a_model() {
    #[derive(Resource, Default)]
    struct Model(HashMap<u8, User>);

    fn check(users: Rows<User>, on_entities: Query<&Row<User>>, model: Res<Model>) {
        assert_eq!(users.len(), model.0.len());
        assert_eq!(
            on_entities.iter().count(),
            model.0.len(),
            "a row entity leaked or went missing"
        );
        for (id, expected) in &model.0 {
            assert_eq!(users.get(&identity(*id)), Some(expected));
        }
    }

    let mut app = app();
    app.init_resource::<Model>();
    let world = app.world_mut();

    let mut state = 0x2545_f491_4f6c_dd1d_u64;
    let mut random = move |below: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % below
    };

    for round in 0..300 {
        let (mut inserts, mut deletes) = (Vec::new(), Vec::new());
        let mut model = world.resource::<Model>().0.clone();
        for _ in 0..random(8) {
            let id = random(24) as u8;
            let new = user(id, &format!("u{id}r{round}"));
            match random(3) {
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

/// With `keep_entities_of`, a row that leaves takes only its `Row` with it. The entity stays,
/// marked, for the app to finish with.
#[test]
fn a_kept_entity_outlives_its_row() {
    use spacetimedb_bevy::RowLeft;

    let mut app = App::new();
    app.add_plugins(
        StdbPlugin::<RemoteModule>::new()
            .entity_table::<User>()
            .keep_entities_of::<User>(),
    );
    let world = app.world_mut();
    apply_update::<RemoteModule>(world, users(vec![user(1, "ada")], vec![])).unwrap();
    let ada = world.resource::<RowEntities<User>>().get(&identity(1)).unwrap();
    world.entity_mut(ada).insert(Nameplate("ada's"));

    apply_update::<RemoteModule>(world, users(vec![], vec![user(1, "ada")])).unwrap();

    assert!(world.get::<Row<User>>(ada).is_none(), "the row is gone");
    assert_eq!(world.get::<Nameplate>(ada), Some(&Nameplate("ada's")));
    fn departed(left: Query<Entity, Added<RowLeft<User>>>) -> Vec<Entity> {
        left.iter().collect()
    }
    assert_eq!(world.run_system_cached(departed).unwrap(), [ada]);
    // The table no longer knows the entity; the app owns it now.
    assert!(world.resource::<RowEntities<User>>().is_empty());

    // The same user coming back is a new row on a new entity.
    apply_update::<RemoteModule>(world, users(vec![user(1, "ada")], vec![])).unwrap();
    assert_ne!(world.resource::<RowEntities<User>>().get(&identity(1)), Some(ada));
}

/// Bevy's own lifecycle observers run the moment a component is inserted, which is in the middle
/// of a transaction. Handlers that look at other rows belong in `StdbTransaction`, which runs
/// after the whole transaction is in place. This test is that caveat, shown.
#[test]
fn lifecycle_observers_see_a_transaction_half_applied() {
    use spacetimedb_bevy::{RowInserted, StdbTransaction};

    #[derive(Resource, Default)]
    struct Observed {
        by_lifecycle_observer: Vec<usize>,
        by_transaction_system: Vec<usize>,
    }

    let mut app = app();
    app.init_resource::<Observed>()
        // Fires once per row, as each row's component lands.
        .add_observer(
            |_: On<Insert<Row<User>>>, users: Query<&Row<User>>, mut observed: ResMut<Observed>| {
                observed.by_lifecycle_observer.push(users.iter().count());
            },
        )
        // Runs once, after the transaction.
        .add_systems(
            StdbTransaction,
            |mut inserted: MessageReader<RowInserted<User>>, users: Rows<User>, mut observed: ResMut<Observed>| {
                for _ in inserted.read() {
                    observed.by_transaction_system.push(users.len());
                }
            },
        );

    // One transaction that updates ada and inserts cy: the update is applied before the insert.
    let world = app.world_mut();
    apply_update::<RemoteModule>(world, users(vec![user(1, "ada"), user(2, "bo")], vec![])).unwrap();
    *world.resource_mut::<Observed>() = Observed::default();
    apply_update::<RemoteModule>(
        world,
        users(vec![user(1, "ada l."), user(3, "cy")], vec![user(1, "ada")]),
    )
    .unwrap();

    let observed = world.resource::<Observed>();
    // The observer ran for ada's update while cy was not there yet: it saw 2 users, then 3.
    assert_eq!(observed.by_lifecycle_observer, [2, 3]);
    // The system ran once the transaction was whole: 3 users, for the one inserted row.
    assert_eq!(observed.by_transaction_system, [3]);
}
