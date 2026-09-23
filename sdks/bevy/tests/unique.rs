//! Lookups by a unique column, in both kinds of storage. No network.

#[allow(dead_code)]
mod view_bindings;

use bevy_app::App;
use bevy_ecs::prelude::*;
use spacetimedb_bevy::__codegen::core::TableUpdate;
use spacetimedb_bevy::__codegen::lib::Identity;
use spacetimedb_bevy::{apply_update, Rows, StdbPlugin};
use view_bindings::{Player, PlayerTable, PlayerTableIdentity, RemoteModule, RemoteUpdate};

fn identity(byte: u8) -> Identity {
    Identity::from_byte_array([byte; 32])
}

fn player(entity_id: u64, owner: u8) -> Player {
    Player {
        entity_id,
        identity: identity(owner),
    }
}

fn apply(app: &mut App, deletes: Vec<Player>, inserts: Vec<Player>) {
    let update = RemoteUpdate {
        player: TableUpdate::from_rows(inserts, deletes),
        ..Default::default()
    };
    apply_update::<RemoteModule>(app.world_mut(), update).unwrap();
}

/// The `entity_id` of the player that `owner` owns.
fn owned_by(app: &mut App, owner: u8) -> Option<u64> {
    app.world_mut()
        .run_system_cached_with(
            |owner: In<u8>, players: Rows<PlayerTable>| {
                players
                    .find(PlayerTableIdentity, &identity(*owner))
                    .map(|player| player.entity_id)
            },
            owner,
        )
        .unwrap()
}

/// A unique column finds the row that holds the value, through every way the value can move:
/// two rows swapping values, a value handed from a deleted row to a new one, and a row leaving.
fn find_follows_the_rows(mut app: App) {
    apply(&mut app, vec![], vec![player(1, 0xA), player(2, 0xB)]);
    assert_eq!(owned_by(&mut app, 0xA), Some(1));
    assert_eq!(owned_by(&mut app, 0xB), Some(2));
    assert_eq!(owned_by(&mut app, 0xC), None);

    // Two rows swap their unique values in one transaction: both old values have to be out of
    // the index before either new one goes in, or one of the two would be lost.
    apply(
        &mut app,
        vec![player(1, 0xA), player(2, 0xB)],
        vec![player(1, 0xB), player(2, 0xA)],
    );
    assert_eq!(owned_by(&mut app, 0xA), Some(2));
    assert_eq!(owned_by(&mut app, 0xB), Some(1));

    // A value handed from a deleted row to a new one in the same transaction.
    apply(&mut app, vec![player(1, 0xB)], vec![player(3, 0xB)]);
    assert_eq!(owned_by(&mut app, 0xB), Some(3));

    apply(&mut app, vec![player(3, 0xB)], vec![]);
    assert_eq!(owned_by(&mut app, 0xB), None);
    assert_eq!(owned_by(&mut app, 0xA), Some(2));
}

#[test]
fn in_a_store() {
    let mut app = App::new();
    app.add_plugins(StdbPlugin::<RemoteModule>::new());
    find_follows_the_rows(app);
}

#[test]
fn on_entities() {
    let mut app = App::new();
    app.add_plugins(StdbPlugin::<RemoteModule>::new().entity_table::<PlayerTable>());
    find_follows_the_rows(app);
}

/// A row whose entity the app despawned is not found by its unique value either, and the value
/// is the next row's once the server has taken the first away.
#[test]
fn a_despawned_entity_leaves_the_index() {
    let mut app = App::new();
    app.add_plugins(StdbPlugin::<RemoteModule>::new().entity_table::<PlayerTable>());
    apply(&mut app, vec![], vec![player(1, 0xA), player(2, 0xB)]);

    let entity = app
        .world_mut()
        .run_system_cached(|players: Rows<PlayerTable>| players.entity(&1))
        .unwrap()
        .expect("player 1 has an entity");
    app.world_mut().despawn(entity);
    assert_eq!(owned_by(&mut app, 0xA), None);
    assert_eq!(owned_by(&mut app, 0xB), Some(2));

    // The despawned row leaves and another takes its value, in one transaction.
    apply(&mut app, vec![player(1, 0xA)], vec![player(3, 0xA)]);
    assert_eq!(owned_by(&mut app, 0xA), Some(3));

    // Nothing of the first row is left to take the value back out.
    apply(&mut app, vec![player(2, 0xB)], vec![]);
    assert_eq!(owned_by(&mut app, 0xA), Some(3));
}
