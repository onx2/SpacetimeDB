//! Key groups: several tables whose rows share entities by primary key. No network.
//!
//! The module here is written by hand, in the shape the generator emits, because no fixture
//! module has two tables keyed by the same id.

// Rows are cloned into updates and kept for the matching delete; `from_ref` would not read better.
#![allow(clippy::cloned_ref_to_slice_refs)]

use bevy_app::App;
use bevy_ecs::prelude::*;
use spacetimedb_bevy::__codegen::core::{
    Module, NoPk, ParseError, RawTableRows, ReducerVisitor, Row as RowType, Table, TableKind, TableUpdate,
    TableVisitor, UpdateVisitor,
};
use spacetimedb_bevy::__codegen::lib as __lib;
use spacetimedb_bevy::{apply_update, Row, RowEntities, Rows, StdbPlugin};

#[derive(Debug)]
struct Game;

macro_rules! table {
    ($row:ident { id: u64, $($field:ident: $ty:ty),* }, $name:literal, $pk:ty, $key:expr) => {
        #[derive(__lib::ser::Serialize, __lib::de::Deserialize, Clone, PartialEq, Debug)]
        #[sats(crate = __lib)]
        struct $row {
            id: u64,
            $($field: $ty),*
        }

        impl Table for $row {
            type Module = Game;
            type Row = $row;
            type Pk = $pk;
            const NAME: &'static str = $name;
            const KIND: TableKind = TableKind::Persistent;
            fn pk(row: &$row) -> Option<&$pk> {
                let key: fn(&$row) -> Option<&$pk> = $key;
                key(row)
            }
        }
    };
}

table!(Position { id: u64, x: i32 }, "position", u64, |row| Some(&row.id));
table!(Health { id: u64, hp: u32 }, "health", u64, |row| Some(&row.id));
// Keyed by the same kind of id, but deliberately left out of the group.
table!(Score { id: u64, points: u32 }, "score", u64, |row| Some(&row.id));
table!(Log { id: u64, line: String }, "log", NoPk, |_| None);

#[derive(Default, Debug)]
struct GameUpdate {
    position: TableUpdate<Position>,
    health: TableUpdate<Health>,
    score: TableUpdate<Score>,
    log: TableUpdate<Log>,
}

impl Module for Game {
    type Update = GameUpdate;

    fn parse_table(_: &mut GameUpdate, table: &str, _: RawTableRows) -> Result<(), ParseError> {
        Err(ParseError::UnknownTable(table.into()))
    }

    fn visit_update<V: UpdateVisitor<Self>>(update: GameUpdate, visitor: &mut V) {
        visitor.table::<Position>(update.position);
        visitor.table::<Health>(update.health);
        visitor.table::<Score>(update.score);
        visitor.table::<Log>(update.log);
    }

    fn visit_tables<V: TableVisitor<Self>>(visitor: &mut V) {
        visitor.table::<Position>();
        visitor.table::<Health>();
        visitor.table::<Score>();
        visitor.table::<Log>();
    }

    fn visit_reducers<V: ReducerVisitor<Self>>(_: &mut V) {}
}

fn changes<R: RowType>(inserts: &[R], deletes: &[R]) -> TableUpdate<R> {
    TableUpdate::from_rows(inserts.iter().cloned(), deletes.iter().cloned())
}

/// What the game hangs on a unit's entity.
#[derive(Component, Debug, PartialEq)]
struct Sprite(&'static str);

/// The name of the group: one entity per unit.
struct Unit;

fn app() -> App {
    let mut app = App::new();
    app.add_plugins(
        StdbPlugin::<Game>::new()
            .entity_table_in::<Position, Unit>()
            .entity_table_in::<Health, Unit>()
            .entity_table::<Score>(),
    );
    app
}

fn unit(world: &World, id: u64) -> Option<Entity> {
    let by_position = world.resource::<RowEntities<Position>>().get(&id);
    let by_health = world.resource::<RowEntities<Health>>().get(&id);
    match (by_position, by_health) {
        (Some(a), Some(b)) => {
            assert_eq!(a, b, "the rows of unit {id} are on different entities");
            Some(a)
        }
        (a, b) => a.or(b),
    }
}

/// Rows with equal keys sit on one entity, so a query over both tables is their join.
#[test]
fn rows_with_one_key_share_an_entity() {
    let mut app = app();
    let world = app.world_mut();
    apply_update::<Game>(
        world,
        GameUpdate {
            position: changes(&[Position { id: 7, x: 1 }, Position { id: 8, x: 2 }], &[]),
            health: changes(&[Health { id: 7, hp: 100 }], &[]),
            score: changes(&[Score { id: 7, points: 3 }], &[]),
            ..Default::default()
        },
    )
    .unwrap();

    fn joined(units: Query<(&Row<Position>, &Row<Health>)>) -> Vec<(u64, i32, u32)> {
        units
            .iter()
            .map(|(position, health)| (position.id, position.x, health.hp))
            .collect()
    }
    assert_eq!(world.run_system_cached(joined).unwrap(), [(7, 1, 100)]);

    let seven = unit(world, 7).unwrap();
    assert_ne!(Some(seven), unit(world, 8));
    // `score` has the same key but is not in the group: its row has an entity of its own.
    let score = world.resource::<RowEntities<Score>>().get(&7).unwrap();
    assert_ne!(score, seven);
    assert!(world.get::<Row<Position>>(score).is_none());

    // `Rows` still reads each table on its own.
    fn counts(positions: Rows<Position>, healths: Rows<Health>) -> (usize, usize) {
        (positions.len(), healths.len())
    }
    assert_eq!(world.run_system_cached(counts).unwrap(), (2, 1));
}

/// The entity is spawned with the first row and despawned when the last has left.
#[test]
fn the_entity_lives_while_any_row_of_the_group_does() {
    let mut app = app();
    let world = app.world_mut();
    let position = Position { id: 7, x: 1 };
    let health = Health { id: 7, hp: 100 };

    apply_update::<Game>(
        world,
        GameUpdate {
            position: changes(&[position.clone()], &[]),
            ..Default::default()
        },
    )
    .unwrap();
    let seven = unit(world, 7).unwrap();
    world.entity_mut(seven).insert(Sprite("knight"));

    // A second table's row arrives later, on the same entity.
    apply_update::<Game>(
        world,
        GameUpdate {
            health: changes(&[health.clone()], &[]),
            ..Default::default()
        },
    )
    .unwrap();
    assert_eq!(unit(world, 7), Some(seven));

    // One of the two leaves: the entity stays, with the other row and the app's component.
    apply_update::<Game>(
        world,
        GameUpdate {
            position: changes(&[], &[position]),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(world.get::<Row<Position>>(seven).is_none());
    assert_eq!(world.get::<Row<Health>>(seven).map(|health| health.hp), Some(100));
    assert_eq!(world.get::<Sprite>(seven), Some(&Sprite("knight")));

    // The last leaves: now the entity goes.
    apply_update::<Game>(
        world,
        GameUpdate {
            health: changes(&[], &[health]),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(world.get_entity(seven).is_err());
    assert_eq!(unit(world, 7), None);
}

/// One transaction takes a unit out of one table and puts it into another. For a moment no table
/// has it, but it is the same unit: the entity, and what the app put on it, must survive.
#[test]
fn moving_between_tables_in_one_transaction_keeps_the_entity() {
    let mut app = app();
    let world = app.world_mut();
    let position = Position { id: 7, x: 1 };
    apply_update::<Game>(
        world,
        GameUpdate {
            position: changes(&[position.clone()], &[]),
            ..Default::default()
        },
    )
    .unwrap();
    let seven = unit(world, 7).unwrap();
    world.entity_mut(seven).insert(Sprite("knight"));

    // `position` is applied before `health`, so the delete is seen first.
    apply_update::<Game>(
        world,
        GameUpdate {
            position: changes(&[], &[position]),
            health: changes(&[Health { id: 7, hp: 50 }], &[]),
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(unit(world, 7), Some(seven), "the unit got a new entity");
    assert_eq!(world.get::<Sprite>(seven), Some(&Sprite("knight")));
    assert_eq!(world.get::<Row<Health>>(seven).map(|health| health.hp), Some(50));
}

/// A key that comes back after its entity was despawned gets a fresh one.
#[test]
fn a_key_that_returns_gets_a_new_entity() {
    let mut app = app();
    let world = app.world_mut();
    let position = Position { id: 7, x: 1 };
    apply_update::<Game>(
        world,
        GameUpdate {
            position: changes(&[position.clone()], &[]),
            ..Default::default()
        },
    )
    .unwrap();
    let first = unit(world, 7).unwrap();
    apply_update::<Game>(
        world,
        GameUpdate {
            position: changes(&[], &[position.clone()]),
            ..Default::default()
        },
    )
    .unwrap();
    apply_update::<Game>(
        world,
        GameUpdate {
            position: changes(&[position], &[]),
            ..Default::default()
        },
    )
    .unwrap();

    let second = unit(world, 7).unwrap();
    assert_ne!(first, second);
    assert!(world.get::<Row<Position>>(second).is_some());
}

/// The app may despawn a shared entity while rows are still on it. Later changes must not panic,
/// and a row arriving for that key afterwards gets an entity again.
#[test]
fn a_shared_entity_the_app_despawned_is_tolerated() {
    let mut app = app();
    let world = app.world_mut();
    let (position, health) = (Position { id: 7, x: 1 }, Health { id: 7, hp: 100 });
    apply_update::<Game>(
        world,
        GameUpdate {
            position: changes(&[position.clone()], &[]),
            health: changes(&[health.clone()], &[]),
            ..Default::default()
        },
    )
    .unwrap();
    let seven = unit(world, 7).unwrap();
    world.despawn(seven);

    apply_update::<Game>(
        world,
        GameUpdate {
            position: changes(&[], &[position.clone()]),
            ..Default::default()
        },
    )
    .unwrap();
    apply_update::<Game>(
        world,
        GameUpdate {
            position: changes(&[position], &[]),
            ..Default::default()
        },
    )
    .unwrap();
    let again = unit(world, 7).unwrap();
    assert!(world.get::<Row<Position>>(again).is_some());
    apply_update::<Game>(
        world,
        GameUpdate {
            health: changes(&[], &[health]),
            ..Default::default()
        },
    )
    .unwrap();
}

#[test]
#[should_panic(expected = "has no primary key")]
fn a_table_without_a_primary_key_cannot_join_a_group() {
    App::new().add_plugins(StdbPlugin::<Game>::new().entity_table_in::<Log, Unit>());
}

/// In a group, one table asking to keep its entities keeps the group's.
#[test]
fn a_kept_group_entity_outlives_its_last_row() {
    use spacetimedb_bevy::RowLeft;

    let mut app = App::new();
    app.add_plugins(
        StdbPlugin::<Game>::new()
            .entity_table_in::<Position, Unit>()
            .entity_table_in::<Health, Unit>()
            .keep_entities_of::<Health>(),
    );
    let world = app.world_mut();
    let (position, health) = (Position { id: 7, x: 1 }, Health { id: 7, hp: 100 });
    apply_update::<Game>(
        world,
        GameUpdate {
            position: changes(&[position.clone()], &[]),
            health: changes(&[health.clone()], &[]),
            ..Default::default()
        },
    )
    .unwrap();
    let seven = unit(world, 7).unwrap();

    apply_update::<Game>(
        world,
        GameUpdate {
            position: changes(&[], &[position]),
            health: changes(&[], &[health.clone()]),
            ..Default::default()
        },
    )
    .unwrap();
    assert!(world.get_entity(seven).is_ok(), "the entity was despawned");
    assert!(world.get::<RowLeft<Health>>(seven).is_some());
    assert!(world.get::<Row<Health>>(seven).is_none());
    assert_eq!(unit(world, 7), None, "the group still knows an entity it gave up");

    // A unit with that key arriving later is a new entity: the kept one belongs to the app now.
    apply_update::<Game>(
        world,
        GameUpdate {
            health: changes(&[health], &[]),
            ..Default::default()
        },
    )
    .unwrap();
    assert_ne!(unit(world, 7), Some(seven));
}
