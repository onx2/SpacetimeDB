//! Entity storage: one entity per resident row, for tables whose rows are things in the game.
//!
//! Chosen per table with [`StdbPlugin::entity_table`](crate::StdbPlugin::entity_table). The row
//! lives in a [`Row<T>`] component, so it can be queried together with an app's own components,
//! and the app can put its own components on the row's entity.
//!
//! |  | store (default) | entity |
//! | --- | --- | --- |
//! | Best for | reference data, logs, anything large | players, units, items in the world |
//! | Read with | [`Rows<T>`](crate::Rows) | `Query<&Row<T>>`, or `Rows<T>` |
//! | Join with local components | by hand | `Query<(&Row<T>, &Transform)>` |
//! | Row-level change detection | `Rows::iter_changed` | `Changed<Row<T>>` |
//! | Cost to apply a change | a hash insert | a spawn, insert or despawn: several times more |
//! | When a row leaves | it is dropped | its entity is despawned with everything on it, or kept and marked [`RowLeft`] |
//!
//! Both kinds get the same row messages, run through the same refcounting, and clear the same way.
//!
//! # Reacting to rows on entities
//!
//! Bevy's lifecycle observers work on [`Row<T>`]: `On<Insert<Row<T>>>` fires for a new row and for
//! an update, `On<Remove<Row<T>>>` when a row leaves. They fire the moment the component changes,
//! which is in the middle of a transaction: an observer for the first of two rows runs before the
//! second row exists. That is fine for work that concerns the one entity, such as attaching a
//! mesh. A handler that looks at other rows belongs in [`StdbTransaction`](crate::StdbTransaction),
//! reading [`RowInserted`](crate::RowInserted) and friends, which run once the whole transaction is
//! in place. `tests/entity.rs` has a test that shows the difference.
//!
//! # Despawning a row's entity yourself
//!
//! The entity is the app's to despawn, but the row is the server's: it stays counted until the
//! server deletes it. Meanwhile it is not found by key, by a unique column or in a query, and no
//! entity is made for it again. Its row messages go on as for any other row, the
//! [`RowDeleted`](crate::RowDeleted) at its end included, so that an app which follows a table
//! through its messages sees the same in either kind of storage.
//!
//! # Storage of the component
//!
//! [`Row<T>`] uses Bevy's table storage. Sparse-set storage was measured for rows that come and go
//! on living entities, as in a key group (`cargo run --release -p spacetimedb_bevy_bench --bin
//! storage_kinds`): adding or removing the component is 2 to 3 times cheaper (about 36 ns against
//! 56 to 120 ns), and iterating it in a join 1.6 to 2 times dearer. Joins run every frame and
//! either cost is small beside the rest of applying a row, so there is one kind of `Row`.

use std::any::TypeId;

use crate::unique::UniqueIndexes;
use std::collections::HashMap;
use std::hash::Hash;
use std::marker::PhantomData;
use std::ops::Deref;

use crate::protocol::{RowUpdate, Table, TableDiff};
use bevy_ecs::lifecycle::HookContext;
use bevy_ecs::prelude::*;
use bevy_ecs::world::DeferredWorld;

use crate::pairing::{Paired, Ticket};

use crate::store::{retain_changed, RowChanges};

/// A resident row of table `T`, on an entity of its own.
///
/// Immutable: the row is what the server says it is. An update replaces the component on the same
/// entity, so `Changed<Row<T>>` sees it and components the app added stay where they are.
/// Derefs to the row, so `row.name` works.
#[derive(Component)]
#[component(immutable)]
pub struct Row<T: Table>(T::Row);

impl<T: Table> Deref for Row<T> {
    type Target = T::Row;
    fn deref(&self) -> &T::Row {
        &self.0
    }
}

/// Marks an entity that a row of table `T` has left, for tables set up with
/// [`StdbPlugin::keep_entities_of`](crate::StdbPlugin::keep_entities_of).
///
/// The row is gone and so is its [`Row<T>`]; the entity and everything else on it remain, for the
/// app to play an exit animation on and despawn when it is done: `Query<Entity, Added<RowLeft<T>>>`.
#[derive(Component)]
pub struct RowLeft<T: Table>(PhantomData<fn() -> T>);

impl<T: Table> Default for RowLeft<T> {
    fn default() -> Self {
        Self(PhantomData)
    }
}

/// Which entity holds which row of table `T`. Present exactly for tables with entity storage.
#[derive(Resource)]
pub struct RowEntities<T: Table> {
    /// From primary key to the row's entity. Empty for a table without one. A row whose
    /// entity the app despawned is not in it: there is nothing left to find.
    by_pk: HashMap<T::Pk, Entity, foldhash::fast::RandomState>,
    /// The entity of each resident row, by the row's ticket.
    by_ticket: Vec<Entity>,
    /// Resident rows that the app took off their entities, by despawning the entity or removing
    /// its [`Row`], under the entity each was on. The server still counts those rows, and what
    /// it does to them next is told to the app as for any other row, from the copy kept here.
    /// Nearly always empty.
    gone: HashMap<Entity, T::Row>,
    /// How many rows are resident, which counts those whose entities the app despawned.
    len: usize,
    /// The key group this table's rows share entities in, if any.
    group: Option<TypeId>,
    /// Leave an entity alive, marked [`RowLeft`], when its row leaves.
    keep: bool,
    /// Whether changes are recorded, for row messages to be written from.
    record: bool,
    /// From the value of each unique column to the row's entity.
    pub(crate) unique: UniqueIndexes<T, Entity>,
}

impl<T: Table> Default for RowEntities<T> {
    fn default() -> Self {
        Self {
            by_pk: HashMap::default(),
            by_ticket: Vec::new(),
            gone: HashMap::new(),
            len: 0,
            group: None,
            keep: false,
            record: true,
            unique: UniqueIndexes::new(),
        }
    }
}

impl<T: Table> RowEntities<T> {
    /// Returns the entity holding the row with primary key `pk`.
    pub fn get(&self, pk: &T::Pk) -> Option<Entity> {
        self.by_pk.get(pk).copied()
    }

    /// Returns the number of resident rows.
    pub fn len(&self) -> usize {
        self.len
    }

    /// Returns `true` if no row of this table is resident.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Creates an empty index, set up as the plugin was asked to: `group` is the table's key group
    /// if it is in one,
    /// `keep` leaves entities alive past their rows, and `row_messages` records what changed.
    pub(crate) fn new(group: Option<TypeId>, keep: bool, row_messages: bool) -> Self {
        Self {
            group,
            keep,
            record: row_messages,
            ..Self::default()
        }
    }
}

/// Entities shared by the rows of several tables, for every key group whose key type is `K`.
///
/// Tables in one group have the same primary key type and mean the same thing by it: the row of
/// `position` with key 7 and the row of `health` with key 7 describe one thing, so they sit on
/// one entity, and `Query<(&Row<Position>, &Row<Health>)>` is their join.
///
/// Groups exist only where the app declares them. A schema cannot say that two tables' keys are
/// related, so none is ever inferred.
#[derive(Resource)]
pub(crate) struct KeyGroups<K> {
    groups: HashMap<TypeId, HashMap<K, Shared>>,
    /// Keys whose last row left during the transaction being applied. Their entities go at its
    /// end, if nothing has come back by then: a thing that moves from one table of the group to
    /// another within a transaction keeps its entity, and what the app put on it.
    emptied: Vec<(TypeId, K)>,
}

struct Shared {
    entity: Entity,
    /// How many tables of the group have a row with this key right now.
    rows: u32,
    /// Some table of the group asked for its entities to be kept.
    keep: bool,
}

impl<K> Default for KeyGroups<K> {
    fn default() -> Self {
        Self {
            groups: HashMap::new(),
            emptied: Vec::new(),
        }
    }
}

/// Keeps the indexes right when something other than this crate removes a row's component,
/// which in practice means the app despawning a row's entity.
///
/// While this crate applies a change to table `T` its [`RowEntities<T>`] is out of the world, so
/// this finds nothing to do then, and the sink does its own bookkeeping.
pub(crate) fn on_row_removed<T: Table>(mut world: DeferredWorld, context: HookContext) {
    if !world.contains_resource::<RowEntities<T>>() {
        return;
    }
    let Some(row) = world.get::<Row<T>>(context.entity).map(|row| row.0.clone()) else {
        return;
    };
    let key = T::pk(&row).cloned();
    let mut index = world.resource_mut::<RowEntities<T>>();
    index.unique.remove(&row);
    index.gone.insert(context.entity, row);
    let Some(key) = key else { return };
    if index.by_pk.get(&key) == Some(&context.entity) {
        index.by_pk.remove(&key);
    }
    let Some(group) = index.group else { return };
    let Some(mut groups) = world.get_resource_mut::<KeyGroups<T::Pk>>() else {
        return;
    };
    if let Some(shared) = groups.groups.get_mut(&group) {
        let left = shared.get_mut(&key).map(|entry| {
            entry.rows = entry.rows.saturating_sub(1);
            entry.rows
        });
        if left == Some(0) {
            shared.remove(&key);
        }
    }
}

/// What the app has said about key groups, across every plugin added to it.
#[derive(Resource, Default)]
pub(crate) struct GroupSweepers {
    /// One per key type in use, run after every transaction. See [`KeyGroups::emptied`].
    pub sweepers: HashMap<TypeId, fn(&mut World)>,
    /// The key type each group uses, and the first table that claimed it, by the group's marker.
    /// A group lives in one [`KeyGroups<K>`], so a table keyed by another type could never share
    /// its entities; registration refuses it rather than let the join be silently empty.
    pub key_of: HashMap<TypeId, (TypeId, &'static str)>,
}

/// Despawns the entities of every key group of type `K` that the transaction just applied
/// emptied, unless a row has come back to one meanwhile or a table of the group keeps its
/// entities. One of the functions [`GroupSweepers`] holds.
pub(crate) fn sweep<K: Eq + Hash + Send + Sync + 'static>(world: &mut World) {
    let mut groups = world.resource_mut::<KeyGroups<K>>();
    let emptied = std::mem::take(&mut groups.emptied);
    let mut gone = Vec::new();
    for (group, key) in emptied {
        let Some(shared) = groups.groups.get_mut(&group) else {
            continue;
        };
        if shared.get(&key).is_some_and(|entry| entry.rows == 0) {
            // Forgotten either way; despawned unless a table of the group keeps its entities.
            gone.extend(
                shared
                    .remove(&key)
                    .filter(|entry| !entry.keep)
                    .map(|entry| entry.entity),
            );
        }
    }
    for entity in gone {
        if let Ok(entity) = world.get_entity_mut(entity) {
            entity.despawn();
        }
    }
}

/// Sweeps every key type in use. Runs at the end of each transaction, once every table of it
/// has been applied.
pub(crate) fn sweep_all(world: &mut World) {
    let sweepers: Vec<fn(&mut World)> = world
        .get_resource::<GroupSweepers>()
        .map(|groups| groups.sweepers.values().copied().collect())
        .unwrap_or_default();
    for sweeper in sweepers {
        sweeper(world);
    }
}

/// The world as one table's storage, for the length of one update.
struct EntitySink<'a, T: Table> {
    world: &'a mut World,
    /// The table's key group, if it is in one.
    shared: Option<(&'a mut KeyGroups<T::Pk>, TypeId)>,
    by_pk: &'a mut HashMap<T::Pk, Entity, foldhash::fast::RandomState>,
    by_ticket: &'a mut Vec<Entity>,
    gone: &'a mut HashMap<Entity, T::Row>,
    keep: bool,
    record: bool,
    changes: RowChanges<T>,
    unique: &'a mut UniqueIndexes<T, Entity>,
}

impl<T: Table> EntitySink<'_, T> {
    /// Returns the entity for a new row: the group's entity for its key if there is one, else a
    /// new one.
    fn place(&mut self, row: T::Row) -> Entity {
        let key = T::pk(&row).cloned();

        match (&mut self.shared, &key) {
            (Some((groups, group)), Some(key)) => {
                let entry = groups.groups.entry(*group).or_default().entry(key.clone());
                let shared = entry.or_insert_with(|| Shared {
                    entity: self.world.spawn_empty().id(),
                    rows: 0,
                    keep: false,
                });
                shared.rows += 1;
                shared.keep |= self.keep;
                // The app may have despawned it while other rows of the group were still there.
                if self.world.get_entity(shared.entity).is_err() {
                    shared.entity = self.world.spawn_empty().id();
                }
                self.world
                    .entity_mut(shared.entity)
                    .remove::<RowLeft<T>>()
                    .insert(Row::<T>(row));
                shared.entity
            }
            _ => self.world.spawn(Row::<T>(row)).id(),
        }
    }

    /// A row left `entity`. Alone on it, the entity goes now; in a group, when the group is done with it.
    fn vacate(&mut self, entity: Entity, row: Option<&T::Row>) {
        if self.keep
            && let Ok(mut entity) = self.world.get_entity_mut(entity)
        {
            entity.insert(RowLeft::<T>::default());
        }
        let Some((groups, group)) = &mut self.shared else {
            if !self.keep
                && let Ok(entity) = self.world.get_entity_mut(entity)
            {
                entity.despawn();
            }
            return;
        };
        let Some(key) = row.and_then(T::pk) else {
            return;
        };
        if let Some(shared) = groups.groups.get_mut(group).and_then(|shared| shared.get_mut(key)) {
            shared.rows = shared.rows.saturating_sub(1);
            if shared.rows == 0 {
                groups.emptied.push((*group, key.clone()));
            }
        }
    }

    fn get(&self, entity: Entity) -> Option<&T::Row> {
        // Not on its entity if the app despawned that, or took its `Row` off.
        let held = self.world.get::<Row<T>>(entity).map(|row| &row.0);
        held.or_else(|| self.gone.get(&entity))
    }

    /// Forgets the row that the app took off `entity`, now that it is back on it or has left for
    /// good, and returns it.
    fn forget_gone(&mut self, entity: Entity) -> Option<T::Row> {
        if self.gone.is_empty() {
            return None;
        }
        self.gone.remove(&entity)
    }

    fn apply(&mut self, paired: Paired<T>) {
        // The storage's names for the rows the diff names by ticket.
        let by_ticket = &*self.by_ticket;
        let mut updates: Vec<RowUpdate<T, Entity>> = paired
            .diff
            .updates
            .into_iter()
            .map(|update| RowUpdate {
                handle: by_ticket[update.handle as usize],
                new: update.new,
            })
            .collect();
        if paired.reconciled {
            retain_changed(&mut updates, |entity| self.get(entity));
        }
        let deletes = paired.diff.deletes.iter();
        let deletes = deletes
            .map(|&ticket| std::mem::replace(&mut self.by_ticket[ticket as usize], Entity::PLACEHOLDER))
            .collect();
        let diff = TableDiff::<T, Entity> {
            deletes,
            updates,
            inserts: paired.diff.inserts,
        };

        let indexed = !self.unique.is_empty();
        if indexed {
            let world = &*self.world;
            self.unique
                .before(&diff, |entity| world.get::<Row<T>>(entity).map(|row| &row.0));
        }
        // Deletes first, so their primary keys are out of the index before inserts reuse them.
        for entity in diff.deletes {
            let mut row = None;
            if let Ok(mut held) = self.world.get_entity_mut(entity) {
                row = held.take::<Row<T>>().map(|Row(row)| row);
                if let Some(key) = row.as_ref().and_then(T::pk)
                    && self.by_pk.get(key) == Some(&entity)
                {
                    self.by_pk.remove(key);
                }
                self.vacate(entity, row.as_ref());
            }
            // Not on its entity only if the app took it off, and then it was kept for this.
            let row = row.or_else(|| self.forget_gone(entity));
            if self.record {
                self.changes.deleted.extend(row);
            }
        }

        // Same entity, new component value: what the app attached stays, and `Changed` fires.
        for update in diff.updates {
            if self.world.get_entity(update.handle).is_err() {
                // The app despawned it. The copy kept of the row follows the server's.
                if let Some(kept) = self.gone.get_mut(&update.handle) {
                    if self.record {
                        let old = std::mem::replace(kept, update.new.clone());
                        self.changes.updated.push((old, update.new));
                    } else {
                        *kept = update.new;
                    }
                }
                continue;
            }
            // Back on its entity, if the app had taken it off, and to be found there again.
            let taken_off = self.forget_gone(update.handle);
            if taken_off.is_some()
                && let Some(key) = T::pk(&update.new)
            {
                self.by_pk.insert(key.clone(), update.handle);
            }
            if indexed {
                self.unique.insert(&update.new, update.handle);
            }
            let mut entity = self.world.entity_mut(update.handle);
            if !self.record {
                entity.insert(Row::<T>(update.new));
                continue;
            }
            let old = entity.get::<Row<T>>().map(|row| row.0.clone()).or(taken_off);
            entity.insert(Row::<T>(update.new.clone()));
            if let Some(old) = old {
                self.changes.updated.push((old, update.new));
            }
        }

        if self.record {
            self.changes.inserted.extend(diff.inserts.iter().cloned());
        }
        let keys: Vec<Option<T::Pk>> = diff.inserts.iter().map(|row| T::pk(row).cloned()).collect();
        let mut inserted = Vec::with_capacity(diff.inserts.len());
        if self.shared.is_some() || indexed {
            for row in diff.inserts {
                let key_of = indexed.then(|| row.clone());
                let entity = self.place(row);
                if let Some(row) = key_of {
                    self.unique.insert(&row, entity);
                }
                inserted.push(entity);
            }
        } else {
            // Alone on their entities, new rows can be spawned as one batch.
            inserted.extend(self.world.spawn_batch(diff.inserts.into_iter().map(Row::<T>)));
        }
        for ((entity, ticket), key) in inserted.into_iter().zip(paired.tickets).zip(keys) {
            if self.by_ticket.len() <= ticket as usize {
                self.by_ticket.resize(ticket as usize + 1, Entity::PLACEHOLDER);
            }
            self.by_ticket[ticket as usize] = entity;
            if let Some(key) = key {
                self.by_pk.insert(key, entity);
            }
        }
    }
}

/// Applies one message's changes to entity-backed table `T`.
pub(crate) fn apply<T: Table>(world: &mut World, paired: Paired<T>) -> RowChanges<T> {
    with_sink::<T>(world, |sink, len| {
        *len = *len + paired.diff.inserts.len() - paired.diff.deletes.len();
        sink.apply(paired);
    })
}

/// Removes every row of table `T` from its entities and returns the rows.
pub(crate) fn clear<T: Table>(world: &mut World) -> Vec<T::Row> {
    let changes = with_sink::<T>(world, |sink, len| {
        let tickets = 0..sink.by_ticket.len() as Ticket;
        let deletes = tickets
            .filter(|&ticket| sink.by_ticket[ticket as usize] != Entity::PLACEHOLDER)
            .collect();
        sink.apply(Paired {
            diff: TableDiff {
                deletes,
                updates: Vec::new(),
                inserts: Vec::new(),
            },
            tickets: Vec::new(),
            events: Vec::new(),
            reconciled: false,
        });
        sink.by_ticket.clear();
        sink.by_pk.clear();
        sink.gone.clear();
        *len = 0;
    });
    changes.deleted
}

fn with_sink<T: Table>(world: &mut World, run: impl FnOnce(&mut EntitySink<'_, T>, &mut usize)) -> RowChanges<T> {
    // Out of the world while the sink borrows the world; nothing else runs meanwhile.
    let mut table = world
        .remove_resource::<RowEntities<T>>()
        .expect("registered by the plugin");
    let mut groups = table.group.map(|group| {
        (
            world
                .remove_resource::<KeyGroups<T::Pk>>()
                .expect("registered by the plugin"),
            group,
        )
    });
    let mut sink = EntitySink {
        world,
        shared: groups.as_mut().map(|(groups, group)| (groups, *group)),
        by_pk: &mut table.by_pk,
        by_ticket: &mut table.by_ticket,
        gone: &mut table.gone,
        keep: table.keep,
        record: table.record,
        changes: RowChanges::default(),
        unique: &mut table.unique,
    };
    run(&mut sink, &mut table.len);
    let changes = sink.changes;
    if let Some((groups, _)) = groups {
        world.insert_resource(groups);
    }
    world.insert_resource(table);
    changes
}
