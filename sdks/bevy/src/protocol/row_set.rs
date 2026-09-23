//! Refcounting of subscribed rows.
//!
//! Overlapping subscriptions each send their own copy of a row they share,
//! and each later sends its own delete. A row is resident from its first insert to its last delete.

use std::any::TypeId;
use std::collections::hash_map::Entry;
use std::hash::Hash;

use bytes::Bytes;
use foldhash::fast::RandomState;
use spacetimedb_lib::bsatn;

use crate::protocol::diff::{RowUpdate, TableDiff};
use crate::protocol::module::{bytes_of, NoPk, ParsedDelete, ParsedRow, Table, TableUpdate};

/// The resident rows of one persistent table: how many subscriptions cover each, and the
/// [`Ticket`] storage keeps it under. The rows themselves live only in the storage.
///
/// A table with a primary key identifies its rows by that key. The server holds one row per key
/// at any moment, so a count per key is a count per row, and a key is smaller and cheaper to hash
/// than the row. The map is the set's own index, and not one that anything outside it reads:
/// the set runs on the parsing task, ahead of what the main thread has applied, so each storage
/// keeps a primary-key index of its own for rows that have actually been stored.
///
/// A table without one identifies its rows by their BSATN bytes, as the official SDK does for
/// every table. Those bytes are copied out of the message, so that a resident row does not keep
/// the whole websocket payload it arrived in alive.
pub struct RowSet<T: Table> {
    rows: Residents<T>,
    /// Counts the updates that paired deletes with inserts. See [`Keyed::touched_in`].
    epoch: u32,
    tickets: Tickets,
}

enum Residents<T: Table> {
    ByPk(HashMap<T::Pk, Keyed>),
    ByBytes(HashMap<Box<[u8]>, Resident>),
}

/// Names a resident row of one table, for as long as it is resident.
///
/// A slot number, like the index half of an `Entity`, and free for reuse once its row has left.
/// It needs no generation half: a ticket is forgotten where it is given out in the same step
/// that frees it, so a stale one cannot exist. `u32` rather than `usize` because two arrays and
/// a map hold one per row, and no client holds four billion rows of one table.
pub type Ticket = u32;

/// Hands out tickets, and takes back the ones whose rows have left.
#[derive(Default)]
struct Tickets {
    free: Vec<Ticket>,
    next: Ticket,
}

impl Tickets {
    /// Returns a ticket for a new row: one a departed row gave back, or the next never used.
    fn take(&mut self) -> Ticket {
        self.free.pop().unwrap_or_else(|| {
            self.next += 1;
            self.next - 1
        })
    }
}

#[derive(Debug)]
struct Resident {
    refcount: u32,
    slot: Slot,
}

/// A resident of a set keyed by primary key, where a message can give a stored row a new value.
#[derive(Debug)]
struct Keyed {
    resident: Resident,
    /// The epoch of the update that last named this key, and the key's place among the keys
    /// that update touched. With this the map is its own record of what a message has touched,
    /// and a key whose count a message leaves as it was is looked up once per row and no more.
    touched_in: u32,
    touch: u32,
}

impl Keyed {
    fn new(refcount: u32, slot: Slot) -> Self {
        Self {
            resident: Resident { refcount, slot },
            touched_in: 0,
            touch: 0,
        }
    }
}

/// What [`commit`] needs of either kind of resident.
trait HasSlot {
    fn slot_mut(&mut self) -> &mut Slot;
    fn into_slot(self) -> Slot;
}

impl HasSlot for Resident {
    fn slot_mut(&mut self) -> &mut Slot {
        &mut self.slot
    }
    fn into_slot(self) -> Slot {
        self.slot
    }
}

impl HasSlot for Keyed {
    fn slot_mut(&mut self) -> &mut Slot {
        &mut self.resident.slot
    }
    fn into_slot(self) -> Slot {
        self.resident.slot
    }
}

#[derive(Debug, Clone, Copy)]
enum Slot {
    /// Resident, and storage has been told about it.
    Stored(Ticket),
    /// On its way to storage; names its place among the rows going there.
    Pending(u32),
}

/// Keys come from a server the client chose to trust, and there is one lookup per row of every
/// message, so this trades `SipHash`'s flood resistance for speed. Still seeded per process.
type HashMap<K, V> = std::collections::HashMap<K, V, RandomState>;

/// Rows on their way into the sink, under the keys they are counted by.
/// `None` for one that the rest of its message took back out.
type Pending<K, R> = Vec<Option<(K, R)>>;

/// A key that the update being applied names, in a set keyed by primary key.
///
/// Rows stay in the message while it is worked out, named here by their index in it, and each
/// then moves once, to where it goes.
struct Touch {
    stored: Option<StoredCount>,
    /// The values this message inserted under the key. Nearly always one.
    first: Option<Value>,
    rest: Vec<Value>,
    /// A delete of the key, for a key with no insert to be read from. `NONE` if there is none.
    delete: u32,
}

/// The stored row with a touched key.
struct StoredCount {
    handle: Ticket,
    /// How many subscriptions covered it before this message, and how many of those are left.
    before: u32,
    left: u32,
}

struct Value {
    /// Index of the first insert of this value.
    insert: u32,
    /// Inserts of this value, less deletes of it.
    count: u32,
}

const NONE: u32 = u32::MAX;

impl Touch {
    fn new(stored: Option<StoredCount>) -> Self {
        Self {
            stored,
            first: None,
            rest: Vec::new(),
            delete: NONE,
        }
    }

    fn insert<R>(&mut self, index: u32, inserts: &[ParsedRow<R>], sources: &[Bytes]) {
        // Nearly always the key's only value, with nothing to compare it to.
        let same = if self.first.is_none() {
            None
        } else {
            let bsatn = bytes_of(sources, inserts[index as usize].at);
            self.first
                .iter_mut()
                .chain(&mut self.rest)
                .find(|value| bytes_of(sources, inserts[value.insert as usize].at) == bsatn)
        };
        if let Some(value) = same {
            value.count += 1;
            return;
        }
        let value = Value {
            insert: index,
            count: 1,
        };
        match self.first {
            None => self.first = Some(value),
            Some(_) => self.rest.push(value),
        }
    }

    /// Counts a delete against the value it names. `false` if nothing with those bytes is left.
    ///
    /// No bytes are kept for the stored row, so a delete that matches nothing this message
    /// inserted is taken to be a delete of it.
    fn delete<R>(&mut self, bsatn: &[u8], inserts: &[ParsedRow<R>], sources: &[Bytes]) -> bool {
        let same = self
            .first
            .iter_mut()
            .chain(&mut self.rest)
            .find(|value| value.count > 0 && bytes_of(sources, inserts[value.insert as usize].at) == bsatn);
        if let Some(value) = same {
            value.count -= 1;
            return true;
        }
        match &mut self.stored {
            Some(stored) if stored.left > 0 => {
                stored.left -= 1;
                true
            }
            _ => false,
        }
    }

    /// Returns the value still counted, and how often. The server holds one row per key, so at
    /// most one is.
    fn counted(&self) -> (u32, Option<u32>) {
        let (mut count, mut insert) = (0, None);
        for value in self.first.iter().chain(&self.rest) {
            if value.count > 0 {
                count += value.count;
                insert = Some(value.insert);
            }
        }
        (count, insert)
    }
}

impl<T: Table> Default for RowSet<T> {
    fn default() -> Self {
        let rows = if TypeId::of::<T::Pk>() == TypeId::of::<NoPk>() {
            Residents::ByBytes(HashMap::default())
        } else {
            Residents::ByPk(HashMap::default())
        };
        Self {
            rows,
            epoch: 0,
            tickets: Tickets::default(),
        }
    }
}

/// Why a server message could not be applied.
#[derive(thiserror::Error, Debug, PartialEq, Eq)]
pub enum RowSetError {
    /// The server and client disagree about what is subscribed.
    /// The set is no longer trustworthy and the connection should be dropped.
    #[error("the server deleted a row of table `{table}` that is not resident")]
    DeleteOfAbsentRow {
        /// The table the delete was for.
        table: &'static str,
    },
    /// A delete named a row whose primary key these bindings could not read.
    /// The client and the module it was generated from do not agree.
    ///
    /// The reason is boxed to keep this type small: every `apply` returns it, the successful
    /// ones included, and a `DecodeError` in it would make `Result<(), RowSetError>` 56 bytes
    /// instead of 32. That did not show in the benchmarks, so it is tidiness and not a fix.
    #[error("failed to read the primary key of a deleted row of table `{table}`")]
    DeleteKey {
        /// The table the delete was for.
        table: &'static str,
        /// Where the decoder gave up.
        #[source]
        source: Box<bsatn::DecodeError>,
    },
}

fn pk_of<T: Table>(row: &T::Row) -> &T::Pk {
    T::pk(row).expect("`Table::pk` is `Some` for every row of a table whose `Pk` is not `NoPk`")
}

/// Reads the primary key of the row a delete names, which is all that is ever read out of one.
fn pk_of_delete<T: Table>(sources: &[Bytes], delete: &ParsedDelete) -> Result<T::Pk, RowSetError> {
    let key = T::pk_from_bsatn(bytes_of(sources, delete.at)).map_err(|source| RowSetError::DeleteKey {
        table: T::NAME,
        source: Box::new(source),
    })?;
    Ok(key.expect("`Table::pk_from_bsatn` is `Some` for every row of a table whose `Pk` is not `NoPk`"))
}

/// What one server message changed in one table, worked out against what was resident.
pub struct Applied<T: Table> {
    /// The net change, naming every row that was already stored by its ticket.
    pub diff: TableDiff<T, Ticket>,
    /// The ticket of each row of `diff.inserts`, in the same order.
    pub tickets: Vec<Ticket>,
}

impl<T: Table> RowSet<T> {
    /// Returns the number of resident rows.
    pub fn len(&self) -> usize {
        match &self.rows {
            Residents::ByPk(rows) => rows.len(),
            Residents::ByBytes(rows) => rows.len(),
        }
    }

    /// Returns `true` if no row of this table is resident.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the ticket of the row with primary key `pk`. Always `None` for tables without one.
    ///
    /// The set's own view, which runs ahead of the storages: a row this names may not have been
    /// applied yet. Nothing outside the set reads it, so it is here for the set's own tests.
    #[cfg(test)]
    fn handle_of(&self, pk: &T::Pk) -> Option<Ticket> {
        let Residents::ByPk(rows) = &self.rows else {
            return None;
        };
        match rows.get(pk)?.resident.slot {
            Slot::Stored(handle) => Some(handle),
            Slot::Pending(_) => None,
        }
    }

    /// Applies one server message's changes to this table and returns the net change.
    ///
    /// `None` if nothing changed on net. `update.events` is ignored; event tables have no
    /// resident rows.
    ///
    /// The rows that go to the sink are moved out of `update`. What is left in it afterwards is
    /// of no further use, and is left so that the caller can choose where it is dropped: after a
    /// large transaction that is a row per delete, each with whatever it owns on the heap.
    ///
    /// # Errors
    ///
    /// [`RowSetError`] if the message deletes a row that is not resident. Nothing has been
    /// reported as changed then, and the set still knows every row in it, but the counts may no
    /// longer be right. The only sound next step is a [`reconcile`](Self::reconcile), which
    /// counts anew and is how the tables are cleared.
    pub fn apply(&mut self, update: &mut TableUpdate<T::Row>) -> Result<Option<Applied<T>>, RowSetError> {
        let tickets = &mut self.tickets;
        match &mut self.rows {
            Residents::ByPk(rows) => apply_by_pk::<T>(rows, &mut self.epoch, tickets, update),
            Residents::ByBytes(rows) => apply_by_bytes::<T>(rows, tickets, update),
        }
    }

    /// Replaces the resident rows with `snapshot` and returns the net change.
    ///
    /// For a reconnect: `snapshot` is every row of this table across all resubscribed queries,
    /// taken at one point in time. Rows present before and after keep their tickets, and are
    /// reported as updates whether or not their contents changed — the set is on the parsing
    /// task and cannot see the stored row to compare it with. The storage, which can, drops the
    /// ones that did not change; `spacetimedb_bevy`'s `retain_changed` is where that happens.
    pub fn reconcile(&mut self, mut snapshot: TableUpdate<T::Row>) -> Option<Applied<T>> {
        let (snapshot, _, sources) = snapshot.parts();
        let snapshot = snapshot.drain(..);
        let tickets = &mut self.tickets;
        match &mut self.rows {
            Residents::ByPk(rows) => {
                let mut before = std::mem::take(rows);
                let mut pending: Pending<T::Pk, T::Row> = Vec::new();
                let mut updates = Vec::new();
                for row in snapshot {
                    match rows.entry(pk_of::<T>(&row.row).clone()) {
                        Entry::Occupied(mut resident) => {
                            resident.get_mut().resident.refcount += 1;
                        }
                        Entry::Vacant(vacant) => {
                            let slot = match before.remove(vacant.key()).map(HasSlot::into_slot) {
                                Some(Slot::Stored(handle)) => {
                                    updates.push(RowUpdate { handle, new: row.row });
                                    Slot::Stored(handle)
                                }
                                _ => {
                                    pending.push(Some((vacant.key().clone(), row.row)));
                                    Slot::Pending(pending.len() as u32 - 1)
                                }
                            };
                            vacant.insert(Keyed::new(1, slot));
                        }
                    }
                }
                let removed = stored_handles(before);
                commit_pending::<T, _, _>(rows, tickets, removed, updates, pending)
            }
            Residents::ByBytes(rows) => {
                let mut before = std::mem::take(rows);
                let mut pending: Pending<Box<[u8]>, T::Row> = Vec::new();
                for row in snapshot {
                    match rows.entry(bytes_of(sources, row.at).into()) {
                        Entry::Occupied(mut resident) => resident.get_mut().refcount += 1,
                        Entry::Vacant(vacant) => {
                            let slot = match before.remove(vacant.key()) {
                                Some(resident) => resident.slot,
                                None => {
                                    pending.push(Some((vacant.key().clone(), row.row)));
                                    Slot::Pending(pending.len() as u32 - 1)
                                }
                            };
                            vacant.insert(Resident { refcount: 1, slot });
                        }
                    }
                }
                let removed = stored_handles(before);
                commit_pending::<T, _, _>(rows, tickets, removed, Vec::new(), pending)
            }
        }
    }
}

fn stored_handles<K, V: HasSlot>(rows: HashMap<K, V>) -> Vec<Ticket> {
    rows.into_values()
        .map(|resident| match resident.into_slot() {
            Slot::Stored(handle) => handle,
            Slot::Pending(_) => unreachable!("no row stays pending between calls"),
        })
        .collect()
}

/// Takes rows that only the failed update put in the set back out of it.
fn forget_pending<K: Eq + Hash, R>(rows: &mut HashMap<K, Resident>, pending: Pending<K, R>) {
    for (key, _) in pending.into_iter().flatten() {
        rows.remove(&key);
    }
}

fn apply_by_bytes<T: Table>(
    rows: &mut HashMap<Box<[u8]>, Resident>,
    tickets: &mut Tickets,
    update: &mut TableUpdate<T::Row>,
) -> Result<Option<Applied<T>>, RowSetError> {
    // Inserts go first. A join can make the server send `delete r, insert r` for a row that
    // stays resident, and counting the delete first would evict it.
    let (inserts, deletes, sources) = update.parts();
    let mut pending: Pending<Box<[u8]>, T::Row> = Vec::new();
    for insert in inserts.drain(..) {
        // Looked up by the message's bytes, which are copied only if the row is new.
        let bsatn = bytes_of(sources, insert.at);
        if let Some(resident) = rows.get_mut(bsatn) {
            resident.refcount += 1;
            continue;
        }
        let key: Box<[u8]> = bsatn.into();
        let slot = Slot::Pending(pending.len() as u32);
        rows.insert(key.clone(), Resident { refcount: 1, slot });
        pending.push(Some((key, insert.row)));
    }

    // The keys are kept beside the handles until the message is known to be sound.
    let (mut removed, mut removed_keys) = (Vec::new(), Vec::new());
    for delete in deletes {
        let bsatn = bytes_of(sources, delete.at);
        let Some(resident) = rows.get_mut(bsatn) else {
            // The sink still holds the rows taken out so far, so the set must too, or the
            // `reconcile` that follows would never find them. It counts them anew.
            for (key, handle) in removed_keys.into_iter().zip(removed) {
                let slot = Slot::Stored(handle);
                rows.insert(key, Resident { refcount: 1, slot });
            }
            forget_pending(rows, pending);
            return Err(RowSetError::DeleteOfAbsentRow { table: T::NAME });
        };
        resident.refcount -= 1;
        if resident.refcount > 0 {
            continue;
        }
        match rows.remove_entry(bsatn).map(|(key, resident)| (key, resident.slot)) {
            Some((key, Slot::Stored(handle))) => {
                removed.push(handle);
                removed_keys.push(key);
            }
            // Inserted and deleted by the same message: never seen by the sink.
            Some((_, Slot::Pending(index))) => pending[index as usize] = None,
            None => unreachable!("found above"),
        }
    }

    Ok(commit_pending::<T, _, _>(rows, tickets, removed, Vec::new(), pending))
}

/// Where an insert of the message goes once the message is worked out.
#[derive(Clone, Copy)]
enum Dest {
    /// Nowhere: one more subscription covering a row as it is, or a row the message took back out.
    Nowhere,
    /// The new value of the row stored under this ticket.
    Update(Ticket),
    /// A new row.
    Insert,
}

fn apply_by_pk<T: Table>(
    rows: &mut HashMap<T::Pk, Keyed>,
    epoch: &mut u32,
    tickets: &mut Tickets,
    update: &mut TableUpdate<T::Row>,
) -> Result<Option<Applied<T>>, RowSetError> {
    // Without deletes no stored row can have changed: every insert is a new key or one more
    // subscription covering a row as it is. Snapshots are like this, and they are the big ones.
    if update.deletes.is_empty() {
        let mut pending: Pending<T::Pk, T::Row> = Vec::with_capacity(update.inserts.len());
        if rows.is_empty() {
            rows.reserve(update.inserts.len());
        }
        for insert in update.inserts.drain(..) {
            match rows.entry(pk_of::<T>(&insert.row).clone()) {
                Entry::Occupied(mut resident) => resident.get_mut().resident.refcount += 1,
                Entry::Vacant(vacant) => {
                    let key = vacant.key().clone();
                    let slot = Slot::Pending(pending.len() as u32);
                    vacant.insert(Keyed::new(1, slot));
                    pending.push(Some((key, insert.row)));
                }
            }
        }
        return Ok(commit_pending::<T, _, _>(
            rows,
            tickets,
            Vec::new(),
            Vec::new(),
            pending,
        ));
    }

    // Residents made by other paths carry epoch 0, so that is never the current one.
    *epoch = epoch.wrapping_add(1);
    if *epoch == 0 {
        for keyed in rows.values_mut() {
            keyed.touched_in = 0;
        }
        *epoch = 1;
    }
    let epoch = *epoch;

    /// Records the first mention, by this message, of a key whose row is stored.
    fn touch_stored(keyed: &mut Keyed, epoch: u32, touched: &mut Vec<Touch>) -> u32 {
        let Slot::Stored(handle) = keyed.resident.slot else {
            unreachable!("no row stays pending between calls");
        };
        keyed.touched_in = epoch;
        keyed.touch = touched.len() as u32;
        touched.push(Touch::new(Some(StoredCount {
            handle,
            before: keyed.resident.refcount,
            left: keyed.resident.refcount,
        })));
        keyed.touch
    }

    let (inserts, deletes, sources) = update.parts();
    // Every key the message names gets an entry here, which its resident points at. The count
    // of a stored row is left alone until the message is known to be sound. A key that is new
    // is counted as the message goes, since an unsound message takes it back out whole.
    let mut touched: Vec<Touch> = Vec::with_capacity(inserts.len().max(deletes.len()));

    // Inserts go first, as above.
    for (index, insert) in inserts.iter().enumerate() {
        let pk = pk_of::<T>(&insert.row);
        let touch = match rows.get_mut(pk) {
            Some(keyed) if keyed.touched_in == epoch => {
                if touched[keyed.touch as usize].stored.is_none() {
                    keyed.resident.refcount += 1;
                }
                keyed.touch
            }
            Some(keyed) => touch_stored(keyed, epoch, &mut touched),
            None => {
                let mut keyed = Keyed::new(1, Slot::Pending(index as u32));
                keyed.touched_in = epoch;
                keyed.touch = touched.len() as u32;
                let touch = keyed.touch;
                rows.insert(pk.clone(), keyed);
                touched.push(Touch::new(None));
                touch
            }
        };
        touched[touch as usize].insert(index as u32, inserts, sources);
    }

    // A delete is never decoded; its key is read out of its bytes, and nothing else of it is
    // read at all. Whatever goes wrong here, the keys this message put in the set come back out
    // below before the error is returned.
    let mut failed = None;
    for (index, delete) in deletes.iter().enumerate() {
        let pk = match pk_of_delete::<T>(sources, delete) {
            Ok(pk) => pk,
            Err(error) => {
                failed = Some(error);
                break;
            }
        };
        let Some(keyed) = rows.get_mut(&pk) else {
            failed = Some(RowSetError::DeleteOfAbsentRow { table: T::NAME });
            break;
        };
        let touch = if keyed.touched_in == epoch {
            keyed.touch
        } else {
            touch_stored(keyed, epoch, &mut touched)
        };
        let touch = &mut touched[touch as usize];
        if touch.delete == NONE {
            touch.delete = index as u32;
        }
        if !touch.delete(bytes_of(sources, delete.at), inserts, sources) {
            failed = Some(RowSetError::DeleteOfAbsentRow { table: T::NAME });
            break;
        }
        if touch.stored.is_none() {
            keyed.resident.refcount -= 1;
        }
    }

    // A key's own copy is not kept: it is read from a row of the message that has it, and for a
    // key that only a delete names, out of that delete's bytes again. Reading it again cannot
    // fail, because the same bytes gave up the same key in the loop above.
    let pk_of_touch = |touch: &Touch, inserts: &[ParsedRow<T::Row>]| -> T::Pk {
        match &touch.first {
            Some(value) => pk_of::<T>(&inserts[value.insert as usize].row).clone(),
            None => pk_of_delete::<T>(sources, &deletes[touch.delete as usize])
                .expect("these bytes gave up a key once already"),
        }
    };

    if let Some(error) = failed {
        for touch in touched.iter().filter(|touch| touch.stored.is_none()) {
            rows.remove(&pk_of_touch(touch, inserts));
        }
        return Err(error);
    }

    let mut dest: Vec<Dest> = vec![Dest::Nowhere; inserts.len()];
    let mut removed = Vec::new();
    let mut new_keys = 0;
    for touch in &touched {
        let (new_count, value) = touch.counted();
        match &touch.stored {
            Some(stored) => {
                let count = stored.left + new_count;
                if count == 0 {
                    rows.remove(&pk_of_touch(touch, inserts));
                    removed.push(stored.handle);
                    continue;
                }
                // While the stored value is still counted, an insert is another subscription
                // starting to cover it. Once it is not, what is counted is its new value.
                if stored.left == 0
                    && let Some(value) = value
                {
                    dest[value as usize] = Dest::Update(stored.handle);
                }
                // Nearly always the same subscriptions cover the row before and after.
                if count != stored.before {
                    let keyed = rows.get_mut(&pk_of_touch(touch, inserts));
                    keyed.expect("touched").resident.refcount = count;
                }
            }
            None => match value {
                // Inserted and deleted by the same message: never seen by the sink.
                None => {
                    rows.remove(&pk_of_touch(touch, inserts));
                }
                Some(value) => {
                    dest[value as usize] = Dest::Insert;
                    new_keys += 1;
                    // Its resident names the first value inserted, which nearly always is it.
                    if touch.first.as_ref().is_some_and(|first| first.insert != value) {
                        let keyed = rows.get_mut(&pk_of_touch(touch, inserts));
                        keyed.expect("touched").resident.slot = Slot::Pending(value);
                    }
                }
            },
        }
    }
    drop(touched);

    let walk = walk_to_write_back(new_keys, rows.len());
    let mut diff = TableDiff {
        deletes: removed,
        updates: Vec::new(),
        inserts: Vec::with_capacity(new_keys),
    };
    let mut keys = Vec::new();
    let mut position = if new_keys > 0 {
        vec![0; inserts.len()]
    } else {
        Vec::new()
    };
    for (index, insert) in inserts.drain(..).enumerate() {
        match dest[index] {
            Dest::Nowhere => {}
            Dest::Update(handle) => diff.updates.push(RowUpdate {
                handle,
                new: insert.row,
            }),
            Dest::Insert => {
                position[index] = diff.inserts.len() as u32;
                if !walk {
                    keys.push(pk_of::<T>(&insert.row).clone());
                }
                diff.inserts.push(insert.row);
            }
        }
    }

    Ok(commit::<T, _, _>(rows, tickets, diff, &position, keys))
}

/// Returns `true` when the handles of new rows should be written back by walking the set once,
/// which needs no keys, rather than by looking each new row's key up. Walking wins when most of
/// the set is new, as with a snapshot.
fn walk_to_write_back(new_rows: usize, residents: usize) -> bool {
    new_rows * 4 >= residents
}

/// Commits rows that were collected under their keys; see [`commit`].
fn commit_pending<T, K, V>(
    rows: &mut HashMap<K, V>,
    tickets: &mut Tickets,
    removed: Vec<Ticket>,
    updates: Vec<RowUpdate<T, Ticket>>,
    pending: Pending<K, T::Row>,
) -> Option<Applied<T>>
where
    T: Table,
    K: Eq + Hash,
    V: HasSlot,
{
    // Where each pending row is among those the sink gets, which skip the rows that a later
    // part of the message cancelled.
    let mut position = Vec::with_capacity(pending.len());
    let (mut keys, mut inserts) = (Vec::new(), Vec::with_capacity(pending.len()));
    let walk = walk_to_write_back(pending.len(), rows.len());
    for row in pending {
        position.push(inserts.len() as u32);
        if let Some((key, row)) = row {
            inserts.push(row);
            if !walk {
                keys.push(key);
            }
        }
    }
    let diff = TableDiff {
        deletes: removed,
        updates,
        inserts,
    };
    commit::<T, _, _>(rows, tickets, diff, &position, keys)
}

/// Gives the diff's new rows their tickets, and records each in the resident that named it.
///
/// A resident on its way in is `Slot::Pending(p)`, and `position[p]` is where its row is in
/// `diff.inserts`. `keys` has the key of each of those rows, or is empty for the tickets to be
/// written back by walking the set.
fn commit<T, K, V>(
    rows: &mut HashMap<K, V>,
    tickets: &mut Tickets,
    diff: TableDiff<T, Ticket>,
    position: &[u32],
    keys: Vec<K>,
) -> Option<Applied<T>>
where
    T: Table,
    K: Eq + Hash,
    V: HasSlot,
{
    if diff.is_empty() {
        return None;
    }

    // A ticket can go to a new row of the diff that frees it: storage applies deletes first.
    tickets.free.extend(&diff.deletes);
    let handles: Vec<Ticket> = diff.inserts.iter().map(|_| tickets.take()).collect();
    if handles.is_empty() {
        return Some(Applied { diff, tickets: handles });
    }
    if keys.is_empty() {
        // In memory order, where a lookup per row would jump about a map that outgrew the cache.
        for resident in rows.values_mut() {
            let slot = resident.slot_mut();
            if let Slot::Pending(pending) = *slot {
                *slot = Slot::Stored(handles[position[pending as usize] as usize]);
            }
        }
    } else {
        for (key, handle) in keys.into_iter().zip(&handles) {
            *rows.get_mut(&key).expect("inserted by the caller").slot_mut() = Slot::Stored(*handle);
        }
    }
    Some(Applied { diff, tickets: handles })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::module::{
        Module, NoPk, ParseError, RawTableRows, ReducerVisitor, TableKind, TableVisitor, UpdateVisitor,
    };

    struct TestModule;

    impl Module for TestModule {
        type Update = ();
        fn parse_table(_: &mut (), _: &str, _: RawTableRows) -> Result<(), ParseError> {
            Ok(())
        }
        fn visit_update<V: UpdateVisitor<Self>>(_: (), _: &mut V) {}
        fn visit_tables<V: TableVisitor<Self>>(_: &mut V) {}
        fn visit_reducers<V: ReducerVisitor<Self>>(_: &mut V) {}
    }

    /// `(id, value)`, with `id` as the primary key of [`Keyed`].
    type TestRow = (u32, u32);

    struct Keyed;
    impl Table for Keyed {
        type Module = TestModule;
        type Row = TestRow;
        type Pk = u32;
        const NAME: &'static str = "keyed";
        const KIND: TableKind = TableKind::Persistent;
        fn pk(row: &TestRow) -> Option<&u32> {
            Some(&row.0)
        }
    }

    /// [`Keyed`] again, but reading a delete's key out of its bytes rather than decoding the
    /// row first, as generated bindings do wherever the key leads the encoding.
    struct KeyedFast;
    impl Table for KeyedFast {
        type Module = TestModule;
        type Row = TestRow;
        type Pk = u32;
        const NAME: &'static str = "keyed_fast";
        const KIND: TableKind = TableKind::Persistent;
        fn pk(row: &TestRow) -> Option<&u32> {
            Some(&row.0)
        }
        fn pk_from_bsatn(mut bsatn: &[u8]) -> Result<Option<u32>, bsatn::DecodeError> {
            bsatn::from_reader(&mut bsatn).map(Some)
        }
    }

    struct Unkeyed;
    impl Table for Unkeyed {
        type Module = TestModule;
        type Row = TestRow;
        type Pk = NoPk;
        const NAME: &'static str = "unkeyed";
        const KIND: TableKind = TableKind::Persistent;
        fn pk(_: &TestRow) -> Option<&NoPk> {
            None
        }
    }

    /// A storage stand-in: it keeps rows by ticket and records what it was told to do.
    #[derive(Default)]
    struct Slab {
        rows: Vec<Option<TestRow>>,
        log: Vec<String>,
    }

    impl Slab {
        fn resident(&self) -> Vec<TestRow> {
            let mut rows: Vec<_> = self.rows.iter().flatten().copied().collect();
            rows.sort();
            rows
        }

        fn take_log(&mut self) -> Vec<String> {
            let mut log = std::mem::take(&mut self.log);
            log.sort();
            log
        }
    }

    impl Slab {
        /// Stores what the set worked out, as a real storage would.
        fn store<T: Table<Row = TestRow>>(&mut self, applied: Option<Applied<T>>) {
            let Some(applied) = applied else { return };
            for ticket in applied.diff.deletes {
                let row = self.rows[ticket as usize].take().expect("live ticket");
                self.log.push(format!("delete {row:?}"));
            }
            for update in applied.diff.updates {
                let new = update.new;
                let old = self.rows[update.handle as usize].replace(new).expect("live ticket");
                self.log.push(format!("update {old:?} -> {new:?}"));
            }
            for (row, ticket) in applied.diff.inserts.into_iter().zip(applied.tickets) {
                if self.rows.len() <= ticket as usize {
                    self.rows.resize(ticket as usize + 1, None);
                }
                self.rows[ticket as usize] = Some(row);
                self.log.push(format!("insert {row:?}"));
            }
        }
    }

    /// Applies `update` and hands the change to `sink`, which is what the plugin's parsing task
    /// and its main thread do between them.
    fn apply<T: Table<Row = TestRow>>(
        set: &mut RowSet<T>,
        update: &mut TableUpdate<TestRow>,
        sink: &mut Slab,
    ) -> Result<(), RowSetError> {
        sink.store(set.apply(update)?);
        Ok(())
    }

    /// The same for a reconnect's snapshot.
    fn reconcile<T: Table<Row = TestRow>>(set: &mut RowSet<T>, snapshot: TableUpdate<TestRow>, sink: &mut Slab) {
        sink.store(set.reconcile(snapshot));
    }

    fn update(inserts: &[TestRow], deletes: &[TestRow]) -> TableUpdate<TestRow> {
        TableUpdate::from_rows(inserts.iter().copied(), deletes.iter().copied())
    }

    fn snapshot(rows: &[TestRow]) -> TableUpdate<TestRow> {
        update(rows, &[])
    }

    #[test]
    fn overlapping_subscriptions_insert_once_and_delete_once() {
        let (mut set, mut sink) = (RowSet::<Keyed>::default(), Slab::default());

        apply(&mut set, &mut update(&[(1, 10)], &[]), &mut sink).unwrap();
        assert_eq!(sink.take_log(), ["insert (1, 10)"]);

        // A second subscription covers the same row.
        apply(&mut set, &mut update(&[(1, 10)], &[]), &mut sink).unwrap();
        assert_eq!(sink.take_log(), [""; 0]);

        // The first subscription ends; the second still covers the row.
        apply(&mut set, &mut update(&[], &[(1, 10)]), &mut sink).unwrap();
        assert_eq!(sink.take_log(), [""; 0]);
        assert_eq!(sink.resident(), [(1, 10)]);

        apply(&mut set, &mut update(&[], &[(1, 10)]), &mut sink).unwrap();
        assert_eq!(sink.take_log(), ["delete (1, 10)"]);
        assert!(set.is_empty());
    }

    /// Two semijoins over the same table can report `delete r, insert r` for a row that stays.
    /// See `handle_delete` in the official SDK's `client_cache.rs`.
    #[test]
    fn delete_and_insert_of_the_same_row_is_no_change() {
        let (mut set, mut sink) = (RowSet::<Keyed>::default(), Slab::default());
        apply(&mut set, &mut update(&[(1, 10)], &[]), &mut sink).unwrap();
        sink.take_log();

        apply(&mut set, &mut update(&[(1, 10)], &[(1, 10)]), &mut sink).unwrap();
        assert_eq!(sink.take_log(), [""; 0]);
        assert_eq!(sink.resident(), [(1, 10)]);
    }

    #[test]
    fn row_inserted_and_deleted_by_one_message_never_reaches_the_sink() {
        let (mut set, mut sink) = (RowSet::<Keyed>::default(), Slab::default());
        apply(&mut set, &mut update(&[(1, 10)], &[(1, 10)]), &mut sink).unwrap();
        assert_eq!(sink.take_log(), [""; 0]);
        assert!(set.is_empty());
    }

    #[test]
    fn same_primary_key_is_an_update_that_keeps_its_handle() {
        let (mut set, mut sink) = (RowSet::<Keyed>::default(), Slab::default());
        apply(&mut set, &mut update(&[(1, 10), (2, 20)], &[]), &mut sink).unwrap();
        sink.take_log();

        apply(
            &mut set,
            &mut update(&[(1, 11), (3, 30)], &[(1, 10), (2, 20)]),
            &mut sink,
        )
        .unwrap();
        assert_eq!(
            sink.take_log(),
            ["delete (2, 20)", "insert (3, 30)", "update (1, 10) -> (1, 11)"]
        );
        // Slot 0 still holds row 1: the update reused the handle.
        assert_eq!(sink.rows[0], Some((1, 11)));

        // The updated row is deleted through the handle it inherited.
        apply(&mut set, &mut update(&[], &[(1, 11)]), &mut sink).unwrap();
        assert_eq!(sink.take_log(), ["delete (1, 11)"]);
    }

    #[test]
    fn update_seen_by_two_subscriptions_is_one_update() {
        let (mut set, mut sink) = (RowSet::<Keyed>::default(), Slab::default());
        apply(&mut set, &mut update(&[(1, 10), (1, 10)], &[]), &mut sink).unwrap();
        sink.take_log();

        apply(
            &mut set,
            &mut update(&[(1, 11), (1, 11)], &[(1, 10), (1, 10)]),
            &mut sink,
        )
        .unwrap();
        assert_eq!(sink.take_log(), ["update (1, 10) -> (1, 11)"]);

        // Both subscriptions must end before the row goes.
        apply(&mut set, &mut update(&[], &[(1, 11)]), &mut sink).unwrap();
        assert_eq!(sink.take_log(), [""; 0]);
    }

    /// The row changes and one of two subscriptions stops matching it.
    #[test]
    fn update_that_leaves_one_subscription_is_still_an_update() {
        let (mut set, mut sink) = (RowSet::<Keyed>::default(), Slab::default());
        apply(&mut set, &mut update(&[(1, 10), (1, 10)], &[]), &mut sink).unwrap();
        sink.take_log();

        apply(&mut set, &mut update(&[(1, 11)], &[(1, 10), (1, 10)]), &mut sink).unwrap();
        assert_eq!(sink.take_log(), ["update (1, 10) -> (1, 11)"]);

        apply(&mut set, &mut update(&[], &[(1, 11)]), &mut sink).unwrap();
        assert_eq!(sink.take_log(), ["delete (1, 11)"]);
    }

    #[test]
    fn without_a_primary_key_a_change_is_a_delete_and_an_insert() {
        let (mut set, mut sink) = (RowSet::<Unkeyed>::default(), Slab::default());
        apply(&mut set, &mut update(&[(1, 10)], &[]), &mut sink).unwrap();
        sink.take_log();

        apply(&mut set, &mut update(&[(1, 11)], &[(1, 10)]), &mut sink).unwrap();
        assert_eq!(sink.take_log(), ["delete (1, 10)", "insert (1, 11)"]);
    }

    #[test]
    fn deleting_an_absent_row_is_an_error() {
        let (mut set, mut sink) = (RowSet::<Keyed>::default(), Slab::default());
        assert_eq!(
            apply(&mut set, &mut update(&[], &[(1, 10)]), &mut sink),
            Err(RowSetError::DeleteOfAbsentRow { table: "keyed" })
        );
    }

    /// Every row that survived is reported as an update, whether or not it changed: the set
    /// runs on the parsing task and cannot reach the stored row to compare with. The storage
    /// drops the ones that did not move — `spacetimedb_bevy`'s `retain_changed`, which
    /// `tests/reconnect_mock.rs` covers end to end. What matters here is that a survivor keeps its
    /// ticket, so that the storage has a row to compare against at all.
    #[test]
    fn reconcile_keeps_handles_of_rows_that_stayed() {
        let (mut set, mut sink) = (RowSet::<Keyed>::default(), Slab::default());
        apply(&mut set, &mut update(&[(1, 10), (2, 20), (3, 30)], &[]), &mut sink).unwrap();
        sink.take_log();

        // While disconnected: row 1 unchanged, row 2 changed, row 3 deleted, row 4 created.
        reconcile(&mut set, snapshot(&[(1, 10), (2, 21), (4, 40)]), &mut sink);
        assert_eq!(
            sink.take_log(),
            [
                "delete (3, 30)",
                "insert (4, 40)",
                "update (1, 10) -> (1, 10)",
                "update (2, 20) -> (2, 21)"
            ]
        );
        assert_eq!(sink.rows[0], Some((1, 10)));
        assert_eq!(sink.rows[1], Some((2, 21)));

        // Refcounts were rebuilt from the snapshot, so the normal stream continues cleanly.
        apply(&mut set, &mut update(&[], &[(1, 10), (2, 21), (4, 40)]), &mut sink).unwrap();
        assert!(set.is_empty());
        assert_eq!(sink.resident(), []);
    }

    #[test]
    fn reconcile_counts_rows_covered_by_several_subscriptions() {
        let (mut set, mut sink) = (RowSet::<Keyed>::default(), Slab::default());
        apply(&mut set, &mut update(&[(1, 10)], &[]), &mut sink).unwrap();
        sink.take_log();

        reconcile(&mut set, snapshot(&[(1, 10), (1, 10)]), &mut sink);
        // One row, counted twice, reported once — as an unchanged update; see above.
        assert_eq!(sink.take_log(), ["update (1, 10) -> (1, 10)"]);

        apply(&mut set, &mut update(&[], &[(1, 10)]), &mut sink).unwrap();
        assert_eq!(sink.resident(), [(1, 10)]);
    }

    #[test]
    fn reconcile_with_nothing_clears_the_table() {
        let (mut set, mut sink) = (RowSet::<Keyed>::default(), Slab::default());
        apply(&mut set, &mut update(&[(1, 10), (2, 20)], &[]), &mut sink).unwrap();
        sink.take_log();

        reconcile(&mut set, snapshot(&[]), &mut sink);
        assert_eq!(sink.take_log(), ["delete (1, 10)", "delete (2, 20)"]);
        assert!(set.is_empty());
    }

    #[test]
    fn keyed_rows_are_found_by_key() {
        let (mut set, mut sink) = (RowSet::<Keyed>::default(), Slab::default());
        apply(&mut set, &mut update(&[(1, 10), (2, 20)], &[]), &mut sink).unwrap();
        assert_eq!(set.handle_of(&2), Some(1));
        apply(&mut set, &mut update(&[(2, 21)], &[(2, 20)]), &mut sink).unwrap();
        assert_eq!(set.handle_of(&2), Some(1));
        apply(&mut set, &mut update(&[], &[(2, 21)]), &mut sink).unwrap();
        assert_eq!(set.handle_of(&2), None);
    }

    #[test]
    fn an_error_leaves_a_set_that_can_be_cleared() {
        let (mut set, mut sink) = (RowSet::<Keyed>::default(), Slab::default());
        apply(&mut set, &mut update(&[(1, 10)], &[]), &mut sink).unwrap();
        sink.take_log();

        // Row 1 is touched and row 2 is pending when the delete of absent row 3 is met.
        assert_eq!(
            apply(
                &mut set,
                &mut update(&[(1, 11), (2, 20)], &[(1, 10), (3, 30)]),
                &mut sink
            ),
            Err(RowSetError::DeleteOfAbsentRow { table: "keyed" })
        );
        assert_eq!(sink.take_log(), [""; 0]);

        reconcile(&mut set, snapshot(&[]), &mut sink);
        assert_eq!(sink.take_log(), ["delete (1, 10)"]);
        assert!(set.is_empty());
    }

    /// The same for a set keyed by bytes, which has taken stored rows out by the time it meets
    /// the delete of an absent one. The sink still holds them, so they have to be findable.
    #[test]
    fn an_error_leaves_an_unkeyed_set_that_can_be_cleared() {
        let (mut set, mut sink) = (RowSet::<Unkeyed>::default(), Slab::default());
        apply(&mut set, &mut update(&[(1, 10), (2, 20)], &[]), &mut sink).unwrap();
        sink.take_log();

        // Row 1 has left the set and row 3 is pending when the delete of absent row 4 is met.
        assert_eq!(
            apply(&mut set, &mut update(&[(3, 30)], &[(1, 10), (4, 40)]), &mut sink),
            Err(RowSetError::DeleteOfAbsentRow { table: "unkeyed" })
        );
        assert_eq!(sink.take_log(), [""; 0]);
        assert_eq!(set.len(), 2);

        // A reconnect's snapshot that still has row 1 finds it, and does not store it twice.
        reconcile(&mut set, snapshot(&[(1, 10)]), &mut sink);
        assert_eq!(sink.take_log(), ["delete (2, 20)"]);
        assert_eq!(sink.resident(), [(1, 10)]);

        reconcile(&mut set, snapshot(&[]), &mut sink);
        assert_eq!(sink.take_log(), ["delete (1, 10)"]);
        assert!(set.is_empty());
    }

    #[test]
    fn deleting_a_pending_row_by_other_bytes_is_an_error() {
        let (mut set, mut sink) = (RowSet::<Keyed>::default(), Slab::default());
        assert_eq!(
            apply(&mut set, &mut update(&[(1, 11)], &[(1, 10)]), &mut sink),
            Err(RowSetError::DeleteOfAbsentRow { table: "keyed" })
        );
        assert!(set.is_empty());
    }

    /// The same key is changed by one message after another: what a message noted about a key
    /// must not be read by the next.
    #[test]
    fn a_key_updated_by_consecutive_messages() {
        let (mut set, mut sink) = (RowSet::<Keyed>::default(), Slab::default());
        apply(&mut set, &mut update(&[(1, 10)], &[]), &mut sink).unwrap();
        sink.take_log();
        for value in 11..15 {
            apply(&mut set, &mut update(&[(1, value)], &[(1, value - 1)]), &mut sink).unwrap();
            let expected = format!("update (1, {}) -> (1, {value})", value - 1);
            assert_eq!(sink.take_log(), [expected]);
        }
        apply(&mut set, &mut update(&[], &[(1, 14)]), &mut sink).unwrap();
        assert_eq!(sink.take_log(), ["delete (1, 14)"]);
        assert!(set.is_empty());
    }

    /// New rows beside updated ones, few among many residents and many among few: the two ways
    /// their handles are written back.
    #[test]
    fn new_rows_in_a_message_with_deletes_are_found_afterwards() {
        for residents in [1, 100] {
            let (mut set, mut sink) = (RowSet::<Keyed>::default(), Slab::default());
            let before: Vec<TestRow> = (0..residents).map(|id| (id, 0)).collect();
            apply(&mut set, &mut update(&before, &[]), &mut sink).unwrap();
            sink.take_log();

            let message = update(&[(0, 1), (1000, 5), (1001, 6)], &[(0, 0)]);
            apply(&mut set, &mut { message }, &mut sink).unwrap();
            assert_eq!(
                sink.take_log(),
                ["insert (1000, 5)", "insert (1001, 6)", "update (0, 0) -> (0, 1)"]
            );
            for (id, value) in [(0, 1), (1000, 5), (1001, 6)] {
                let handle = set.handle_of(&id).expect("resident");
                assert_eq!(sink.rows[handle as usize], Some((id, value)));
            }

            apply(&mut set, &mut update(&[], &[(1000, 5), (0, 1)]), &mut sink).unwrap();
            assert_eq!(sink.take_log(), ["delete (0, 1)", "delete (1000, 5)"]);
            assert_eq!(set.len(), residents as usize);
        }
    }

    /// A new key gets one value, loses it, and gets another, all in one message.
    #[test]
    fn a_new_key_whose_first_value_does_not_last() {
        let (mut set, mut sink) = (RowSet::<Keyed>::default(), Slab::default());
        apply(&mut set, &mut update(&[(9, 90)], &[]), &mut sink).unwrap();
        sink.take_log();

        apply(
            &mut set,
            &mut update(&[(1, 10), (1, 11), (2, 20)], &[(1, 10), (9, 90)]),
            &mut sink,
        )
        .unwrap();
        assert_eq!(sink.take_log(), ["delete (9, 90)", "insert (1, 11)", "insert (2, 20)"]);
        let handle = set.handle_of(&1).expect("resident");
        assert_eq!(sink.rows[handle as usize], Some((1, 11)));
        apply(&mut set, &mut update(&[], &[(1, 11), (2, 20)]), &mut sink).unwrap();
        assert!(set.is_empty());
    }

    /// A subscription starts to cover a row in the message that another stops covering one.
    #[test]
    fn counts_that_a_message_with_deletes_changes() {
        let (mut set, mut sink) = (RowSet::<Keyed>::default(), Slab::default());
        apply(&mut set, &mut update(&[(1, 10), (2, 20), (2, 20)], &[]), &mut sink).unwrap();
        sink.take_log();

        // Row 1 is covered twice from here, row 2 once.
        apply(&mut set, &mut update(&[(1, 10)], &[(2, 20)]), &mut sink).unwrap();
        assert_eq!(sink.take_log(), [""; 0]);

        apply(&mut set, &mut update(&[], &[(1, 10), (2, 20)]), &mut sink).unwrap();
        assert_eq!(sink.take_log(), ["delete (2, 20)"]);
        apply(&mut set, &mut update(&[], &[(1, 10)]), &mut sink).unwrap();
        assert_eq!(sink.take_log(), ["delete (1, 10)"]);
        assert!(set.is_empty());
    }

    /// A keyed table reads a delete's key and nothing else, so a delete the row type cannot be
    /// made of still removes the row its key names. A table that leaves `pk_from_bsatn` at the
    /// default reads the whole row for the same key, and refuses the same delete.
    #[test]
    fn only_the_key_of_a_delete_is_read() {
        // `(1, 10)`, with two bytes where the second column's four should be.
        let truncated = &[1, 0, 0, 0, 10, 0][..];

        let (mut set, mut sink) = (RowSet::<KeyedFast>::default(), Slab::default());
        apply(&mut set, &mut update(&[(1, 10)], &[]), &mut sink).unwrap();
        sink.take_log();
        let mut message = update(&[], &[]);
        message.push_raw_delete(truncated);
        apply(&mut set, &mut message, &mut sink).unwrap();
        assert_eq!(sink.take_log(), ["delete (1, 10)"]);
        assert!(set.is_empty());

        let (mut set, mut sink) = (RowSet::<Keyed>::default(), Slab::default());
        apply(&mut set, &mut update(&[(1, 10)], &[]), &mut sink).unwrap();
        sink.take_log();
        let mut message = update(&[], &[]);
        message.push_raw_delete(truncated);
        assert!(matches!(
            apply(&mut set, &mut message, &mut sink),
            Err(RowSetError::DeleteKey { table: "keyed", .. })
        ));
        assert_eq!(sink.resident(), [(1, 10)]);
    }

    /// A delete whose key cannot be read at all ends the connection, and leaves the set and the
    /// sink as they were, including the keys the same message had just put in.
    #[test]
    fn a_delete_whose_key_does_not_read_is_refused() {
        let (mut set, mut sink) = (RowSet::<KeyedFast>::default(), Slab::default());
        apply(&mut set, &mut update(&[(1, 10)], &[]), &mut sink).unwrap();
        sink.take_log();

        // Two bytes, where the key alone wants four.
        let mut message = update(&[(2, 20)], &[]);
        message.push_raw_delete(&[7, 0]);
        assert!(matches!(
            apply(&mut set, &mut message, &mut sink),
            Err(RowSetError::DeleteKey {
                table: "keyed_fast",
                ..
            })
        ));
        assert_eq!(sink.take_log(), [""; 0]);
        assert_eq!(sink.resident(), [(1, 10)]);
        assert_eq!(set.len(), 1);
        assert!(set.handle_of(&2).is_none());
    }

    /// What is left of the message is the caller's to drop: every delete, and no insert.
    #[test]
    fn apply_leaves_the_deletes_in_the_message() {
        let (mut set, mut sink) = (RowSet::<Keyed>::default(), Slab::default());
        apply(&mut set, &mut update(&[(1, 10)], &[]), &mut sink).unwrap();
        let mut message = update(&[(1, 11)], &[(1, 10)]);
        apply(&mut set, &mut message, &mut sink).unwrap();
        assert_eq!((message.inserts.len(), message.deletes.len()), (0, 1));
    }

    #[test]
    fn unkeyed_rows_are_counted_by_their_bytes() {
        let (mut set, mut sink) = (RowSet::<Unkeyed>::default(), Slab::default());
        apply(&mut set, &mut update(&[(1, 10), (1, 10), (1, 11)], &[]), &mut sink).unwrap();
        assert_eq!(sink.take_log(), ["insert (1, 10)", "insert (1, 11)"]);
        apply(&mut set, &mut update(&[], &[(1, 10)]), &mut sink).unwrap();
        assert_eq!(sink.take_log(), [""; 0]);
        apply(&mut set, &mut update(&[], &[(1, 10), (1, 11)]), &mut sink).unwrap();
        assert_eq!(sink.take_log(), ["delete (1, 10)", "delete (1, 11)"]);
        assert_eq!(
            apply(&mut set, &mut update(&[(2, 20)], &[(1, 10)]), &mut sink),
            Err(RowSetError::DeleteOfAbsentRow { table: "unkeyed" })
        );
        assert!(set.is_empty());
    }
}
