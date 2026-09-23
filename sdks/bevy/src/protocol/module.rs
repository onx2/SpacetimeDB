//! Traits implemented by generated bindings.
//!
//! A module is described by types, not values: one [`Module`] marker, one [`Table`] marker per
//! table or view, and one args struct per reducer and procedure. Nothing here knows where rows
//! are stored; that is for whoever applies a [`crate::protocol::TableDiff`].

use std::fmt::Debug;
use std::hash::Hash;

use bytes::Bytes;
use spacetimedb_client_api_messages::websocket::{
    common::{BsatnRowList, RowListLen as _, RowSizeHint},
    v2 as ws,
};
use spacetimedb_lib::{bsatn, de::DeserializeOwned, ser::Serialize};

use crate::protocol::threads::{self, Threads};

/// A row type of some table.
pub trait Row: DeserializeOwned + Serialize + Clone + Debug + Send + Sync + 'static {}
impl<T: DeserializeOwned + Serialize + Clone + Debug + Send + Sync + 'static> Row for T {}

/// Whether the server keeps a table's rows or only announces them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableKind {
    /// Rows persist until deleted. Subject to refcounting in a [`crate::protocol::RowSet`].
    Persistent,
    /// Rows are announced once and never cached.
    Event,
}

/// The primary key type of tables that have none.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum NoPk {}

/// A table or view of a module.
///
/// Implemented on a marker type, because several tables may share one row type.
/// When only one table uses a row type, the row type can be its own marker.
pub trait Table: Send + Sync + 'static {
    /// The module this table belongs to.
    type Module: Module;
    /// The type one row decodes into.
    type Row: Row;
    /// [`NoPk`] if the table has no primary key.
    type Pk: Eq + Hash + Clone + Send + Sync + 'static;

    /// The name the server uses for this table, including any submodule namespace.
    const NAME: &'static str;
    /// Whether the server keeps this table's rows or only announces them.
    const KIND: TableKind;

    /// Returns the primary key of `row`, or `None` for every row if the table has no primary key.
    ///
    /// A delete and an insert with equal primary keys in one transaction are an update.
    fn pk(row: &Self::Row) -> Option<&Self::Pk>;

    /// Returns the primary key of the row `bsatn` encodes, reading as little of the encoding as
    /// it can, or `None` for every row if the table has no primary key.
    ///
    /// This is how a [`ParsedDelete`] is read, and the only thing ever read out of one: a delete
    /// names the row it removes by repeating its bytes, and a keyed table wants nothing of it but
    /// the key. Bytes after the key are not looked at, so a delete is not held against the row
    /// type the way an insert is.
    ///
    /// The default decodes the whole row and takes its key, which is right for every table and
    /// fast for none. Generated bindings override it where the columns before the key are all
    /// fixed width, which is where the saving is: skipping a column costs nothing, while decoding
    /// one that owns a `String` costs an allocation and a free.
    ///
    /// # Errors
    ///
    /// [`DecodeError`](bsatn::DecodeError) if `bsatn` does not begin with a row of this table.
    fn pk_from_bsatn(bsatn: &[u8]) -> Result<Option<Self::Pk>, bsatn::DecodeError> {
        let row: Self::Row = bsatn::from_slice(bsatn)?;
        Ok(Self::pk(&row).cloned())
    }

    /// Visits the table's unique columns other than its primary key, for storage to index.
    fn visit_unique_columns<V: UniqueColumnVisitor<Self>>(_visitor: &mut V)
    where
        Self: Sized,
    {
    }
}

/// A column of a table in which no two rows hold the same value, so a value finds one row.
///
/// Implemented on a marker type per column. The primary key is not one of these: every storage
/// indexes it anyway, and [`Table::pk`] is how.
pub trait UniqueColumn: Send + Sync + 'static {
    /// The table this column belongs to.
    type Table: Table;
    /// The column's type, which storage indexes rows by.
    type Key: Eq + Hash + Clone + Send + Sync + 'static;
    /// The column's name in the module.
    const NAME: &'static str;

    /// Returns this column's value in `row`.
    fn key(row: &<Self::Table as Table>::Row) -> &Self::Key;
}

/// Receives each unique column of a table from [`Table::visit_unique_columns`].
pub trait UniqueColumnVisitor<T: Table> {
    /// Called once for each unique column `C` of `T`.
    fn column<C: UniqueColumn<Table = T>>(&mut self);
}

/// The arguments of a reducer.
pub trait Reducer: Serialize + Clone + Send + Sync + 'static {
    /// The module this reducer belongs to.
    type Module: Module;
    /// The name the server calls it by.
    const NAME: &'static str;
}

/// The arguments of a procedure.
pub trait Procedure: Serialize + Clone + Send + Sync + 'static {
    /// The module this procedure belongs to.
    type Module: Module;
    /// What the procedure returns.
    type Output: DeserializeOwned + Clone + Send + Sync + 'static;
    /// The name the server calls it by.
    const NAME: &'static str;
}

/// Where a row's bytes are in the [`TableUpdate`] that carries them: which of its sources, and
/// which stretch of that source.
///
/// The bytes are the row's identity: a delete names the row it removes by repeating its bytes.
/// They are named by place rather than held, because holding them is a share in the message's
/// reference count per row, taken and given back with an atomic each, and contended for by
/// anything that would decode rows of one message on several threads.
#[derive(Debug, Clone, Copy)]
pub(crate) struct At {
    source: u32,
    start: usize,
    end: usize,
}

/// A row together with where the bytes it was decoded from are, in the [`TableUpdate`] it is
/// part of: `bytes_of` reads them, given that update's sources.
///
/// Made only by decoding. It can move from one `TableUpdate` to another only as a row: its
/// bytes are those of the update it was decoded into.
#[derive(Debug)]
pub struct ParsedRow<R> {
    /// The decoded row.
    pub row: R,
    /// Which of the update's sources, and where in it.
    pub(crate) at: At,
}

/// A row a message removes, which is nothing but where its bytes are.
///
/// A delete names the row it removes by repeating its bytes, and that is all anything wants of
/// it: a table without a primary key matches those bytes against what is resident, and a table
/// with one reads the key out of them with [`Table::pk_from_bsatn`]. So a delete is never
/// decoded, and a list of deletes costs what it costs to measure — for 100,000 rows that own a
/// `String` each, 100,000 allocations fewer than decoding them, which is the greater part of
/// what a mass update used to spend on parsing.
///
/// The rest of a deleted row is therefore never held against the row type. A delete whose key
/// reads and whose other columns are nonsense now removes the row it names, where before it
/// ended the connection.
#[derive(Debug)]
pub struct ParsedDelete {
    /// Which of the update's sources, and where in it.
    pub(crate) at: At,
}

/// All changes to one table carried by one server message.
#[derive(Debug)]
pub struct TableUpdate<R> {
    /// Rows the message adds, or that a new subscription now covers.
    pub inserts: Vec<ParsedRow<R>>,
    /// Rows the message removes, or that a dropped subscription no longer covers.
    pub deletes: Vec<ParsedDelete>,
    /// Rows of an event table. Never cached, so their bytes are not kept.
    pub events: Vec<R>,
    /// The row lists that `inserts` and `deletes` were decoded from.
    sources: Vec<Bytes>,
}

impl<R> Default for TableUpdate<R> {
    fn default() -> Self {
        Self {
            inserts: Vec::new(),
            deletes: Vec::new(),
            events: Vec::new(),
            sources: Vec::new(),
        }
    }
}

impl<R> TableUpdate<R> {
    /// Returns `true` if the update carries no rows of any kind.
    pub fn is_empty(&self) -> bool {
        self.inserts.is_empty() && self.deletes.is_empty() && self.events.is_empty()
    }

    /// Returns the bytes `delete` names. `delete` must be one of this update's.
    #[cfg(test)]
    pub(crate) fn delete_bytes(&self, delete: &ParsedDelete) -> &[u8] {
        bytes_of(&self.sources, delete.at)
    }

    /// Returns the parts separately, for code that reads the bytes of some rows while it moves
    /// others out.
    pub(crate) fn parts(&mut self) -> (&mut Vec<ParsedRow<R>>, &[ParsedDelete], &[Bytes]) {
        (&mut self.inserts, &self.deletes, &self.sources)
    }
}

/// Returns the bytes at `at`, given the sources of the update it belongs to.
/// Free function so that it can be called while parts of that update are borrowed separately.
pub(crate) fn bytes_of(sources: &[Bytes], at: At) -> &[u8] {
    &sources[at.source as usize][at.start..at.end]
}

/// One table's rows as they came off the wire. Opaque: bindings pass it to [`TableUpdate::append`].
pub struct RawTableRows(pub(crate) RawRows);

/// Which kind of server message a table's rows came out of, and so which list they belong in.
pub(crate) enum RawRows {
    /// Part of a transaction update.
    Update(ws::TableUpdate),
    /// Initial rows of a subscription.
    Inserts(BsatnRowList),
    /// Rows a dropped subscription no longer covers.
    Deletes(BsatnRowList),
}

impl<R: Row> TableUpdate<R> {
    /// Creates an update from rows rather than by decoding a message, for tests and tools.
    /// Each row is encoded, since its bytes are what a later delete is matched by, and for a
    /// delete they are all that is kept.
    pub fn from_rows(inserts: impl IntoIterator<Item = R>, deletes: impl IntoIterator<Item = R>) -> Self {
        let mut update = Self::default();
        let mut bytes = Vec::new();
        let encode = |bytes: &mut Vec<u8>, row: &R| {
            let start = bytes.len();
            bsatn::to_writer(bytes, row).expect("a row encodes");
            At {
                source: 0,
                start,
                end: bytes.len(),
            }
        };
        for row in inserts {
            let at = encode(&mut bytes, &row);
            update.inserts.push(ParsedRow { row, at });
        }
        for row in deletes {
            let at = encode(&mut bytes, &row);
            update.deletes.push(ParsedDelete { at });
        }
        update.sources.push(bytes.into());
        update
    }

    /// Adds a delete that names `bsatn`, whatever `bsatn` is.
    ///
    /// For tests of what a delete costs the row type, which since nothing decodes one is only
    /// its leading key: the server has sent a message this client cannot make a row of, and the
    /// question is which part of it the client was going to read.
    #[cfg(test)]
    pub(crate) fn push_raw_delete(&mut self, bsatn: &[u8]) {
        let at = At {
            source: self.sources.len() as u32,
            start: 0,
            end: bsatn.len(),
        };
        self.sources.push(Bytes::copy_from_slice(bsatn));
        self.deletes.push(ParsedDelete { at });
    }

    /// Decodes `rows` and adds them to this update.
    ///
    /// The server may send rows for the same table several times in one message,
    /// once per query set, so this appends rather than replaces.
    ///
    /// Only the inserts are decoded: a delete is kept as the stretch of bytes it names, which is
    /// everything [`ParsedDelete`] is for.
    ///
    /// # Errors
    ///
    /// [`ParseError`] if an inserted row is not the row type these bindings expect, or if a row
    /// list's offsets reach past the data it came with.
    pub fn append(&mut self, table: &'static str, rows: RawTableRows) -> Result<(), ParseError> {
        match rows.0 {
            RawRows::Inserts(rows) => self.decode_rows(table, rows),
            RawRows::Deletes(rows) => self.record_deletes(table, rows),
            RawRows::Update(update) => {
                for rows in update.rows {
                    match rows {
                        ws::TableUpdateRows::PersistentTable(rows) => {
                            self.record_deletes(table, rows.deletes)?;
                            self.decode_rows(table, rows.inserts)?;
                        }
                        ws::TableUpdateRows::EventTable(rows) => {
                            for bsatn in &rows.events {
                                self.events.push(decode_row(table, &bsatn)?);
                            }
                        }
                    }
                }
                Ok(())
            }
        }
    }

    /// Decoding a long list splits it over the threads [`crate::protocol::lend_threads`] was given, if it
    /// was given any. Rows are independent, so the parts do not talk to each other, and the rows
    /// come out in the order they were sent either way.
    ///
    /// This was tried once before and was slower (docs/HISTORY.md, 2026-09-20): every row then
    /// held a slice of the one payload, and the threads spent their time on its reference count.
    /// Rows name their bytes by place now and the payload is shared as a plain `&[u8]`, so no
    /// part of this touches a count another part is touching.
    fn decode_rows(&mut self, table: &'static str, rows: BsatnRowList) -> Result<(), ParseError> {
        let count = rows.len();
        if count == 0 {
            return Ok(());
        }
        let (hint, data) = rows.into_inner();
        let source = self.sources.len() as u32;
        let out = &mut self.inserts;
        out.reserve(count);
        match threads::lent().filter(|_| count >= MIN_SPLIT) {
            Some(threads) => decode_in_parts(table, &hint, &data, source, out, count, threads)?,
            None => {
                for index in 0..count {
                    out.push(decode_at(table, &hint, &data, source, index)?);
                }
            }
        }
        self.sources.push(data);
        Ok(())
    }

    /// Records where each row of a delete list is, without decoding any of it.
    ///
    /// This is the whole of the work a delete costs, and it is why there is nothing here to hand
    /// to other threads: finding a row is arithmetic on the list's size hint, while decoding one
    /// allocates for every `String` it owns, and that allocation is what
    /// [`decode_rows`](Self::decode_rows) splits.
    ///
    /// # Errors
    ///
    /// [`ParseError::RowBounds`] if the list names a row outside the data it came with. The
    /// bytes themselves are not read, so nothing else here can fail.
    fn record_deletes(&mut self, table: &'static str, rows: BsatnRowList) -> Result<(), ParseError> {
        let count = rows.len();
        if count == 0 {
            return Ok(());
        }
        let (hint, data) = rows.into_inner();
        let source = self.sources.len() as u32;
        self.deletes.reserve(count);
        for index in 0..count {
            let at = row_at(table, &hint, &data, source, index)?;
            self.deletes.push(ParsedDelete { at });
        }
        self.sources.push(data);
        Ok(())
    }
}

/// A row list shorter than this is decoded where it is parsed: handing parts of it to other
/// threads costs more than they save. Chosen so that a list that is split is worth a thread per
/// [`MIN_PART`] rows.
const MIN_SPLIT: usize = 4096;

/// No part of a split row list is shorter than this, however many threads are free.
const MIN_PART: usize = 2048;

/// One thread's share of a row list: which rows are its, and where it puts them.
struct Part<R> {
    /// This part's rows, in the order the server sent them.
    rows: Vec<ParsedRow<R>>,
    /// The index in the row list of the first of them.
    first: usize,
    /// How many rows are this part's. `rows` is this long unless the part stopped short.
    len: usize,
    /// Why this part stopped, if it did.
    failed: Option<ParseError>,
}

/// Decodes `count` rows of `data` into `out`, over `threads`. `count` must not be zero.
///
/// Each part decodes into a vector of its own, and they are appended in order, so the rows come
/// out as the server sent them however the work was divided.
///
/// **This was `unsafe` until 2026-09-22**, with the parts writing into `out`'s spare capacity so
/// that each row moved once instead of twice. That is faster — a 100,000-row snapshot parses in
/// 2.09 ms against 2.67, and a big transaction in 3.05 against 3.76 — and it was still the wrong
/// trade. The whole of the difference falls on the parsing task, so it moves a join by about half
/// a millisecond and no frame by anything; measured on the published harness that is 7.6% of
/// joining and 0% of the longest frame. Half a millisecond off a join, once, is not what the only
/// `unsafe` in a client library is worth. See the progress log in `docs/GAMEPLAN.md`; do not put
/// it back for a percentage.
///
/// # Panics
///
/// If `threads` did not run every job it was given to its end, which is the contract of
/// [`Threads::run`]. Rows already decoded are dropped with their parts, whether this panics,
/// returns an error, or a job panicked and [`Threads::run`] let it through — which is the other
/// thing the safe version gets for nothing, since the old one had to drop them by hand.
fn decode_in_parts<R: Row>(
    table: &'static str,
    hint: &RowSizeHint,
    data: &[u8],
    source: u32,
    out: &mut Vec<ParsedRow<R>>,
    count: usize,
    threads: &dyn Threads,
) -> Result<(), ParseError> {
    // Never zero, so never a division by zero, however few threads or rows there are.
    let per = count.div_ceil(threads.count().min(count / MIN_PART).max(1));
    let mut parts: Vec<Part<R>> = (0..count)
        .step_by(per)
        .map(|first| Part {
            rows: Vec::new(),
            first,
            len: per.min(count - first),
            failed: None,
        })
        .collect();

    let mut jobs: Vec<Box<dyn FnMut() + Send + '_>> = parts
        .iter_mut()
        .map(|part| {
            Box::new(move || {
                part.rows.reserve(part.len);
                for offset in 0..part.len {
                    match decode_at(table, hint, data, source, part.first + offset) {
                        Ok(row) => part.rows.push(row),
                        Err(error) => {
                            part.failed = Some(error);
                            return;
                        }
                    }
                }
            }) as Box<dyn FnMut() + Send>
        })
        .collect();
    threads.run(&mut jobs);
    drop(jobs);

    if let Some(error) = parts.iter_mut().find_map(|part| part.failed.take()) {
        return Err(error);
    }
    // Every part having decoded every row of its own part is what makes the list whole, so it is
    // checked rather than assumed: a part would also stop short if a `Threads` did not run its job.
    assert!(
        parts.iter().all(|part| part.rows.len() == part.len),
        "a `Threads` left a job of a row list unrun, or ran one only part way"
    );
    for part in &mut parts {
        out.append(&mut part.rows);
    }
    Ok(())
}

/// Says which stretch of a row list's data row `index` claims, which may not be one the data
/// has. Arithmetic only, as `BsatnRowList` does it, which it does only for whoever takes a
/// `Bytes` of each row.
fn row_bounds(hint: &RowSizeHint, data: &[u8], index: usize) -> (usize, usize) {
    match hint {
        RowSizeHint::FixedSize(size) => {
            let size = *size as usize;
            (index * size, (index + 1) * size)
        }
        RowSizeHint::RowOffsets(offsets) => (
            offsets[index] as usize,
            offsets.get(index + 1).map_or(data.len(), |end| *end as usize),
        ),
    }
}

/// Says where row `index` of a row list is in the list's data, without reading it.
fn row_at(table: &'static str, hint: &RowSizeHint, data: &[u8], source: u32, index: usize) -> Result<At, ParseError> {
    let (start, end) = row_bounds(hint, data, index);
    if data.get(start..end).is_none() {
        return Err(ParseError::RowBounds { table });
    }
    Ok(At { source, start, end })
}

/// Decodes row `index` of a row list, and says where in the list's data it was.
///
/// The bounds are taken and checked here rather than through [`row_at`], so that a row this
/// decodes is measured against the data once and not twice: this runs for every inserted row of
/// every message, and a second bounds check per row shows up at 100,000 of them.
fn decode_at<R: Row>(
    table: &'static str,
    hint: &RowSizeHint,
    data: &[u8],
    source: u32,
    index: usize,
) -> Result<ParsedRow<R>, ParseError> {
    let (start, end) = row_bounds(hint, data, index);
    let bsatn = data.get(start..end).ok_or(ParseError::RowBounds { table })?;
    Ok(ParsedRow {
        row: decode_row(table, bsatn)?,
        at: At { source, start, end },
    })
}

fn decode_row<R: Row>(table: &'static str, bsatn: &[u8]) -> Result<R, ParseError> {
    bsatn::from_slice(bsatn).map_err(|source| ParseError::Row { table, source })
}

/// Why a server message's rows could not be turned into the module's types. Always fatal to the
/// connection: the client and the module it was generated from do not agree.
#[derive(thiserror::Error, Debug)]
pub enum ParseError {
    /// The module has a table these bindings were not generated from.
    #[error("the server sent rows for table `{0}`, which these bindings do not know")]
    UnknownTable(Box<str>),
    /// A row's bytes are not the row type these bindings expect.
    #[error("failed to decode a row of table `{table}`")]
    Row {
        /// The table the row belongs to.
        table: &'static str,
        /// Where the decoder gave up.
        #[source]
        source: bsatn::DecodeError,
    },
    /// A row list's offsets reach past the data it came with.
    #[error("a row list of table `{table}` names a row outside its data")]
    RowBounds {
        /// The table the row list belongs to.
        table: &'static str,
    },
}

/// A module, implemented by generated bindings on a marker type.
///
/// Parsing and applying are separate steps so that parsing can run off the main thread:
/// [`Self::parse_table`] fills a [`Self::Update`] wherever is convenient,
/// and [`Self::visit_update`] later hands each table's part to whoever owns the row storage.
pub trait Module: Sized + Send + Sync + 'static {
    /// A [`TableUpdate`] for every table of the module.
    type Update: Default + Debug + Send + 'static;

    /// Decodes `rows` of the table the server calls `table` into `update`.
    ///
    /// Generated as a `match` on the name that calls [`TableUpdate::append`] on the matching
    /// field, or returns [`ParseError::UnknownTable`].
    ///
    /// # Errors
    ///
    /// [`ParseError`] if the module has a table these bindings do not know, or a row does not
    /// decode.
    fn parse_table(update: &mut Self::Update, table: &str, rows: RawTableRows) -> Result<(), ParseError>;

    /// Calls `visitor` once per table with that table's part of `update`.
    fn visit_update<V: UpdateVisitor<Self>>(update: Self::Update, visitor: &mut V);

    /// Calls `visitor` once per table. Used to set up storage for every table.
    fn visit_tables<V: TableVisitor<Self>>(visitor: &mut V);

    /// Calls `visitor` once per reducer. Used to set up result delivery for every reducer.
    fn visit_reducers<V: ReducerVisitor<Self>>(visitor: &mut V);

    /// Calls `visitor` once per procedure. Modules without procedures keep the default.
    fn visit_procedures<V: ProcedureVisitor<Self>>(_visitor: &mut V) {}
}

/// Receives each table's changes from [`Module::visit_update`].
pub trait UpdateVisitor<M: Module> {
    /// Called once for each table `T` of `M`, with that table's part of the update.
    fn table<T: Table<Module = M>>(&mut self, update: TableUpdate<T::Row>);
}

/// Receives each table of a module from [`Module::visit_tables`].
pub trait TableVisitor<M: Module> {
    /// Called once for each table `T` of `M`.
    fn table<T: Table<Module = M>>(&mut self);
}

/// Receives each reducer of a module from [`Module::visit_reducers`].
pub trait ReducerVisitor<M: Module> {
    /// Called once for each reducer `R` of `M`.
    fn reducer<R: Reducer<Module = M>>(&mut self);
}

/// Parses the rows of a `TransactionUpdate`, which names the query sets it reached.
/// Each table may appear once per set; [`TableUpdate::append`] adds them up.
pub(crate) fn parse_transaction_update<M: Module>(raw: ws::TransactionUpdate) -> Result<M::Update, ParseError> {
    let mut update = M::Update::default();
    for query_set in raw.query_sets {
        for table in query_set.tables {
            let name = table.table_name.clone();
            M::parse_table(&mut update, &name, RawTableRows(RawRows::Update(table)))?;
        }
    }
    Ok(update)
}

/// Parses the rows of a `SubscribeApplied` (all inserts) or an `UnsubscribeApplied` (all deletes).
pub(crate) fn parse_query_rows<M: Module>(raw: ws::QueryRows, inserts: bool) -> Result<M::Update, ParseError> {
    let mut update = M::Update::default();
    for table in raw.tables {
        let rows = if inserts {
            RawRows::Inserts(table.rows)
        } else {
            RawRows::Deletes(table.rows)
        };
        M::parse_table(&mut update, &table.table, RawTableRows(rows))?;
    }
    Ok(update)
}

/// Receives each procedure of a module from [`Module::visit_procedures`].
pub trait ProcedureVisitor<M: Module> {
    /// Called once for each procedure `P` of `M`.
    fn procedure<P: Procedure<Module = M>>(&mut self);
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use super::*;

    /// A row that owns something, so that a part's rows are a leak if they go unnoticed.
    type TestRow = (u32, String);

    /// Every job on a thread of its own, so that the parts really do run at the same time.
    struct OneThreadEach;

    impl Threads for OneThreadEach {
        fn count(&self) -> usize {
            8
        }

        fn run<'a>(&self, jobs: &mut [Box<dyn FnMut() + Send + 'a>]) {
            std::thread::scope(|scope| {
                for job in jobs {
                    scope.spawn(job);
                }
            });
        }
    }

    /// Makes row `index` of the list one no bindings can read: its first field's length says it
    /// runs past everything that follows it.
    fn spoil(hint: &RowSizeHint, data: &mut [u8], index: usize) {
        let RowSizeHint::RowOffsets(offsets) = hint else {
            unreachable!("the list is one of offsets")
        };
        let at = offsets[index] as usize;
        data[at..at + 8].fill(0xFF);
    }

    /// How many [`Counted`] rows have been dropped, over the whole test binary.
    static DROPPED: AtomicUsize = AtomicUsize::new(0);

    /// A row that says when it is dropped. Everything about it is owned, so a row that is not
    /// dropped is a leak.
    #[derive(spacetimedb_lib::ser::Serialize, spacetimedb_lib::de::Deserialize, Clone, Debug)]
    #[sats(crate = spacetimedb_lib)]
    struct Counted {
        name: String,
    }

    impl Drop for Counted {
        fn drop(&mut self) {
            DROPPED.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// `count` [`Counted`] rows and the row list that carries them.
    fn counted_row_list(count: usize) -> (RowSizeHint, Vec<u8>) {
        let mut data = Vec::new();
        let mut offsets = Vec::new();
        for index in 0..count {
            offsets.push(data.len() as u64);
            let row = Counted {
                name: "x".repeat(index % 7 + 1),
            };
            bsatn::to_writer(&mut data, &row).expect("a row encodes");
        }
        (RowSizeHint::RowOffsets(offsets.into()), data)
    }

    /// `count` rows, and the row list that carries them: rows of several lengths, so that the
    /// list is one of offsets rather than of a fixed size.
    fn row_list(count: usize) -> (RowSizeHint, Vec<u8>, Vec<TestRow>) {
        let rows: Vec<TestRow> = (0..count).map(|index| (index as u32, "x".repeat(index % 7))).collect();
        let mut data = Vec::new();
        let mut offsets = Vec::new();
        for row in &rows {
            offsets.push(data.len() as u64);
            bsatn::to_writer(&mut data, row).expect("a row encodes");
        }
        (RowSizeHint::RowOffsets(offsets.into()), data, rows)
    }

    /// The same rows, in the same order, wherever they were decoded.
    #[test]
    fn parts_decode_what_one_thread_decodes() {
        let count = 10_000;
        let (hint, data, rows) = row_list(count);

        let mut serial: Vec<ParsedRow<TestRow>> = Vec::with_capacity(count);
        for index in 0..count {
            serial.push(decode_at("t", &hint, &data, 3, index).expect("the row decodes"));
        }

        let mut parts: Vec<ParsedRow<TestRow>> = Vec::with_capacity(count);
        decode_in_parts("t", &hint, &data, 3, &mut parts, count, &OneThreadEach).expect("the rows decode");

        assert_eq!(parts.len(), count);
        for (index, (part, one)) in parts.iter().zip(&serial).enumerate() {
            assert_eq!(part.row, rows[index]);
            assert_eq!(
                (part.row.clone(), part.at.source, part.at.start, part.at.end),
                (one.row.clone(), one.at.source, one.at.start, one.at.end)
            );
        }
    }

    /// Rows of a fixed size are found by multiplying rather than by looking up, on every thread.
    #[test]
    fn parts_decode_a_fixed_size_list() {
        let count = 10_000;
        let rows: Vec<(u32, u32)> = (0..count).map(|index| (index as u32, 7)).collect();
        let mut data = Vec::new();
        for row in &rows {
            bsatn::to_writer(&mut data, row).expect("a row encodes");
        }
        let hint = RowSizeHint::FixedSize((data.len() / count) as u16);

        let mut parts: Vec<ParsedRow<(u32, u32)>> = Vec::with_capacity(count);
        decode_in_parts("t", &hint, &data, 0, &mut parts, count, &OneThreadEach).expect("the rows decode");

        assert_eq!(parts.len(), count);
        assert!(parts.iter().zip(&rows).all(|(part, row)| part.row == *row));
    }

    /// One row the bindings cannot read fails the whole list, whichever part it was in.
    #[test]
    fn a_row_that_does_not_decode_fails_the_list() {
        let count = 10_000;
        let (hint, mut data, _) = row_list(count);
        spoil(&hint, &mut data, count / 2);

        let mut parts: Vec<ParsedRow<TestRow>> = Vec::with_capacity(count);
        let error = decode_in_parts("t", &hint, &data, 0, &mut parts, count, &OneThreadEach)
            .expect_err("the spoiled row does not decode");
        assert!(matches!(error, ParseError::Row { table: "t", .. }));
        assert!(parts.is_empty());
    }

    /// Nothing decodes a delete, so a list of deletes this client could not make rows of is
    /// recorded all the same; the same list of inserts is the parse error it always was.
    #[test]
    fn a_delete_list_is_recorded_without_being_decoded() {
        let count = 1_000;
        let (hint, mut data, _) = row_list(count);
        spoil(&hint, &mut data, count / 2);
        let list = || BsatnRowList::new(hint.clone(), Bytes::copy_from_slice(&data));

        let mut update = TableUpdate::<TestRow>::default();
        update
            .append("t", RawTableRows(RawRows::Deletes(list())))
            .expect("a delete is never read past its bytes");
        assert_eq!(update.deletes.len(), count);
        assert_eq!(update.delete_bytes(&update.deletes[0]).len(), 8);

        let mut update = TableUpdate::<TestRow>::default();
        let error = update
            .append("t", RawTableRows(RawRows::Inserts(list())))
            .expect_err("the spoiled row does not decode");
        assert!(matches!(error, ParseError::Row { table: "t", .. }));
    }

    /// A delete list is still measured against the data it came with, which is the one thing
    /// that can go wrong when nothing is decoded.
    #[test]
    fn a_delete_outside_its_data_is_a_parse_error() {
        let (_, data, _) = row_list(4);
        let offsets = vec![0, data.len() as u64 + 1];
        let list = BsatnRowList::new(RowSizeHint::RowOffsets(offsets.into()), Bytes::copy_from_slice(&data));

        let mut update = TableUpdate::<TestRow>::default();
        let error = update
            .append("t", RawTableRows(RawRows::Deletes(list)))
            .expect_err("the second row starts past the data");
        assert!(matches!(error, ParseError::RowBounds { table: "t" }));
    }

    /// The rows the parts had decoded before one of them failed are dropped, exactly once each.
    ///
    /// The parts own their rows, so this is what `Vec` does by itself when they go out of scope,
    /// and the test is here because it did not use to be: the rows were written into the output
    /// vector's spare capacity, which never lengthens on a failed list, so they had to be dropped
    /// by hand. Every one of them owns something, and the count says so either way.
    #[test]
    fn what_the_parts_decoded_before_a_failure_is_dropped() {
        /// Jobs one at a time, in order, so that exactly which rows were decoded is known.
        struct InOrder;

        impl Threads for InOrder {
            fn count(&self) -> usize {
                8
            }

            fn run<'a>(&self, jobs: &mut [Box<dyn FnMut() + Send + 'a>]) {
                for job in jobs {
                    job();
                }
            }
        }

        let count: usize = 10_000;
        let per = count.div_ceil(InOrder.count().min(count / MIN_PART));
        // A row inside a part rather than at the start of one, so that the part has a prefix to
        // drop and a tail it never reached.
        let spoiled = per * 2 + 1;
        let decoded: usize = (0..count)
            .step_by(per)
            .map(|first| {
                let rows = per.min(count - first);
                if (first..first + rows).contains(&spoiled) {
                    spoiled - first
                } else {
                    rows
                }
            })
            .sum();

        let (hint, mut data) = counted_row_list(count);
        spoil(&hint, &mut data, spoiled);

        let before = DROPPED.load(Ordering::Relaxed);
        let mut parts: Vec<ParsedRow<Counted>> = Vec::with_capacity(count);
        decode_in_parts("t", &hint, &data, 0, &mut parts, count, &InOrder)
            .expect_err("the spoiled row does not decode");
        assert!(parts.is_empty());
        assert_eq!(DROPPED.load(Ordering::Relaxed) - before, decoded);
    }

    /// Whatever the pool says, no part is smaller than [`MIN_PART`], so a list is not split into
    /// more parts than it has work for.
    #[test]
    fn a_short_list_is_split_into_few_parts() {
        /// Counts the parts it is given.
        struct Counting(Arc<std::sync::Mutex<usize>>);

        impl Threads for Counting {
            fn count(&self) -> usize {
                64
            }

            fn run<'a>(&self, jobs: &mut [Box<dyn FnMut() + Send + 'a>]) {
                *self.0.lock().expect("no other thread holds it") = jobs.len();
                for job in jobs {
                    job();
                }
            }
        }

        let count = MIN_SPLIT;
        let (hint, data, _) = row_list(count);
        let seen = Arc::new(std::sync::Mutex::new(0));
        let mut parts: Vec<ParsedRow<TestRow>> = Vec::with_capacity(count);
        decode_in_parts("t", &hint, &data, 0, &mut parts, count, &Counting(Arc::clone(&seen)))
            .expect("the rows decode");

        assert_eq!(*seen.lock().expect("the run is over"), count / MIN_PART);
        assert_eq!(parts.len(), count);
    }
}
