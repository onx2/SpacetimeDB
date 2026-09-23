//! Reflection for the `SpacetimeDB` types generated rows are made of.
//!
//! Generated rows derive [`Reflect`], which is what puts them in the
//! [`AppTypeRegistry`](bevy_ecs::reflect::AppTypeRegistry) for an inspector, a scene or `bevy_remote`
//! to read. A row is only as reflectable as its columns, and a column may be an `Identity`, a
//! `Timestamp` or a `u256` — types belonging to `SpacetimeDB`, which cannot implement a trait
//! belonging to Bevy in a crate belonging to neither.
//!
//! The way around that is `bevy_reflect`'s remote types: a wrapper declared here stands in for the
//! foreign type, and a field of that type names its wrapper with `#[reflect(remote = ...)]`. The
//! generator writes those attributes; nothing here is meant to be named by hand.
//!
//! Every wrapper is opaque: reflection sees one value, not the bytes inside it. That is the honest
//! shape of these types — an `Identity` is a name, not a number anyone should patch a digit of —
//! and each carries `Debug`, `PartialEq` and `Clone` so an inspector can print it, compare it and
//! copy it.
//!
//! ## What a column may be
//!
//! A column holds one of these types directly, or an `Option` or a `Vec` of one, and a wrapper is
//! declared for each of those three shapes. Anything deeper — a `Vec<Vec<Identity>>`, or an
//! `Option<Vec<Timestamp>>` — has no wrapper, and the generator marks such a field
//! `#[reflect(ignore)]`: the row still reflects, and that one column is not shown. No module in
//! the `SpacetimeDB` test suite has such a column.

use crate::protocol::{Module, Table};
use bevy_reflect::{reflect_remote, GetTypeRegistration, Reflect};
use spacetimedb_lib::{sats, ConnectionId, Identity, ScheduleAt, TimeDuration, Timestamp, Uuid};

/// Hands every table of a module to a visitor that needs its rows reflectable.
///
/// Generated bindings implement this alongside [`Module`], listing the same tables as
/// [`Module::visit_tables`]. It exists because that one cannot serve: its visitor is handed a
/// [`Table`], and `Table` does not ask its row type for `Reflect` — the crate that declares it
/// knows nothing of Bevy, and is meant not to. Generated code names each table concretely, so
/// there it costs nothing to promise what the row type derives anyway.
///
/// Bindings generated before this existed do not implement it, and the compiler says so where a
/// plugin asks for it; generating them again is the fix.
pub trait ReflectTables: Module {
    /// Calls [`ReflectTableVisitor::table`] once for each table and view of the module.
    fn visit_reflect_tables<V: ReflectTableVisitor<Self>>(visitor: &mut V);
}

/// Receives each table of a module from [`ReflectTables::visit_reflect_tables`], with what it
/// takes to read the rows through reflection.
pub trait ReflectTableVisitor<M: Module> {
    /// Called once for each table `T` of `M`.
    fn table<T: Table<Module = M>>(&mut self)
    where
        T::Row: Reflect + GetTypeRegistration;
}

/// Declares the opaque remote wrappers for one `SpacetimeDB` type: the type itself, an `Option` of
/// it and a `Vec` of it, which are the three shapes a column can have it in.
macro_rules! sats_remote {
    ($(#[$doc:meta])* $ty:ty => $one:ident, $option:ident, $vec:ident) => {
        $(#[$doc])*
        #[reflect_remote($ty)]
        #[derive(Clone, Debug, PartialEq)]
        #[reflect(opaque)]
        #[reflect(Debug, PartialEq, Clone)]
        pub struct $one;

        #[doc = concat!("Stands in for `Option<", stringify!($ty), ">`. See [`", stringify!($one), "`].")]
        #[reflect_remote(Option<$ty>)]
        #[derive(Clone, Debug, PartialEq)]
        #[reflect(opaque)]
        #[reflect(Debug, PartialEq, Clone)]
        pub struct $option;

        #[doc = concat!("Stands in for `Vec<", stringify!($ty), ">`. See [`", stringify!($one), "`].")]
        #[reflect_remote(Vec<$ty>)]
        #[derive(Clone, Debug, PartialEq)]
        #[reflect(opaque)]
        #[reflect(Debug, PartialEq, Clone)]
        pub struct $vec;
    };
}

sats_remote! {
    /// Stands in for [`Identity`], which a Bevy crate cannot implement `Reflect` for.
    Identity => IdentityRemote, OptionIdentityRemote, VecIdentityRemote
}

sats_remote! {
    /// Stands in for [`ConnectionId`], which a Bevy crate cannot implement `Reflect` for.
    ConnectionId => ConnectionIdRemote, OptionConnectionIdRemote, VecConnectionIdRemote
}

sats_remote! {
    /// Stands in for [`Timestamp`], which a Bevy crate cannot implement `Reflect` for.
    Timestamp => TimestampRemote, OptionTimestampRemote, VecTimestampRemote
}

sats_remote! {
    /// Stands in for [`TimeDuration`], which a Bevy crate cannot implement `Reflect` for.
    TimeDuration => TimeDurationRemote, OptionTimeDurationRemote, VecTimeDurationRemote
}

sats_remote! {
    /// Stands in for [`Uuid`], which a Bevy crate cannot implement `Reflect` for.
    Uuid => UuidRemote, OptionUuidRemote, VecUuidRemote
}

sats_remote! {
    /// Stands in for [`ScheduleAt`], which a Bevy crate cannot implement `Reflect` for.
    ScheduleAt => ScheduleAtRemote, OptionScheduleAtRemote, VecScheduleAtRemote
}

sats_remote! {
    /// Stands in for [`u256`](sats::u256), which a Bevy crate cannot implement `Reflect` for.
    sats::u256 => U256Remote, OptionU256Remote, VecU256Remote
}

sats_remote! {
    /// Stands in for [`i256`](sats::i256), which a Bevy crate cannot implement `Reflect` for.
    sats::i256 => I256Remote, OptionI256Remote, VecI256Remote
}

/// Registers every wrapper in this module with a type registry.
///
/// A remote wrapper has to be registered for the field that names it to be read, so the `dev`
/// feature's plugin calls this once. Registering a type twice is harmless.
pub fn register_types(registry: &mut bevy_reflect::TypeRegistry) {
    macro_rules! register {
        ($($ty:ty),* $(,)?) => { $(registry.register::<$ty>();)* };
    }
    register!(
        IdentityRemote,
        OptionIdentityRemote,
        VecIdentityRemote,
        ConnectionIdRemote,
        OptionConnectionIdRemote,
        VecConnectionIdRemote,
        TimestampRemote,
        OptionTimestampRemote,
        VecTimestampRemote,
        TimeDurationRemote,
        OptionTimeDurationRemote,
        VecTimeDurationRemote,
        UuidRemote,
        OptionUuidRemote,
        VecUuidRemote,
        ScheduleAtRemote,
        OptionScheduleAtRemote,
        VecScheduleAtRemote,
        U256Remote,
        OptionU256Remote,
        VecU256Remote,
        I256Remote,
        OptionI256Remote,
        VecI256Remote,
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use bevy_reflect::{structs::Struct, FromReflect, PartialReflect, Reflect, TypeRegistry};

    /// A row of the shape the generator emits: columns of `SpacetimeDB` types, each naming its
    /// wrapper, in each of the three shapes a column can have.
    #[derive(Reflect, Clone, Debug, PartialEq)]
    struct Row {
        name: String,
        #[reflect(remote = IdentityRemote)]
        owner: Identity,
        #[reflect(remote = OptionIdentityRemote)]
        invited_by: Option<Identity>,
        #[reflect(remote = VecIdentityRemote)]
        members: Vec<Identity>,
        #[reflect(remote = TimestampRemote)]
        created: Timestamp,
        #[reflect(remote = VecU256Remote)]
        scores: Vec<sats::u256>,
    }

    fn row() -> Row {
        Row {
            name: "guild".to_owned(),
            owner: Identity::ZERO,
            invited_by: None,
            members: vec![Identity::ZERO],
            created: Timestamp::UNIX_EPOCH,
            scores: vec![sats::u256::from(7u8)],
        }
    }

    #[test]
    fn a_row_of_spacetimedb_types_reflects_every_column() {
        let row = row();
        let fields = match row.reflect_ref() {
            bevy_reflect::ReflectRef::Struct(fields) => fields,
            _ => panic!("a row reflects as a struct"),
        };
        assert_eq!(fields.field_len(), 6);
        // Every column is readable, including the ones whose types belong to SpacetimeDB.
        for index in 0..fields.field_len() {
            assert!(fields.field_at(index).is_some());
        }
        assert_eq!(fields.name_at(1), Some("owner"));
    }

    #[test]
    fn a_wrapper_prints_what_the_type_prints() {
        let row = row();
        let owner = row.field("owner").expect("the column is there");
        // The wrapper is what reflection sees, and its `Debug` is the identity's own.
        // Reflection sees the wrapper, under a path of this crate's, and prints the identity
        // through it: an inspector that knows nothing of SpacetimeDB still shows the value.
        assert_eq!(owner.reflect_type_path(), "spacetimedb_bevy::reflect::IdentityRemote");
        assert!(format!("{owner:?}").contains(&format!("{:?}", Identity::ZERO)));
    }

    #[test]
    fn a_registered_row_finds_its_wrappers() {
        let mut registry = TypeRegistry::new();
        registry.register::<Row>();
        register_types(&mut registry);
        assert!(registry.get(core::any::TypeId::of::<IdentityRemote>()).is_some());
        assert!(registry.get(core::any::TypeId::of::<VecU256Remote>()).is_some());
    }

    #[test]
    fn a_row_can_be_cloned_through_reflection() {
        let row = row();
        let clone = row.to_dynamic_struct().expect("a row clones");
        let rebuilt = Row::from_reflect(&clone).expect("every column can be read back");
        assert_eq!(rebuilt, row);
    }
}
