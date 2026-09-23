//! Bindings for the Bevy SDK (`sdks/bevy`), reached as `spacetime generate --lang bevy`.
//!
//! What comes out mirrors the Rust backend's layout, one file per type, table, reducer and
//! procedure plus a `mod.rs`, but implements the Bevy SDK's traits instead of the Rust SDK's:
//!
//! - every type derives `Reflect` on top of the usual `Serialize`, `Deserialize`, `Clone`,
//!   `PartialEq` and `Debug`, with SpacetimeDB's own types read through opaque stand-ins;
//! - each table gets a marker with its `Table` impl (its primary key, a fast path for reading a
//!   deleted row's key, and its unique columns), the row type itself when no other table shares it;
//! - each reducer and procedure gets an args struct, and `mod.rs` a trait with one method per
//!   call on the `Reducers` and `Procedures` system params;
//! - `mod.rs` also holds the typed `query` module and the `Module` impl that ties it all together.
//!
//! Generated code names the SDK only through `spacetimedb_bevy::__codegen`, so what it refers to
//! is a stable surface however the crate is organised inside. The `Reflect` derive resolves
//! `bevy_reflect` through the manifest of the crate holding the bindings, which must therefore
//! depend on `bevy` or on `bevy_reflect`.

use std::collections::BTreeSet;
use std::fmt::Write as _;
use std::ops::Deref;

use convert_case::{Case, Casing};
use spacetimedb_lib::sats::layout::PrimitiveType;
use spacetimedb_lib::sats::AlgebraicTypeRef;
use spacetimedb_schema::def::{ModuleDef, ProcedureDef, ReducerDef, ScopedTypeName, TableDef, TypeDef};
use spacetimedb_schema::identifier::Identifier;
use spacetimedb_schema::schema::TableSchema;
use spacetimedb_schema::type_for_generate::{AlgebraicTypeDef, AlgebraicTypeUse, TypespaceForGenerate};

use super::code_indenter::{CodeIndenter, Indenter};
use super::util::{
    collect_case, iter_indexes, iter_procedures, iter_reducers, iter_tables, iter_types, iter_unique_cols, iter_views,
    print_auto_generated_file_comment, print_auto_generated_version_comment, type_ref_name, CodegenVisibility,
};
use super::{CodegenOptions, Lang, OutputFile};

const INDENT: &str = "    ";

const IMPORTS: &[&str] = &[
    "use spacetimedb_bevy::__codegen::{core as __core, lib as __lib, query as __query, reflect as __reflect};",
    "use spacetimedb_bevy::reflect as __sats_reflect;",
];

/// What every generated type derives. `Reflect` sits in the same attribute as the rest because
/// `rustfmt` merges neighbouring `derive`s anyway.
const DERIVES: &str =
    "#[derive(__lib::ser::Serialize, __lib::de::Deserialize, Clone, PartialEq, Debug, __reflect::Reflect)]\n#[sats(crate = __lib)]\n";

/// The Bevy backend. See the module documentation.
pub struct Bevy;

impl Lang for Bevy {
    fn generate_type_files(&self, module: &ModuleDef, typ: &TypeDef) -> Vec<OutputFile> {
        let type_name = collect_case(Case::Pascal, typ.accessor_name.name_segments());
        let typespace = module.typespace_for_generate();

        let mut output = CodeIndenter::new(String::new(), INDENT);
        let out = &mut output;
        print_file_header(out, false);

        let mut body = String::new();
        match &typespace[typ.ty] {
            AlgebraicTypeDef::Product(product) => {
                print_type_imports(module, out, &product.elements, Some(typ.ty));
                write!(body, "{DERIVES}pub struct {type_name}").unwrap();
                write_fields(module, &mut body, &product.elements, "super::__remotes");
            }
            AlgebraicTypeDef::Sum(sum) => {
                print_type_imports(module, out, &sum.variants, Some(typ.ty));
                writeln!(body, "{DERIVES}pub enum {type_name} {{").unwrap();
                for (variant, ty) in sum.variants.iter() {
                    let variant = variant.deref().to_case(Case::Pascal);
                    match ty {
                        AlgebraicTypeUse::Unit => writeln!(body, "    {variant},").unwrap(),
                        ty => {
                            let remote = remote_attr(module, ty, "super::__remotes")
                                .map_or(String::new(), |attr| format!("{attr} "));
                            writeln!(body, "    {variant}({remote}{}),", type_string(module, ty, "")).unwrap();
                        }
                    }
                }
                body.push_str("}\n");
            }
            AlgebraicTypeDef::PlainEnum(plain) => {
                writeln!(body, "{DERIVES}#[derive(Copy, Eq, Hash)]\npub enum {type_name} {{").unwrap();
                for variant in plain.variants.iter() {
                    writeln!(body, "    {},", variant.deref().to_case(Case::Pascal)).unwrap();
                }
                body.push_str("}\n");
            }
        }
        out.newline();
        write!(out, "{body}");

        // Typed queries name columns of a row type, so the column structs live with the type.
        // Tables that share one row type offer every column indexed in any of them; the server
        // refuses a join on a column the table lacks.
        let owners: Vec<TableDef> = all_tables(module, CodegenVisibility::IncludePrivate)
            .into_iter()
            .filter(|table| table.product_type_ref == typ.ty)
            .collect();
        if !owners.is_empty()
            && let AlgebraicTypeDef::Product(product) = &typespace[typ.ty]
        {
            let mut body = String::new();
            write_query_columns(module, &mut body, &type_name, product.elements.iter(), false);
            let indexed: BTreeSet<usize> = owners
                .iter()
                .flat_map(|table| {
                    iter_indexes(table)
                        .filter_map(|index| index.algorithm.columns().as_singleton())
                        .map(|col| col.idx())
                })
                .collect();
            write_query_columns(
                module,
                &mut body,
                &type_name,
                indexed.iter().map(|&index| &product.elements[index]),
                true,
            );
            // Event tables cannot be looked up in a join. A row type that tables share can be if
            // any of them can, and the server refuses the one that cannot.
            if owners.iter().any(|table| !table.is_event) {
                writeln!(body, "\nimpl __query::CanBeLookupTable for {type_name} {{}}").unwrap();
            }
            write!(out, "{body}");
        }

        vec![OutputFile {
            filename: type_module_name(&typ.accessor_name) + ".rs",
            code: output.into_inner(),
        }]
    }

    fn generate_table_file_from_schema(&self, module: &ModuleDef, table: &TableDef, schema: TableSchema) -> OutputFile {
        let typespace = module.typespace_for_generate();
        let type_ref = table.product_type_ref;
        let row_type = type_ref_name(module, type_ref);
        let product = typespace[type_ref]
            .as_product()
            .expect("a table's row type is a product");

        let mut output = CodeIndenter::new(String::new(), INDENT);
        let out = &mut output;
        print_file_header(out, false);
        writeln!(
            out,
            "use super::{}::{row_type};",
            type_ref_module_name(module, type_ref)
        );
        writeln!(out, "use super::RemoteModule;");
        print_type_imports(module, out, &product.elements, None);
        out.newline();

        let marker = marker_name(module, table);
        let mut body = String::new();
        if marker != row_type {
            writeln!(
                body,
                "/// The table `{}`, which shares its row type with another table.\npub struct {marker};\n",
                table.name.deref()
            )
            .unwrap();
        }

        let pk = table.primary_key.map(|col| &product.elements[col.idx()]);
        let (pk_ty, pk_body) = match pk {
            Some((name, ty)) => (type_string(module, ty, ""), format!("Some(&row.{})", field_name(name))),
            None => ("__core::NoPk".to_string(), "None".to_string()),
        };
        let kind = if table.is_event { "Event" } else { "Persistent" };
        let row_arg = if pk.is_some() { "row" } else { "_" };
        let pk_from_bsatn = pk_from_bsatn(module, table, &product.elements, &pk_ty);

        // Unique columns other than the primary key, whose type can key a hash map.
        let pk_name = pk.map(|(name, _)| name.deref());
        let unique: Vec<(String, &str, String)> = iter_unique_cols(typespace, &schema, product)
            .map(|(name, ty)| (name.deref(), ty))
            .filter(|(name, ty)| Some(*name) != pk_name && hashable(typespace, ty))
            .map(|(name, ty)| {
                let column = format!("{marker}{}", name.to_case(Case::Pascal));
                (column, name, type_string(module, ty, ""))
            })
            .collect();
        let visit_unique = if unique.is_empty() {
            String::new()
        } else {
            let columns: String = unique
                .iter()
                .map(|(column, ..)| format!("        visitor.column::<{column}>();\n"))
                .collect();
            format!(
                "    fn visit_unique_columns<V: __core::UniqueColumnVisitor<Self>>(visitor: &mut V) {{\n{columns}    }}\n"
            )
        };
        write!(
            body,
            "impl __core::Table for {marker} {{
    type Module = RemoteModule;
    type Row = {row_type};
    type Pk = {pk_ty};
    const NAME: &'static str = {name:?};
    const KIND: __core::TableKind = __core::TableKind::{kind};
    fn pk({row_arg}: &{row_type}) -> Option<&{pk_ty}> {{
        {pk_body}
    }}
{pk_from_bsatn}{visit_unique}}}
",
            name = table.name.deref(),
        )
        .unwrap();
        for (column, name, key) in &unique {
            write!(
                body,
                "
/// The unique column `{name}` of table `{table}`, for `Rows::find`.
pub struct {column};

impl __core::UniqueColumn for {column} {{
    type Table = {marker};
    type Key = {key};
    const NAME: &'static str = {name:?};
    fn key(row: &{row_type}) -> &{key} {{
        &row.{field}
    }}
}}
",
                table = table.name.deref(),
                field = field_name(name),
            )
            .unwrap();
        }
        write!(out, "{body}");

        OutputFile {
            filename: table_module_name(&table.accessor_name) + ".rs",
            code: output.into_inner(),
        }
    }

    fn generate_reducer_file(&self, module: &ModuleDef, reducer: &ReducerDef) -> OutputFile {
        let args = args_name(module, &reducer.accessor_name);
        let mut output = CodeIndenter::new(String::new(), INDENT);
        let out = &mut output;
        print_file_header(out, false);
        writeln!(out, "use super::RemoteModule;");
        print_type_imports(module, out, &reducer.params_for_generate.elements, None);
        out.newline();

        let mut body = String::new();
        write!(
            body,
            "/// Arguments of the reducer `{}`.\n{DERIVES}pub struct {args}",
            reducer.name.deref()
        )
        .unwrap();
        write_fields(
            module,
            &mut body,
            &reducer.params_for_generate.elements,
            "super::__remotes",
        );
        write!(
            body,
            "\nimpl __core::Reducer for {args} {{\n    type Module = RemoteModule;\n    const NAME: &'static str = {:?};\n}}\n",
            reducer.name.deref()
        )
        .unwrap();
        write!(out, "{body}");

        OutputFile {
            filename: reducer_module_name(&reducer.accessor_name) + ".rs",
            code: output.into_inner(),
        }
    }

    fn generate_procedure_file(&self, module: &ModuleDef, procedure: &ProcedureDef) -> OutputFile {
        let args = args_name(module, &procedure.accessor_name);
        let mut output = CodeIndenter::new(String::new(), INDENT);
        let out = &mut output;
        print_file_header(out, false);
        writeln!(out, "use super::RemoteModule;");
        let mut roots: Vec<(Identifier, AlgebraicTypeUse)> = procedure.params_for_generate.elements.to_vec();
        roots.push((
            procedure.accessor_name.clone(),
            procedure.return_type_for_generate.clone(),
        ));
        print_type_imports(module, out, &roots, None);
        out.newline();

        let mut body = String::new();
        write!(
            body,
            "/// Arguments of the procedure `{}`.\n{DERIVES}pub struct {args}",
            procedure.name.deref()
        )
        .unwrap();
        write_fields(
            module,
            &mut body,
            &procedure.params_for_generate.elements,
            "super::__remotes",
        );
        write!(
            body,
            "\nimpl __core::Procedure for {args} {{\n    type Module = RemoteModule;\n    type Output = {};\n    const NAME: &'static str = {:?};\n}}\n",
            type_string(module, &procedure.return_type_for_generate, ""),
            procedure.name.deref()
        )
        .unwrap();
        write!(out, "{body}");

        OutputFile {
            filename: procedure_module_name(&procedure.accessor_name) + ".rs",
            code: output.into_inner(),
        }
    }

    fn generate_global_files(&self, module: &ModuleDef, options: &CodegenOptions) -> Vec<OutputFile> {
        let visibility = options.visibility;
        let tables = all_tables(module, visibility);
        let reducers: Vec<&ReducerDef> = iter_reducers(module, visibility).collect();
        let procedures: Vec<&ProcedureDef> = iter_procedures(module, visibility).collect();

        let mut output = CodeIndenter::new(String::new(), INDENT);
        let out = &mut output;
        print_file_header(out, true);
        out.newline();

        // One `pub mod` per generated file, and a re-export of what each defines.
        for typ in iter_types(module) {
            writeln!(out, "pub mod {};", type_module_name(&typ.accessor_name));
        }
        for table in &tables {
            writeln!(out, "pub mod {};", table_module_name(&table.accessor_name));
        }
        for reducer in &reducers {
            writeln!(out, "pub mod {};", reducer_module_name(&reducer.accessor_name));
        }
        for procedure in &procedures {
            writeln!(out, "pub mod {};", procedure_module_name(&procedure.accessor_name));
        }
        out.newline();
        for typ in iter_types(module) {
            writeln!(
                out,
                "pub use {}::{};",
                type_module_name(&typ.accessor_name),
                collect_case(Case::Pascal, typ.accessor_name.name_segments())
            );
        }
        for table in &tables {
            writeln!(out, "pub use {}::*;", table_module_name(&table.accessor_name));
        }
        for reducer in &reducers {
            writeln!(
                out,
                "pub use {}::{};",
                reducer_module_name(&reducer.accessor_name),
                args_name(module, &reducer.accessor_name)
            );
        }
        for procedure in &procedures {
            writeln!(
                out,
                "pub use {}::{};",
                procedure_module_name(&procedure.accessor_name),
                args_name(module, &procedure.accessor_name)
            );
        }

        let mut body = String::new();
        body.push_str(
            "\n/// The module these bindings were generated from.\n#[derive(Debug)]\npub struct RemoteModule;\n",
        );
        write_remotes(module, &mut body, &reducers, &procedures);
        write_call_trait(module, &mut body, "RemoteReducers", "Reducers", "reducer", &reducers);
        write_call_trait(
            module,
            &mut body,
            "RemoteProcedures",
            "Procedures",
            "procedure",
            &procedures,
        );
        write_query_module(module, &mut body, &tables);
        write_module_impl(module, &mut body, &tables, &reducers, &procedures);
        write!(out, "{body}");

        vec![OutputFile {
            filename: "mod.rs".to_string(),
            code: output.into_inner(),
        }]
    }
}

fn print_file_header(out: &mut Indenter, include_version: bool) {
    print_auto_generated_file_comment(out);
    if include_version {
        print_auto_generated_version_comment(out);
    }
    writeln!(out, "#![allow(unused, clippy::all)]");
    for line in IMPORTS {
        writeln!(out, "{line}");
    }
}

/// Every table and view the bindings cover, views converted to the table they look like,
/// in a fixed order.
fn all_tables(module: &ModuleDef, visibility: CodegenVisibility) -> Vec<TableDef> {
    let mut tables: Vec<TableDef> = iter_tables(module, visibility)
        .cloned()
        .chain(iter_views(module).map(|view| TableDef::from(view.clone())))
        .collect();
    tables.sort_by(|a, b| a.accessor_name.cmp(&b.accessor_name));
    tables
}

/// The type a table's `Table` impl hangs on: its row type, unless another table or view shares
/// it, in which case a marker of the table's own.
fn marker_name(module: &ModuleDef, table: &TableDef) -> String {
    let shared = module
        .tables()
        .map(|other| other.product_type_ref)
        .chain(module.views().map(|view| view.product_type_ref))
        .filter(|&type_ref| type_ref == table.product_type_ref)
        .count()
        > 1;
    if shared {
        table.accessor_name.deref().to_case(Case::Pascal) + "Table"
    } else {
        type_ref_name(module, table.product_type_ref)
    }
}

/// The name of a reducer's or procedure's args struct: the call's name, with `Args` appended
/// when one of the module's types already has that name.
fn args_name(module: &ModuleDef, accessor_name: &str) -> String {
    let name = accessor_name.to_case(Case::Pascal);
    let taken = iter_types(module).any(|typ| collect_case(Case::Pascal, typ.accessor_name.name_segments()) == name);
    if taken {
        name + "Args"
    } else {
        name
    }
}

fn type_module_name(type_name: &ScopedTypeName) -> String {
    collect_case(Case::Snake, type_name.name_segments()) + "_type"
}

fn type_ref_module_name(module: &ModuleDef, type_ref: AlgebraicTypeRef) -> String {
    let (name, _) = module
        .type_def_from_ref(type_ref)
        .expect("a referenced type is defined");
    type_module_name(name)
}

fn table_module_name(accessor_name: &Identifier) -> String {
    accessor_name.deref().to_case(Case::Snake) + "_table"
}

fn reducer_module_name(accessor_name: &str) -> String {
    accessor_name.to_case(Case::Snake) + "_reducer"
}

fn procedure_module_name(accessor_name: &str) -> String {
    accessor_name.to_case(Case::Snake) + "_procedure"
}

/// A column or argument as a Rust field: snake case, raw if it is a keyword.
fn field_name(name: &str) -> String {
    ident(&name.to_case(Case::Snake))
}

/// Returns `name` as an identifier, raw if it is a keyword.
fn ident(name: &str) -> String {
    const KEYWORDS: &[&str] = &[
        "as", "async", "await", "break", "const", "continue", "dyn", "else", "enum", "extern", "false", "fn", "for",
        "if", "impl", "in", "let", "loop", "match", "mod", "move", "mut", "pub", "ref", "return", "static", "struct",
        "trait", "true", "type", "unsafe", "use", "where", "while", "abstract", "become", "box", "do", "final", "gen",
        "macro", "override", "priv", "try", "typeof", "unsized", "virtual", "yield",
    ];
    // These cannot be raw identifiers.
    const NEVER_RAW: &[&str] = &["self", "Self", "super", "crate"];
    if NEVER_RAW.contains(&name) {
        format!("{name}_")
    } else if KEYWORDS.contains(&name) {
        format!("r#{name}")
    } else {
        name.to_owned()
    }
}

/// Spells `ty` as Rust, naming the module's own types with `refs` in front of them: nothing in a
/// file that imports them, `super::` from a submodule of `mod.rs`.
fn write_type(module: &ModuleDef, out: &mut String, ty: &AlgebraicTypeUse, refs: &str) {
    match ty {
        AlgebraicTypeUse::Unit => out.push_str("()"),
        AlgebraicTypeUse::Never => out.push_str("std::convert::Infallible"),
        AlgebraicTypeUse::Identity => out.push_str("__lib::Identity"),
        AlgebraicTypeUse::ConnectionId => out.push_str("__lib::ConnectionId"),
        AlgebraicTypeUse::Timestamp => out.push_str("__lib::Timestamp"),
        AlgebraicTypeUse::TimeDuration => out.push_str("__lib::TimeDuration"),
        AlgebraicTypeUse::Uuid => out.push_str("__lib::Uuid"),
        AlgebraicTypeUse::ScheduleAt => out.push_str("__lib::ScheduleAt"),
        AlgebraicTypeUse::Option(inner) => {
            out.push_str("Option<");
            write_type(module, out, inner, refs);
            out.push('>');
        }
        AlgebraicTypeUse::Result { ok_ty, err_ty } => {
            out.push_str("Result<");
            write_type(module, out, ok_ty, refs);
            out.push_str(", ");
            write_type(module, out, err_ty, refs);
            out.push('>');
        }
        AlgebraicTypeUse::Primitive(prim) => out.push_str(match prim {
            PrimitiveType::Bool => "bool",
            PrimitiveType::I8 => "i8",
            PrimitiveType::U8 => "u8",
            PrimitiveType::I16 => "i16",
            PrimitiveType::U16 => "u16",
            PrimitiveType::I32 => "i32",
            PrimitiveType::U32 => "u32",
            PrimitiveType::I64 => "i64",
            PrimitiveType::U64 => "u64",
            PrimitiveType::I128 => "i128",
            PrimitiveType::U128 => "u128",
            PrimitiveType::I256 => "__lib::sats::i256",
            PrimitiveType::U256 => "__lib::sats::u256",
            PrimitiveType::F32 => "f32",
            PrimitiveType::F64 => "f64",
        }),
        AlgebraicTypeUse::String => out.push_str("String"),
        AlgebraicTypeUse::Array(elem) => {
            out.push_str("Vec<");
            write_type(module, out, elem, refs);
            out.push('>');
        }
        AlgebraicTypeUse::Ref(r) => {
            out.push_str(refs);
            out.push_str(&type_ref_name(module, *r));
        }
    }
}

fn type_string(module: &ModuleDef, ty: &AlgebraicTypeUse, refs: &str) -> String {
    let mut s = String::new();
    write_type(module, &mut s, ty, refs);
    s
}

/// Prints `use super::<module>::<Type>;` for every type `roots` refer to, except `skip`, which is
/// the type being defined: `struct Foo { foos: Vec<Foo> }` must not import itself.
fn print_type_imports(
    module: &ModuleDef,
    out: &mut Indenter,
    roots: &[(Identifier, AlgebraicTypeUse)],
    skip: Option<AlgebraicTypeRef>,
) {
    let mut refs = BTreeSet::new();
    for (_, ty) in roots {
        ty.for_each_ref(|r| {
            refs.insert(r);
        });
    }
    if let Some(skip) = skip {
        refs.remove(&skip);
    }
    for r in refs {
        writeln!(
            out,
            "use super::{}::{};",
            type_ref_module_name(module, r),
            type_ref_name(module, r)
        );
    }
}

/// Writes the braces and fields of a struct, each field with the stand-in it reflects through if
/// it needs one.
fn write_fields(module: &ModuleDef, out: &mut String, elements: &[(Identifier, AlgebraicTypeUse)], remotes: &str) {
    if elements.is_empty() {
        out.push_str(" {}\n");
        return;
    }
    out.push_str(" {\n");
    for (name, ty) in elements {
        if let Some(attr) = remote_attr(module, ty, remotes) {
            writeln!(out, "    {attr}").unwrap();
        }
        writeln!(out, "    pub {}: {},", field_name(name), type_string(module, ty, "")).unwrap();
    }
    out.push_str("}\n");
}

/// Returns `true` if `ty` implements `Reflect` as it is spelled.
///
/// SpacetimeDB's own types do not: they belong to another crate, and so does `Reflect`, so no
/// implementation may be written for the pair in generated code. A type of the module's own
/// does, because the bindings derive it. Everything else is plain as long as what it holds is.
fn reflects_plainly(ty: &AlgebraicTypeUse) -> bool {
    match ty {
        // `u256` and `i256` are SpacetimeDB's own, unlike the numbers Rust has.
        AlgebraicTypeUse::Primitive(PrimitiveType::I256 | PrimitiveType::U256) => false,
        AlgebraicTypeUse::Unit
        | AlgebraicTypeUse::Primitive(_)
        | AlgebraicTypeUse::String
        | AlgebraicTypeUse::Ref(_) => true,
        AlgebraicTypeUse::Never
        | AlgebraicTypeUse::Identity
        | AlgebraicTypeUse::ConnectionId
        | AlgebraicTypeUse::Timestamp
        | AlgebraicTypeUse::TimeDuration
        | AlgebraicTypeUse::Uuid
        | AlgebraicTypeUse::ScheduleAt => false,
        AlgebraicTypeUse::Option(inner) | AlgebraicTypeUse::Array(inner) => reflects_plainly(inner),
        AlgebraicTypeUse::Result { ok_ty, err_ty } => reflects_plainly(ok_ty) && reflects_plainly(err_ty),
    }
}

/// The stand-in the SDK already declares for `ty`, if it has one: a SpacetimeDB type held
/// directly, in an `Option` or in a `Vec`.
fn shared_remote(ty: &AlgebraicTypeUse) -> Option<String> {
    let (shape, inner) = match ty {
        AlgebraicTypeUse::Option(inner) => ("Option", &**inner),
        AlgebraicTypeUse::Array(elem) => ("Vec", &**elem),
        other => ("", other),
    };
    let name = match inner {
        AlgebraicTypeUse::Identity => "Identity",
        AlgebraicTypeUse::ConnectionId => "ConnectionId",
        AlgebraicTypeUse::Timestamp => "Timestamp",
        AlgebraicTypeUse::TimeDuration => "TimeDuration",
        AlgebraicTypeUse::Uuid => "Uuid",
        AlgebraicTypeUse::ScheduleAt => "ScheduleAt",
        AlgebraicTypeUse::Primitive(PrimitiveType::U256) => "U256",
        AlgebraicTypeUse::Primitive(PrimitiveType::I256) => "I256",
        _ => return None,
    };
    Some(format!("{shape}{name}Remote"))
}

/// The name of the stand-in the bindings declare for `ty`: the type spelled as one identifier,
/// so `Result<Identity, String>` becomes `ResultIdentityString`.
fn remote_name(module: &ModuleDef, ty: &AlgebraicTypeUse) -> String {
    match ty {
        AlgebraicTypeUse::Unit => "Unit".into(),
        AlgebraicTypeUse::Never => "Never".into(),
        AlgebraicTypeUse::Identity => "Identity".into(),
        AlgebraicTypeUse::ConnectionId => "ConnectionId".into(),
        AlgebraicTypeUse::Timestamp => "Timestamp".into(),
        AlgebraicTypeUse::TimeDuration => "TimeDuration".into(),
        AlgebraicTypeUse::Uuid => "Uuid".into(),
        AlgebraicTypeUse::ScheduleAt => "ScheduleAt".into(),
        AlgebraicTypeUse::String => "String".into(),
        AlgebraicTypeUse::Primitive(prim) => format!("{prim:?}"),
        AlgebraicTypeUse::Option(inner) => format!("Option{}", remote_name(module, inner)),
        AlgebraicTypeUse::Result { ok_ty, err_ty } => {
            format!("Result{}{}", remote_name(module, ok_ty), remote_name(module, err_ty))
        }
        AlgebraicTypeUse::Array(elem) => format!("Vec{}", remote_name(module, elem)),
        AlgebraicTypeUse::Ref(r) => type_ref_name(module, *r),
    }
}

/// The `#[reflect(remote = ..)]` attribute a field of type `ty` needs, or `None` when the type
/// reflects as it is. `remotes` is the path of the bindings' own stand-in module from here.
fn remote_attr(module: &ModuleDef, ty: &AlgebraicTypeUse, remotes: &str) -> Option<String> {
    if reflects_plainly(ty) {
        return None;
    }
    let path = match shared_remote(ty) {
        Some(name) => format!("__sats_reflect::{name}"),
        None => format!("{remotes}::{}", remote_name(module, ty)),
    };
    Some(format!("#[reflect(remote = {path})]"))
}

/// Writes the module of stand-ins for column and argument types the SDK has none for. Each is
/// opaque: reflection sees one value and prints it, which is all an inspector can honestly do
/// with a `Result<Identity, String>`.
fn write_remotes(module: &ModuleDef, out: &mut String, reducers: &[&ReducerDef], procedures: &[&ProcedureDef]) {
    let typespace = module.typespace_for_generate();
    let mut needed: BTreeSet<(String, String)> = BTreeSet::new();
    let mut want = |ty: &AlgebraicTypeUse| {
        if !reflects_plainly(ty) && shared_remote(ty).is_none() {
            needed.insert((remote_name(module, ty), type_string(module, ty, "super::")));
        }
    };
    for typ in iter_types(module) {
        match &typespace[typ.ty] {
            AlgebraicTypeDef::Product(product) => product.elements.iter().for_each(|(_, ty)| want(ty)),
            AlgebraicTypeDef::Sum(sum) => sum.variants.iter().for_each(|(_, ty)| want(ty)),
            AlgebraicTypeDef::PlainEnum(_) => {}
        }
    }
    for reducer in reducers {
        reducer.params_for_generate.elements.iter().for_each(|(_, ty)| want(ty));
    }
    for procedure in procedures {
        procedure
            .params_for_generate
            .elements
            .iter()
            .for_each(|(_, ty)| want(ty));
    }
    if needed.is_empty() {
        return;
    }
    out.push_str(
        "\n/// Stand-ins that let columns of these types be read through `Reflect`.\n\
         ///\n\
         /// Each is opaque: reflection sees the value and prints it, without reaching inside.\n\
         pub mod __remotes {\n    use super::{__lib, __reflect};\n",
    );
    for (name, ty) in needed {
        writeln!(
            out,
            "\n    #[__reflect::reflect_remote({ty})]\n    \
             #[derive(Clone, Debug, PartialEq)]\n    \
             #[reflect(opaque)]\n    \
             #[reflect(Debug, PartialEq, Clone)]\n    \
             pub struct {name};"
        )
        .unwrap();
    }
    out.push_str("}\n");
}

/// Whether BSATN writes a value of this type in a number of bytes fixed by the type, so that
/// reading past it allocates nothing.
fn fixed_width(ty: &AlgebraicTypeUse) -> bool {
    matches!(
        ty,
        AlgebraicTypeUse::Primitive(_)
            | AlgebraicTypeUse::Identity
            | AlgebraicTypeUse::ConnectionId
            | AlgebraicTypeUse::Timestamp
            | AlgebraicTypeUse::TimeDuration
            | AlgebraicTypeUse::Uuid
    )
}

/// Whether a value of this type can key a hash map, which is what a unique column's index needs.
fn hashable(typespace: &TypespaceForGenerate, ty: &AlgebraicTypeUse) -> bool {
    match ty {
        AlgebraicTypeUse::Primitive(prim) => !matches!(prim, PrimitiveType::F32 | PrimitiveType::F64),
        AlgebraicTypeUse::String
        | AlgebraicTypeUse::Identity
        | AlgebraicTypeUse::ConnectionId
        | AlgebraicTypeUse::Timestamp
        | AlgebraicTypeUse::TimeDuration
        | AlgebraicTypeUse::Uuid => true,
        AlgebraicTypeUse::Ref(r) => typespace[r].is_plain_enum(),
        _ => false,
    }
}

/// Returns `Table::pk_from_bsatn` for `table`, or nothing where the default is as good.
///
/// A delete names the row it removes by repeating its bytes, and the only thing ever read out of
/// one is its primary key. Reading the key alone spares a keyed table the rest of the row, which
/// for a mass update is where most of the parsing went. It is worth overriding the default only
/// where the key can be reached without decoding anything that owns heap: every column before it
/// must be of a type BSATN writes in a fixed number of bytes. A `String`, a `Vec`, an `Option` or
/// a module type in front of the key leaves the default, which decodes the row and takes the key.
fn pk_from_bsatn(
    module: &ModuleDef,
    table: &TableDef,
    elements: &[(Identifier, AlgebraicTypeUse)],
    pk_ty: &str,
) -> String {
    let Some(key) = table.primary_key else {
        return String::new();
    };
    let before = &elements[..key.idx()];
    if !before.iter().all(|(_, ty)| fixed_width(ty)) {
        return String::new();
    }
    let skip: String = before
        .iter()
        .map(|(name, ty)| {
            format!(
                "        let _{}: {} = __lib::bsatn::from_reader(&mut bsatn)?;\n",
                name.deref().to_case(Case::Snake),
                type_string(module, ty, "")
            )
        })
        .collect();
    let read_past = if skip.is_empty() {
        String::new()
    } else {
        format!("        // The columns before the key, read past and dropped.\n{skip}")
    };
    format!(
        "    fn pk_from_bsatn(mut bsatn: &[u8]) -> Result<Option<{pk_ty}>, __lib::bsatn::DecodeError> {{\n{read_past}        __lib::bsatn::from_reader(&mut bsatn).map(Some)\n    }}\n"
    )
}

/// Writes the struct of columns of row type `row` for typed queries, and its `HasCols` or, for
/// the indexed columns, `HasIxCols` impl.
fn write_query_columns<'a>(
    module: &ModuleDef,
    out: &mut String,
    row: &str,
    columns: impl Iterator<Item = &'a (Identifier, AlgebraicTypeUse)>,
    indexed: bool,
) {
    let columns: Vec<_> = columns.collect();
    let (kind, which, has, method) = if indexed {
        ("Ix", "indexed ", "HasIxCols", "ix_cols")
    } else {
        ("", "", "HasCols", "cols")
    };
    writeln!(
        out,
        "\n/// The {which}columns of `{row}`, for typed queries.\npub struct {row}{kind}Cols {{"
    )
    .unwrap();
    for (name, ty) in &columns {
        writeln!(
            out,
            "    pub {}: __query::{kind}Col<{row}, {}>,",
            field_name(name),
            type_string(module, ty, "")
        )
        .unwrap();
    }
    writeln!(
        out,
        "}}\n\nimpl __query::{has} for {row} {{\n    type {kind}Cols = {row}{kind}Cols;\n    fn {method}(table: &'static str) -> {row}{kind}Cols {{\n        {row}{kind}Cols {{"
    )
    .unwrap();
    for (name, _) in &columns {
        writeln!(
            out,
            "            {}: __query::{kind}Col::new(table, {:?}),",
            field_name(name),
            name.deref()
        )
        .unwrap();
    }
    writeln!(out, "        }}\n    }}\n}}").unwrap();
}

/// Writes a trait with one method per call, implemented for the matching system param.
fn write_call_trait<C: CallLike>(
    module: &ModuleDef,
    out: &mut String,
    name: &str,
    param: &str,
    noun: &str,
    calls: &[&C],
) {
    if calls.is_empty() {
        return;
    }
    let method = |call: &C| ident(&call.accessor().to_case(Case::Snake));
    let params = |call: &C| -> String {
        call.params()
            .iter()
            .map(|(name, ty)| format!(", {}: {}", field_name(name), type_string(module, ty, "")))
            .collect()
    };
    let literal = |call: &C| -> String {
        let names: Vec<String> = call.params().iter().map(|(name, _)| field_name(name)).collect();
        if names.is_empty() {
            "{}".to_string()
        } else {
            format!("{{ {} }}", names.join(", "))
        }
    };
    writeln!(
        out,
        "\n/// One method per {noun} on [`spacetimedb_bevy::{param}`].\npub trait {name} {{"
    )
    .unwrap();
    for call in calls {
        writeln!(
            out,
            "    fn {}(&self{}) -> spacetimedb_bevy::RequestId;",
            method(call),
            params(call)
        )
        .unwrap();
    }
    writeln!(
        out,
        "}}\n\nimpl {name} for spacetimedb_bevy::{param}<'_, RemoteModule> {{"
    )
    .unwrap();
    for call in calls {
        writeln!(
            out,
            "    fn {}(&self{}) -> spacetimedb_bevy::RequestId {{\n        self.call({} {})\n    }}",
            method(call),
            params(call),
            args_name(module, call.accessor()),
            literal(call)
        )
        .unwrap();
    }
    out.push_str("}\n");
}

/// What a reducer and a procedure have in common, as far as their args structs go.
trait CallLike {
    fn accessor(&self) -> &str;
    fn params(&self) -> &[(Identifier, AlgebraicTypeUse)];
}

impl CallLike for ReducerDef {
    fn accessor(&self) -> &str {
        &self.accessor_name
    }
    fn params(&self) -> &[(Identifier, AlgebraicTypeUse)] {
        &self.params_for_generate.elements
    }
}

impl CallLike for ProcedureDef {
    fn accessor(&self) -> &str {
        &self.accessor_name
    }
    fn params(&self) -> &[(Identifier, AlgebraicTypeUse)] {
        &self.params_for_generate.elements
    }
}

/// Writes the `query` module: one function per table to start a typed query from.
fn write_query_module(module: &ModuleDef, out: &mut String, tables: &[TableDef]) {
    out.push_str(
        "\n/// Where typed queries start: `query::user().filter(|user| user.online.eq(true))`.\n\
         /// Pass one to `commands.subscribe_to::<RemoteModule, _>(..)`.\n\
         pub mod query {\n    use super::__query;\n\n",
    );
    for table in tables {
        writeln!(
            out,
            "    /// Every row of `{name}`, to narrow with `filter` or join.\n    pub fn {accessor}() -> __query::Table<super::{row}> {{\n        __query::Table::new({name:?})\n    }}",
            name = table.name.deref(),
            accessor = field_name(&table.accessor_name),
            row = type_ref_name(module, table.product_type_ref),
        )
        .unwrap();
    }
    out.push_str("}\n");
}

/// Writes `RemoteUpdate`, the `Module` impl and the `ReflectTables` impl.
fn write_module_impl(
    module: &ModuleDef,
    out: &mut String,
    tables: &[TableDef],
    reducers: &[&ReducerDef],
    procedures: &[&ProcedureDef],
) {
    let field_of = |table: &TableDef| field_name(&table.accessor_name);

    out.push_str("\n/// The changes one server message makes, per table.\n#[derive(Default, Debug)]\npub struct RemoteUpdate {\n");
    for table in tables {
        writeln!(
            out,
            "    pub {}: __core::TableUpdate<{}>,",
            field_of(table),
            type_ref_name(module, table.product_type_ref)
        )
        .unwrap();
    }
    out.push_str("}\n\nimpl __core::Module for RemoteModule {\n    type Update = RemoteUpdate;\n\n");

    out.push_str("    fn parse_table(\n        update: &mut RemoteUpdate,\n        table: &str,\n        rows: __core::RawTableRows,\n    ) -> Result<(), __core::ParseError> {\n        match table {\n");
    for table in tables {
        writeln!(
            out,
            "            {name:?} => update.{}.append({name:?}, rows),",
            field_of(table),
            name = table.name.deref()
        )
        .unwrap();
    }
    out.push_str("            unknown => Err(__core::ParseError::UnknownTable(unknown.into())),\n        }\n    }\n\n");

    let unused = |items: usize| if items == 0 { "_visitor" } else { "visitor" };
    writeln!(
        out,
        "    fn visit_update<V: __core::UpdateVisitor<Self>>(update: RemoteUpdate, {}: &mut V) {{",
        unused(tables.len())
    )
    .unwrap();
    if tables.is_empty() {
        out.push_str("        let RemoteUpdate {} = update;\n");
    }
    for table in tables {
        writeln!(
            out,
            "        visitor.table::<{}>(update.{});",
            marker_name(module, table),
            field_of(table)
        )
        .unwrap();
    }
    writeln!(
        out,
        "    }}\n\n    fn visit_tables<V: __core::TableVisitor<Self>>({}: &mut V) {{",
        unused(tables.len())
    )
    .unwrap();
    for table in tables {
        writeln!(out, "        visitor.table::<{}>();", marker_name(module, table)).unwrap();
    }
    writeln!(
        out,
        "    }}\n\n    fn visit_reducers<V: __core::ReducerVisitor<Self>>({}: &mut V) {{",
        unused(reducers.len())
    )
    .unwrap();
    for reducer in reducers {
        writeln!(
            out,
            "        visitor.reducer::<{}>();",
            args_name(module, &reducer.accessor_name)
        )
        .unwrap();
    }
    out.push_str("    }\n");
    if !procedures.is_empty() {
        out.push_str("\n    fn visit_procedures<V: __core::ProcedureVisitor<Self>>(visitor: &mut V) {\n");
        for procedure in procedures {
            writeln!(
                out,
                "        visitor.procedure::<{}>();",
                args_name(module, &procedure.accessor_name)
            )
            .unwrap();
        }
        out.push_str("    }\n");
    }
    out.push_str("}\n");

    // The same tables again, promising what `Table` cannot: that the rows reflect.
    writeln!(
        out,
        "\nimpl __sats_reflect::ReflectTables for RemoteModule {{\n    \
         fn visit_reflect_tables<V: __sats_reflect::ReflectTableVisitor<Self>>({}: &mut V) {{",
        unused(tables.len())
    )
    .unwrap();
    for table in tables {
        writeln!(out, "        visitor.table::<{}>();", marker_name(module, table)).unwrap();
    }
    out.push_str("    }\n}\n");
}
