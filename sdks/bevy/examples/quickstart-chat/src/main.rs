//! The SpacetimeDB quickstart chat, as a Bevy app: a window with a message list, the people in
//! the room, and a line to type into.
//!
//! From the repository root:
//!
//! ```sh
//! spacetime start
//! spacetime publish -p templates/chat-console-rs/spacetimedb --server local -y quickstart-chat
//! spacetime generate --lang bevy -p templates/chat-console-rs/spacetimedb \
//!     -o sdks/bevy/examples/quickstart-chat/src/module_bindings -y
//! cd sdks/bevy/examples/quickstart-chat && cargo run
//! ```
//!
//! Type a line and press Enter to send it, `/name <name>` to set your name. `STDB_URI` and
//! `STDB_DATABASE` override where it connects. Run it twice to talk to yourself.
//!
//! What it demonstrates: a subscription as an entity with an observer for when its rows are in;
//! both handler tiers, and why each system is in the one it is; reading a table by key;
//! `RowUpdated` carrying the row as it was and as it is; reducer results; and what a lost
//! connection looks like from inside the app.

// Written by `spacetime generate --lang bevy`, from the command in the header above. Nothing here
// is special to an example: this is the directory, in the place and under the name, that an app
// of your own would have.
mod module_bindings;

use bevy::input_focus::tab_navigation::{TabGroup, TabIndex, TabNavigationPlugin};
use bevy::input_focus::{AutoFocus, InputFocus};
use bevy::prelude::*;
use bevy::text::{EditableText, TextCursorStyle};
use bevy::ui_widgets::TextInput;
use module_bindings::{Message, RemoteModule, RemoteReducers, SendMessage, SetName, User};
use spacetimedb_bevy::__codegen::lib::Identity;
use spacetimedb_bevy::{
    ConnectOptions, ConnectionLost, ConnectionState, Disconnected, ReducerResult, ReducerStatus, Reducers, RowInserted,
    RowUpdated, Rows, StdbCommandsExt, StdbIdentity, StdbPlugin, StdbState, StdbTransaction, SubscriptionApplied,
};

/// How many lines the transcript keeps. Older ones are despawned as new ones arrive, which is
/// what stops a long session from spawning an entity per message for ever.
const SCROLLBACK: usize = 200;

fn main() {
    let uri = std::env::var("STDB_URI").unwrap_or_else(|_| "http://127.0.0.1:3000".into());
    let database = std::env::var("STDB_DATABASE").unwrap_or_else(|_| "quickstart-chat".into());
    let options = ConnectOptions::new(uri, database);

    App::new()
        .add_plugins(DefaultPlugins.set(WindowPlugin {
            primary_window: Some(Window {
                title: "SpacetimeDB quickstart chat".into(),
                resolution: (820, 620).into(),
                ..default()
            }),
            ..default()
        }))
        // What makes the text field focusable, and Tab move between fields in a bigger app.
        .add_plugins(TabNavigationPlugin)
        .add_plugins(StdbPlugin::<RemoteModule>::new().connect_to(options))
        .add_systems(Startup, (setup, subscribe))
        .add_systems(
            OnEnter(StdbState::<RemoteModule>::connected()),
            |me: Res<StdbIdentity<RemoteModule>>, mut commands: Commands| {
                commands.queue(say(format!("connected as {}", short(&me.identity))));
            },
        )
        // Strict: these look other rows up because of a change, so they run once per transaction,
        // against exactly that transaction's state.
        .add_systems(StdbTransaction, (show_new_messages, show_user_changes))
        // Relaxed: these need only what the message itself carries, or only the latest state.
        .add_systems(
            Update,
            (send_what_was_typed, report_failures, roster, status_line, trim_scrollback),
        )
        .add_observer(|lost: On<ConnectionLost<RemoteModule>>, mut commands: Commands| {
            commands.queue(say(format!("connection lost ({})", lost.reason)));
        })
        .add_observer(|closed: On<Disconnected<RemoteModule>>, mut commands: Commands| {
            commands.queue(say(format!("disconnected: {}", closed.reason)));
        })
        .run();
}

/// The transcript's parent: every line is a child of this.
#[derive(Component)]
struct Transcript;

/// The list of who is here.
#[derive(Component)]
struct Roster;

/// The line under the input saying where the connection is.
#[derive(Component)]
struct StatusLine;

/// The field that is typed into.
#[derive(Component)]
struct Entry;

fn setup(mut commands: Commands) {
    commands.spawn(Camera2d);
    let text = |size: f32| TextFont {
        font_size: FontSize::Px(size),
        ..default()
    };

    commands.spawn((
        Node {
            width: percent(100),
            height: percent(100),
            flex_direction: FlexDirection::Column,
            padding: px(12).all(),
            row_gap: px(8),
            ..default()
        },
        BackgroundColor(Color::srgb(0.10, 0.11, 0.14)),
        // Everything focusable inside one group, so Tab cycles within the window.
        TabGroup::new(0),
        children![
            // The transcript and the roster, side by side.
            (
                Node {
                    flex_grow: 1.0,
                    column_gap: px(12),
                    min_height: px(0),
                    ..default()
                },
                children![
                    (
                        Node {
                            flex_grow: 1.0,
                            flex_direction: FlexDirection::Column,
                            justify_content: JustifyContent::FlexEnd,
                            overflow: Overflow::clip(),
                            padding: px(8).all(),
                            row_gap: px(2),
                            ..default()
                        },
                        BackgroundColor(Color::srgb(0.13, 0.14, 0.18)),
                        Transcript,
                    ),
                    (
                        Node {
                            width: px(190),
                            flex_direction: FlexDirection::Column,
                            padding: px(8).all(),
                            ..default()
                        },
                        BackgroundColor(Color::srgb(0.13, 0.14, 0.18)),
                        children![(Text::new("in the room"), text(13.0), Roster)],
                    ),
                ],
            ),
            // The line to type into.
            (
                Node {
                    padding: px(8).all(),
                    ..default()
                },
                BackgroundColor(Color::srgb(0.16, 0.17, 0.22)),
                BorderColor::all(Color::srgb(0.30, 0.32, 0.40)),
                // `TextInput` and `EditableText` are Bevy's own: this is an ordinary entity with
                // a couple of components on it, not a widget this example had to build.
                TextInput,
                EditableText {
                    allow_newlines: false,
                    ..default()
                },
                TextCursorStyle::default(),
                TextLayout::no_wrap(),
                text(15.0),
                // Focused at startup, so the window can be typed into straight away.
                AutoFocus,
                TabIndex(0),
                Entry,
            ),
            (
                Text::new("connecting..."),
                text(12.0),
                TextColor(Color::srgb(0.55, 0.58, 0.66)),
                StatusLine
            ),
        ],
    ));
}

/// Adds one line to the transcript. A command, so an observer can call it without a `Query`.
fn say(line: String) -> impl Command {
    move |world: &mut World| {
        let Some(transcript) = world.query_filtered::<Entity, With<Transcript>>().iter(world).next() else {
            return;
        };
        let child = world
            .spawn((
                Text::new(line),
                TextFont {
                    font_size: FontSize::Px(14.0),
                    ..default()
                },
            ))
            .id();
        world.entity_mut(transcript).add_child(child);
    }
}

/// The transcript is entities, so it needs a bound: the oldest go when there are too many.
fn trim_scrollback(transcript: Single<&Children, With<Transcript>>, mut commands: Commands) {
    if transcript.len() > SCROLLBACK {
        for &line in &transcript[..transcript.len() - SCROLLBACK] {
            commands.entity(line).despawn();
        }
    }
}

/// One subscription entity for the whole session. The observer shows the backlog once its rows
/// are in: it can be spawned before the connection is up, and is sent again after a reconnect.
fn subscribe(mut commands: Commands) {
    commands
        .subscribe::<RemoteModule, _>(["SELECT * FROM user", "SELECT * FROM message"])
        .observe(
            |_: On<SubscriptionApplied>, users: Rows<User>, messages: Rows<Message>, mut commands: Commands| {
                let mut backlog: Vec<&Message> = messages.iter().collect();
                backlog.sort_by_key(|message| message.sent);
                for message in backlog.iter().rev().take(20).rev() {
                    let line = format!("{}: {}", name_of(&users, &message.sender), message.text);
                    commands.queue(say(line));
                }
                let online = users.iter().filter(|user| user.online).count();
                commands.queue(say(format!("-- {} messages, {online} online --", backlog.len())));
            },
        );
}

/// Messages as they arrive. This resolves a sender's name through another table because of a
/// change, so it runs in `StdbTransaction`, where the user row the server promised is already in.
fn show_new_messages(
    mut inserted: MessageReader<RowInserted<Message>>,
    applied: Query<(), Changed<spacetimedb_bevy::SubscriptionState>>,
    users: Rows<User>,
    mut commands: Commands,
) {
    // The backlog arrives as inserts too; the subscription's observer has shown it already.
    if !applied.is_empty() {
        inserted.clear();
    }
    for message in inserted.read() {
        let line = format!("{}: {}", name_of(&users, &message.row.sender), message.row.text);
        commands.queue(say(line));
    }
}

/// `RowUpdated` carries the row as it was and as it is, which is the only way to tell a rename
/// from someone arriving.
fn show_user_changes(mut updated: MessageReader<RowUpdated<User>>, mut commands: Commands) {
    for RowUpdated { old, new, .. } in updated.read() {
        if old.name != new.name {
            commands.queue(say(format!("* {} is now {}", display(old), display(new))));
        }
        if old.online != new.online {
            let what = if new.online { "joined" } else { "left" };
            commands.queue(say(format!("* {} {what}", display(new))));
        }
    }
}

/// Enter sends what is in the field. `EditableText` holds the text; this reads it, empties it,
/// and calls a reducer with it.
fn send_what_was_typed(
    keys: Res<ButtonInput<KeyCode>>,
    focus: Res<InputFocus>,
    mut entry: Query<&mut EditableText, With<Entry>>,
    state: Res<State<StdbState<RemoteModule>>>,
    reducers: Reducers<RemoteModule>,
    mut commands: Commands,
) {
    if !keys.just_pressed(KeyCode::Enter) {
        return;
    }
    let Some(mut typed) = focus.get().and_then(|entity| entry.get_mut(entity).ok()) else {
        return;
    };
    let line = typed.value().to_string().trim().to_owned();
    if line.is_empty() {
        return;
    }
    typed.clear();
    if *state.get() != StdbState::<RemoteModule>::connected() {
        commands.queue(say("not connected yet".into()));
        return;
    }
    match line.strip_prefix("/name") {
        Some(name) => {
            reducers.set_name(name.trim().to_owned());
        }
        None => {
            reducers.send_message(line);
        }
    }
}

/// The module rejects empty names and messages; say so instead of failing silently.
fn report_failures(
    mut named: MessageReader<ReducerResult<SetName>>,
    mut sent: MessageReader<ReducerResult<SendMessage>>,
    mut commands: Commands,
) {
    let named = named.read().map(|result| (&result.status, "set a name"));
    let sent = sent.read().map(|result| (&result.status, "send"));
    for (status, what) in named.chain(sent) {
        if let ReducerStatus::Failed(why) = status {
            commands.queue(say(format!("! could not {what}: {why}")));
        }
    }
}

/// Who is here, read straight off the table every frame. A table is cheap to read; this is a few
/// dozen rows and does not need to be told when it changes.
fn roster(users: Rows<User>, mut text: Single<&mut Text, With<Roster>>) {
    let mut here: Vec<String> = users.iter().filter(|user| user.online).map(display).collect();
    here.sort();
    text.0 = format!("in the room ({})\n\n{}", here.len(), here.join("\n"));
}

fn status_line(state: Res<State<StdbState<RemoteModule>>>, mut text: Single<&mut Text, With<StatusLine>>) {
    text.0 = match state.get().connection {
        ConnectionState::Connected => "enter sends  ·  /name <name> renames you".to_owned(),
        other => format!("{other:?}..."),
    };
}

fn name_of(users: &Rows<User>, identity: &Identity) -> String {
    users.get(identity).map_or_else(|| short(identity), display)
}

fn display(user: &User) -> String {
    user.name.clone().unwrap_or_else(|| short(&user.identity))
}

fn short(identity: &Identity) -> String {
    identity.to_hex().chars().take(8).collect()
}
