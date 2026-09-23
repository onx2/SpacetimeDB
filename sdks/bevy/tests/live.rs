//! End to end against a running server. Ignored by default. From the repository root:
//!
//! ```sh
//! spacetime start
//! spacetime publish -p templates/chat-console-rs/spacetimedb --server local -y spacetimedb-bevy-test
//! cd sdks/bevy && cargo test --test live -- --ignored
//! ```
//!
//! `tests/chat_bindings` is what `spacetime generate --lang bevy` writes for that module.

// A test that talks to a server says what it is doing, on the console, as the official suites do.
#![allow(clippy::disallowed_macros)]

mod chat_bindings;

/// Published from `templates/chat-console-rs/spacetimedb`; see this file's docs.
const DATABASE: &str = "spacetimedb-bevy-test";

use std::time::{Duration, Instant, SystemTime};

use bevy_app::{App, Update};
use bevy_ecs::prelude::*;
use bevy_state::prelude::*;
use chat_bindings::{Message, RemoteModule, RemoteReducers, SendMessage, SetName, User};
use spacetimedb_bevy::{
    ConnectOptions, Disconnected, ReducerCall, ReducerFinished, ReducerResult, ReducerStatus, Reducers, RowInserted,
    Rows, StdbCommandsExt, StdbConnection, StdbIdentity, StdbPlugin, StdbState, StdbTransaction, Subscription,
    SubscriptionApplied, SubscriptionState,
};

/// `.0`: messages are gone while users remain. `.1`: users were ever seen empty before that.
#[derive(Resource, Default)]
struct Narrowed(bool, bool);

fn watch_narrowed(users: Rows<User>, messages: Rows<Message>, script: Res<Script>, mut narrowed: ResMut<Narrowed>) {
    if !script.done || narrowed.0 {
        return;
    }
    narrowed.1 |= users.is_empty();
    narrowed.0 = messages.is_empty() && !users.is_empty();
}

/// Set once both tables have been emptied by an unsubscribe.
#[derive(Resource, Default)]
struct Emptied(bool);

fn watch_emptied(users: Rows<User>, messages: Rows<Message>, script: Res<Script>, mut emptied: ResMut<Emptied>) {
    emptied.0 = script.done && users.is_empty() && messages.is_empty();
}

#[derive(Resource)]
struct Script {
    name: String,
    text: String,
    log: Vec<String>,
    done: bool,
}

/// Runs as an observer on the subscription entity, at the transaction boundary.
fn on_applied(
    applied: On<SubscriptionApplied>,
    states: Query<&SubscriptionState>,
    users: Rows<User>,
    me: Res<StdbIdentity<RemoteModule>>,
    reducers: Reducers<RemoteModule>,
    mut script: ResMut<Script>,
) {
    assert_eq!(states.get(applied.entity), Ok(&SubscriptionState::Applied));
    if script.done {
        // The replacement subscription later in the test applies too; the script runs once.
        script.log.push("applied again".into());
        return;
    }
    // The module's `client_connected` reducer inserted our user before the snapshot was taken.
    assert!(users.get(&me.identity).is_some_and(|user| user.online));
    script.log.push("applied".into());
    // Two calls in one frame: v3 sends them in one payload, and results must keep this order.
    reducers.set_name(script.name.clone());
    reducers.send_message(script.text.clone());
}

fn on_results(
    mut named: MessageReader<ReducerResult<SetName>>,
    mut sent: MessageReader<ReducerResult<SendMessage>>,
    mut inserted: MessageReader<RowInserted<Message>>,
    users: Rows<User>,
    messages: Rows<Message>,
    me: Res<StdbIdentity<RemoteModule>>,
    mut script: ResMut<Script>,
) {
    for result in named.read() {
        assert_eq!(result.status, ReducerStatus::Committed);
        // Strict delivery: the rename is already visible when its result arrives.
        assert_eq!(users.get(&me.identity).unwrap().name.as_ref(), Some(&result.args.name));
        script.log.push("named".into());
    }
    for result in sent.read() {
        assert_eq!(result.status, ReducerStatus::Committed);
        assert!(messages.iter().any(|message| message.text == result.args.text));
        // The row message of the same transaction is readable in the same run.
        assert!(inserted.read().any(|message| message.row.text == result.args.text));
        script.log.push("sent".into());
        script.done = true;
    }
}

fn run_until(app: &mut App, what: &str, done: impl Fn(&World) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done(app.world()) {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        app.update();
        std::thread::sleep(Duration::from_millis(5));
    }
}

#[test]
#[ignore = "needs a local server with the chat module published; see the file's docs"]
fn connect_subscribe_call_disconnect() {
    scripted_session(ConnectOptions::new("http://127.0.0.1:3000", DATABASE));
}

/// Ends the TLS terminator with the test, pass or fail.
struct KillOnDrop(std::process::Child);

impl Drop for KillOnDrop {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

/// The same session over `wss://`: `socat` terminates TLS in front of the local server with a
/// certificate made for the occasion, which the client is told to trust.
#[test]
#[ignore = "needs a local server with the chat module published, and `openssl` and `socat`"]
fn the_same_session_over_tls() {
    use std::process::{Command, Stdio};

    let dir = std::env::temp_dir().join(format!("stdb-bevy-tls-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = |name: &str| dir.join(name).to_str().unwrap().to_owned();
    let openssl = |args: &[&str]| {
        let status = Command::new("openssl")
            .args(args)
            .stderr(Stdio::null())
            .status()
            .expect("`openssl` is installed");
        assert!(status.success(), "openssl {args:?}");
    };
    // A certificate authority, and a server certificate signed by it: rustls refuses a
    // certificate that is its own authority.
    openssl(&[
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-days",
        "1",
        "-subj",
        "/CN=test ca",
        "-keyout",
        &path("ca-key.pem"),
        "-out",
        &path("ca.pem"),
    ]);
    openssl(&[
        "req",
        "-newkey",
        "rsa:2048",
        "-nodes",
        "-subj",
        "/CN=localhost",
        "-keyout",
        &path("key.pem"),
        "-out",
        &path("request.pem"),
    ]);
    std::fs::write(
        path("extensions"),
        "subjectAltName=DNS:localhost\nbasicConstraints=CA:FALSE\n",
    )
    .unwrap();
    openssl(&[
        "x509",
        "-req",
        "-in",
        &path("request.pem"),
        "-days",
        "1",
        "-CA",
        &path("ca.pem"),
        "-CAkey",
        &path("ca-key.pem"),
        "-CAcreateserial",
        "-extfile",
        &path("extensions"),
        "-out",
        &path("cert.pem"),
    ]);
    openssl(&[
        "x509",
        "-in",
        &path("ca.pem"),
        "-outform",
        "DER",
        "-out",
        &path("ca.der"),
    ]);

    // A port nothing else is using, freed just before `socat` takes it.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let _terminator = KillOnDrop(
        Command::new("socat")
            .arg(format!(
                "openssl-listen:{port},bind=127.0.0.1,reuseaddr,fork,verify=0,cert={},key={}",
                path("cert.pem"),
                path("key.pem"),
            ))
            .arg("tcp:127.0.0.1:3000")
            // The readiness check below connects without a handshake, which `socat` reports.
            .stderr(Stdio::null())
            .spawn()
            .expect("`socat` is installed"),
    );
    let deadline = Instant::now() + Duration::from_secs(5);
    while std::net::TcpStream::connect(("127.0.0.1", port)).is_err() {
        assert!(Instant::now() < deadline, "socat never started listening");
        std::thread::sleep(Duration::from_millis(20));
    }

    let mut options = ConnectOptions::new(format!("wss://localhost:{port}"), DATABASE);
    options.root_certificates = vec![std::fs::read(path("ca.der")).unwrap()];
    scripted_session(options);
    let _ = std::fs::remove_dir_all(&dir);
}

fn scripted_session(options: ConnectOptions) {
    let stamp = SystemTime::UNIX_EPOCH.elapsed().unwrap().as_millis();
    let mut app = App::new();
    app.add_plugins(StdbPlugin::<RemoteModule>::new().connect_to(options))
        .insert_resource(Script {
            name: format!("bevy-{stamp}"),
            text: format!("hello from bevy at {stamp}"),
            log: Vec::new(),
            done: false,
        })
        .init_resource::<Emptied>()
        .init_resource::<Narrowed>()
        .add_systems(StdbTransaction, (on_results, watch_narrowed, watch_emptied).chain())
        .add_systems(
            Update,
            |state: Res<State<StdbState<RemoteModule>>>, mut last: Local<Option<StdbState<RemoteModule>>>| {
                if *last != Some(**state) {
                    println!("state: {:?}", **state);
                    *last = Some(**state);
                }
            },
        )
        .add_observer(|closed: On<Disconnected<RemoteModule>>| println!("disconnected: {}", closed.reason));

    // Spawned before the connection exists: it waits, then is sent once connected.
    let subscription = app
        .world_mut()
        .commands()
        .subscribe::<RemoteModule, _>(["SELECT * FROM user", "SELECT * FROM message"])
        .observe(on_applied)
        .id();
    app.world_mut().flush();
    assert_eq!(
        app.world().get::<SubscriptionState>(subscription),
        Some(&SubscriptionState::Waiting)
    );

    run_until(&mut app, "the scripted calls", |world| world.resource::<Script>().done);
    assert_eq!(app.world().resource::<Script>().log, ["applied", "named", "sent"]);

    // Replacing the queries on the same entity: messages leave, users never do.
    app.world_mut()
        .entity_mut(subscription)
        .insert(Subscription::<RemoteModule>::new(["SELECT * FROM user"]));
    run_until(&mut app, "the narrowed subscription", |world| {
        world.resource::<Narrowed>().0
    });
    assert!(
        !app.world().resource::<Narrowed>().1,
        "users were dropped during the replacement"
    );
    assert_eq!(app.world().resource::<Script>().log.last().unwrap(), "applied again");

    // Despawning unsubscribes; the server answers with the rows to drop.
    app.world_mut().despawn(subscription);
    run_until(&mut app, "the unsubscribe", |world| world.resource::<Emptied>().0);

    app.world_mut().resource::<StdbConnection<RemoteModule>>().disconnect();
    run_until(&mut app, "the disconnect", |world| {
        *world.resource::<State<StdbState<RemoteModule>>>().get() == StdbState::<RemoteModule>::disconnected()
    });
    assert!(app.world().get_resource::<StdbIdentity<RemoteModule>>().is_none());
    app.world_mut()
        .run_system_cached(|users: Rows<User>, messages: Rows<Message>| {
            assert!(users.is_empty() && messages.is_empty());
        })
        .unwrap();
}

/// A server that is not there is reported as [`Disconnected`], and the handlers in
/// `StdbTransaction`, which may count on [`StdbIdentity<RemoteModule>`], do not run for it.
#[test]
fn failed_first_connection_is_reported() {
    #[derive(Resource, Default)]
    struct Reason(Option<String>);

    // Bound and dropped, so nothing listens there.
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let mut app = App::new();
    app.add_plugins(
        StdbPlugin::<RemoteModule>::new().connect_to(ConnectOptions::new(format!("http://127.0.0.1:{port}"), DATABASE)),
    )
    .init_resource::<Reason>()
    .add_systems(StdbTransaction, |_: Res<StdbIdentity<RemoteModule>>| {})
    .add_observer(|closed: On<Disconnected<RemoteModule>>, mut reason: ResMut<Reason>| {
        reason.0 = Some(closed.reason.to_string());
    });
    run_until(&mut app, "the failure", |world| world.resource::<Reason>().0.is_some());
    assert_eq!(app.world().resource::<Reason>().0.as_deref(), Some("could not connect"));
    assert_eq!(
        *app.world().resource::<State<StdbState<RemoteModule>>>().get(),
        StdbState::<RemoteModule>::disconnected()
    );
}

/// How long a reducer call takes to come back: the median of some calls made one after another.
fn round_trip(confirmed_reads: Option<bool>) -> Duration {
    #[derive(Resource, Default)]
    struct Results(usize);

    let mut options = ConnectOptions::new("http://127.0.0.1:3000", DATABASE);
    options.confirmed_reads = confirmed_reads;
    let mut app = App::new();
    app.add_plugins(StdbPlugin::<RemoteModule>::new().connect_to(options))
        .init_resource::<Results>()
        .add_systems(
            StdbTransaction,
            |mut results: MessageReader<ReducerResult<SendMessage>>, mut seen: ResMut<Results>| {
                for result in results.read() {
                    assert_eq!(result.status, ReducerStatus::Committed);
                    seen.0 += 1;
                }
            },
        );
    run_until(&mut app, "the connection", |world| {
        *world.resource::<State<StdbState<RemoteModule>>>().get() == StdbState::<RemoteModule>::connected()
    });

    let mut trips = Vec::new();
    for call in 1..=15 {
        let start = Instant::now();
        app.world_mut()
            .resource::<StdbConnection<RemoteModule>>()
            .call_reducer(SendMessage {
                text: format!("confirmed reads {confirmed_reads:?}, call {call}"),
            });
        // Without `run_until`'s sleep, which is longer than what is being measured.
        while app.world().resource::<Results>().0 < call {
            assert!(start.elapsed() < Duration::from_secs(10), "no result");
            app.update();
            std::thread::yield_now();
        }
        trips.push(start.elapsed());
    }
    trips.sort();
    trips[trips.len() / 2]
}

/// Calls come back under every setting of `ConnectOptions::confirmed_reads`. What the setting
/// changes cannot be asserted from outside the server, as the server's own tests note: a
/// confirmed result waits for the transaction to be on disk, which on a local disk takes no
/// longer than the noise in a round trip. The times are printed for whoever is curious.
#[test]
#[ignore = "needs a local server with the chat module published; see the file's docs"]
fn calls_return_with_and_without_confirmed_reads() {
    for confirmed_reads in [Some(false), Some(true), None] {
        let median = round_trip(confirmed_reads);
        println!("confirmed reads {confirmed_reads:?}: a call returns in {median:?}");
    }
}

/// A subscription to a typed query: only the rows it asks for arrive.
#[test]
#[ignore = "needs a local server with the chat module published; see the file's docs"]
fn typed_query_subscription() {
    use chat_bindings::query;

    let mut app = App::new();
    app.add_plugins(
        StdbPlugin::<RemoteModule>::new().connect_to(ConnectOptions::new("http://127.0.0.1:3000", DATABASE)),
    );
    // The database has users from every earlier run, nearly all of them offline.
    let subscription = app
        .world_mut()
        .commands()
        .subscribe_to::<RemoteModule, _>(query::user().filter(|user| user.online.eq(true)))
        .id();
    run_until(&mut app, "the subscription", |world| {
        world.get::<SubscriptionState>(subscription) == Some(&SubscriptionState::Applied)
    });
    app.world_mut()
        .run_system_cached(|users: Rows<User>, me: Res<StdbIdentity<RemoteModule>>| {
            assert!(users.get(&me.identity).is_some());
            assert!(users.iter().all(|user| user.online));
        })
        .unwrap();
}

/// A call made as an entity: context rides on the entity, the result arrives at its observer
/// with the rows already in place, and the entity is gone afterwards.
#[test]
#[ignore = "needs a local server with the chat module published; see the file's docs"]
fn call_as_entity() {
    #[derive(Component)]
    struct Context(&'static str);

    #[derive(Resource, Default)]
    struct Outcome(Option<String>);

    let stamp = SystemTime::UNIX_EPOCH.elapsed().unwrap().as_nanos();
    let text = format!("entity call {stamp}");

    let mut app = App::new();
    app.add_plugins(
        StdbPlugin::<RemoteModule>::new().connect_to(ConnectOptions::new("http://127.0.0.1:3000", DATABASE)),
    )
    .init_resource::<Outcome>();

    let call_text = text.clone();
    app.world_mut()
        .commands()
        .subscribe::<RemoteModule, _>(["SELECT * FROM message"])
        .observe(move |_: On<SubscriptionApplied>, mut commands: Commands| {
            commands
                .call_reducer(SendMessage {
                    text: call_text.clone(),
                })
                .insert(Context("from the send button"))
                .observe(
                    |done: On<ReducerFinished<SendMessage>>,
                     contexts: Query<&Context>,
                     messages: Rows<Message>,
                     mut outcome: ResMut<Outcome>| {
                        assert_eq!(done.status, ReducerStatus::Committed);
                        assert!(messages.iter().any(|message| message.text == done.args.text));
                        outcome.0 = Some(contexts.get(done.entity).unwrap().0.to_owned());
                    },
                );
        });
    app.world_mut().flush();

    let mut seen_in_flight = false;
    let deadline = Instant::now() + Duration::from_secs(10);
    while app.world().resource::<Outcome>().0.is_none() {
        assert!(Instant::now() < deadline, "timed out waiting for the call to finish");
        app.update();
        seen_in_flight |= app
            .world_mut()
            .query::<&ReducerCall<SendMessage>>()
            .iter(app.world())
            .any(|call| call.args().text == text);
        std::thread::sleep(Duration::from_millis(1));
    }

    assert!(seen_in_flight, "the call was never visible to a query while in flight");
    assert_eq!(
        app.world().resource::<Outcome>().0.as_deref(),
        Some("from the send button")
    );
    let remaining = app
        .world_mut()
        .query::<&ReducerCall<SendMessage>>()
        .iter(app.world())
        .count();
    assert_eq!(remaining, 0, "the call entity outlived its result");
}
