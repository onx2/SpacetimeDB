//! Losing the connection, against a running server reached through a proxy the test controls.
//! Ignored by default; see `live.rs` for how to set the server up.

// A test that talks to a server says what it is doing, on the console, as the official suites do.
#![allow(clippy::disallowed_macros)]

mod chat_bindings;

use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bevy_app::App;
use bevy_ecs::prelude::*;
use bevy_state::prelude::*;
use chat_bindings::{RemoteModule, RemoteReducers, User};
use spacetimedb_bevy::{
    CloseReason, ConnectOptions, ConnectionLost, Disconnected, Reconnect, ReconnectPolicy, Reducers, Row, RowDeleted,
    RowInserted, RowUpdated, Rows, StdbCommandsExt, StdbIdentity, StdbPlugin, StdbState, StdbTransaction,
    SubscriptionState,
};

const DATABASE: &str = "spacetimedb-bevy-test";

/// Forwards TCP to the server until told to fail in one of two ways.
#[derive(Clone)]
struct Proxy {
    port: u16,
    /// Refuse new connections and cut existing ones: a server restart, or the network going away.
    down: Arc<AtomicBool>,
    /// Keep sockets open but deliver nothing: a dead path that no one reports.
    stalled: Arc<AtomicBool>,
    live: Arc<Mutex<Vec<TcpStream>>>,
}

impl Proxy {
    fn start() -> Self {
        Self::start_in_front_of("127.0.0.1:3000".into())
    }

    fn start_in_front_of(server: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let proxy = Self {
            port: listener.local_addr().unwrap().port(),
            down: Arc::default(),
            stalled: Arc::default(),
            live: Arc::default(),
        };
        let accepting = proxy.clone();
        std::thread::spawn(move || {
            for client in listener.incoming().flatten() {
                if accepting.down.load(Ordering::Relaxed) {
                    continue;
                }
                let Ok(server) = TcpStream::connect(&server) else {
                    continue;
                };
                let mut live = accepting.live.lock().unwrap();
                for (from, to) in [(&client, &server), (&server, &client)] {
                    let (mut from, mut to) = (from.try_clone().unwrap(), to.try_clone().unwrap());
                    let stalled = accepting.stalled.clone();
                    std::thread::spawn(move || {
                        let mut buffer = [0; 16 * 1024];
                        while let Ok(read @ 1..) = from.read(&mut buffer) {
                            if !stalled.load(Ordering::Relaxed) && to.write_all(&buffer[..read]).is_err() {
                                break;
                            }
                        }
                        let _ = to.shutdown(Shutdown::Both);
                    });
                }
                live.extend([client, server]);
            }
        });
        proxy
    }

    fn cut(&self) {
        self.down.store(true, Ordering::Relaxed);
        for stream in self.live.lock().unwrap().drain(..) {
            let _ = stream.shutdown(Shutdown::Both);
        }
    }

    fn restore(&self) {
        self.down.store(false, Ordering::Relaxed);
        self.stalled.store(false, Ordering::Relaxed);
    }

    fn options(&self) -> ConnectOptions {
        ConnectOptions::new(format!("http://127.0.0.1:{}", self.port), DATABASE)
    }
}

#[derive(Resource, Default)]
struct Lost(Vec<String>);

fn app(options: ConnectOptions) -> (App, Entity) {
    let mut app = App::new();
    app.add_plugins(
        StdbPlugin::<RemoteModule>::new()
            .connect_to(options)
            .with_reconnect(ReconnectPolicy::Backoff {
                initial: Duration::from_millis(50),
                max: Duration::from_millis(200),
                max_attempts: None,
            }),
    )
    .init_resource::<Lost>()
    .add_observer(|lost: On<ConnectionLost<RemoteModule>>, mut log: ResMut<Lost>| {
        log.0.push(format!("attempt {}: {}", lost.attempt, lost.reason));
    })
    .add_observer(|closed: On<Disconnected<RemoteModule>>, mut log: ResMut<Lost>| {
        log.0.push(format!("final: {}", closed.reason));
    });
    let subscription = app
        .world_mut()
        .commands()
        .subscribe::<RemoteModule, _>(["SELECT * FROM user"])
        .id();
    app.world_mut().flush();
    (app, subscription)
}

fn run_until(app: &mut App, what: &str, done: impl Fn(&World) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done(app.world()) {
        assert!(Instant::now() < deadline, "timed out waiting for {what}");
        app.update();
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn state(world: &World) -> StdbState<RemoteModule> {
    *world.resource::<State<StdbState<RemoteModule>>>().get()
}

fn user_count(app: &mut App) -> usize {
    app.world_mut()
        .run_system_cached(|users: Rows<User>| users.len())
        .unwrap()
}

#[test]
#[ignore = "needs a local server with the chat module published"]
fn cut_connection_comes_back_as_the_same_identity() {
    let proxy = Proxy::start();
    let (mut app, subscription) = app(proxy.options());
    let applied =
        move |world: &World| world.get::<SubscriptionState>(subscription) == Some(&SubscriptionState::Applied);

    run_until(&mut app, "the first subscription", applied);
    let identity = app.world().resource::<StdbIdentity<RemoteModule>>().identity;
    assert!(user_count(&mut app) > 0);

    proxy.cut();
    run_until(&mut app, "the drop to be noticed", |world| {
        state(world) == StdbState::<RemoteModule>::reconnecting()
    });
    // The last known rows stay readable, and the subscription waits for the next connection.
    assert!(user_count(&mut app) > 0);
    assert_eq!(
        app.world().get::<SubscriptionState>(subscription),
        Some(&SubscriptionState::Waiting)
    );
    assert!(app.world().get_resource::<StdbIdentity<RemoteModule>>().is_none());

    // Attempts keep failing while the proxy is down.
    run_until(&mut app, "a few failed attempts", |world| {
        world.resource::<Lost>().0.len() >= 3
    });
    assert_eq!(state(app.world()), StdbState::<RemoteModule>::reconnecting());

    proxy.restore();
    run_until(&mut app, "the subscription to be re-sent and applied", applied);
    assert_eq!(state(app.world()), StdbState::<RemoteModule>::connected());
    assert_eq!(
        app.world().resource::<StdbIdentity<RemoteModule>>().identity,
        identity,
        "reconnected as someone else"
    );
    assert!(user_count(&mut app) > 0);

    // The policy is a resource: turn it off, and the next drop is final.
    app.world_mut().resource_mut::<Reconnect<RemoteModule>>().policy = ReconnectPolicy::Never;
    proxy.cut();
    run_until(&mut app, "the final disconnect", |world| {
        state(world) == StdbState::<RemoteModule>::disconnected()
    });
    assert_eq!(user_count(&mut app), 0);
    assert!(app.world().resource::<Lost>().0.last().unwrap().starts_with("final: "));
}

#[test]
#[ignore = "needs a local server with the chat module published"]
fn silent_connection_times_out() {
    let proxy = Proxy::start();
    let mut options = proxy.options();
    options.idle_timeout = Duration::from_millis(150);
    let (mut app, subscription) = app(options);
    let applied =
        move |world: &World| world.get::<SubscriptionState>(subscription) == Some(&SubscriptionState::Applied);
    run_until(&mut app, "the first subscription", applied);

    // Sockets stay open and nothing arrives: only the ping can find this out.
    proxy.stalled.store(true, Ordering::Relaxed);
    run_until(&mut app, "the timeout", |world| {
        state(world) == StdbState::<RemoteModule>::reconnecting()
    });
    assert_eq!(
        app.world().resource::<Lost>().0[0],
        format!("attempt 1: {}", CloseReason::TimedOut)
    );

    proxy.restore();
    run_until(&mut app, "the reconnect", applied);
}

#[test]
#[ignore = "needs a local server with the chat module published"]
fn disconnect_while_reconnecting_stops_trying() {
    let proxy = Proxy::start();
    let (mut app, subscription) = app(proxy.options());
    run_until(&mut app, "the first subscription", move |world| {
        world.get::<SubscriptionState>(subscription) == Some(&SubscriptionState::Applied)
    });

    proxy.cut();
    run_until(&mut app, "the drop to be noticed", |world| {
        state(world) == StdbState::<RemoteModule>::reconnecting()
    });
    app.world_mut()
        .resource::<spacetimedb_bevy::StdbConnection<RemoteModule>>()
        .disconnect();
    run_until(&mut app, "the final disconnect", |world| {
        state(world) == StdbState::<RemoteModule>::disconnected()
    });
    assert_eq!(user_count(&mut app), 0);

    // Nothing is retried afterwards, even once the server is reachable again.
    proxy.restore();
    let attempts = app.world().resource::<Lost>().0.len();
    for _ in 0..60 {
        app.update();
        std::thread::sleep(Duration::from_millis(5));
    }
    assert_eq!(state(app.world()), StdbState::<RemoteModule>::disconnected());
    assert_eq!(app.world().resource::<Lost>().0.len(), attempts);
}

/// A reconnect keeps the rows: every live subscription goes out again as one `SubscribeBatch`,
/// the server answers with one snapshot, and the app is told only about what changed while it
/// was away. Rows that are entities stay the same entities.
type Identity = spacetimedb_bevy::__codegen::lib::Identity;

fn user_entities(users: Query<(Entity, &Row<User>)>) -> Vec<(Entity, Identity)> {
    let mut entities: Vec<_> = users.iter().map(|(entity, user)| (entity, user.identity)).collect();
    entities.sort();
    entities
}

#[test]
#[ignore = "needs a local server with the chat module published"]
fn reconnect_reports_only_what_changed() {
    #[derive(Resource, Default, Debug, PartialEq)]
    struct Seen {
        inserted: Vec<Option<String>>,
        updated: Vec<(Option<String>, Option<String>)>,
        deleted: usize,
    }

    let proxy = Proxy::start();
    let (mut app, subscription) = {
        let mut app = App::new();
        app.add_plugins(
            StdbPlugin::<RemoteModule>::new()
                .entity_table::<User>()
                .connect_to(proxy.options())
                .with_reconnect(ReconnectPolicy::Backoff {
                    initial: Duration::from_millis(50),
                    max: Duration::from_millis(200),
                    max_attempts: None,
                }),
        )
        .init_resource::<Seen>()
        .init_resource::<Lost>()
        .add_observer(|lost: On<ConnectionLost<RemoteModule>>, mut log: ResMut<Lost>| {
            log.0.push(format!("attempt {}: {}", lost.attempt, lost.reason));
        })
        .add_systems(
            StdbTransaction,
            |mut inserted: MessageReader<RowInserted<User>>,
             mut updated: MessageReader<RowUpdated<User>>,
             mut deleted: MessageReader<RowDeleted<User>>,
             mut seen: ResMut<Seen>| {
                seen.inserted
                    .extend(inserted.read().map(|message| message.row.name.clone()));
                seen.updated.extend(
                    updated
                        .read()
                        .map(|message| (message.old.name.clone(), message.new.name.clone())),
                );
                seen.deleted += deleted.read().count();
            },
        );
        let subscription = app
            .world_mut()
            .commands()
            .subscribe::<RemoteModule, _>(["SELECT * FROM user"])
            .id();
        app.world_mut().flush();
        (app, subscription)
    };
    let applied =
        move |world: &World| world.get::<SubscriptionState>(subscription) == Some(&SubscriptionState::Applied);
    run_until(&mut app, "the first subscription", applied);

    // Someone else, connected straight to the server, who will change things while we are away.
    let mut other = App::new();
    other.add_plugins(
        StdbPlugin::<RemoteModule>::new().connect_to(ConnectOptions::new("http://127.0.0.1:3000", DATABASE)),
    );
    other
        .world_mut()
        .commands()
        .subscribe::<RemoteModule, _>(["SELECT * FROM user"]);
    run_until(&mut other, "the other client", |world| {
        state(world) == StdbState::<RemoteModule>::connected()
    });
    let other_identity = other.world().resource::<StdbIdentity<RemoteModule>>().identity;
    let sees_other = move |world: &World| {
        let mut found = false;
        // No system access to a `&World`; entities are reachable directly.
        for entity in world.iter_entities() {
            found |= entity
                .get::<Row<User>>()
                .is_some_and(|user| user.identity == other_identity);
        }
        found
    };
    run_until(&mut app, "the other client's user", sees_other);

    let entities_before = app.world_mut().run_system_cached(user_entities).unwrap();
    *app.world_mut().resource_mut::<Seen>() = Seen::default();

    proxy.cut();
    run_until(&mut app, "the drop to be noticed", |world| {
        state(world) == StdbState::<RemoteModule>::reconnecting()
    });
    // While we are away, the other one takes a name.
    other
        .world_mut()
        .run_system_cached(|reducers: Reducers<RemoteModule>| {
            reducers.set_name("renamed while you were away".into());
        })
        .unwrap();
    for _ in 0..40 {
        other.update();
        std::thread::sleep(Duration::from_millis(5));
    }
    // Our going and coming back is seen by the module as offline and online again: no net change.

    proxy.restore();
    run_until(&mut app, "the subscription to be applied again", applied);
    println!("{:?}", app.world().resource::<Lost>().0);

    let seen = app.world().resource::<Seen>();
    assert_eq!(seen.deleted, 0, "rows were emptied: {seen:?}");
    assert_eq!(seen.inserted, [], "rows were inserted again: {seen:?}");
    assert_eq!(seen.updated, [(None, Some("renamed while you were away".to_owned()))]);
    let entities_after = app.world_mut().run_system_cached(user_entities).unwrap();
    assert_eq!(entities_before, entities_after, "entities were replaced");

    // Updates flow again after the snapshot.
    other
        .world_mut()
        .run_system_cached(|reducers: Reducers<RemoteModule>| {
            reducers.set_name("and again".into());
        })
        .unwrap();
    other.update();
    run_until(&mut app, "a live update after the reconnect", |world| {
        world.resource::<Seen>().updated.len() == 2
    });
}
