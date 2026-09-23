//! An attempt to connect that gets no answer: given up on in time, and called off at once by
//! `disconnect`. No server: the host is a socket that accepts a connection and says nothing.

mod chat_bindings;

use std::net::TcpListener;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bevy_app::App;
use bevy_ecs::prelude::*;
use bevy_state::prelude::*;
use chat_bindings::RemoteModule;
use spacetimedb_bevy::{
    CloseReason, ConnectError, ConnectOptions, Disconnected, StdbConnection, StdbPlugin, StdbState,
};

#[derive(Resource, Default)]
struct Ended(Vec<Arc<CloseReason>>);

/// An app that connects, at startup, to a host that never answers the websocket handshake.
/// The listener is returned to be kept: the host must stay there for as long as the test runs.
fn app(connect_timeout: Duration) -> (App, TcpListener) {
    let silent = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = silent.local_addr().unwrap().port();
    let mut options = ConnectOptions::new(format!("http://127.0.0.1:{port}"), "chat");
    options.connect_timeout = connect_timeout;

    let mut app = App::new();
    app.add_plugins(StdbPlugin::<RemoteModule>::new().connect_to(options))
        .init_resource::<Ended>()
        .add_observer(|closed: On<Disconnected<RemoteModule>>, mut ended: ResMut<Ended>| {
            ended.0.push(closed.reason.clone());
        });
    (app, silent)
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

fn ended(world: &World) -> bool {
    !world.resource::<Ended>().0.is_empty() && state(world) == StdbState::<RemoteModule>::disconnected()
}

#[test]
fn a_host_that_never_answers_is_given_up_on() {
    let timeout = Duration::from_millis(200);
    let (mut app, _silent) = app(timeout);
    run_until(&mut app, "the attempt to be given up", ended);

    let reasons = &app.world().resource::<Ended>().0;
    assert!(
        matches!(
            reasons[..],
            [ref reason] if matches!(**reason, CloseReason::Connect(ConnectError::TimedOut(after)) if after == timeout)
        ),
        "{reasons:?}"
    );
    assert!(!app.world().resource::<StdbConnection<RemoteModule>>().is_open());
}

#[test]
fn disconnect_calls_off_an_attempt_that_is_still_opening() {
    // Longer than the test waits, so that only `disconnect` can be what ends the attempt.
    let (mut app, _silent) = app(Duration::from_secs(60));
    run_until(&mut app, "the attempt to start", |world| {
        state(world) == StdbState::<RemoteModule>::connecting()
    });
    // Into the handshake, where nothing reads what `disconnect` closes.
    std::thread::sleep(Duration::from_millis(100));

    app.world().resource::<StdbConnection<RemoteModule>>().disconnect();
    run_until(&mut app, "the attempt to be called off", ended);

    let reasons = &app.world().resource::<Ended>().0;
    assert!(
        matches!(reasons[..], [ref reason] if matches!(**reason, CloseReason::Requested)),
        "{reasons:?}"
    );
    // And free for the next one.
    assert!(!app.world().resource::<StdbConnection<RemoteModule>>().is_open());
}
