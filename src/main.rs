use anyhow::{Result, bail};
use pipewire as pw;
use pipewire::node::NodeState;
use pw::{node::Node, types::ObjectType};
use std::cell::RefCell;
use std::collections::HashMap;
use std::fs::{File, TryLockError};
use std::rc::Rc;
use tracing::{debug, error, info};
use tracing_subscriber::EnvFilter;
use zbus::blocking::Connection;
use zbus::zvariant::OwnedFd;

const LOCK_FILE_PATH: &str = "/tmp/caffeinate.lock";

const DBUS_LOGIND_SERVICE: &str = "org.freedesktop.login1";
const DBUS_LOGIND_PATH: &str = "/org/freedesktop/login1";
const DBUS_LOGIND_MANAGER_INTERFACE: &str = "org.freedesktop.login1.Manager";

#[derive(Debug, Clone, Copy, PartialEq)]
enum AudioState {
    Running,
    NotRunning,
}

fn main() -> Result<()> {
    init_tracing();

    let _instance_lock = ensure_single_instance()?;

    pw::init();

    let pw_main_loop = pw::main_loop::MainLoopRc::new(None)?;
    let pw_context = pw::context::ContextRc::new(&pw_main_loop, None)?;
    let pw_core = pw_context.connect_rc(None)?;
    let pw_registry = pw_core.get_registry_rc()?;
    let pw_registry_weak = pw_registry.downgrade();

    let node_proxies: Rc<RefCell<HashMap<u32, Box<dyn std::any::Any>>>> =
        Rc::new(RefCell::new(HashMap::new()));

    let running_nodes: Rc<RefCell<HashMap<u32, ()>>> = Rc::new(RefCell::new(HashMap::new()));

    let node_proxies_clone = Rc::clone(&node_proxies);

    let (audio_state_tx, audio_state_rx) = std::sync::mpsc::sync_channel::<AudioState>(1);

    let _registry_listener = pw_registry
        .add_listener_local()
        .global(move |global| {
            if global.type_ != ObjectType::Node {
                return;
            }

            let Some(registry) = pw_registry_weak.upgrade() else {
                return;
            };

            let node_name = global
                .props
                .as_ref()
                .and_then(|p| p.get("node.name"))
                .unwrap_or("unknown");

            debug!(id = global.id, name = node_name, "Node registered");

            let node: Node = match registry.bind(global) {
                Ok(n) => n,
                Err(e) => {
                    error!("Failed to bind to node {}: {}", global.id, e);
                    return;
                }
            };

            let running_nodes = Rc::clone(&running_nodes);
            let audio_state_tx = audio_state_tx.clone();

            let node_listener = node
                .add_listener_local()
                .info(move |info| {
                    let state = info.state();

                    debug!(id = ?info.id(), state = ?state, "Node listener");

                    match state {
                        NodeState::Running => {
                            if let Err(e) = audio_state_tx.send(AudioState::Running) {
                                error!("Failed to send audio state to channel: {e}");
                                return;
                            }

                            running_nodes.borrow_mut().insert(info.id(), ());
                        }
                        _ => {
                            let mut running_nodes = running_nodes.borrow_mut();

                            if let Some(_) = running_nodes.remove(&info.id())
                                && running_nodes.is_empty()
                                && let Err(e) = audio_state_tx.send(AudioState::NotRunning)
                            {
                                error!("Failed to send audio state to channel: {e}");
                            }
                        }
                    }
                })
                .register();

            node_proxies
                .borrow_mut()
                .insert(global.id, Box::new((node, node_listener)));
        })
        .global_remove(move |id| {
            if node_proxies_clone.borrow_mut().remove(&id).is_some() {
                debug!(id, "Node removed");
            }
        })
        .register();

    let system_conn = Connection::system()?;

    let mut inhibit_lock_fd = Option::<OwnedFd>::None;

    std::thread::spawn(move || {
        while let Ok(status) = audio_state_rx.recv() {
            match status {
                AudioState::Running => {
                    if inhibit_lock_fd.is_some() {
                        continue;
                    }

                    let fd = match acquire_inhibitor_lock(&system_conn) {
                        Ok(fd) => {
                            info!("Inhibit lock acquired");
                            fd
                        }
                        Err(e) => {
                            error!("Failed to acquire idle inhibit lock: {:?}", e);
                            inhibit_lock_fd = None;
                            continue;
                        }
                    };

                    inhibit_lock_fd = Some(fd);
                }
                AudioState::NotRunning if inhibit_lock_fd.is_some() => {
                    let _ = inhibit_lock_fd.take().unwrap();
                    info!("Inhibit lock released");
                }
                _ => {}
            }
        }
    });

    pw_main_loop.run();

    unsafe {
        pw::deinit();
    }

    Ok(())
}

fn acquire_inhibitor_lock(conn: &Connection) -> Result<OwnedFd> {
    // https://www.freedesktop.org/software/systemd/man/latest/org.freedesktop.login1.html
    let fd = conn
        .call_method(
            Some(DBUS_LOGIND_SERVICE),
            DBUS_LOGIND_PATH,
            Some(DBUS_LOGIND_MANAGER_INTERFACE),
            "Inhibit",
            &("idle", "caffeinate-rs", "Audio playback detected", "block"),
        )?
        .body()
        .deserialize::<OwnedFd>()?;

    Ok(fd)
}

fn ensure_single_instance() -> Result<File> {
    debug!("Opening lock file '{}'", LOCK_FILE_PATH);

    let file = match std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(LOCK_FILE_PATH)
    {
        Ok(f) => f,
        Err(err) => bail!("Could not open lock file: {}", err),
    };

    if let Err(lock_error) = file.try_lock() {
        if let TryLockError::Error(err) = lock_error {
            bail!("Failed to lock file: {}", err);
        }

        // NOTE: There are only two possible errors: either TryLockError::Error(e) or
        // TryLockError::WouldBlock

        bail!("Another instance is running");
    }

    Ok(file)
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("LOG_LEVEL").unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_line_number(true)
        .init();
}
