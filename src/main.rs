use anyhow::{Result, bail};
use pipewire_native::context::Context;
use pipewire_native::properties::Properties;
use pipewire_native::proxy::node::{Node, NodeEvents, NodeState};
use pipewire_native::proxy::registry::RegistryEvents;
use pipewire_native::thread_loop::ThreadLoop;
use pipewire_native::types as pw_types;
use std::collections::HashMap;
use std::fs::{File, TryLockError};
use std::sync::{Arc, Mutex};
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

    pipewire_native::init();

    let pw_main_loop = ThreadLoop::new(&Properties::new()).unwrap();
    let pw_context = Context::new(pw_main_loop.main_loop(), Properties::new())?;
    let pw_core = pw_context.connect(None)?;

    let pw_registry = pw_core.registry()?;
    let pw_registry_clone = pw_registry.clone();

    let (audio_state_tx, audio_state_rx) = std::sync::mpsc::sync_channel::<AudioState>(1);

    let running_nodes: Arc<Mutex<HashMap<u32, ()>>> = Arc::new(Mutex::new(HashMap::new()));

    pw_registry.add_listener(RegistryEvents {
        global: Some(Box::new(move |id, _, type_, version, _| {
            if type_ == pw_types::interface::NODE
                && let Ok(object) = pw_registry_clone.bind(id, type_, version)
            {
                let node = match object.downcast::<Node>() {
                    Some(n) => n,
                    None => {
                        error!("Failed to downcast proxy object to Node");
                        return;
                    }
                };

                let audio_state_tx = audio_state_tx.clone();
                let running_nodes = Arc::clone(&running_nodes);

                node.add_listener(NodeEvents {
                    info: Some(Box::new(move |info| {
                        use pipewire_native::proxy::node::NodeChangeMask;

                        // https://docs.pipewire.org/src_2pipewire_2node_8h.html
                        // NOTE: The NodeState enum is off by one, so 3 here means the audio is running.
                        // This might be a bug in the pipewire_native crate.

                        let node_state = match info.state {
                            NodeState::Error => NodeState::Creating,
                            NodeState::Creating => NodeState::Suspended,
                            NodeState::Suspended => NodeState::Idle,
                            NodeState::Idle => NodeState::Running,
                            _ => NodeState::Error,
                        };

                        debug!("Node id: {} - state: {:?}", info.id, node_state);

                        if info.mask.contains(NodeChangeMask::STATE) {
                            match node_state {
                                NodeState::Running => {
                                    if let Err(e) = audio_state_tx.send(AudioState::Running) {
                                        error!("Failed to send audio state to channel: {e}");
                                        return;
                                    }

                                    running_nodes.lock().unwrap().insert(info.id, ());
                                }
                                _ => {
                                    if let Ok(mut rn) = running_nodes.try_lock()
                                        && let Some(_) = rn.remove(&info.id)
                                        && rn.is_empty()
                                        && let Err(e) = audio_state_tx.send(AudioState::NotRunning)
                                    {
                                        error!("Failed to send audio state to channel: {e}");
                                    }
                                }
                            }
                        }
                    })),
                    param: None,
                });
            }
        })),
        ..Default::default()
    });

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

    std::thread::park();

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
