use anyhow::{Result, bail};
use pipewire_native::context::Context;
use pipewire_native::properties::Properties;
use pipewire_native::proxy::node::{Node, NodeEvents};
use pipewire_native::proxy::registry::RegistryEvents;
use pipewire_native::thread_loop::ThreadLoop;
use pipewire_native::types;
use std::fs::{File, TryLockError};
use tracing::{debug, error, info};
use tracing_subscriber::EnvFilter;
use zbus::blocking::Connection;
use zbus::zvariant::OwnedFd;

const LOCK_FILE_PATH: &str = "/tmp/caffeinate.lock";

const DBUS_LOGIND_SERVICE: &str = "org.freedesktop.login1";
const DBUS_LOGIND_PATH: &str = "/org/freedesktop/login1";
const DBUS_LOGIND_MANAGER_INTERFACE: &str = "org.freedesktop.login1.Manager";

#[derive(Debug, Clone, Copy, PartialEq)]
enum PlaybackStatus {
    Playing,
    StoppedOrPaused,
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("LOG_LEVEL").unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .with_line_number(true)
        .init();
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

    let (playback_status_tx, playback_status_rx) =
        std::sync::mpsc::sync_channel::<PlaybackStatus>(1);

    let mut last_playback_status = PlaybackStatus::StoppedOrPaused;

    pw_registry.add_listener(RegistryEvents {
        global: Some(Box::new(move |id, _, type_, version, _| {
            if type_ == types::interface::NODE {
                if let Ok(object) = pw_registry_clone.bind(id, type_, version) {
                    let node = object.downcast::<Node>().unwrap();

                    let playback_status_tx = playback_status_tx.clone();

                    node.add_listener(NodeEvents {
                        info: Some(Box::new(move |info| {
                            use pipewire_native::proxy::node::NodeChangeMask;

                            // https://docs.pipewire.org/src_2pipewire_2node_8h.html
                            // PW_NODE_STATE_ERROR = -1
                            // PW_NODE_STATE_CREATING = 0
                            // PW_NODE_STATE_SUSPENDED = 1
                            // PW_NODE_STATE_IDLE = 2
                            // PW_NODE_STATE_RUNNING = 3

                            if info.mask.contains(NodeChangeMask::STATE) {
                                match info.state as u32 {
                                    3 => {
                                        if last_playback_status == PlaybackStatus::Playing {
                                            return;
                                        }
                                        last_playback_status = PlaybackStatus::Playing;

                                        playback_status_tx.send(PlaybackStatus::Playing).unwrap();
                                    }
                                    _ => {
                                        if last_playback_status == PlaybackStatus::StoppedOrPaused {
                                            return;
                                        }
                                        last_playback_status = PlaybackStatus::StoppedOrPaused;

                                        playback_status_tx
                                            .send(PlaybackStatus::StoppedOrPaused)
                                            .unwrap();
                                    }
                                }
                            }
                        })),
                        param: None,
                    });
                }
            }
        })),
        ..Default::default()
    });

    pw_main_loop.run();

    let system_conn = Connection::system()?;

    let mut inhibit_lock_fd = Option::<OwnedFd>::None;
    let mut is_lock_active = false;

    std::thread::spawn(move || {
        while let Ok(status) = playback_status_rx.recv() {
            match status {
                PlaybackStatus::Playing => {
                    if is_lock_active {
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

                    inhibit_lock_fd.replace(fd);
                    is_lock_active = true;
                }
                PlaybackStatus::StoppedOrPaused => {
                    if is_lock_active {
                        if inhibit_lock_fd.take().is_some() {
                            is_lock_active = false;
                            info!("Inhibit lock released");
                        } else {
                            error!("Inhibit lock not acquired yet");
                        }
                    }
                }
            }
        }
    });

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
            &(
                "idle",
                "caffeinate-rs",
                "Media player is playing something",
                "block",
            ),
        )?
        .body()
        .deserialize::<OwnedFd>()?;

    Ok(fd)
}

fn ensure_single_instance() -> Result<File> {
    debug!("opening lock file '{}'", LOCK_FILE_PATH);

    let file = match std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(LOCK_FILE_PATH)
    {
        Ok(f) => f,
        Err(err) => bail!("could not open lock file: {}", err),
    };

    if let Err(lock_error) = file.try_lock() {
        if let TryLockError::Error(err) = lock_error {
            bail!("failed to lock file: {}", err);
        }

        // NOTE: there is only two possible errors, either TryLockError::Error(e) and
        // TryLockError::WouldBlock

        bail!("another instance is running");
    }

    Ok(file)
}
