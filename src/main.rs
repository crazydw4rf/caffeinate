use anyhow::{Result, bail};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::fs::{File, TryLockError};
use tracing::{debug, error, info};
use zbus::zvariant::{OwnedFd, OwnedValue};
use zbus::{Connection, MatchRule, MessageStream};

const LOCK_FILE_PATH: &str = "/tmp/caffeinate.lock";

const DBUS_LOGIND_SERVICE: &str = "org.freedesktop.login1";
const DBUS_LOGIND_PATH: &str = "/org/freedesktop/login1";
const DBUS_LOGIND_MANAGER_INTERFACE: &str = "org.freedesktop.login1.Manager";

const DBUS_PROPERTIES_SIGNAL_INTERFACE: &str = "org.freedesktop.DBus.Properties";
const DBUS_PROPERTIES_SIGNAL_MEMBER: &str = "PropertiesChanged";

const DBUS_MPRIS_PATH: &str = "/org/mpris/MediaPlayer2";

enum PlaybackStatus {
    Playing,
    StoppedOrPaused,
}

fn init_tracing() {
    let log_level = std::env::var("LOG_LEVEL").unwrap_or("info".to_string());

    tracing_subscriber::fmt()
        .with_env_filter(log_level)
        .with_line_number(true)
        .init();
}

#[tokio::main]
async fn main() -> Result<()> {
    init_tracing();

    let _instance_lock = ensure_single_instance()?;

    let system_conn = Connection::system().await?;
    let sesion_conn = Connection::session().await?;

    let mpris_rule = MatchRule::builder()
        .msg_type(zbus::message::Type::Signal)
        .interface(DBUS_PROPERTIES_SIGNAL_INTERFACE)?
        .member(DBUS_PROPERTIES_SIGNAL_MEMBER)?
        .path(DBUS_MPRIS_PATH)?
        .build();

    let mut mpris_signal_stream =
        MessageStream::for_match_rule(mpris_rule, &sesion_conn, Some(1)).await?;

    let mut inhibit_lock_fd = Option::<OwnedFd>::None;
    let mut is_lock_active = false;

    let (playback_status_tx, mut playback_status_rx) =
        tokio::sync::mpsc::channel::<PlaybackStatus>(1);

    tokio::spawn(async move {
        while let Some(status) = playback_status_rx.recv().await {
            match status {
                PlaybackStatus::Playing => {
                    if is_lock_active {
                        continue;
                    }

                    let fd = match acquire_inhibitor_lock(&system_conn).await {
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
                        // NOTE: Dropping the fd will release the inhibit lock.
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

    while let Some(Ok(msg)) = mpris_signal_stream.next().await {
        // https://specifications.freedesktop.org/mpris/latest/Player_Interface.html
        if let Ok((_, dict, _)) = msg
            .body()
            .deserialize::<(String, HashMap<String, OwnedValue>, Vec<String>)>()
            && let Some(status_value) = dict.get("PlaybackStatus")
            && let Ok(status) = status_value.downcast_ref::<&str>()
        {
            match status {
                "Playing" => {
                    debug!("Active media playback detected (Playing)");
                    if let Err(e) = playback_status_tx.send(PlaybackStatus::Playing).await {
                        error!("Failed to send playback status: {:?}", e);
                    }
                }
                _ => {
                    debug!("Active media playback detected (Paused/Stopped)");
                    if let Err(e) = playback_status_tx
                        .send(PlaybackStatus::StoppedOrPaused)
                        .await
                    {
                        error!("Failed to send playback status: {:?}", e);
                    }
                }
            }
        }
    }

    Ok(())
}

async fn acquire_inhibitor_lock(conn: &zbus::Connection) -> Result<OwnedFd> {
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
        )
        .await?
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
