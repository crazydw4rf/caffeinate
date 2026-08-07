use std::collections::HashMap;
use std::fs::{File, TryLockError};

use anyhow::Result;
use futures_util::StreamExt;
use tracing::{debug, error, info};
use zbus::zvariant::{OwnedFd, OwnedValue};
use zbus::{Connection, MatchRule, MessageStream};

const LOCK_FILE: &str = "/tmp/caffeinate.lock";

#[tokio::main]
async fn main() -> Result<()> {
    let log_level = std::env::var("LOG_LEVEL").unwrap_or("info".to_string());

    tracing_subscriber::fmt()
        .with_env_filter(log_level)
        .with_line_number(true)
        .init();

    let _file_lock = match check_single_instance() {
        Some(f) => f,
        None => return Ok(()),
    };

    let system_conn = Connection::system().await?;
    let sesion_conn = Connection::session().await?;

    let mpris_rule = MatchRule::builder()
        .msg_type(zbus::message::Type::Signal)
        .interface("org.freedesktop.DBus.Properties")?
        .member("PropertiesChanged")?
        .path("/org/mpris/MediaPlayer2")?
        .build();

    let mut mpris_signal_stream =
        MessageStream::for_match_rule(mpris_rule, &sesion_conn, Some(1)).await?;

    let mut inhibit_fd = Option::<OwnedFd>::None;
    let mut is_lock_active = false;

    let (status_tx, mut status_rx) = tokio::sync::mpsc::channel::<Option<()>>(1);

    tokio::spawn(async move {
        while let Some(status) = status_rx.recv().await {
            match status {
                Some(_) => {
                    if is_lock_active {
                        continue;
                    }

                    inhibit_fd = match inhibit_block(&system_conn).await {
                        Ok(fd) => {
                            info!("Inhibit lock acquired");
                            Some(fd)
                        }
                        Err(e) => {
                            error!("Failed to acquire idle inhibit lock: {:?}", e);
                            inhibit_fd = None;
                            continue;
                        }
                    };
                    is_lock_active = true;
                }
                None => {
                    if is_lock_active {
                        // NOTE: Dropping the fd will release the inhibit lock.
                        if inhibit_fd.take().is_some() {
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
                    if let Err(e) = status_tx.send(Some(())).await {
                        error!("Failed to send playback status: {:?}", e);
                    }
                }
                _ => {
                    debug!("Active media playback detected (Paused/Stopped)");
                    if let Err(e) = status_tx.send(None).await {
                        error!("Failed to send playback status: {:?}", e);
                    }
                }
            }
        }
    }

    Ok(())
}

async fn inhibit_block(conn: &zbus::Connection) -> Result<OwnedFd> {
    // https://www.freedesktop.org/software/systemd/man/latest/org.freedesktop.login1.html
    let fd = conn
        .call_method(
            Some("org.freedesktop.login1"),
            "/org/freedesktop/login1",
            Some("org.freedesktop.login1.Manager"),
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

fn check_single_instance() -> Option<File> {
    debug!("opening lock file '{}'", LOCK_FILE);

    let file = match std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(LOCK_FILE)
    {
        Ok(f) => f,
        Err(err) => {
            error!(error = ?err, "could not open lock file '{}'", LOCK_FILE);

            return None;
        }
    };

    if let Err(lock_error) = file.try_lock() {
        if let TryLockError::Error(err) = lock_error {
            error!(
                error = ?err,
                "error occurred while trying to lock file '{}'", LOCK_FILE
            );

            return None;
        }

        error!("another instance is running");
        return None;
    }

    Some(file)
}
