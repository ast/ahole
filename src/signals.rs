//! Stopping cleanly.
//!
//! Both halves keep their blob store in a scratch directory that is removed by
//! `Drop`, which only runs if we unwind instead of being killed. So every
//! "stop now" signal has to arrive as a value we can return on, not as a
//! default-terminate.

/// Resolves on ctrl-c, or on SIGTERM where there is one.
#[cfg(unix)]
pub async fn interrupted() {
    use tokio::signal::unix::{SignalKind, signal};

    let Ok(mut term) = signal(SignalKind::terminate()) else {
        let _ = tokio::signal::ctrl_c().await;
        return;
    };
    tokio::select! {
        _ = tokio::signal::ctrl_c() => {}
        _ = term.recv() => {}
    }
}

#[cfg(not(unix))]
pub async fn interrupted() {
    let _ = tokio::signal::ctrl_c().await;
}
