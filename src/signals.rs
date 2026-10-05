//! Stopping cleanly.
//!
//! Both halves keep their blob store in a scratch directory that is removed by
//! `Drop`, which only runs if we unwind instead of being killed. So every
//! "stop now" signal has to arrive as a value we can return on, not as a
//! default-terminate.

/// Resolves on ctrl-c, or on SIGTERM where there is one.
///
/// The listening starts with the call, not with the first poll: from then on a
/// signal is held for whoever awaits the future rather than killing the
/// process. Call this before creating anything that needs cleaning up.
#[cfg(unix)]
pub fn interrupted() -> impl Future<Output = ()> {
    use tokio::signal::unix::{Signal, SignalKind, signal};

    async fn received(signal: std::io::Result<Signal>) {
        match signal {
            Ok(mut signal) => {
                signal.recv().await;
            }
            // We could not listen for this one, so it keeps its default action.
            Err(_) => std::future::pending().await,
        }
    }

    let int = signal(SignalKind::interrupt());
    let term = signal(SignalKind::terminate());
    async move {
        tokio::select! {
            () = received(int) => {}
            () = received(term) => {}
        }
    }
}

/// Resolves on ctrl-c. Unlike the unix version, this one only starts listening
/// when it is first polled.
#[cfg(not(unix))]
pub async fn interrupted() {
    let _ = tokio::signal::ctrl_c().await;
}
