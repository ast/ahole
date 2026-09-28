//! indicatif wiring: one [`MultiProgress`] per run, and the bar styles.

use std::time::Duration;

use indicatif::{HumanDuration, MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};

/// Bars draw to stderr so that stdout stays pipeable (`ahole send x | tee`).
pub fn multi(enabled: bool) -> MultiProgress {
    let mp = MultiProgress::new();
    mp.set_draw_target(if enabled {
        ProgressDrawTarget::stderr()
    } else {
        ProgressDrawTarget::hidden()
    });
    mp
}

fn styled(template: &str) -> ProgressBar {
    let style = ProgressStyle::with_template(template)
        .expect("progress template is valid")
        .progress_chars("#>-");
    ProgressBar::hidden().with_style(style)
}

/// Counts files, not bytes: the outer bar while importing a directory.
pub fn counter(msg: &'static str) -> ProgressBar {
    styled("{msg:>12} [{bar:30.cyan/blue}] {pos}/{len}").with_message(msg)
}

/// Counts bytes, with a rate and an eta. Used for imports, transfers, exports.
pub fn bytes() -> ProgressBar {
    styled("{msg:>12} [{bar:30.cyan/blue}] {bytes}/{total_bytes} {binary_bytes_per_sec} eta {eta}")
}

/// For the steps that have no measurable size, like dialing a peer.
pub fn spinner(msg: impl Into<String>) -> ProgressBar {
    let pb = ProgressBar::hidden().with_style(
        ProgressStyle::with_template("{spinner:.green} {msg}").expect("spinner template is valid"),
    );
    pb.set_message(msg.into());
    pb.enable_steady_tick(Duration::from_millis(100));
    pb
}

/// `HumanDuration` rounds anything under a second down to "0 seconds", which
/// is most transfers over a LAN. Show those in milliseconds instead.
pub fn took(elapsed: Duration) -> String {
    if elapsed < Duration::from_secs(1) {
        format!("{} ms", elapsed.as_millis())
    } else {
        HumanDuration(elapsed).to_string()
    }
}
