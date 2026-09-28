//! End-to-end tests that drive the real binary.
//!
//! Both sides run with `--relay none`, so nothing leaves the machine: the
//! ticket carries direct addresses and the two endpoints dial each other over
//! loopback. That also exercises the path where there is no relay to wait for.

use std::{
    io::{BufRead, BufReader},
    path::Path,
    process::{Child, Command, Stdio},
    sync::mpsc,
    thread,
    time::Duration,
};

fn ahole() -> Command {
    let mut cmd = Command::new(env!("CARGO_BIN_EXE_ahole"));
    cmd.args(["--relay", "none", "--no-progress"]);
    cmd
}

/// Starts a sender and reads the ticket off its stdout. The sender keeps
/// running until someone fetches from it.
///
/// A thread goes on draining stdout until the sender exits. It has to: closing
/// the pipe early would kill the sender with a broken pipe the next time it
/// printed anything.
fn start_send(path: &Path) -> (Child, String) {
    let mut child = ahole()
        .arg("send")
        .arg(path)
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawning the sender");
    let stdout = child.stdout.take().expect("piped stdout");
    let (tx, rx) = mpsc::channel();
    thread::spawn(move || {
        let mut tx = Some(tx);
        for line in BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            if let Some(ticket) = line.strip_prefix("ahole recv ")
                && let Some(tx) = tx.take()
            {
                let _ = tx.send(ticket.to_string());
            }
        }
    });
    match rx.recv_timeout(Duration::from_secs(30)) {
        Ok(ticket) => (child, ticket),
        Err(_) => {
            let _ = child.kill();
            panic!("the sender printed no ticket within 30s");
        }
    }
}

fn recv(ticket: &str, to: &Path) {
    let out = ahole()
        .args(["recv", ticket])
        .arg("--to")
        .arg(to)
        .output()
        .expect("running the receiver");
    assert!(
        out.status.success(),
        "receive failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn a_directory_makes_the_round_trip() {
    let src = tempfile::tempdir().unwrap();
    let holiday = src.path().join("holiday");
    std::fs::create_dir_all(holiday.join("raw")).unwrap();
    std::fs::write(holiday.join("notes.txt"), b"we went to the sea").unwrap();
    // Big enough to need more than one chunk, so the transfer is not trivial.
    let photo = vec![7u8; 300 * 1024];
    std::fs::write(holiday.join("raw/IMG_1.jpg"), &photo).unwrap();

    let (mut sender, ticket) = start_send(&holiday);
    let dest = tempfile::tempdir().unwrap();
    recv(&ticket, dest.path());

    assert_eq!(
        std::fs::read(dest.path().join("holiday/notes.txt")).unwrap(),
        b"we went to the sea"
    );
    assert_eq!(
        std::fs::read(dest.path().join("holiday/raw/IMG_1.jpg")).unwrap(),
        photo
    );

    // The whole point of the provider-event bookkeeping: the sender notices it
    // is finished and stops on its own.
    let status = sender.wait().expect("waiting for the sender");
    assert!(status.success(), "the sender should exit cleanly");

    // Neither side may leave its scratch store behind.
    assert!(!has_scratch(&holiday), "sender left a scratch directory");
    assert!(
        !has_scratch(dest.path()),
        "receiver left a scratch directory"
    );
}

#[test]
fn a_single_file_keeps_its_name() {
    let src = tempfile::tempdir().unwrap();
    let file = src.path().join("report.pdf");
    std::fs::write(&file, b"%PDF-1.4 not really").unwrap();

    let (mut sender, ticket) = start_send(&file);
    let dest = tempfile::tempdir().unwrap();
    recv(&ticket, dest.path());

    assert_eq!(
        std::fs::read(dest.path().join("report.pdf")).unwrap(),
        b"%PDF-1.4 not really"
    );
    assert!(sender.wait().unwrap().success());
}

#[test]
fn an_existing_file_is_not_clobbered_without_asking() {
    let src = tempfile::tempdir().unwrap();
    let file = src.path().join("notes.txt");
    // Bigger than one chunk on purpose: a receiver's size probe asks for the
    // last chunk of every file, and for a file that fits in one chunk that
    // probe hands over the whole thing — which would let the sender count this
    // transfer as finished and quit before the second attempt below.
    let contents = vec![b'n'; 64 * 1024];
    std::fs::write(&file, &contents).unwrap();

    let (mut sender, ticket) = start_send(&file);
    let dest = tempfile::tempdir().unwrap();
    std::fs::write(dest.path().join("notes.txt"), b"precious").unwrap();

    let out = ahole()
        .args(["recv", &ticket])
        .arg("--to")
        .arg(dest.path())
        .output()
        .unwrap();
    assert!(!out.status.success(), "receiving should have refused");
    assert_eq!(
        std::fs::read(dest.path().join("notes.txt")).unwrap(),
        b"precious",
        "the existing file must be untouched"
    );

    // ... and it goes through once we say so.
    let out = ahole()
        .args(["recv", &ticket, "--overwrite"])
        .arg("--to")
        .arg(dest.path())
        .output()
        .unwrap();
    assert!(
        out.status.success(),
        "overwriting receive failed: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(
        std::fs::read(dest.path().join("notes.txt")).unwrap(),
        contents
    );

    assert!(sender.wait().unwrap().success());
}

/// True if the directory holds one of our `.ahole-*` scratch stores.
fn has_scratch(dir: &Path) -> bool {
    let parent = if dir.is_dir() {
        dir
    } else {
        dir.parent().unwrap()
    };
    std::fs::read_dir(parent)
        .unwrap()
        .filter_map(Result::ok)
        .any(|e| e.file_name().to_string_lossy().starts_with(".ahole-"))
}
