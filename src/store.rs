//! Scratch directories for the blob store.
//!
//! Both sides need a blob store on disk for the duration of one transfer and
//! never after it. The store outlives neither side's process, so the directory
//! is created up front and removed by [`ScratchDir`]'s `Drop` — which is why
//! nothing in this program calls `std::process::exit` on a path that matters.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use iroh_blobs::Hash;
use rand::RngExt;

/// A directory that is removed when this value is dropped.
#[derive(Debug)]
pub struct ScratchDir {
    path: PathBuf,
}

impl ScratchDir {
    pub fn create(path: PathBuf) -> Result<Self> {
        anyhow::ensure!(
            !path.exists(),
            "scratch directory {} already exists — a previous run may still be going",
            path.display()
        );
        std::fs::create_dir_all(&path)
            .with_context(|| format!("creating scratch directory {}", path.display()))?;
        Ok(Self { path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for ScratchDir {
    fn drop(&mut self) {
        match std::fs::remove_dir_all(&self.path) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
            Err(err) => eprintln!("warning: could not remove {}: {err}", self.path.display()),
        }
    }
}

/// Where the sender's store goes: *beside the file being sent*, not in `/tmp`.
///
/// `ImportMode::TryReference` hardlinks or reflinks the payload into the store
/// when both live on one filesystem, and falls back to a full copy when they
/// don't. For a multi-gigabyte file that is the difference between instant and
/// a coffee break — and `/tmp` is frequently a different (and much smaller)
/// filesystem than the one holding your data.
pub fn send_dir(beside: &Path) -> PathBuf {
    beside.join(format!(".ahole-send-{:016x}", rand::rng().random::<u64>()))
}

/// The receiver's store, inside the target directory for the same reason.
pub fn recv_dir(target: &Path, hash: &Hash) -> PathBuf {
    target.join(format!(".ahole-recv-{}", hash.to_hex()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scratch_dir_cleans_up_after_itself() {
        let parent = tempfile::tempdir().unwrap();
        let path = send_dir(parent.path());
        {
            let scratch = ScratchDir::create(path.clone()).unwrap();
            assert!(scratch.path().is_dir());
            std::fs::write(scratch.path().join("blob"), b"data").unwrap();
        }
        assert!(
            !path.exists(),
            "the directory should be gone with its contents"
        );
    }

    #[test]
    fn scratch_dir_refuses_an_existing_directory() {
        let parent = tempfile::tempdir().unwrap();
        let path = parent.path().join("taken");
        std::fs::create_dir(&path).unwrap();
        assert!(ScratchDir::create(path).is_err());
    }
}
