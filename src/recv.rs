//! The receiving half: dial the sender named in the ticket, learn what is on
//! offer, pull it, and write it out under the names the sender chose.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use indicatif::{HumanBytes, MultiProgress};
use iroh_blobs::{
    api::{
        Store,
        blobs::{ExportMode, ExportOptions, ExportProgressItem},
        remote::GetProgressItem,
    },
    format::collection::Collection,
    get::request::get_hash_seq_and_sizes,
    protocol::{ChunkRanges, GetRequest},
    store::fs::FsStore,
};
use n0_future::StreamExt;
use thiserror::Error;

use crate::{CommonArgs, RecvArgs, progress, relay, signals, store};

/// Refuse to buffer a hash sequence larger than this while reading the file
/// list. 32 MiB is a lot of filenames and nowhere near enough to hurt.
const MAX_HASH_SEQ: u64 = 1024 * 1024 * 32;

/// Names arrive from whoever made the ticket, so they are checked, not trusted.
#[derive(Debug, Error)]
pub enum NameError {
    #[error("the sender offered an unsafe path: {0:?}")]
    Unsafe(String),
    #[error("{0} already exists — pass --overwrite to replace it")]
    Exists(PathBuf),
}

pub async fn run(args: RecvArgs, common: &CommonArgs) -> Result<()> {
    // Listening before the scratch store exists; see `signals::interrupted`.
    let interrupted = signals::interrupted();

    std::fs::create_dir_all(&args.to).with_context(|| format!("creating {}", args.to.display()))?;
    let target = args
        .to
        .canonicalize()
        .with_context(|| format!("cannot read {}", args.to.display()))?;

    let scratch = store::ScratchDir::create(store::recv_dir(&target, &args.ticket.hash()))?;
    let mp = progress::multi(!common.no_progress);
    let db = FsStore::load(scratch.path())
        .await
        .context("opening the blob store")?;
    let endpoint = relay::bind(common.relay.clone(), &common.relay_token, vec![]).await?;

    // Interrupting has to come back here as a value: the scratch store is
    // removed on the way out, and a killed process removes nothing.
    let result = tokio::select! {
        result = fetch(&db, &endpoint, &args, &target, &mp, common.verbose) => result,
        () = interrupted => Err(anyhow::anyhow!("interrupted")),
    };

    endpoint.close().await;
    db.shutdown().await.context("closing the blob store")?;
    drop(scratch);
    result
}

async fn fetch(
    db: &FsStore,
    endpoint: &iroh::Endpoint,
    args: &RecvArgs,
    target: &Path,
    mp: &MultiProgress,
    verbose: u8,
) -> Result<()> {
    let ticket = &args.ticket;
    let content = ticket.hash_and_format();

    let spinner = mp.add(progress::spinner("connecting"));
    let conn = endpoint
        .connect(ticket.addr().clone(), iroh_blobs::ALPN)
        .await
        .context("dialing the sender")?;

    // Sizes first: this asks only for the last chunk of each child, which is
    // enough to learn every size and to prove the peer really has the data.
    spinner.set_message("reading the file list");
    let (_hash_seq, sizes) = get_hash_seq_and_sizes(&conn, &content.hash, MAX_HASH_SEQ, None)
        .await
        .context("asking for the file list")?;

    // Then the two small blobs that carry the names: the hash sequence itself
    // (index 0) and the collection metadata (child 0). Fetching just these is
    // what lets us print the listing before pulling any payload.
    let listing = GetRequest::builder()
        .root(ChunkRanges::all())
        .child(0, ChunkRanges::all())
        .build(content.hash);
    db.remote()
        .execute_get(conn.clone(), listing)
        .complete()
        .await
        .context("fetching the file list")?;
    let collection = Collection::load(content.hash, db.as_ref())
        .await
        .context("reading the file list")?;
    spinner.finish_and_clear();
    mp.remove(&spinner);

    // `sizes` covers the children of the hash sequence, not the sequence
    // itself: sizes[0] is the collection metadata and the files follow.
    let file_sizes: Vec<u64> = sizes.iter().copied().skip(1).collect();
    let payload_size: u64 = file_sizes.iter().sum();
    for ((name, _), size) in collection.iter().zip(&file_sizes) {
        println!("  {name}  {}", HumanBytes(*size));
    }
    println!(
        "{} file(s), {} into {}",
        collection.len(),
        HumanBytes(payload_size),
        target.display()
    );

    // Every destination is worked out before a byte is downloaded, so a name
    // clash or a hostile name fails now rather than half a transfer later.
    let targets = plan(&collection, target, args.overwrite)?;

    let local = db.remote().local(content).await?;
    let total: u64 = sizes.iter().sum();
    let pb = mp.add(progress::bytes());
    pb.set_length(total);
    pb.set_message("receiving");
    let base = local.local_bytes();
    let mut stream = db.remote().execute_get(conn, local.missing()).stream();
    let mut stats = None;
    while let Some(item) = stream.next().await {
        match item {
            GetProgressItem::Progress(offset) => pb.set_position(base + offset),
            GetProgressItem::Done(done) => {
                stats = Some(done);
                break;
            }
            GetProgressItem::Error(cause) => {
                pb.finish_and_clear();
                return Err(anyhow::Error::new(cause).context("downloading"));
            }
        }
    }
    pb.finish_and_clear();
    mp.remove(&pb);

    export(db, &collection, &targets, mp).await?;

    match stats {
        Some(stats) if verbose > 0 => println!(
            "received {} in {} ({}/s)",
            HumanBytes(payload_size),
            progress::took(stats.elapsed),
            HumanBytes((stats.total_bytes_read() as f64 / stats.elapsed.as_secs_f64()) as u64),
        ),
        _ => println!("received {}", HumanBytes(payload_size)),
    }
    Ok(())
}

/// Writes every blob out of the store and into its final place.
async fn export(
    db: &Store,
    collection: &Collection,
    targets: &[PathBuf],
    mp: &MultiProgress,
) -> Result<()> {
    let overall = mp.add(progress::counter("writing"));
    overall.set_length(collection.len() as u64);
    for ((name, hash), target) in collection.iter().zip(targets) {
        if let Some(parent) = target.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        let pb = mp.add(progress::bytes());
        pb.set_message(name.clone());
        let mut stream = db
            .export_with_opts(ExportOptions {
                hash: *hash,
                target: target.clone(),
                // Copy, not reference: the store is deleted when we are done.
                mode: ExportMode::Copy,
            })
            .stream()
            .await;
        while let Some(item) = stream.next().await {
            match item {
                ExportProgressItem::Size(size) => pb.set_length(size),
                ExportProgressItem::CopyProgress(offset) => pb.set_position(offset),
                ExportProgressItem::Done => break,
                ExportProgressItem::Error(cause) => {
                    pb.finish_and_clear();
                    return Err(
                        anyhow::Error::new(cause).context(format!("writing {}", target.display()))
                    );
                }
            }
        }
        pb.finish_and_clear();
        mp.remove(&pb);
        overall.inc(1);
    }
    overall.finish_and_clear();
    mp.remove(&overall);
    Ok(())
}

/// Maps collection names onto local paths, refusing anything that would escape
/// the target directory or clobber an existing file.
fn plan(collection: &Collection, target: &Path, overwrite: bool) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::with_capacity(collection.len());
    for (name, _) in collection.iter() {
        let path = export_path(target, name)?;
        if path.exists() && !overwrite {
            return Err(NameError::Exists(path).into());
        }
        paths.push(path);
    }
    Ok(paths)
}

/// Joins a collection name onto `target`, one validated component at a time.
///
/// The name comes from a peer, so `..`, absolute paths and empty components are
/// all rejected rather than normalised: the result always stays under `target`.
fn export_path(target: &Path, name: &str) -> Result<PathBuf, NameError> {
    let unsafe_name = || NameError::Unsafe(name.to_string());
    let mut path = target.to_path_buf();
    if name.is_empty() {
        return Err(unsafe_name());
    }
    for component in name.split('/') {
        if component.is_empty()
            || component == "."
            || component == ".."
            || component.contains('\\')
            || component.contains('\0')
            || Path::new(component).components().count() != 1
        {
            return Err(unsafe_name());
        }
        path.push(component);
    }
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_names_land_under_the_target() {
        let target = Path::new("/tmp/downloads");
        assert_eq!(
            export_path(target, "holiday/raw/IMG_1.jpg").unwrap(),
            Path::new("/tmp/downloads/holiday/raw/IMG_1.jpg")
        );
        assert_eq!(
            export_path(target, "notes.txt").unwrap(),
            Path::new("/tmp/downloads/notes.txt")
        );
    }

    #[test]
    fn names_that_would_escape_are_refused() {
        let target = Path::new("/tmp/downloads");
        for bad in [
            "../etc/passwd",
            "holiday/../../etc/passwd",
            "/etc/passwd",
            "",
            "holiday//notes.txt",
            "./notes.txt",
            "holiday/",
        ] {
            assert!(
                export_path(target, bad).is_err(),
                "{bad:?} should have been refused"
            );
        }
    }
}
