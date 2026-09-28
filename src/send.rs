//! The sending half: import a path into a blob store, serve it, hand out a
//! ticket, and stop once the other side has it all.

use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use futures_buffered::BufferedStreamExt;
use indicatif::{HumanBytes, MultiProgress, ProgressBar};
use iroh::protocol::Router;
use iroh_blobs::{
    BlobFormat, BlobsProtocol,
    api::{
        Store, TempTag,
        blobs::{AddPathOptions, AddProgressItem, ImportMode},
    },
    format::collection::Collection,
    provider::events::{
        ConnectMode, EventMask, EventSender, ProviderMessage, RequestMode, RequestUpdate,
    },
    store::fs::FsStore,
    ticket::BlobTicket,
};
use n0_future::{FuturesUnordered, StreamExt, task::AbortOnDropHandle};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};
use tracing::debug;
use walkdir::WalkDir;

use crate::{CommonArgs, SendArgs, progress, relay, signals, store};

/// Index 0 of a get request is the hash sequence itself and index 1 the
/// collection metadata; the files start at 2. Used for the progress bar only —
/// what counts as sent comes from the provider's own stats.
const FIRST_PAYLOAD_INDEX: u64 = 2;

#[derive(Debug, Error)]
pub enum ImportError {
    #[error("{name} could not be imported")]
    Failed {
        name: String,
        #[source]
        source: std::io::Error,
    },
    #[error("the import of {name} ended without producing a tag")]
    NoTag { name: String },
    #[error("{0} contains no regular files")]
    Empty(PathBuf),
    #[error("file names must be valid utf-8, and {0} is not")]
    NotUtf8(PathBuf),
}

/// What a finished transfer tells us, for the summary line.
#[derive(Debug)]
struct Transfer {
    peer: String,
    bytes: u64,
    elapsed: Duration,
}

pub async fn run(args: SendArgs, common: &CommonArgs) -> Result<()> {
    let path = args
        .path
        .canonicalize()
        .with_context(|| format!("cannot read {}", args.path.display()))?;
    // Names in the collection are relative to the parent, so a file arrives
    // under its own name and a directory keeps its top-level folder.
    let root = path
        .parent()
        .context("cannot send the filesystem root")?
        .to_path_buf();

    let scratch = store::ScratchDir::create(
        args.store_dir
            .clone()
            .unwrap_or_else(|| store::send_dir(&root)),
    )?;
    let mp = progress::multi(!common.no_progress);
    let db = FsStore::load(scratch.path())
        .await
        .context("opening the blob store")?;

    // The provider reports who connects and how much of what it sends; that
    // stream is both the progress bar and the "we are done here" signal.
    let (event_tx, event_rx) = mpsc::channel(32);
    let blobs = BlobsProtocol::new(
        &db,
        Some(EventSender::new(
            event_tx,
            EventMask {
                connected: ConnectMode::Notify,
                get: RequestMode::NotifyLog,
                ..EventMask::DEFAULT
            },
        )),
    );

    let started = Instant::now();
    let (tag, collection, payload_size) =
        import(&path, &root, blobs.store(), &mp, args.jobs).await?;
    let import_time = started.elapsed();

    let endpoint = relay::bind(
        common.relay.clone(),
        &common.relay_token,
        vec![iroh_blobs::ALPN.to_vec()],
    )
    .await?;
    let router = Router::builder(endpoint)
        .accept(iroh_blobs::ALPN, blobs.clone())
        .spawn();

    // The ticket carries the addresses we have at the time we build it, so it
    // has to wait until we know how we are reachable.
    if let Err(err) = relay::wait_addressable(router.endpoint(), &common.relay).await {
        // Shutting the router down also shuts down the store: the blobs
        // protocol handler owns that on our behalf.
        let _ = router.shutdown().await;
        return Err(err);
    }
    let ticket = BlobTicket::new(router.endpoint().addr(), tag.hash(), BlobFormat::HashSeq);

    let kind = if path.is_dir() { "directory" } else { "file" };
    println!(
        "imported {kind} {} — {} file(s), {} in {}",
        path.display(),
        collection.len(),
        HumanBytes(payload_size),
        progress::took(import_time),
    );
    if common.verbose > 0 {
        for (name, hash) in collection.iter() {
            println!("    {} {name}", hash.fmt_short());
        }
    }
    println!();
    println!("ahole recv {ticket}");
    println!();

    let (done_tx, done_rx) = oneshot::channel();
    let watcher = AbortOnDropHandle::new(tokio::spawn(watch_provider(
        mp.clone(),
        event_rx,
        payload_size,
        done_tx,
    )));

    let transfer = tokio::select! {
        done = done_rx, if !args.serve => done.ok(),
        () = signals::interrupted() => None,
    };

    // Dropping the tag unprotects the data; everything else here is about
    // letting the peer finish cleanly and clearing the bars before we print.
    drop(tag);
    drop(blobs);
    // `Router::shutdown` runs the blobs protocol handler's shutdown, which
    // closes the store for us — closing it again here would be an error.
    let _ = tokio::time::timeout(Duration::from_secs(2), router.shutdown()).await;
    drop(router);
    drop(db);
    let _ = watcher.await;

    match transfer {
        Some(t) => println!(
            "sent {} to {} in {}",
            HumanBytes(t.bytes),
            t.peer,
            progress::took(t.elapsed)
        ),
        None => println!("stopped"),
    }
    Ok(())
}

/// Walks `path`, imports every regular file, and ties them together in a
/// [`Collection`] so that names and sizes travel with the data.
async fn import(
    path: &Path,
    root: &Path,
    db: &Store,
    mp: &MultiProgress,
    jobs: Option<usize>,
) -> Result<(TempTag, Collection, u64)> {
    let files = entries(path, root)?;
    let overall = mp.add(progress::counter("importing"));
    overall.set_length(files.len() as u64);

    let parallelism = jobs.unwrap_or_else(num_cpus::get).max(1);
    let mut imported = n0_future::stream::iter(files)
        .map(|(name, file)| {
            let db = db.clone();
            let mp = mp.clone();
            let overall = overall.clone();
            async move {
                let pb = mp.add(progress::bytes());
                pb.set_message(name.clone());
                let mut stream = db
                    .add_path_with_opts(AddPathOptions {
                        path: file,
                        // Hardlink or reflink instead of copying, when the
                        // store and the payload share a filesystem.
                        mode: ImportMode::TryReference,
                        format: BlobFormat::Raw,
                    })
                    .stream()
                    .await;
                let mut size = 0;
                let tag = loop {
                    let Some(item) = stream.next().await else {
                        return Err(ImportError::NoTag { name }.into());
                    };
                    match item {
                        AddProgressItem::Size(bytes) => {
                            size = bytes;
                            pb.set_length(bytes);
                        }
                        AddProgressItem::CopyProgress(offset) => pb.set_position(offset),
                        AddProgressItem::CopyDone => pb.set_position(0),
                        AddProgressItem::OutboardProgress(offset) => pb.set_position(offset),
                        AddProgressItem::Error(source) => {
                            pb.finish_and_clear();
                            mp.remove(&pb);
                            return Err(ImportError::Failed { name, source }.into());
                        }
                        AddProgressItem::Done(tag) => {
                            pb.finish_and_clear();
                            mp.remove(&pb);
                            break tag;
                        }
                    }
                };
                overall.inc(1);
                anyhow::Ok((name, tag, size))
            }
        })
        .buffered_unordered(parallelism)
        .collect::<Vec<_>>()
        .await
        .into_iter()
        .collect::<Result<Vec<_>>>()?;
    overall.finish_and_clear();
    mp.remove(&overall);

    imported.sort_by(|(a, _, _), (b, _, _)| a.cmp(b));
    let payload_size = imported.iter().map(|(_, _, size)| *size).sum();
    // The per-file tags must outlive the collection's own import: they are what
    // keeps the data from being collected before the collection protects it.
    let (collection, tags) = imported
        .into_iter()
        .map(|(name, tag, _)| ((name, tag.hash()), tag))
        .unzip::<_, _, Collection, Vec<_>>();
    let tag = collection.clone().store(db).await?;
    drop(tags);
    Ok((tag, collection, payload_size))
}

/// The (name, path) pairs to import: every regular file under `path`, named
/// relative to `root`. Symlinks are skipped rather than followed.
fn entries(path: &Path, root: &Path) -> Result<Vec<(String, PathBuf)>> {
    let mut files = Vec::new();
    for entry in WalkDir::new(path) {
        let entry = entry.context("walking the directory")?;
        if !entry.file_type().is_file() {
            continue;
        }
        let file = entry.into_path();
        let relative = file
            .strip_prefix(root)
            .expect("walkdir stays under the root it was given");
        let name = relative
            .to_str()
            .ok_or_else(|| ImportError::NotUtf8(relative.to_path_buf()))?
            // Collection names are '/'-separated regardless of the platform.
            .replace(std::path::MAIN_SEPARATOR, "/");
        files.push((name, file));
    }
    if files.is_empty() {
        return Err(ImportError::Empty(path.to_path_buf()).into());
    }
    Ok(files)
}

/// Turns provider events into a progress bar, and decides when the transfer is
/// over: all payload bytes sent, and the peer has hung up (or two seconds have
/// passed, in case it hangs around).
async fn watch_provider(
    mp: MultiProgress,
    mut events: mpsc::Receiver<ProviderMessage>,
    payload_size: u64,
    done: oneshot::Sender<Transfer>,
) {
    // `payload_bytes_sent` counts the hash sequence and the collection metadata
    // as payload too, so the total runs a couple of hundred bytes ahead of the
    // files themselves. That only matters for a transfer smaller than that, and
    // the peer has those bytes by then anyway.
    let mut peers: HashMap<u64, String> = HashMap::new();
    let mut requests = FuturesUnordered::new();
    let sent = Arc::new(AtomicU64::new(0));
    let mut bar: Option<ProgressBar> = None;
    let mut done = Some(done);
    let mut first_byte: Option<Instant> = None;
    let mut peer = String::from("?");
    let mut grace: Option<Pin<Box<tokio::time::Sleep>>> = None;

    loop {
        let complete = sent.load(Ordering::Relaxed) >= payload_size;
        tokio::select! {
            biased;
            event = events.recv() => {
                let Some(event) = event else { break };
                match event {
                    ProviderMessage::ClientConnectedNotify(msg) => {
                        let who = msg
                            .endpoint_id
                            .map(|id| id.fmt_short().to_string())
                            .unwrap_or_else(|| "?".to_string());
                        peers.insert(msg.connection_id, who);
                    }
                    ProviderMessage::ConnectionClosed(msg) => {
                        peers.remove(&msg.connection_id);
                        // The peer got everything and went away: that is the
                        // cleanest possible "we are finished" signal.
                        if complete && let Some(tx) = done.take() {
                            let _ = tx.send(Transfer {
                                peer: peer.clone(),
                                bytes: sent.load(Ordering::Relaxed).min(payload_size),
                                elapsed: first_byte.map(|t| t.elapsed()).unwrap_or_default(),
                            });
                        }
                    }
                    ProviderMessage::GetRequestReceivedNotify(msg) => {
                        peer = peers
                            .get(&msg.connection_id)
                            .cloned()
                            .unwrap_or_else(|| "?".to_string());
                        first_byte.get_or_insert_with(Instant::now);
                        let pb = bar
                            .get_or_insert_with(|| {
                                let pb = mp.add(progress::bytes());
                                pb.set_length(payload_size);
                                pb.set_message("sending");
                                pb
                            })
                            .clone();
                        requests.push(track_request(pb, sent.clone(), msg.rx));
                    }
                    _ => {}
                }
            }
            Some(()) = requests.next(), if !requests.is_empty() => {
                if sent.load(Ordering::Relaxed) >= payload_size && grace.is_none() {
                    // Everything is on the wire. Give the peer a moment to
                    // close the connection itself before we pull it down.
                    grace = Some(Box::pin(tokio::time::sleep(Duration::from_secs(2))));
                }
            }
            () = async { grace.as_mut().expect("guarded by is_some").await }, if grace.is_some() => {
                grace = None;
                if let Some(tx) = done.take() {
                    let _ = tx.send(Transfer {
                        peer: peer.clone(),
                        bytes: sent.load(Ordering::Relaxed).min(payload_size),
                        elapsed: first_byte.map(|t| t.elapsed()).unwrap_or_default(),
                    });
                }
            }
        }
    }

    if let Some(pb) = bar {
        pb.finish_and_clear();
        mp.remove(&pb);
    }
}

/// Follows one get request: drives the bar while it runs, and adds what really
/// went out to `sent` when it ends.
///
/// Only the provider's own stats can say how much was sent. A blob's `size` is
/// the size of the whole blob even when the peer asked for a single chunk of
/// it — a receiver starts by probing the last chunk of every file to learn the
/// sizes, and crediting those probes with whole files would have us believe a
/// transfer finished before it began.
async fn track_request(
    pb: ProgressBar,
    sent: Arc<AtomicU64>,
    mut updates: irpc::channel::mpsc::Receiver<RequestUpdate>,
) {
    // Both are estimates for the bar alone, reset when the request ends.
    let mut shown = 0;
    let mut in_flight = 0;
    while let Ok(Some(update)) = updates.recv().await {
        match update {
            RequestUpdate::Started(started) => {
                debug!(index = started.index, size = started.size, "sending blob");
                shown += in_flight;
                in_flight = if started.index >= FIRST_PAYLOAD_INDEX {
                    started.size
                } else {
                    0
                };
                pb.set_position(sent.load(Ordering::Relaxed) + shown);
            }
            RequestUpdate::Progress(p) => {
                if in_flight > 0 {
                    pb.set_position(
                        sent.load(Ordering::Relaxed) + shown + p.end_offset.min(in_flight),
                    );
                }
            }
            RequestUpdate::Completed(done) => {
                sent.fetch_add(done.stats.payload_bytes_sent, Ordering::Relaxed);
                pb.set_position(sent.load(Ordering::Relaxed));
            }
            RequestUpdate::Aborted(done) => {
                sent.fetch_add(done.stats.payload_bytes_sent, Ordering::Relaxed);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_single_file_is_named_after_itself() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("holiday.zip");
        std::fs::write(&file, b"data").unwrap();

        let files = entries(&file, dir.path()).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].0, "holiday.zip");
    }

    #[test]
    fn a_directory_keeps_its_top_level_folder() {
        let dir = tempfile::tempdir().unwrap();
        let holiday = dir.path().join("holiday");
        std::fs::create_dir_all(holiday.join("raw")).unwrap();
        std::fs::write(holiday.join("notes.txt"), b"hi").unwrap();
        std::fs::write(holiday.join("raw/IMG_1.jpg"), b"jpeg").unwrap();

        let mut names: Vec<_> = entries(&holiday, dir.path())
            .unwrap()
            .into_iter()
            .map(|(name, _)| name)
            .collect();
        names.sort();
        assert_eq!(names, ["holiday/notes.txt", "holiday/raw/IMG_1.jpg"]);
    }

    #[test]
    fn an_empty_directory_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let empty = dir.path().join("empty");
        std::fs::create_dir(&empty).unwrap();
        assert!(entries(&empty, dir.path()).is_err());
    }
}
