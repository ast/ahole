//! ahole — send a file or a directory to a friend, peer to peer.
//!
//!   ahole send <PATH>     # prints a ticket, serves until someone takes it
//!   ahole recv <TICKET>   # fetches it into the current directory
//!
//! The two can be on different machines behind different NATs: iroh finds a
//! path via a relay and upgrades to a direct connection when it can. Number 0's
//! public relays are the default and need no setup, so a ticket is the only
//! thing that has to travel out of band. `--relay` and `--relay-token` (or
//! `IROH_RELAY` and `IROH_RELAY_TOKEN`) point it at a private relay instead.

mod progress;
mod recv;
mod relay;
mod send;
mod signals;
mod store;

use std::path::PathBuf;

use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use iroh::RelayMode;
use iroh_blobs::ticket::BlobTicket;

#[derive(Debug, Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(flatten)]
    common: CommonArgs,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Args)]
struct CommonArgs {
    /// Home relay: a URL, `n0` for Number 0's public relays, or `none` for
    /// direct connections only.
    #[arg(
        long,
        global = true,
        env = "IROH_RELAY",
        default_value = relay::DEFAULT_RELAY,
        value_parser = relay::parse_relay,
    )]
    relay: RelayMode,

    /// Bearer token for a relay running `access.shared_token`. Empty by
    /// default; ignored by relays that take no token, such as n0's.
    #[arg(
        long,
        global = true,
        env = "IROH_RELAY_TOKEN",
        default_value = relay::DEFAULT_RELAY_TOKEN,
        hide_env_values = true,
    )]
    relay_token: String,

    /// Don't draw progress bars.
    #[arg(long, global = true)]
    no_progress: bool,

    /// Print more: per-file hashes, transfer rates.
    #[arg(short, long, global = true, action = clap::ArgAction::Count)]
    verbose: u8,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Offer a file or directory and print a ticket for it.
    Send(SendArgs),
    /// Fetch whatever a ticket points at.
    Recv(RecvArgs),
}

#[derive(Debug, Args)]
struct SendArgs {
    /// The file or directory to send.
    path: PathBuf,

    /// Keep serving after the first transfer, until ctrl-c.
    #[arg(long)]
    serve: bool,

    /// How many files to import at once. Defaults to the number of cores.
    #[arg(short, long)]
    jobs: Option<usize>,

    /// Where to keep the temporary blob store. Defaults to a hidden directory
    /// beside the data, so importing can hardlink instead of copying.
    #[arg(long)]
    store_dir: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct RecvArgs {
    /// The ticket printed by `ahole send`.
    ticket: BlobTicket,

    /// Where to write what we receive.
    #[arg(long, default_value = ".")]
    to: PathBuf,

    /// Replace files that are already there.
    #[arg(long)]
    overwrite: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Send(args) => send::run(args, &cli.common).await,
        Command::Recv(args) => recv::run(args, &cli.common).await,
    }
}
