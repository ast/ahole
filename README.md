# ahole

Send a file or a directory to a friend, peer to peer. A magic-wormhole clone
built on [iroh](https://iroh.computer) and `iroh-blobs`.

```
$ ahole send holiday/
imported directory /home/albin/holiday — 3 file(s), 2.38 MiB in 412 ms

ahole recv blobabscggzb4vjuyrv37tkbdsb7dbxwd7buwa2gogedij4wiofpwxs4uaiy…

sent 2.38 MiB to ecd912629d in 1 second
```

```
$ ahole recv blobabscggzb4vjuyrv37tkbdsb7dbxwd7buwa2gogedij4wiofpwxs4uaiy…
  holiday/notes.txt  19 B
  holiday/raw/IMG_1.jpg  1.91 MiB
  holiday/raw/IMG_2.jpg  488.28 KiB
3 file(s), 2.38 MiB into /home/albin/downloads
received 2.38 MiB
```

Send someone the ticket over any channel you already trust; everything else is
automatic. The two machines can sit behind different NATs — they meet through a
relay and upgrade to a direct connection when the hole punching works out.
`send` stops on its own once someone has taken the data; `--serve` keeps it up
for more.

## Getting it

Grab a binary from [the releases page](../../releases) — macOS (Apple Silicon or
Intel) and Linux (x86_64 or arm64, statically linked):

```
tar xzf ahole-aarch64-apple-darwin.tar.gz
./ahole --version
```

**On macOS**, a binary downloaded from the internet is quarantined, and this one
is not signed by a paid Apple developer account, so Gatekeeper will refuse it
until you say otherwise:

```
xattr -d com.apple.quarantine ahole
```

Or build it yourself with `cargo build --release`, which needs no such ritual.

### With Nix

The repository is a flake, so you can run it without installing anything:

```
nix run github:ast/ahole -- send holiday/
```

To have it on a NixOS machine for good, take the flake as an input and add its
overlay, which puts `ahole` in `pkgs`:

```nix
# flake.nix
inputs.ahole = {
  url = "github:ast/ahole";
  inputs.nixpkgs.follows = "nixpkgs";
};

# in a NixOS module
nixpkgs.overlays = [ inputs.ahole.overlays.default ];
environment.systemPackages = [ pkgs.ahole ];
```

The overlay builds with your system's own nixpkgs, from the committed
`Cargo.lock`. The `follows` line is optional: it keeps a second nixpkgs, which
only the dev shell would use, out of your lock file. The flake's own `packages`
output, and so `nix run`, covers x86_64-linux only.

## Usage

```
ahole send <PATH> [--serve] [-j JOBS] [--store-dir DIR]
ahole recv <TICKET> [--to DIR] [--overwrite]

--relay <URL|n0|none>   home relay          [env: IROH_RELAY]
--relay-token <TOKEN>   relay bearer token  [env: IROH_RELAY_TOKEN]
--no-progress           don't draw progress bars
-v                      per-file hashes, transfer rates
```

Out of the box it uses [Number 0's](https://iroh.computer) public relays, which
need no account and no configuration. `--relay none` stays off relays
altogether, which is all you need on a LAN — the ticket carries direct
addresses.

### Using your own relay

If you run an [iroh relay](https://github.com/n0-computer/iroh), point both ends
at it. Set it once in your shell and forget about it:

```sh
export IROH_RELAY=https://relay.example./
export IROH_RELAY_TOKEN=…        # only if it runs access.shared_token
```

A ticket carries the sender's relay URL, so whoever receives from you needs to
be able to reach that relay too — which for a token-gated relay means having the
token. Neither is compiled into the binary; without them you are simply back on
n0's relays.

If the relay turns you away, `ahole` says so and tells you which endpoint id to
add to its allowlist, rather than hanging.

## Trust model

**The ticket is the whole secret.** Anyone holding it can fetch the data for as
long as the sender is running. Send it over something you already trust.

Names inside a transfer come from whoever made the ticket, so `recv` validates
every path component and refuses anything that would escape the target
directory or overwrite an existing file (`--overwrite` to allow the latter).

Data is verified against its BLAKE3 hash as it streams, so a corrupted or
tampered byte fails the transfer rather than landing on disk.

## How it works

`send` imports the path into a temporary `iroh-blobs` store, hashing every file
with BLAKE3, and ties them together in a *collection* — a list of (name, hash)
pairs — so names and sizes travel with the data. Even a single file gets one.
The store lives in a hidden directory *beside the data*, not in `/tmp`, so
`ImportMode::TryReference` can hardlink instead of copying: 1 GiB imports in
about a second.

The ticket is the collection's root hash plus the sender's address. `recv`
dials it, asks for the size of every child, fetches just the two small blobs
that carry the file names, and prints the listing before pulling a byte of
payload. Then it downloads what it is missing, verifies it and writes it out.

Meanwhile the sender watches the provider's own event stream to drive the
progress bar and to notice when the payload has gone out and the peer has hung
up. That is what lets it exit by itself rather than waiting for a ctrl-c.

Both sides delete their scratch store on the way out — including on ctrl-c and
`SIGTERM`, which is why nothing here calls `exit()` on a path that matters.

Everything on the wire is stock iroh: `iroh_blobs::ALPN`, its `BlobsProtocol`
handler, its collection format and its `BlobTicket`. There is no bespoke
protocol, and tickets interoperate with [sendme](https://github.com/n0-computer/sendme)
in both directions.

## Known limits

- No resume across runs: the receiver's scratch store is deleted when it stops,
  so an interrupted transfer starts over.
- Exports copy out of the store, so a receive briefly needs twice the space.
- Symlinks are skipped, and empty directories are not sent (only files are).

## Development

Enter the directory and [direnv](https://direnv.net) brings up a shell with the
toolchain in it (`direnv allow` once, the first time). Without direnv, `nix
develop` does the same thing. Without nix, a normal `cargo` toolchain is enough
for everything except the static musl build.

```
cargo test        # unit tests, plus end-to-end tests that drive the binary
cargo clippy --all-targets
build-static      # statically linked musl binary, in the nix shell
RUST_LOG=iroh=debug cargo run -- send ./some-dir    # watch the connection form
```

The end-to-end tests run both halves with `--relay none`, so they stay on
loopback and need no network.

Put your own relay settings in `.env.local` (gitignored, loaded by `.envrc`) so
they apply inside the project without ever reaching a commit:

```sh
IROH_RELAY=https://relay.example./
IROH_RELAY_TOKEN=…
```

### Why the flake carries its own Rust

nixpkgs' `rustc` ships a standard library for this machine only, and its
musl-cross `rustc` ships one for musl only. Neither can build a musl binary
alone, because the proc macros in the dependency tree have to be compiled for
the host. So the flake takes one multi-target toolchain from
[rust-overlay](https://github.com/oxalica/rust-overlay), and pairs it with
nixpkgs' musl C compiler — `ring` compiles C, and static linking needs musl's
`libc.a`.

Only the musl target is steered by the shell's environment: plain `cargo build`
and `cargo test` still build for this machine.

## Licence

MIT or Apache-2.0, at your option.
