//! Binding an endpoint, against n0's relays or a private one.
//!
//! Lifted from `irohexp`, which worked out the awkward parts: attaching a
//! bearer token to a custom relay, and turning "the relay silently refuses us"
//! into an error message that says what to do about it.

use std::time::Duration;

use anyhow::{Context, Result};
use iroh::{Endpoint, RelayMap, RelayMode, RelayUrl, Watcher, endpoint::presets};

/// Number 0 runs public relays and they need no account, so that is what this
/// falls back to: a fresh build works for anyone with nothing to configure.
///
/// Point `--relay` (or `IROH_RELAY`) at your own relay to use that instead. If
/// it runs `access.shared_token`, `--relay-token` / `IROH_RELAY_TOKEN` carries
/// the token — keeping both out of the binary, where a shared secret has no
/// business being.
pub const DEFAULT_RELAY: &str = "n0";

/// No token by default: n0's relays take none, and a private relay's token
/// belongs in the environment rather than in a published binary.
pub const DEFAULT_RELAY_TOKEN: &str = "";

/// Turns the `--relay` argument into a [`RelayMode`].
pub fn parse_relay(s: &str) -> Result<RelayMode> {
    Ok(match s {
        "n0" | "default" => RelayMode::Default,
        "none" | "disabled" => RelayMode::Disabled,
        url => RelayMode::custom([url.parse::<RelayUrl>().context("invalid relay URL")?]),
    })
}

/// Attaches a bearer token to every relay in the mode, for relays running with
/// `access.shared_token`. Sent as `Authorization: Bearer <token>` on the
/// websocket upgrade. n0's public relays take no token, so it is dropped there.
fn with_token(relay: RelayMode, token: &str) -> RelayMode {
    match relay {
        RelayMode::Custom(map) if !token.is_empty() => {
            RelayMode::Custom(RelayMap::with_auth_token(map, token.to_string()))
        }
        relay => relay,
    }
}

/// `presets::N0` bundles n0's relays *and* n0's pkarr/DNS address lookup.
/// Applying `relay_mode` afterwards swaps out only the relays, keeping the
/// lookup service — so peers can still find us by bare endpoint id. What gets
/// published there is a signed record saying "reach me at <relay>", so n0
/// learns our relay's URL but carries none of our traffic.
pub async fn bind(relay: RelayMode, token: &str, alpns: Vec<Vec<u8>>) -> Result<Endpoint> {
    Endpoint::builder(presets::N0)
        .relay_mode(with_token(relay, token))
        .alpns(alpns)
        .bind()
        .await
        .context("binding endpoint")
}

/// Waits until the endpoint has something worth putting in a ticket.
///
/// With a relay, that means being reachable through it. With `--relay none`
/// there is nothing to wait for but the socket: a ticket can then only carry
/// direct addresses, and those exist as soon as we are bound. Waiting for
/// `online()` in that case would wait forever.
pub async fn wait_addressable(endpoint: &Endpoint, relay: &RelayMode) -> Result<()> {
    if !matches!(relay, RelayMode::Disabled) {
        return wait_online(endpoint).await;
    }
    let mut addrs = endpoint.watch_addr();
    for _ in 0..50 {
        if addrs.get().ip_addrs().next().is_some() {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    anyhow::bail!("bound, but no local addresses to put in a ticket")
}

/// `Endpoint::online()` waits for a working home relay, and a relay that keeps
/// rejecting us means it waits forever. Time it out and report why the relay
/// said no, rather than hanging with no output.
pub async fn wait_online(endpoint: &Endpoint) -> Result<()> {
    if tokio::time::timeout(Duration::from_secs(10), endpoint.online())
        .await
        .is_ok()
    {
        return Ok(());
    }

    // The relay actor retries with backoff, so the status only carries a
    // failure between attempts. Sample a few times to catch one.
    let mut relays = endpoint.home_relay_status();
    for _ in 0..20 {
        for relay in relays.get() {
            if let Some(reason) = relay.auth_denied_reason() {
                anyhow::bail!(
                    "relay {} refused us: {reason}.\n\
                     It runs access control. Either pass --relay-token (or set \
                     IROH_RELAY_TOKEN) if it uses access.shared_token, or have the \
                     operator add this endpoint to access.allowlist:\n  {}",
                    relay.url(),
                    endpoint.id(),
                );
            }
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    anyhow::bail!("no home relay after 10s: is the relay reachable?")
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A stand-in for somebody's private relay.
    const EXAMPLE_RELAY: &str = "https://relay.example./";

    #[test]
    fn parse_relay_understands_the_three_shapes() {
        assert!(matches!(parse_relay("n0").unwrap(), RelayMode::Default));
        assert!(matches!(parse_relay("none").unwrap(), RelayMode::Disabled));
        let RelayMode::Custom(map) = parse_relay(EXAMPLE_RELAY).unwrap() else {
            panic!("a URL should give a custom relay map");
        };
        assert_eq!(map.urls::<Vec<_>>().len(), 1);
        assert!(parse_relay("not a url").is_err());
    }

    #[test]
    fn a_token_only_attaches_to_a_custom_relay() {
        // n0's relays take no token, so passing one must not change the mode.
        assert!(matches!(
            with_token(RelayMode::Default, "tok"),
            RelayMode::Default
        ));
        assert!(matches!(
            with_token(parse_relay(EXAMPLE_RELAY).unwrap(), ""),
            RelayMode::Custom(_)
        ));
    }
}
