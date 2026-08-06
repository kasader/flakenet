# flakenet

[![Go Reference](https://pkg.go.dev/badge/github.com/kasader/flakenet/flakenet.svg)](https://pkg.go.dev/github.com/kasader/flakenet)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![GitHub Release](https://img.shields.io/github/v/release/kasader/flakenet?include_prereleases)](https://github.com/kasader/flakenet/releases)

Wrap a `net.Conn` or `net.PacketConn` to simulate bandwidth limits, latency,
jitter, and packet loss in Go tests.

`flakenet` works at the connection level, so tests stay hermetic and run
anywhere.

## Usage

```go
lat := &policy.LatencyVar{}
lat.Set(100 * time.Millisecond)

conn := flakenet.NewPacketConn(udpConn, flakenet.PacketProfile{
    Latency: lat,
    Jitter:  policy.RandomJitter(20 * time.Millisecond),
    Loss:    policy.RandomLoss(0.01),
})

// Conditions can change while the connection is live.
lat.Set(500 * time.Millisecond)
```

## Policies

Conditions are values, not constants. The `Var` types are safe for concurrent
use and can be reset on an active connection, so a test can degrade a link
mid-transfer without reconnecting.

- `Bandwidth`: throughput ceiling in bits/sec (`StaticBandwidth`, `BandwidthVar`)
- `Latency`: base propagation delay
- `Jitter`: delivery-time variance (`RandomJitter` is amplitude-based)
- `Loss`: packet drops (`RandomLoss`)
- `Fault`: trigger failures or closure on demand

## Development

Tooling comes from the Nix flake. `direnv allow` (or `nix develop`) gets you a
shell with the pinned Go toolchain, `golangci-lint`, and `govulncheck`.

```sh
make        # list targets
make init   # install the pre-commit hook
make ci     # fmt-check, lint, test, vuln
```

## Relation to `lossy`

Builds on [cevatbarisyilmaz/lossy][1] with two changes: queued delivery instead
of a goroutine per packet, which bounds memory at high throughput, and FIFO
ordering on streams, which fixes byte interleaving under high jitter.

[1]: https://github.com/cevatbarisyilmaz/lossy
