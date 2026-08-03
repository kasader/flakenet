# netem

[![Go Reference](https://pkg.go.dev/badge/github.com/kasader/netem/netem.svg)](https://pkg.go.dev/github.com/kasader/netem)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![GitHub Release](https://img.shields.io/github/v/release/kasader/netem?include_prereleases)](https://github.com/kasader/netem/releases)

Wrap a `net.Conn` or `net.PacketConn` to simulate bandwidth limits, latency,
jitter, and packet loss in Go tests. No root, no `tc`, no external processes.

OS-level tools like `tc-netem` shape traffic for the whole machine. `netem`
works at the connection level, so tests stay hermetic and run anywhere.

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

## Conn vs PacketConn

`Conn` is stream-oriented. Delayed bytes queue in FIFO order, so jitter shows up
as head-of-line blocking rather than reordered or interleaved bytes.

`PacketConn` is datagram-oriented. Packets reorder naturally, and a later
datagram can overtake an earlier one under sufficient jitter.

## Relation to `lossy`

Builds on [cevatbarisyilmaz/lossy][1] with two changes: queued delivery instead
of a goroutine per packet, which bounds memory at high throughput, and FIFO
ordering on streams, which fixes byte interleaving under high jitter.

[1]: https://github.com/cevatbarisyilmaz/lossy
