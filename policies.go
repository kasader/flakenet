package flakenet

import "time"

// Latency models the delay of a network transmission.
type Latency interface {
	// Duration returns the delay for the current operation.
	Duration() time.Duration
}

// Bandwidth models the capacity of the link.
type Bandwidth interface {
	// Limit returns the allowed throughput in bits per second.
	Limit() uint64
}

// Jitter models the variance in transmission delay.
type Jitter interface {
	// Duration returns the random variance to add to the latency.
	Duration() time.Duration
}

// Loss models the unreliability of a datagram link.
type Loss interface {
	// Drop returns true if the current datagram should be discarded.
	Drop() bool
}

// Fault models the stability of a connection.
type Fault interface {
	// ShouldClose returns true if the connection should be severed abruptly.
	ShouldClose() bool
}
