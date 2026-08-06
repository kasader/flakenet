package flakenet

import (
	"sync"
	"time"
)

// wire is a virtual serialization clock for a single link.
//
// Transmissions queue behind one another rather than each paying the
// serialization delay in isolation, which is what makes Bandwidth a
// throughput ceiling instead of just added latency.
type wire struct {
	mu   sync.Mutex
	next time.Time // when the link finishes what it is currently sending
}

// reserve claims the link for size bytes and returns the time at which those
// bytes finish serializing.
func (w *wire) reserve(bandwidth Bandwidth, size, overhead int) time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()

	now := time.Now()
	start := w.next
	// An idle link starts immediately; a busy one queues behind itself.
	if start.Before(now) {
		start = now
	}

	finish := start.Add(transmissionTime(bandwidth, size, overhead))
	w.next = finish
	return finish
}
