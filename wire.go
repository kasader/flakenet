package flakenet

import (
	"sync"
	"sync/atomic"
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

// stickyErr records the first failure seen on the asynchronous write path.
//
// Because delivery is deferred, the Write that queued a segment has usually
// returned by the time the underlying socket rejects it. Holding the error
// lets the next Write or Close report it instead of dropping it.
type stickyErr struct {
	first atomic.Pointer[error]
}

func (s *stickyErr) record(err error) {
	if err != nil {
		s.first.CompareAndSwap(nil, &err)
	}
}

func (s *stickyErr) sticky() error {
	if p := s.first.Load(); p != nil {
		return *p
	}
	return nil
}
