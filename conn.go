package flakenet

import (
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// StreamProfile extends the link with stream-specific behaviors.
type StreamProfile struct {
	// MTU (Maximum Transmission Unit) is the largest packet size allowed.
	// This value includes L3/L4 headers.
	//
	// Defaults to [EthernetDefaultMTU] if 0.
	MTU uint

	Latency   Latency
	Jitter    Jitter
	Bandwidth Bandwidth
	Fault     Fault
}

type writeReq struct {
	data []byte
	due  time.Time
}

// Conn wraps an existing [net.Conn] to emulate network conditions for
// stream-oriented protocols.
//
// To prevent stream corruption, Conn uses an internal FIFO queue (writeCh)
// to ensure that data is written to the underlying socket in the exact
// order it was received from the application, even in the presence of
// latency and jitter.
type Conn struct {
	net.Conn
	headerSize    int
	mss           int // maximum segment size used for bandwidth calculations
	p             StreamProfile
	writeCh       chan writeReq // writeCh acts as a FIFO queue to prevent stream reordering.
	writeDeadline atomic.Value
	mu            sync.Mutex
	nextWireTime  time.Time // Tracks when the next segment can be physically sent
	stopOnce      sync.Once
	stopCh        chan struct{}
}

// NewConn wraps an existing net.Conn to emulate network conditions for stream-oriented
// protocols like TCP. It ensures that data order is strictly preserved even when
// jitter or latency is applied.
func NewConn(c net.Conn, p StreamProfile) net.Conn {
	headerSize := getHeaderSize(c.LocalAddr())
	mtu := p.MTU
	if mtu == 0 {
		mtu = EthernetDefaultMTU
	}
	// Enforce minimum mss (prevent infinite loop).
	mss := max(1, int(mtu)-headerSize)

	nc := &Conn{
		Conn:       c,
		headerSize: headerSize,
		mss:        mss,
		p:          p,

		// Buffered to allow bursting.
		// TODO: Should the WriteCh length be configurable?
		writeCh: make(chan writeReq, 1024),
		stopCh:  make(chan struct{}),
	}
	nc.writeDeadline.Store(time.Time{})
	go nc.linkLoop()
	return nc
}

// Close implements net.Conn.
func (c *Conn) Close() error {
	c.stopOnce.Do(func() {
		if c.stopCh != nil {
			// TODO: if this channel is closed before we
			// return from c.Conn.Close() we could fail
			// to return an error in Write()
			close(c.stopCh)
		}
	})
	return c.Conn.Close()
}

// SetDeadline implements net.Conn.
func (c *Conn) SetDeadline(t time.Time) error {
	c.writeDeadline.Store(t)
	return c.Conn.SetDeadline(t)
}

// SetWriteDeadline implements net.Conn.
func (c *Conn) SetWriteDeadline(t time.Time) error {
	c.writeDeadline.Store(t)
	return c.Conn.SetWriteDeadline(t)
}

// Write implements net.Conn.
func (c *Conn) Write(b []byte) (n int, err error) {
	if c.isWriteDeadline() {
		return 0, os.ErrDeadlineExceeded
	}

	sent := 0
	for sent < len(b) {
		chunk := b[sent:min(sent+c.mss, len(b))]
		finishTime := c.reserveWire(len(chunk))
		req := writeReq{
			data: make([]byte, len(chunk)),
			due:  finishTime.Add(delayTime(c.p.Latency, c.p.Jitter)),
		}
		copy(req.data, chunk)

		select {
		case <-c.stopCh:
			// simulation is stopped; flush out the remaining data immediately
			nRaw, errRaw := c.Conn.Write(b[sent:])
			return sent + nRaw, errRaw
		case c.writeCh <- req:
			sent += len(chunk)
		}
	}
	return sent, nil
}

var _ net.Conn = (*Conn)(nil)

// Handles writes in strict order.
func (c *Conn) linkLoop() {
	for {
		select {
		case <-c.stopCh:
			return
		case req := <-c.writeCh:
			// Perform fault injection before writing.
			if c.p.Fault != nil && c.p.Fault.ShouldClose() {
				c.Close()
			}
			// Wait until due time.
			wait := time.Until(req.due)
			if wait > 0 {
				time.Sleep(wait)
			}
			// Write; and because we pull from the channel we can
			// assume that our packets must be written in order.
			c.Conn.Write(req.data)
		}
	}
}

func (c *Conn) isWriteDeadline() bool {
	wdl := c.writeDeadline.Load().(time.Time)
	return !wdl.IsZero() && wdl.Before(time.Now())
}

// reserveWire calculates when a chunk of data will finish serializing on the wire.
// It updates the virtual clock (nextWireTime) in a thread-safe manner.
func (c *Conn) reserveWire(chunkSize int) time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	startTime := c.nextWireTime

	// If the wire is idle, we start immediately.
	// If the wire is busy, we queue behind the current transmission.
	if startTime.Before(now) {
		startTime = now
	}

	delay := transmissionTime(c.p.Bandwidth, chunkSize, c.headerSize)
	finishTime := startTime.Add(delay)

	c.nextWireTime = finishTime
	return finishTime
}
