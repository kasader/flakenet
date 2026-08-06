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
	wire
	stickyErr
	headerSize    int
	mss           int // maximum segment size used for bandwidth calculations
	p             StreamProfile
	writeCh       chan writeReq // writeCh acts as a FIFO queue to prevent stream reordering.
	writeDeadline atomic.Value
	stopOnce      sync.Once
	stopCh        chan struct{}
	doneCh        chan struct{} // closed once the link loop has finished draining
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
		doneCh:  make(chan struct{}),
	}
	nc.writeDeadline.Store(time.Time{})
	go nc.linkLoop()
	return nc
}

// Close implements net.Conn.
//
// Close is graceful: segments the link loop has already accepted are flushed
// before the underlying conn is closed, ignoring their remaining schedule. A
// link severed by Fault discards them instead.
func (c *Conn) Close() error {
	c.signalStop()
	<-c.doneCh

	err := c.Conn.Close()
	if err == nil {
		// Surface a deferred write failure the caller never had a chance to see.
		err = c.sticky()
	}
	return err
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
	if err := c.sticky(); err != nil {
		return 0, err
	}
	if c.isWriteDeadline() {
		return 0, os.ErrDeadlineExceeded
	}

	sent := 0
	for sent < len(b) {
		chunk := b[sent:min(sent+c.mss, len(b))]
		finishTime := c.reserve(c.p.Bandwidth, len(chunk), c.headerSize)
		req := writeReq{
			data: make([]byte, len(chunk)),
			due:  finishTime.Add(delayTime(c.p.Latency, c.p.Jitter)),
		}
		copy(req.data, chunk)

		select {
		case <-c.stopCh:
			// The link is down. Writing straight to the socket here would
			// bypass the queue and reorder the stream, so report instead.
			return sent, net.ErrClosed
		case c.writeCh <- req:
			sent += len(chunk)
		}
	}
	return sent, nil
}

var _ net.Conn = (*Conn)(nil)

// signalStop stops the link loop without waiting for it to finish, so it is
// safe to call from the loop itself.
func (c *Conn) signalStop() {
	c.stopOnce.Do(func() { close(c.stopCh) })
}

// drain flushes segments already accepted by the link loop, ignoring their
// remaining schedule. Only the link loop consumes writeCh and it has stopped
// by the time we get here, so the queued count cannot shrink underneath us.
func (c *Conn) drain() {
	for n := len(c.writeCh); n > 0; n-- {
		req := <-c.writeCh
		_, err := c.Conn.Write(req.data)
		c.record(err)
	}
}

// Handles writes in strict order.
func (c *Conn) linkLoop() {
	defer close(c.doneCh)

	for {
		select {
		case <-c.stopCh:
			c.drain()
			return
		case req := <-c.writeCh:
			// Perform fault injection before writing.
			if c.p.Fault != nil && c.p.Fault.ShouldClose() {
				// A severed link drops what it was carrying, so stop without
				// draining and leave the closed socket alone.
				c.signalStop()
				_ = c.Conn.Close()
				return
			}
			// Wait until due time.
			wait := time.Until(req.due)
			if wait > 0 {
				time.Sleep(wait)
			}
			// Write; and because we pull from the channel we can
			// assume that our packets must be written in order.
			_, err := c.Conn.Write(req.data)
			c.record(err)
		}
	}
}

func (c *Conn) isWriteDeadline() bool {
	wdl := c.writeDeadline.Load().(time.Time)
	return !wdl.IsZero() && wdl.Before(time.Now())
}
