package flakenet

import (
	"container/heap"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// packetReq holds the data and the scheduled arrival time.
type packetReq struct {
	data []byte
	addr net.Addr
	due  time.Time
}

// packetHeap is a Min-Heap sorted by 'due' time.
type packetHeap []packetReq

func (h packetHeap) Len() int           { return len(h) }
func (h packetHeap) Less(i, j int) bool { return h[i].due.Before(h[j].due) }
func (h packetHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *packetHeap) Push(x any) {
	*h = append(*h, x.(packetReq))
}

func (h *packetHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// PacketProfile extends the link with datagram-specific behaviors.
type PacketProfile struct {
	// MTU (Maximum Transmission Unit) is the largest packet size allowed.
	// This value includes L3/L4 headers.
	//
	// Defaults to [EthernetDefaultMTU] if 0.
	MTU uint

	Latency   Latency
	Jitter    Jitter
	Bandwidth Bandwidth
	Loss      Loss
}

// PacketConn wraps an existing [net.PacketConn] to emulate network conditions
// for packet-oriented protocols.
//
// Unlike Conn, PacketConn allows for natural packet reordering if jitter
// configurations cause a later packet to be scheduled for delivery earlier
// than a previous one.
type PacketConn struct {
	net.PacketConn
	wire
	stickyErr
	headerSize    int
	mss           int // largest payload the link MTU admits
	p             PacketProfile
	writeCh       chan packetReq
	writeDeadline atomic.Value
	stopOnce      sync.Once
	stopCh        chan struct{}
	doneCh        chan struct{} // closed once the link loop has finished draining
}

// NewPacketConn wraps an existing net.PacketConn to emulate network conditions
// for packet-oriented protocols like UDP.
func NewPacketConn(c net.PacketConn, p PacketProfile) net.PacketConn {
	headerSize := getHeaderSize(c.LocalAddr())
	mtu := p.MTU
	if mtu == 0 {
		mtu = EthernetDefaultMTU
	}
	// Enforce minimum mss.
	mss := max(1, int(mtu)-headerSize)

	nc := &PacketConn{
		PacketConn: c,
		headerSize: headerSize,
		mss:        mss,
		p:          p,

		// TODO: Should the WriteCh length be configurable?
		writeCh: make(chan packetReq, 1024),
		stopCh:  make(chan struct{}),
		doneCh:  make(chan struct{}),
	}
	nc.writeDeadline.Store(time.Time{})
	go nc.linkLoop()
	return nc
}

// Close implements net.PacketConn.
//
// Close is graceful: datagrams the link loop has already accepted are
// delivered in due order before the underlying conn is closed, ignoring
// their remaining wait.
func (c *PacketConn) Close() error {
	c.signalStop()
	<-c.doneCh

	err := c.PacketConn.Close()
	if err == nil {
		// Surface a deferred write failure the caller never had a chance to see.
		err = c.sticky()
	}
	return err
}

// SetDeadline implements net.PacketConn.
func (c *PacketConn) SetDeadline(t time.Time) error {
	c.writeDeadline.Store(t)
	return c.PacketConn.SetDeadline(t)
}

// SetWriteDeadline implements net.PacketConn.
func (c *PacketConn) SetWriteDeadline(t time.Time) error {
	c.writeDeadline.Store(t)
	return c.PacketConn.SetWriteDeadline(t)
}

// WriteTo implements net.PacketConn.
func (c *PacketConn) WriteTo(p []byte, addr net.Addr) (n int, err error) {
	if err := c.sticky(); err != nil {
		return 0, err
	}
	if c.isWriteDeadline() {
		return 0, os.ErrDeadlineExceeded
	}
	// A datagram is all-or-nothing, so an oversized one never reaches the wire
	// and must not consume link time.
	if len(p) > c.mss {
		return 0, ErrMessageTooLong
	}
	// Reserve the link first so concurrent datagrams queue behind one another
	// instead of each paying the serialization delay independently.
	finish := c.reserve(c.p.Bandwidth, len(p), c.headerSize)
	due := finish.Add(delayTime(c.p.Latency, c.p.Jitter))

	req := packetReq{
		data: make([]byte, len(p)),
		addr: addr,
		due:  due,
	}
	copy(req.data, p)

	// A select picks at random among ready cases, so a down link has to be
	// checked on its own. Otherwise a write could still land in the queue while
	// stopCh is closed, purely because the buffer had room.
	select {
	case <-c.stopCh:
		return 0, net.ErrClosed
	default:
	}

	expired, stop := deadlineTimer(c.writeDeadline.Load().(time.Time))
	defer stop()

	select {
	case <-c.stopCh:
		return 0, net.ErrClosed
	case <-expired:
		return 0, os.ErrDeadlineExceeded
	case c.writeCh <- req:
		return len(p), nil
	}
}

var _ net.PacketConn = (*PacketConn)(nil)

func (c *PacketConn) isWriteDeadline() bool {
	wdl := c.writeDeadline.Load().(time.Time)
	return !wdl.IsZero() && wdl.Before(time.Now())
}

// signalStop stops the link loop without waiting for it to finish, so it is
// safe to call from the loop itself.
func (c *PacketConn) signalStop() {
	c.stopOnce.Do(func() { close(c.stopCh) })
}

// send applies the loss policy and delivers the datagram.
func (c *PacketConn) send(packet packetReq) {
	if c.p.Loss != nil && c.p.Loss.Drop() {
		return
	}
	_, err := c.PacketConn.WriteTo(packet.data, packet.addr)
	c.record(err)
}

// drain delivers everything still queued or scheduled, in due order, ignoring
// the remaining wait. Only the link loop consumes writeCh and it has stopped
// by the time we get here, so the queued count cannot shrink underneath us.
func (c *PacketConn) drain(pq *packetHeap) {
	for n := len(c.writeCh); n > 0; n-- {
		heap.Push(pq, <-c.writeCh)
	}
	for pq.Len() > 0 {
		c.send(heap.Pop(pq).(packetReq))
	}
}

// Handles writes in due order (scheduled).
func (c *PacketConn) linkLoop() {
	defer close(c.doneCh)

	pq := &packetHeap{}
	heap.Init(pq)

	// Create a timer but stop it immediately so it doesn't fire yet.
	timer := time.NewTimer(0)
	timer.Stop()

	// Ensure we clean up the timer when the loop exits.
	defer timer.Stop()
	for {
		select {
		case <-c.stopCh:
			c.drain(pq)
			return

		case req := <-c.writeCh:
			heap.Push(pq, req)
			// Reset timer if this is the new head of the queue
			if req.due.Equal((*pq)[0].due) {
				timer.Reset(time.Until(req.due))
			}

		case <-timer.C:
			if pq.Len() == 0 {
				continue
			}
			now := time.Now()
			for pq.Len() > 0 {
				next := (*pq)[0]
				if next.due.After(now) {
					// Next packet is in the future.
					// Reset timer for the remainder and go back to sleep.
					timer.Reset(next.due.Sub(now))
					break
				}
				c.send(heap.Pop(pq).(packetReq))
			}
		}
	}
}
