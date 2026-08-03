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
	headerSize    int
	mss           int
	p             PacketProfile
	writeCh       chan packetReq
	writeDeadline atomic.Value
	stopOnce      sync.Once
	stopCh        chan struct{}
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
		headerSize: getHeaderSize(c.LocalAddr()),
		mss:        mss,
		p:          p,

		// TODO: Should the WriteCh length be configurable?
		writeCh: make(chan packetReq, 1024),
		stopCh:  make(chan struct{}),
	}
	nc.writeDeadline.Store(time.Time{})
	go nc.linkLoop()
	return nc
}

// Close implements net.PacketConn.
func (c *PacketConn) Close() error {
	c.stopOnce.Do(func() {
		if c.stopCh != nil {
			close(c.stopCh)
		}
	})
	return c.PacketConn.Close()
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
	if c.isWriteDeadline() {
		return 0, os.ErrDeadlineExceeded
	}
	serializationDelay := transmissionTime(c.p.Bandwidth, len(p), c.headerSize)
	propagationDelay := delayTime(c.p.Latency, c.p.Jitter)

	totalDelay := serializationDelay + propagationDelay
	due := time.Now().Add(totalDelay)

	req := packetReq{
		data: make([]byte, len(p)),
		addr: addr,
		due:  due,
	}
	copy(req.data, p)

	select {
	case <-c.stopCh:
		return c.PacketConn.WriteTo(p, addr)
	case c.writeCh <- req:
		return len(p), nil
	}
}

var _ net.PacketConn = (*PacketConn)(nil)

func (c *PacketConn) isWriteDeadline() bool {
	wdl := c.writeDeadline.Load().(time.Time)
	return !wdl.IsZero() && wdl.Before(time.Now())
}

// Handles writes in due order (scheduled).
func (c *PacketConn) linkLoop() {
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
				packet := heap.Pop(pq).(packetReq)

				// Apply loss policy.
				drop := false
				if c.p.Loss != nil {
					drop = c.p.Loss.Drop()
				}
				if !drop {
					c.PacketConn.WriteTo(packet.data, packet.addr)
				}
			}
		}
	}
}
