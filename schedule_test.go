package flakenet_test

import (
	"net"
	"slices"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/kasader/flakenet"
	"github.com/kasader/flakenet/policy"
)

// These tests run inside a synctest bubble, where time is virtual and advances
// only once every goroutine is durably blocked. That makes delivery schedules
// exact instead of approximate, which is the only way to reach the edge cases
// in PacketConn.linkLoop's heap and timer handling on purpose rather than by
// luck.

// delivery is one datagram as it reached the wire.
type delivery struct {
	payload string
	at      time.Time
}

// memPacketConn is an in-memory net.PacketConn. A real socket would block on
// the network poller, which sits outside the bubble, so datagram scheduling
// can only be observed deterministically through a fake.
type memPacketConn struct {
	mu     sync.Mutex
	got    []delivery
	closed chan struct{}
}

func newMemPacketConn() *memPacketConn {
	return &memPacketConn{closed: make(chan struct{})}
}

func (m *memPacketConn) WriteTo(p []byte, _ net.Addr) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.got = append(m.got, delivery{payload: string(p), at: time.Now()})
	return len(p), nil
}

func (m *memPacketConn) deliveries() []delivery {
	m.mu.Lock()
	defer m.mu.Unlock()
	return slices.Clone(m.got)
}

func (m *memPacketConn) ReadFrom([]byte) (int, net.Addr, error) {
	<-m.closed
	return 0, nil, net.ErrClosed
}

func (m *memPacketConn) Close() error {
	select {
	case <-m.closed:
	default:
		close(m.closed)
	}
	return nil
}

// A v4 UDP addr so header accounting matches a real loopback socket.
func (m *memPacketConn) LocalAddr() net.Addr {
	return &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1}
}

func (m *memPacketConn) SetDeadline(time.Time) error      { return nil }
func (m *memPacketConn) SetReadDeadline(time.Time) error  { return nil }
func (m *memPacketConn) SetWriteDeadline(time.Time) error { return nil }

var testAddr = &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 2}

// payloads returns the delivered payloads in the order they hit the wire.
func payloads(got []delivery) []string {
	out := make([]string, len(got))
	for i, d := range got {
		out[i] = d.payload
	}
	return out
}

// TestSchedule_Latency pins the arrival to the exact configured delay, with no
// tolerance window.
func TestSchedule_Latency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mem := newMemPacketConn()
		sender := flakenet.NewPacketConn(mem, flakenet.PacketProfile{
			Latency: policy.StaticLatency(100 * time.Millisecond),
		})
		defer sender.Close()

		start := time.Now()
		if _, err := sender.WriteTo([]byte("ping"), testAddr); err != nil {
			t.Fatal(err)
		}
		time.Sleep(200 * time.Millisecond)

		got := mem.deliveries()
		if len(got) != 1 {
			t.Fatalf("delivered %d datagrams, want 1", len(got))
		}
		if d := got[0].at.Sub(start); d != 100*time.Millisecond {
			t.Errorf("arrived after %v, want exactly 100ms", d)
		}
	})
}

// TestSchedule_EqualDueTimes covers two datagrams scheduled for the same
// instant, where the heap's Less comparison gives no ordering.
func TestSchedule_EqualDueTimes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mem := newMemPacketConn()
		sender := flakenet.NewPacketConn(mem, flakenet.PacketProfile{
			Latency: policy.StaticLatency(50 * time.Millisecond),
		})
		defer sender.Close()

		// No bandwidth limit and no time passing between the calls, so both
		// land on the same due time.
		start := time.Now()
		for _, p := range []string{"first", "second"} {
			if _, err := sender.WriteTo([]byte(p), testAddr); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(100 * time.Millisecond)

		got := mem.deliveries()
		if len(got) != 2 {
			t.Fatalf("delivered %d datagrams, want 2", len(got))
		}
		for _, d := range got {
			if at := d.at.Sub(start); at != 50*time.Millisecond {
				t.Errorf("%q arrived after %v, want exactly 50ms", d.payload, at)
			}
		}
	})
}

// TestSchedule_LateWriteBecomesHead covers a datagram queued second but due
// first, which has to displace the head of the heap and reset the timer.
func TestSchedule_LateWriteBecomesHead(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mem := newMemPacketConn()
		// +50ms then -50ms against a 100ms base, so the second datagram is due
		// 100ms before the first.
		sender := flakenet.NewPacketConn(mem, flakenet.PacketProfile{
			Latency: policy.StaticLatency(100 * time.Millisecond),
			Jitter:  &forcedJitter{},
		})
		defer sender.Close()

		start := time.Now()
		for _, p := range []string{"slow", "fast"} {
			if _, err := sender.WriteTo([]byte(p), testAddr); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(300 * time.Millisecond)

		got := mem.deliveries()
		if want := []string{"fast", "slow"}; !slices.Equal(payloads(got), want) {
			t.Fatalf("delivery order = %v, want %v", payloads(got), want)
		}
		if at := got[0].at.Sub(start); at != 50*time.Millisecond {
			t.Errorf("fast arrived after %v, want exactly 50ms", at)
		}
		if at := got[1].at.Sub(start); at != 150*time.Millisecond {
			t.Errorf("slow arrived after %v, want exactly 150ms", at)
		}
	})
}

// TestSchedule_TimerAfterEmptyQueue covers the loop going idle. Once the heap
// drains, the timer has already fired, and the next datagram must still be
// scheduled rather than left waiting on a spent timer.
func TestSchedule_TimerAfterEmptyQueue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		mem := newMemPacketConn()
		sender := flakenet.NewPacketConn(mem, flakenet.PacketProfile{
			Latency: policy.StaticLatency(20 * time.Millisecond),
		})
		defer sender.Close()

		start := time.Now()
		for i, p := range []string{"one", "two", "three"} {
			if _, err := sender.WriteTo([]byte(p), testAddr); err != nil {
				t.Fatal(err)
			}
			// Let the queue empty completely between sends.
			time.Sleep(100 * time.Millisecond)

			got := mem.deliveries()
			if len(got) != i+1 {
				t.Fatalf("after %q: delivered %d, want %d", p, len(got), i+1)
			}
			want := time.Duration(i)*100*time.Millisecond + 20*time.Millisecond
			if at := got[i].at.Sub(start); at != want {
				t.Errorf("%q arrived after %v, want exactly %v", p, at, want)
			}
		}
	})
}

// TestSchedule_BandwidthSerializes pins the queuing behavior a shared wire
// clock is supposed to produce: uniform spacing, not a simultaneous burst.
func TestSchedule_BandwidthSerializes(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// 972 bytes plus 28 of IPv4+UDP overhead is 8000 bits, which at
		// 8 Mbit/s is 1ms of serialization per datagram.
		const (
			packets = 8
			size    = 972
			serial  = time.Millisecond
		)

		mem := newMemPacketConn()
		sender := flakenet.NewPacketConn(mem, flakenet.PacketProfile{
			Bandwidth: policy.StaticBandwidth(8_000_000),
		})
		defer sender.Close()

		start := time.Now()
		for range packets {
			if _, err := sender.WriteTo(make([]byte, size), testAddr); err != nil {
				t.Fatal(err)
			}
		}
		time.Sleep(100 * time.Millisecond)

		got := mem.deliveries()
		if len(got) != packets {
			t.Fatalf("delivered %d datagrams, want %d", len(got), packets)
		}
		for i, d := range got {
			want := time.Duration(i+1) * serial
			// A microsecond of slack absorbs the float rounding in
			// transmissionTime; there is no scheduler noise in a bubble.
			if at := d.at.Sub(start); (at - want).Abs() > time.Microsecond {
				t.Errorf("datagram %d arrived after %v, want ~%v", i, at, want)
			}
		}
	})
}
