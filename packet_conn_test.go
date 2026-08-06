package flakenet_test

import (
	"bytes"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/kasader/flakenet"
	"github.com/kasader/flakenet/policy"
)

// Helper to create a real UDP listener on a random localhost port.
func newLocalListener(t *testing.T) net.PacketConn {
	t.Helper()
	c, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	return c
}

// TestPacketConn_Latency verifies that data is actually delayed by the specified duration.
func TestPacketConn_Latency(t *testing.T) {
	// 1. Setup the Receiver (Real UDP)
	receiver := newLocalListener(t)
	defer receiver.Close()

	// 2. Setup the Sender (Wrapped with flakenet)
	senderRaw := newLocalListener(t)
	defer senderRaw.Close()

	// Configure 50ms latency
	const latency = 50 * time.Millisecond
	sender := flakenet.NewPacketConn(senderRaw, flakenet.PacketProfile{
		Latency: policy.StaticLatency(latency),
		// MTU/Loss/Bandwidth defaults apply
	})
	defer sender.Close()

	// 3. The Test: Write a packet
	payload := []byte("hello-world")
	start := time.Now()

	// WriteTo returns immediately (non-blocking) in your implementation
	_, err := sender.WriteTo(payload, receiver.LocalAddr())
	if err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}

	// 4. Verify: Read from the receiver
	buffer := make([]byte, 1024)
	if err := receiver.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}

	n, _, err := receiver.ReadFrom(buffer)
	if err != nil {
		t.Fatalf("ReadFrom failed: %v", err)
	}

	// 5. Assertions
	elapsed := time.Since(start)

	// A. Verify Content
	if !bytes.Equal(buffer[:n], payload) {
		t.Errorf("corrupted payload: got %q, want %q", buffer[:n], payload)
	}

	// B. Verify Latency
	// We allow a small margin (e.g., 5ms) for scheduler noise.
	if elapsed < latency {
		t.Errorf("packet arrived too fast! want >%v, got %v", latency, elapsed)
	}
}

// forcedJitter switches between +50ms and -50ms to force Packet B to overtake Packet A.
type forcedJitter struct {
	count atomic.Int32
}

// Implementation of flakenet.Jitter interface.
func (f *forcedJitter) Duration() time.Duration {
	if f.count.Add(1)%2 == 1 {
		return 50 * time.Millisecond // First packet: high delay
	}
	return -50 * time.Millisecond // Second packet: low delay
}

// TestPacketConn_Reordering verifies that the packet ordering
// can become mixed when the delay between subsequent packets is variable.
func TestPacketConn_Reordering(t *testing.T) {
	receiver := newLocalListener(t)
	defer receiver.Close()

	senderRaw := newLocalListener(t)
	defer senderRaw.Close()

	jitterPolicy := &forcedJitter{}

	sender := flakenet.NewPacketConn(senderRaw, flakenet.PacketProfile{
		Latency: policy.StaticLatency(100 * time.Millisecond),
		Jitter:  jitterPolicy, // Injecting our custom deterministic policy
	})
	defer sender.Close()

	payloadA := []byte("Packet A")
	payloadB := []byte("Packet B")

	// Send Packet A (will have ~150ms total latency)
	_, _ = sender.WriteTo(payloadA, receiver.LocalAddr())
	// Send Packet B (will have ~50ms total latency)
	_, _ = sender.WriteTo(payloadB, receiver.LocalAddr())

	buf := make([]byte, 1024)

	// Because of the forced jitter, Packet B should arrive first
	n, _, _ := receiver.ReadFrom(buf)
	if string(buf[:n]) != "Packet B" {
		t.Errorf("bad ordering: got %q, want %q", buf[:n], "Packet B")
	}

	// Packet A should arrive second
	n, _, _ = receiver.ReadFrom(buf)
	if string(buf[:n]) != "Packet A" {
		t.Errorf("bad ordering: got %q, want %q", buf[:n], "Packet A")
	}
}

// TestPacketConn_Bandwidth verifies that Bandwidth is a throughput ceiling and
// not just added latency: datagrams must queue behind one another on the wire.
func TestPacketConn_Bandwidth(t *testing.T) {
	receiver := newLocalListener(t)
	defer receiver.Close()

	senderRaw := newLocalListener(t)
	defer senderRaw.Close()

	// 1 Mbit/s with 1250-byte payloads is ~10.2ms of serialization each,
	// counting the 28 bytes of IPv4+UDP overhead on the loopback.
	const (
		packets = 8
		size    = 1_250
	)
	sender := flakenet.NewPacketConn(senderRaw, flakenet.PacketProfile{
		Bandwidth: policy.StaticBandwidth(1_000_000),
	})
	defer sender.Close()

	payload := make([]byte, size)
	start := time.Now()
	for range packets {
		if _, err := sender.WriteTo(payload, receiver.LocalAddr()); err != nil {
			t.Fatalf("WriteTo failed: %v", err)
		}
	}

	if err := receiver.SetReadDeadline(time.Now().Add(5 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 2_048)
	for i := range packets {
		if _, _, err := receiver.ReadFrom(buf); err != nil {
			t.Fatalf("packet %d: %v", i, err)
		}
	}
	elapsed := time.Since(start)

	// Serializing all 8 takes ~82ms. Without a shared wire clock every packet
	// pays ~10ms on its own and the whole batch lands in roughly that time.
	if floor := 60 * time.Millisecond; elapsed < floor {
		t.Errorf("batch drained in %v, want >%v; bandwidth is not limiting", elapsed, floor)
	}
}

// TestPacketConn_CloseDrainsQueuedData verifies that a graceful Close delivers
// datagrams the link loop already accepted.
func TestPacketConn_CloseDrainsQueuedData(t *testing.T) {
	receiver := newLocalListener(t)
	defer receiver.Close()

	senderRaw := newLocalListener(t)
	defer senderRaw.Close()

	sender := flakenet.NewPacketConn(senderRaw, flakenet.PacketProfile{
		Latency: policy.StaticLatency(500 * time.Millisecond),
	})

	payload := []byte("queued")
	if _, err := sender.WriteTo(payload, receiver.LocalAddr()); err != nil {
		t.Fatalf("WriteTo failed: %v", err)
	}
	if err := sender.Close(); err != nil {
		t.Fatalf("Close() = %v, want nil", err)
	}

	if err := receiver.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1_024)
	n, _, err := receiver.ReadFrom(buf)
	if err != nil {
		t.Fatalf("queued datagram was discarded on Close: %v", err)
	}
	if !bytes.Equal(buf[:n], payload) {
		t.Errorf("received %q, want %q", buf[:n], payload)
	}
}
