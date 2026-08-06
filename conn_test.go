package flakenet_test

import (
	"bytes"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	flakenet "github.com/kasader/flakenet"
	"github.com/kasader/flakenet/policy"
)

// TestConn_Latency verifies that data is actually delayed by the specified duration.
func TestConn_Latency(t *testing.T) {
	// 1. Create a pipe (simulates a TCP connection)
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	// 2. Wrap the client with 100ms latency
	latency := 100 * time.Millisecond
	emulatedClient := flakenet.NewConn(client, flakenet.StreamProfile{
		Latency: policy.StaticLatency(latency),
	})

	// 3. Start a receiver in the background
	doneCh := make(chan time.Time)
	go func() {
		buf := make([]byte, 1024)
		// This Read will block until the data arrives "over the wire"
		_, _ = server.Read(buf)
		doneCh <- time.Now()
	}()

	// 4. Send data
	start := time.Now()
	if _, err := emulatedClient.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}

	// 5. Measure arrival time
	arrival := <-doneCh
	elapsed := arrival.Sub(start)

	// 6. Verify (Allowing 10ms for scheduler overhead)
	if elapsed < latency {
		t.Errorf("Too fast! Expected >%v, got %v", latency, elapsed)
	}
	if elapsed > latency+(50*time.Millisecond) {
		t.Errorf("Too slow! Expected ~%v, got %v", latency, elapsed)
	}
}

// TestConn_Ordering verifies that TCP streams remain ordered
// even if Jitter creates "faster" packets that try to jump the queue.
func TestConn_Ordering(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	// Use a LARGE jitter (±50ms) on a 100ms base.
	// This ensures that sometimes Packet B wants to arrive before Packet A.
	// Our Link Actor implementation must prevent this reordering.
	emulatedConn := flakenet.NewConn(c1, flakenet.StreamProfile{
		Latency: policy.StaticLatency(100 * time.Millisecond),
		Jitter:  policy.RandomJitter(50 * time.Millisecond),
	})

	// Reader
	readErr := make(chan error, 1)
	readData := make(chan string, 1)
	go func() {
		// Read 2 chunks (expecting "Hello" then "World")
		buf := make([]byte, 10)
		// ReadFull ensures we get all bytes
		if _, err := io.ReadFull(c2, buf); err != nil {
			readErr <- err
			return
		}
		readData <- string(buf)
	}()

	// Writer: Send two distinct packets back-to-back
	go func() {
		emulatedConn.Write([]byte("Hello"))
		emulatedConn.Write([]byte("World"))
	}()

	// Verify
	select {
	case err := <-readErr:
		t.Fatal(err)
	case data := <-readData:
		// If TCP worked, we must get "HelloWorld".
		// If reordered, we might get "WorldHello" or mixed bytes.
		if data != "HelloWorld" {
			t.Errorf("Stream corrupted! Expected 'HelloWorld', got '%s'", data)
		}
	case <-time.After(1 * time.Second):
		t.Fatal("Test timed out")
	}
}

// TestConn_Dynamic verifies we can change latency on the fly.
func TestConn_Dynamic(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	// 1. Setup a LatencyVar (Thread-Safe Variable)
	latVar := &policy.LatencyVar{}
	latVar.Set(10 * time.Millisecond) // Start Fast

	emulatedConn := flakenet.NewConn(c1, flakenet.StreamProfile{
		Latency: latVar,
	})

	// Helper to measure a round trip
	measure := func() time.Duration {
		start := time.Now()
		go emulatedConn.Write([]byte("x"))

		buf := make([]byte, 1)
		c2.Read(buf)
		return time.Since(start)
	}

	// 2. Measure Fast
	if d := measure(); d > 50*time.Millisecond {
		t.Errorf("Expected fast (<50ms), got %v", d)
	}

	// 3. Change configuration ON THE FLY
	latVar.Set(200 * time.Millisecond)

	// 4. Measure Slow
	if d := measure(); d < 200*time.Millisecond {
		t.Errorf("Expected slow (>200ms) after update, got %v", d)
	}
}

// TestConn_Segmentation verifies that a payload larger than the MSS is split
// into segments rather than retransmitted whole once per segment.
func TestConn_Segmentation(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	// Header overhead on a pipe is worst-case IPv6, so the MSS is 100-40.
	emulatedConn := flakenet.NewConn(c1, flakenet.StreamProfile{MTU: 100})

	payload := make([]byte, 1_000)
	for i := range payload {
		payload[i] = byte(i)
	}

	got := make(chan []byte, 1)
	go func() {
		buf := make([]byte, len(payload))
		if _, err := io.ReadFull(c2, buf); err != nil {
			got <- nil
			return
		}
		got <- buf
	}()

	n, err := emulatedConn.Write(payload)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(payload) {
		t.Errorf("Write() = %d, want %d", n, len(payload))
	}

	select {
	case received := <-got:
		if !bytes.Equal(received, payload) {
			t.Error("payload corrupted in transit")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for payload")
	}

	// Anything still queued means we wrote more than the payload.
	if err := c2.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	extra := make([]byte, 1)
	if _, err := c2.Read(extra); err == nil {
		t.Error("extra bytes on the wire, payload was duplicated")
	}
}

var errWriteFailed = errors.New("write failed")

// failConn fails every write to the underlying socket.
type failConn struct {
	net.Conn
}

func (failConn) Write([]byte) (int, error) { return 0, errWriteFailed }

// TestConn_StickyWriteError verifies that a write which fails after Write has
// already returned is reported to the caller rather than dropped.
func TestConn_StickyWriteError(t *testing.T) {
	c1, c2 := net.Pipe()
	defer c1.Close()
	defer c2.Close()

	emulatedConn := flakenet.NewConn(failConn{Conn: c1}, flakenet.StreamProfile{})

	// The first write only queues, so it cannot know the socket is broken.
	if _, err := emulatedConn.Write([]byte("first")); err != nil {
		t.Fatalf("Write() = %v, want nil while queueing", err)
	}

	var got error
	for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
		if _, err := emulatedConn.Write([]byte("again")); err != nil {
			got = err
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !errors.Is(got, errWriteFailed) {
		t.Fatalf("Write() = %v, want %v", got, errWriteFailed)
	}
	if err := emulatedConn.Close(); !errors.Is(err, errWriteFailed) {
		t.Errorf("Close() = %v, want %v", err, errWriteFailed)
	}
}
