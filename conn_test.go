package flakenet_test

import (
	"bytes"
	"errors"
	"io"
	"net"
	"os"
	"testing"
	"testing/synctest"
	"time"

	flakenet "github.com/kasader/flakenet"
	"github.com/kasader/flakenet/policy"
)

// These tests run inside a synctest bubble so time is virtual: delays are exact
// rather than approximate, and a test that waits on the link costs no wall
// clock. Every bubble goroutine has to finish before the test does, which is
// why the emulated conn is always closed.

// TestConn_Latency verifies that data is delayed by exactly the specified duration.
func TestConn_Latency(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client, server := net.Pipe()
		defer client.Close()
		defer server.Close()

		const latency = 100 * time.Millisecond
		emulatedClient := flakenet.NewConn(client, flakenet.StreamProfile{
			Latency: policy.StaticLatency(latency),
		})
		defer emulatedClient.Close()

		doneCh := make(chan time.Time, 1)
		go func() {
			buf := make([]byte, 1024)
			// This Read will block until the data arrives "over the wire"
			_, _ = server.Read(buf)
			doneCh <- time.Now()
		}()

		start := time.Now()
		if _, err := emulatedClient.Write([]byte("ping")); err != nil {
			t.Fatal(err)
		}

		if elapsed := (<-doneCh).Sub(start); elapsed != latency {
			t.Errorf("arrived after %v, want exactly %v", elapsed, latency)
		}
	})
}

// TestConn_Ordering verifies that TCP streams remain ordered
// even if Jitter creates "faster" packets that try to jump the queue.
func TestConn_Ordering(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
		defer emulatedConn.Close()

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

		select {
		case err := <-readErr:
			t.Fatal(err)
		case data := <-readData:
			// If TCP worked, we must get "HelloWorld".
			// If reordered, we might get "WorldHello" or mixed bytes.
			if data != "HelloWorld" {
				t.Errorf("Stream corrupted! Expected 'HelloWorld', got '%s'", data)
			}
		}
	})
}

// TestConn_Dynamic verifies we can change latency on the fly.
func TestConn_Dynamic(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c1, c2 := net.Pipe()
		defer c1.Close()
		defer c2.Close()

		latVar := &policy.LatencyVar{}
		latVar.Set(10 * time.Millisecond) // Start Fast

		emulatedConn := flakenet.NewConn(c1, flakenet.StreamProfile{
			Latency: latVar,
		})
		defer emulatedConn.Close()

		measure := func() time.Duration {
			start := time.Now()
			go emulatedConn.Write([]byte("x"))

			buf := make([]byte, 1)
			c2.Read(buf)
			return time.Since(start)
		}

		if d := measure(); d != 10*time.Millisecond {
			t.Errorf("measured %v, want exactly 10ms", d)
		}

		// Change configuration ON THE FLY
		latVar.Set(200 * time.Millisecond)

		if d := measure(); d != 200*time.Millisecond {
			t.Errorf("measured %v after update, want exactly 200ms", d)
		}
	})
}

// TestConn_Segmentation verifies that a payload larger than the MSS is split
// into segments rather than retransmitted whole once per segment.
func TestConn_Segmentation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c1, c2 := net.Pipe()
		defer c1.Close()
		defer c2.Close()

		// Header overhead on a pipe is worst-case IPv6, so the MSS is 100-40.
		emulatedConn := flakenet.NewConn(c1, flakenet.StreamProfile{MTU: 100})
		defer emulatedConn.Close()

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

		if received := <-got; !bytes.Equal(received, payload) {
			t.Error("payload corrupted in transit")
		}

		// Anything still queued means we wrote more than the payload.
		if err := c2.SetReadDeadline(time.Now().Add(100 * time.Millisecond)); err != nil {
			t.Fatal(err)
		}
		extra := make([]byte, 1)
		if _, err := c2.Read(extra); err == nil {
			t.Error("extra bytes on the wire, payload was duplicated")
		}
	})
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
	synctest.Test(t, func(t *testing.T) {
		c1, c2 := net.Pipe()
		defer c1.Close()
		defer c2.Close()

		emulatedConn := flakenet.NewConn(failConn{Conn: c1}, flakenet.StreamProfile{})
		defer emulatedConn.Close()

		// The first write only queues, so it cannot know the socket is broken.
		if _, err := emulatedConn.Write([]byte("first")); err != nil {
			t.Fatalf("Write() = %v, want nil while queueing", err)
		}

		// Wait returns once the link loop is parked again, so it has attempted
		// the write and recorded the failure.
		synctest.Wait()

		if _, err := emulatedConn.Write([]byte("again")); !errors.Is(err, errWriteFailed) {
			t.Fatalf("Write() = %v, want %v", err, errWriteFailed)
		}
		if err := emulatedConn.Close(); !errors.Is(err, errWriteFailed) {
			t.Errorf("Close() = %v, want %v", err, errWriteFailed)
		}
	})
}

// TestConn_CloseDrainsQueuedData verifies that a graceful Close flushes data
// the link loop already accepted rather than discarding it.
func TestConn_CloseDrainsQueuedData(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c1, c2 := net.Pipe()
		defer c1.Close()
		defer c2.Close()

		// The latency far exceeds the test, so the segment is certain to still
		// be in flight when Close lands.
		emulatedConn := flakenet.NewConn(c1, flakenet.StreamProfile{
			Latency: policy.StaticLatency(500 * time.Millisecond),
		})

		payload := []byte("queued")
		got := make(chan []byte, 1)
		go func() {
			buf := make([]byte, len(payload))
			if _, err := io.ReadFull(c2, buf); err != nil {
				got <- nil
				return
			}
			got <- buf
		}()

		if _, err := emulatedConn.Write(payload); err != nil {
			t.Fatal(err)
		}
		if err := emulatedConn.Close(); err != nil {
			t.Fatalf("Close() = %v, want nil", err)
		}

		if received := <-got; !bytes.Equal(received, payload) {
			t.Errorf("received %q, want %q", received, payload)
		}
	})
}

// TestConn_FaultSeversLink verifies that an injected fault closes the conn and
// discards what it was carrying, rather than writing to the closed socket.
func TestConn_FaultSeversLink(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c1, c2 := net.Pipe()
		defer c2.Close()

		emulatedConn := flakenet.NewConn(c1, flakenet.StreamProfile{
			Fault: policy.FaultFunc(func() bool { return true }),
		})

		if _, err := emulatedConn.Write([]byte("dropped")); err != nil {
			t.Fatalf("Write() = %v, want nil while queueing", err)
		}

		// The link loop severs the conn and exits, so once it is parked the
		// fault has already been applied.
		synctest.Wait()

		buf := make([]byte, 16)
		if n, err := c2.Read(buf); err == nil {
			t.Errorf("Read() = %q, want an error; the severed link still delivered", buf[:n])
		}

		if _, err := emulatedConn.Write([]byte("x")); !errors.Is(err, net.ErrClosed) {
			t.Errorf("Write() after fault = %v, want %v", err, net.ErrClosed)
		}
	})
}

// blockingConn stalls the link loop until released, ignoring deadlines, so the
// queue behind it stays full.
type blockingConn struct {
	net.Conn

	release chan struct{}
}

func (b blockingConn) Write(p []byte) (int, error) {
	<-b.release
	return len(p), nil
}

// TestConn_WriteDeadlineWhileBlocked verifies that a Write blocked on a full
// queue gives up at its deadline instead of waiting indefinitely.
func TestConn_WriteDeadlineWhileBlocked(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		c1, c2 := net.Pipe()
		defer c1.Close()
		defer c2.Close()

		release := make(chan struct{})
		emulatedConn := flakenet.NewConn(
			blockingConn{Conn: c1, release: release},
			flakenet.StreamProfile{},
		)
		// Cleanup runs bottom-up: release the socket first so the drain that
		// Close performs has somewhere to go.
		defer emulatedConn.Close()
		defer close(release)

		if err := emulatedConn.SetWriteDeadline(
			time.Now().Add(300 * time.Millisecond),
		); err != nil {
			t.Fatal(err)
		}

		blocked := make(chan error, 1)
		go func() {
			// Fills the queue instantly, then blocks on the send.
			for {
				if _, err := emulatedConn.Write([]byte("x")); err != nil {
					blocked <- err
					return
				}
			}
		}()

		if err := <-blocked; !errors.Is(err, os.ErrDeadlineExceeded) {
			t.Errorf("Write() = %v, want %v", err, os.ErrDeadlineExceeded)
		}
	})
}
