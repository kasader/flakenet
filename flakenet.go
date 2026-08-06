package flakenet

import (
	"net"
	"time"
)

const (
	// IPv4HeaderSize is the min size of an IPv4 header in bytes.
	IPv4HeaderSize = 20
	// IPv6HeaderSize is the fixed size of an IPv6 header in bytes.
	IPv6HeaderSize = 40
	// TCPHeaderSize is the min size of a TCP header in bytes, excluding options.
	TCPHeaderSize = 20
	// UDPHeaderSize is the fixed size of a UDP header in bytes.
	UDPHeaderSize = 8
)

const (
	// EthernetDefaultMTU is the standard MTU for most WANs and Internet traffic (1500 bytes).
	EthernetDefaultMTU = 1_500
	// EthernetJumboFrameMTU is used in data center environments to reduce CPU overhead (9000 bytes).
	EthernetJumboFrameMTU = 9_000
	// IPMaximumMTU represents the maximum possible size of an IP packet.
	IPMaximumMTU = 65_535
)

// LinkProfile defines the shared physical properties of a network link.
type LinkProfile struct {
	Latency   Latency
	Jitter    Jitter
	Bandwidth Bandwidth
}

// getHeaderSize returns the combined L3+L4 per-packet overhead implied by addr.
func getHeaderSize(addr net.Addr) int {
	// Mock and custom conns can hand back a nil LocalAddr. A nil typed
	// pointer still tells us the transport, so only the IP is unknown and
	// the worst-case L3 fallback below covers it.
	if addr == nil {
		return IPv6HeaderSize
	}
	var ip net.IP
	var transport int
	switch v := addr.(type) {
	case *net.UDPAddr:
		transport = UDPHeaderSize
		if v != nil {
			ip = v.IP
		}
	case *net.TCPAddr:
		transport = TCPHeaderSize
		if v != nil {
			ip = v.IP
		}
	case *net.IPAddr:
		// Raw IP sockets carry the transport header in the payload.
		if v != nil {
			ip = v.IP
		}
	// Best-effort to parse custom implementation (if provided).
	default:
		host, _, err := net.SplitHostPort(addr.String())
		if err != nil {
			// I guess just try parsing directly if SplitHostPort fails...?
			ip = net.ParseIP(addr.String())
		} else {
			ip = net.ParseIP(host)
		}
		transport = transportHeaderSize(addr.Network())
	}
	// Determine our header overhead from the [net.IP].
	overhead := IPv6HeaderSize // Assume worst case (IPv6)
	if ip != nil && ip.To4() != nil {
		overhead = IPv4HeaderSize
	}
	return overhead + transport
}

// transportHeaderSize maps a [net.Addr] network name to its L4 overhead.
// Networks we don't recognize contribute nothing.
func transportHeaderSize(network string) int {
	switch network {
	case "tcp", "tcp4", "tcp6":
		return TCPHeaderSize
	case "udp", "udp4", "udp6":
		return UDPHeaderSize
	default:
		return 0
	}
}

func transmissionTime(bandwidth Bandwidth, size, overhead int) time.Duration {
	if bandwidth == nil {
		return 0
	}
	bps := bandwidth.Limit()
	if bps == 0 {
		return 0
	}
	// Convert byte-size to bit-size (we measure in bits/second).
	totalBits := float64(size+overhead) * 8.0

	seconds := totalBits / float64(bps)
	return time.Duration(seconds * float64(time.Second))
}

func delayTime(latency Latency, jitter Jitter) time.Duration {
	var delay time.Duration
	if latency != nil {
		delay += latency.Duration()
	}
	if jitter != nil {
		delay += jitter.Duration()
	}
	if delay < 0 {
		delay = 0
	}
	return delay
}
