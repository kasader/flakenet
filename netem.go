package netem

import (
	"net"
	"time"
)

const (
	// IPv4HeaderSize is the min size of an IPv4 header in bytes.
	IPv4HeaderSize = 20
	// IPv6HeaderSize is the fixed size of an IPv6 header in bytes.
	IPv6HeaderSize = 40
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

func getHeaderSize(addr net.Addr) int {
	var ip net.IP
	switch v := addr.(type) {
	case *net.UDPAddr:
		ip = v.IP
	case *net.TCPAddr:
		ip = v.IP
	case *net.IPAddr:
		ip = v.IP
	// Best-effort to parse custom implementation (if provided).
	default:
		host, _, err := net.SplitHostPort(addr.String())
		if err != nil {
			// I guess just try parsing directly if SplitHostPort fails...?
			ip = net.ParseIP(addr.String())
		} else {
			ip = net.ParseIP(host)
		}
	}
	// Determine our header overhead from the [net.IP].
	overhead := IPv6HeaderSize // Assume worst case (IPv6)
	if ip != nil && ip.To4() != nil {
		overhead = IPv4HeaderSize
	}
	return overhead
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
