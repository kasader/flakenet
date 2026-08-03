package netem

import (
	"net"
	"testing"
)

// stubAddr is a custom net.Addr, exercising the best-effort default branch.
type stubAddr struct {
	network string
	addr    string
}

func (a stubAddr) Network() string { return a.network }
func (a stubAddr) String() string  { return a.addr }

func TestGetHeaderSize(t *testing.T) {
	tests := []struct {
		name string
		addr net.Addr
		want int
	}{
		{
			"udp4",
			&net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 53},
			IPv4HeaderSize + UDPHeaderSize,
		},
		{"udp6", &net.UDPAddr{IP: net.IPv6loopback, Port: 53}, IPv6HeaderSize + UDPHeaderSize},
		{
			"tcp4",
			&net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 80},
			IPv4HeaderSize + TCPHeaderSize,
		},
		{"tcp6", &net.TCPAddr{IP: net.IPv6loopback, Port: 80}, IPv6HeaderSize + TCPHeaderSize},
		{"ip4", &net.IPAddr{IP: net.IPv4(127, 0, 0, 1)}, IPv4HeaderSize},
		{"ip6", &net.IPAddr{IP: net.IPv6loopback}, IPv6HeaderSize},
		{"custom tcp4", stubAddr{"tcp", "127.0.0.1:80"}, IPv4HeaderSize + TCPHeaderSize},
		{"custom udp6", stubAddr{"udp6", "[::1]:53"}, IPv6HeaderSize + UDPHeaderSize},
		// Unparseable address and unknown network: worst-case L3, no L4.
		{"pipe", stubAddr{"pipe", "pipe"}, IPv6HeaderSize},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getHeaderSize(tt.addr); got != tt.want {
				t.Errorf("getHeaderSize(%v) = %d, want %d", tt.addr, got, tt.want)
			}
		})
	}
}
