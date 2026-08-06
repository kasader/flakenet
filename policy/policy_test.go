package policy_test

import (
	"testing"
	"time"

	"github.com/kasader/flakenet"
	"github.com/kasader/flakenet/policy"
)

// Every exported policy type must satisfy the interface it is written against.
// FaultVar shipped unusable because nothing asserted this.
var (
	_ flakenet.Latency   = policy.LatencyFunc(nil)
	_ flakenet.Latency   = (*policy.LatencyVar)(nil)
	_ flakenet.Jitter    = policy.JitterFunc(nil)
	_ flakenet.Jitter    = (*policy.JitterVar)(nil)
	_ flakenet.Bandwidth = policy.BandwidthFunc(nil)
	_ flakenet.Bandwidth = (*policy.BandwidthVar)(nil)
	_ flakenet.Loss      = policy.LossFunc(nil)
	_ flakenet.Loss      = (*policy.LossVar)(nil)
	_ flakenet.Fault     = policy.FaultFunc(nil)
	_ flakenet.Fault     = (*policy.FaultVar)(nil)
)

func TestRandomJitterRange(t *testing.T) {
	tests := []struct {
		name      string
		amplitude time.Duration
		want      time.Duration // absolute bound
	}{
		{"positive", 10 * time.Millisecond, 10 * time.Millisecond},
		{"negative", -10 * time.Millisecond, 10 * time.Millisecond},
		{"zero", 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			j := policy.RandomJitter(tt.amplitude)
			for range 1_000 {
				got := j.Duration()
				if got < -tt.want || got > tt.want {
					t.Fatalf("Duration() = %v, want within [%v, %v]", got, -tt.want, tt.want)
				}
			}
		})
	}
}

// The documented range is inclusive on both ends, so with an amplitude of 1ns
// every value in {-1, 0, 1} must be reachable.
func TestRandomJitterInclusive(t *testing.T) {
	j := policy.RandomJitter(1)
	seen := map[time.Duration]bool{}
	for range 1_000 {
		seen[j.Duration()] = true
	}
	for _, want := range []time.Duration{-1, 0, 1} {
		if !seen[want] {
			t.Errorf("never drew %v in 1000 samples, range is not inclusive", want)
		}
	}
}

func TestFaultVar(t *testing.T) {
	var v policy.FaultVar

	v.Set(1.0)
	if !v.ShouldClose() {
		t.Error("ShouldClose() = false at rate 1.0, want true")
	}

	v.Set(0.0)
	if v.ShouldClose() {
		t.Error("ShouldClose() = true at rate 0.0, want false")
	}
}
