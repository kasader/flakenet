package policy_test

import (
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/kasader/flakenet/policy"
)

// scenario draws interleaved decisions from every policy a Source vends, so the
// recorded sequence depends on the single underlying stream rather than on each
// policy in isolation.
func scenario(s *policy.Source) []string {
	loss := s.RandomLoss(0.3)
	jitter := s.RandomJitter(10 * time.Millisecond)
	fault := s.RandomClose(0.1)

	var got []string
	for range 200 {
		if loss.Drop() {
			got = append(got, "drop")
		}
		got = append(got, jitter.Duration().String())
		if fault.ShouldClose() {
			got = append(got, "close")
		}
	}
	return got
}

func TestSourceIsReproducible(t *testing.T) {
	first := scenario(policy.NewSource(42))
	second := scenario(policy.NewSource(42))

	if !slices.Equal(first, second) {
		t.Error("same seed produced different scenarios")
	}
}

func TestSourceSeedMatters(t *testing.T) {
	if slices.Equal(scenario(policy.NewSource(42)), scenario(policy.NewSource(43))) {
		t.Error("different seeds produced identical scenarios; is the seed used at all?")
	}
}

// The distributions must still hold at the boundaries, seeded or not.
func TestSourceRespectsRates(t *testing.T) {
	s := policy.NewSource(1)

	never, always := s.RandomLoss(0.0), s.RandomLoss(1.0)
	shut, stable := s.RandomClose(1.0), s.RandomClose(0.0)
	quiet := s.RandomJitter(0)

	for range 100 {
		if never.Drop() {
			t.Fatal("Drop() = true at rate 0.0")
		}
		if !always.Drop() {
			t.Fatal("Drop() = false at rate 1.0")
		}
		if !shut.ShouldClose() {
			t.Fatal("ShouldClose() = false at rate 1.0")
		}
		if stable.ShouldClose() {
			t.Fatal("ShouldClose() = true at rate 0.0")
		}
		if d := quiet.Duration(); d != 0 {
			t.Fatalf("Duration() = %v at amplitude 0, want 0", d)
		}
	}
}

// A Source is shared by every policy on a link, so concurrent draws must be
// safe. Run under -race to mean anything.
func TestSourceConcurrentDraws(_ *testing.T) {
	s := policy.NewSource(7)
	loss := s.RandomLoss(0.5)
	jitter := s.RandomJitter(time.Millisecond)

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 500 {
				loss.Drop()
				jitter.Duration()
			}
		})
	}
	wg.Wait()
}
