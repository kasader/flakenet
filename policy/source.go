package policy

import (
	"math/rand/v2"
	"sync"
	"time"
)

// rng is the randomness a policy draws from. It exists so the seeded and
// package-global policies share one implementation of each distribution.
type rng interface {
	Float64() float64
	Int64N(n int64) int64
}

// global draws from the package-level math/rand/v2 generator, which is safe
// for concurrent use and seeded unpredictably at startup.
type global struct{}

func (global) Float64() float64     { return rand.Float64() }
func (global) Int64N(n int64) int64 { return rand.Int64N(n) }

// locked guards a seeded generator, which unlike the global one is not safe for
// concurrent use.
type locked struct {
	mu sync.Mutex
	r  *rand.Rand
}

func (l *locked) Float64() float64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.r.Float64()
}

func (l *locked) Int64N(n int64) int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.r.Int64N(n)
}

// Source is a seeded, reproducible origin for stochastic policies.
//
// Policies built from the package-level constructors draw from the global
// generator, so a run that fails at a 1% loss rate cannot be replayed. The
// policies a Source vends all draw from one seeded stream instead, which makes
// the whole scenario reproducible from a single seed rather than one seed per
// policy.
//
// A Source is safe for concurrent use. Note that it serializes every draw:
// math/rand/v2's *Rand is not goroutine-safe, unlike the global generator, so
// reproducibility costs a mutex per packet. That is cheap next to the sleeping
// the link already does.
//
// Reproducibility holds for the values a policy draws, not for delivery
// timing, which still follows the wall clock.
type Source struct {
	r *locked
}

// NewSource returns a Source drawing from the given seed. Two Sources with the
// same seed vend policies that make the same sequence of decisions.
func NewSource(seed uint64) *Source {
	return &Source{r: &locked{r: rand.New(rand.NewPCG(seed, seed))}}
}

// RandomJitter is the [RandomJitter] policy drawn from this Source.
func (s *Source) RandomJitter(amplitude time.Duration) JitterFunc {
	return JitterFunc(func() time.Duration { return jitterIn(s.r, amplitude) })
}

// RandomLoss is the [RandomLoss] policy drawn from this Source.
func (s *Source) RandomLoss(rate float64) LossFunc {
	return LossFunc(func() bool { return dropAt(s.r, rate) })
}

// RandomClose is the [RandomClose] policy drawn from this Source.
func (s *Source) RandomClose(rate float64) FaultFunc {
	return FaultFunc(func() bool { return dropAt(s.r, rate) })
}
