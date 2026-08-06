package policy

import (
	"math/rand/v2"
	"sync/atomic"
	"time"
)

// JitterFunc enables a simple function to satisfy the [Jitter] interface.
type JitterFunc func() time.Duration

// Duration implements the [Jitter] interface.
func (f JitterFunc) Duration() time.Duration { return f() }

// jitterIn returns a value uniformly distributed in the inclusive range
// [-amplitude, +amplitude].
//
// A negative amplitude is treated as its absolute value; disabling jitter
// silently would weaken the emulation rather than report the mistake.
func jitterIn(amplitude time.Duration) time.Duration {
	n := int64(amplitude)
	if n < 0 {
		n = -n
	}
	if n == 0 {
		return 0
	}
	return time.Duration(rand.Int64N(2*n+1) - n)
}

// RandomJitter returns a jitter function that selects a random value
// uniformly distributed in the inclusive range [-amplitude, +amplitude].
//
// For example, RandomJitter(10*time.Millisecond) will return a duration
// randomly chosen between -10ms and +10ms.
func RandomJitter(amplitude time.Duration) JitterFunc {
	return JitterFunc(func() time.Duration { return jitterIn(amplitude) })
}

// JitterVar is a thread-safe, mutable [Jitter] provider.
// It allows you to change the jitter of a running simulation.
//
// Uses the [RandomJitter] policy. For other policies, please implement a
// custom LossVar implementation.
type JitterVar struct{ val atomic.Int64 }

// Set updates the jitter safely.
func (v *JitterVar) Set(d time.Duration) { v.val.Store(int64(d)) }

// Duration implements the [Jitter] interface.
func (v *JitterVar) Duration() time.Duration {
	return jitterIn(time.Duration(v.val.Load()))
}
