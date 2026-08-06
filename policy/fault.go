package policy

import (
	"math"
	"sync/atomic"
)

// FaultFunc enables a simple function to satisfy the [Fault] interface.
type FaultFunc func() bool

// ShouldClose implements the [Fault] interface.
func (f FaultFunc) ShouldClose() bool { return f() }

// RandomClose returns a function that closes connections with probability rate (0.0 to 1.0).
func RandomClose(rate float64) FaultFunc {
	return FaultFunc(func() bool { return dropAt(rate) })
}

// FaultVar is a thread-safe, mutable [Fault] provider.
// It allows you to change the random fault rate of a running simulation.
//
// Uses the [RandomClose] policy. For other policies, please implement a
// custom FaultVar implementation.
type FaultVar struct {
	val atomic.Uint64
}

// Set updates the fault rate safely.
func (v *FaultVar) Set(rate float64) { v.val.Store(math.Float64bits(rate)) }

// ShouldClose implements the [Fault] interface.
func (v *FaultVar) ShouldClose() bool {
	return dropAt(math.Float64frombits(v.val.Load()))
}
