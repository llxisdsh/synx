//go:build !race

package opt

import (
	_ "unsafe" // for linkname
)

const Race_ = false

// Sema is a zero-allocation semaphore optimized for performance.
// In !race mode, it is a direct wrapper around runtime.semacquire/semrelease.
type Sema struct {
	val uint32
}

func (s *Sema) Acquire() {
	runtime_semacquire(&s.val)
}

func (s *Sema) Release() {
	runtime_semrelease(&s.val, false, 0)
}

//go:linkname runtime_semacquire sync.runtime_Semacquire
func runtime_semacquire(s *uint32)

//go:linkname runtime_semrelease sync.runtime_Semrelease
func runtime_semrelease(s *uint32, handoff bool, skipframes int)
