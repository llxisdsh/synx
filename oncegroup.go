package synx

import (
	"bytes"
	"errors"
	"fmt"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"

	"github.com/llxisdsh/pb"
)

// Result holds the results of Do, so they can be passed on a channel.
type Result[V any] struct {
	Val    V
	Err    error
	Shared bool
}

// call represents an in-flight or completed OnceGroup.Do call
type call[V any] struct {
	wg    sync.WaitGroup
	val   V
	err   error
	dups  int32
	chans []chan<- Result[V]
}

// OnceGroup represents a class of work and forms a namespace in
// which units of work can be executed with duplicate suppression.
type OnceGroup[K comparable, V any] struct {
	m pb.MapOf[K, *call[V]]
}

// Do execute and returns the results of the given function, making
// sure that only one execution is in-flight for a given key at a
// time. If a duplicate comes in, the duplicate caller waits for the
// original to complete and receives the same results.
// The return value shared indicates whether v was given to multiple callers.
func (g *OnceGroup[K, V]) Do(
	key K,
	fn func() (V, error),
) (V, error, bool) {
	var c *call[V]
	_, loaded := g.m.ProcessEntry(
		key,
		func(l *pb.EntryOf[K, *call[V]]) (*pb.EntryOf[K, *call[V]], *call[V], bool) {
			if l != nil {
				c = l.Value
				atomic.AddInt32(&c.dups, 1)
				return l, c, true
			}
			c = &call[V]{}
			c.wg.Add(1)
			return &pb.EntryOf[K, *call[V]]{Value: c}, c, false
		},
	)
	if loaded {
		c.wg.Wait()
		var e *panicError
		if errors.As(c.err, &e) {
			panic(e)
		} else if errors.Is(c.err, errGoexit) {
			runtime.Goexit()
		}
		return c.val, c.err, true
	}

	// Primary executes with panic/Goexit semantics compatible with x/sync/singleflight.
	g.doCall(c, key, fn)
	shared := atomic.LoadInt32(&c.dups) > 0
	return c.val, c.err, shared
}

// DoChan is like Do but returns a channel that will receive the
// results when they are ready.
//
// The returned channel will not be closed.
func (g *OnceGroup[K, V]) DoChan(
	key K,
	fn func() (V, error),
) <-chan Result[V] {
	ch := make(chan Result[V], 1)
	var c *call[V]
	_, loaded := g.m.ProcessEntry(
		key,
		func(l *pb.EntryOf[K, *call[V]]) (*pb.EntryOf[K, *call[V]], *call[V], bool) {
			if l != nil {
				c = l.Value
				atomic.AddInt32(&c.dups, 1)
				c.chans = append(c.chans, ch)
				return l, c, true
			}
			c = &call[V]{
				chans: append(
					make([]chan<- Result[V], 0, runtime.GOMAXPROCS(0)),
					ch,
				),
			}
			c.wg.Add(1)
			return &pb.EntryOf[K, *call[V]]{Value: c}, c, false
		},
	)
	if loaded {
		return ch
	}
	go g.doCall(c, key, fn)
	return ch
}

// Forget tells the group to stop tracking a key. Future calls
// to Do for this key will invoke the function rather than waiting for
// an existing call to complete.
func (g *OnceGroup[K, V]) Forget(key K) {
	g.m.Delete(key)
}

// ForgetUnshared deletes the key only if no duplicates joined.
func (g *OnceGroup[K, V]) ForgetUnshared(key K) bool {
	_, ok := g.m.ProcessEntry(
		key,
		func(l *pb.EntryOf[K, *call[V]]) (*pb.EntryOf[K, *call[V]], *call[V], bool) {
			if l != nil && atomic.LoadInt32(&l.Value.dups) == 0 {
				return nil, nil, true
			}
			return l, nil, false
		},
	)
	return ok
}

// doCall runs fn with panic/Goexit semantics and broadcasts results.
func (g *OnceGroup[K, V]) doCall(
	c *call[V],
	key K,
	fn func() (V, error),
) {
	normalReturn := false
	recovered := false

	defer func() {
		// Mark Goexit if the goroutine terminated without normal return
		// and without a recovered panic.
		if !normalReturn && !recovered {
			c.err = errGoexit
		}

		// Complete the call and remove the key atomically.
		c.wg.Done()

		var chs []chan<- Result[V]
		_, _ = g.m.ProcessEntry(
			key,
			func(l *pb.EntryOf[K, *call[V]]) (*pb.EntryOf[K, *call[V]], *call[V], bool) {
				if l != nil && l.Value == c {
					chs = append(chs, l.Value.chans...)
					return nil, nil, false
				}
				return l, nil, false
			},
		)
		if len(chs) == 0 {
			chs = c.chans
		}

		// After wg.Done, duplicates in Do() will wake and re-panic/goexit.
		var e *panicError
		if errors.As(c.err, &e) {
			// Match x/sync: ensure panic is unrecoverable and visible.
			if len(chs) > 0 {
				//goland:noinspection All
				go panic(e)
				select {}
			} else {
				panic(e)
			}
		} else if errors.Is(c.err, errGoexit) {
			// Primary goroutine already Goexit'ed; nothing to do here.
		} else {
			// Normal return: notify DoChan waiters.
			shared := atomic.LoadInt32(&c.dups) > 0
			for _, ch := range chs {
				ch <- Result[V]{Val: c.val, Err: c.err, Shared: shared}
			}
		}
	}()

	// Distinguish panic from Goexit via double-defer with inner wrapper,
	// matching the structure of the official implementation.
	func() {
		defer func() {
			if !normalReturn {
				// Only recover when not a normal return, so we can
				// differentiate panic vs Goexit.
				if r := recover(); r != nil {
					c.err = newPanicError(r)
				}
			}
		}()

		c.val, c.err = fn()
		normalReturn = true
	}()

	if !normalReturn {
		recovered = true
	}
}

// -------------------------
// Panic/Goexit handling
// -------------------------

// panicError mirrors the type used by x/sync/singleflight.
// A panicError is an arbitrary value recovered from a panic
// with the stack trace during the execution of given function.
type panicError struct {
	value any
	stack []byte
}

// Error implements error interface.
func (p *panicError) Error() string {
	return fmt.Sprintf("%v\n\n%s", p.value, p.stack)
}

// Unwrap returns the underlying error value, if any.
func (p *panicError) Unwrap() error {
	if err, ok := p.value.(error); ok {
		return err
	}
	return nil
}

func newPanicError(v any) error {
	stack := debug.Stack()
	// Trim first line "goroutine N [status]:" which can be misleading.
	if line := bytes.IndexByte(stack[:], '\n'); line >= 0 {
		stack = stack[line+1:]
	}
	return &panicError{value: v, stack: stack}
}

var errGoexit = errors.New("runtime.Goexit was called")
