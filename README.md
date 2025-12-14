# synx

[![Go Reference](https://pkg.go.dev/badge/github.com/llxisdsh/synx.svg)](https://pkg.go.dev/github.com/llxisdsh/synx)

**synx** is a lightweight, high-performance concurrency toolkit for Go, designed for critical paths where latency and allocation matter.

## Installation

```bash
go get github.com/llxisdsh/synx
```

## Core Components

### 🚀 Map & FlatMap

State-of-the-art concurrent map implementations, streamlined from [**llxisdsh/pb**](https://github.com/llxisdsh/pb).

| Component     | Description                                                                      | Ideal Use Case                         |
|---------------|----------------------------------------------------------------------------------|----------------------------------------|
| **`Map`**     | **Lock-free reads**, fine-grained write locking. Drop-in `sync.Map` replacement. | General purpose, mixed R/W workloads.  |
| **`FlatMap`** | **Seqlock-based**, open-addressing with inline storage.                          | Read-heavy, cache-sensitive scenarios. |

> **Note**: These components retain the core high-performance logic of `llxisdsh/pb` but are packaged here for lightweight integration. For comprehensive benchmarks and advanced architectural details, please refer to the [upstream repository](https://github.com/llxisdsh/pb).

### ⚡ OnceGroup

Generic, high-performance duplicate suppression (singleflight).

- **Generic**: `OnceGroup[K comparable, V any]`.
- **Robust**: Preserves `panic` and `Goexit` semantics (unlike `x/sync/singleflight`).
- **Fast**: ~20× faster than `singleflight` for same-key operations with near-zero allocations.

### 🔒 Synchronization Primitives

Atomic, low-overhead coordination tools built on runtime semaphores.

| Primitive | Metaphor | Behavior | Key Usage |
|:---|:---|:---|:---|
| **`Latch`** | **One-time Door** | Starts closed. Once `Open()`, stays open forever. | Initialization, Shutdown signal. |
| **`Gate`** | **Manual Door** | `Open()`/`Close()`/`Pulse()`. Supports broadcast wakeups. | Pausing/Resuming, Cond-like signals. |
| **`Rally`** | **Meeting Point** | `Meet(n)` waits until n parties arrive, then releases all. | CyclicBarrier, MapReduce stages. |
| **`FairSemaphore`**| **FIFO Queue** | Strict FIFO ordering for permit acquisition. | Anti-starvation scenarios. |
| **`Epoch`** | **Milestone** | `WaitAtLeast(n)` blocks until counter reaches n. No thundering herd. | Phase coordination, Version gates. |
| **`Phaser`** | **Dynamic Barrier** | Dynamic party registration with split-phase `Arrive()`/`AwaitAdvance()`. | Java-style Phaser, Pipeline stages. |
| **`BitLock`** | **Bit Lock** | Spins on a specific bit mask. | Fine-grained, memory-constrained locks. |
| **`TicketLock`** | **Ticket Queue** | FIFO spin-lock with ticket algorithm. | Fair mutex, Latency-sensitive paths. |
| **`RWLock`** | **Read-Write Lock** | Spin-based R/W lock, writer-preferred. | Read-heavy, low-latency access. |
| **`TicketLockGroup`** | **Keyed Lock** | Per-key locking with auto-cleanup. | User/Resource isolation. |
| **`RWLockGroup`** | **Keyed R/W Lock** | Per-key R/W locking with auto-cleanup. | Config/Data partitioning. |
| **`Barter`** | **Exchanger** | Two goroutines swap values at a sync point. | Producer-Consumer handoff. |

> **Design Philosophy**: Minimal footprint, direct `runtime_semacquire` integration. Most primitives are zero-alloc on hot paths.

## Quick Start

### Concurrent Map

```go
package main

import "github.com/llxisdsh/synx"

func main() {
    // 1. Standard Map (Lock-free reads, sync.Map compatible)
    var m synx.Map[string, int]
    m.Store("foo", 1)

    // 2. FlatMap (Seqlock-based, inline storage)
    fm := synx.NewFlatMap[string, int](synx.WithCapacity(1000))
    fm.Store("bar", 2)
}
```

### OnceGroup

```go
var g synx.OnceGroup[string, string]
// Coalesce duplicate requests
val, err, shared := g.Do("key", func() (string, error) {
    return "expensive-op", nil
})
```

### Primitives Gallery

```go
// Latch: One-shot signal
var l synx.Latch
go func() { l.Open() }()
l.Wait()

// Gate: Reusable open/close + broadcast
var g synx.Gate
g.Open()   // All waiters pass
g.Close()  // Future waiters block
g.Pulse()  // Wake current waiters only (stays closed)

// Rally: Cyclic barrier
var r synx.Rally
r.Meet(3)  // Blocks until 3 parties arrive

// Epoch: Milestone waiter
var e synx.Epoch
go func() { e.Add(5) }()
e.WaitAtLeast(5)  // No thundering herd

// Phaser: Dynamic barrier (Java-style)
p := synx.NewPhaser()
p.Register()
phase := p.ArriveAndAwaitAdvance()

// Barter: Two-party value exchange
b := synx.NewBarter[string]()
// G1: v := b.Exchange("hello") -> receives "world"
// G2: v := b.Exchange("world") -> receives "hello"
```
