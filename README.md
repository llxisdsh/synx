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
| **`Gate`** | **Manual Door** | Can be `Open()` and `Close()` repeatedly. | Pausing/Resuming workers. |
| **`Pulse`** | **Heartbeat** | `Wait()` blocks until the *next* `Beat()`. No state retention. | Broadcasts, Periodic wakeups. |
| **`Rally`** | **Meeting Point** | `Meet(n)` waits until n parties arrive, then releases all. | CyclicBarrier, MapReduce stages. |
| **`Semaphore`**| **Parking Lot** | `Acquire(n)` / `Release(n)` tokens. | Rate limiting, Resource pools. |
| **`BitLock`** | **Bit Lock** | Spins on a specific bit mask. | Fine-grained, memory-constrained locks. |

> **Design Philosophy**: Zero allocation on hot paths, 8-16 bytes footprint, direct `runtime_semacquire` integration for maximum efficiency.

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
// 1. Latch (One-shot)
var l synx.Latch
go func() { l.Open() }() 
l.Wait() // Returns immediately if already open

// 2. Gate (Reusable)
var g synx.Gate
g.Open()  // All waiters pass
g.Close() // Subsequent waiters block
g.Wait()

// 3. Pulse (Notification)
var p synx.Pulse
go func() {
    for {
        time.Sleep(time.Second) 
        p.Beat() // Wakes all CURRENT waiters
    }
}()
p.Wait() // Blocks until NEXT beat

// 4. Rally (Cyclic Barrier)
var r synx.Rally
// Wait for 3 goroutines to arrive
r.Meet(3) // All 3 block until the 3rd one arrives

// 5. Semaphore (Counting)
s := synx.NewSemaphore(10) // 10 permits
s.Acquire(1)
defer s.Release(1)

// 6. BitLock (Bit-stealing)
var meta uint64 
const lockBit = 1 << 63
// Acquires lock using ONLY the highest bit
synx.BitLockUint64(&meta, lockBit) 
// ... critical section ...
// meta's lower 63 bits are still usable for data!
synx.BitUnlockUint64(&meta, lockBit)
```
