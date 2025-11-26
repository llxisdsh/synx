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

| Component           | Description                                                                      | Ideal Use Case                         |
|---------------------|----------------------------------------------------------------------------------|----------------------------------------|
| **`Map[K, V]`**     | **Lock-free reads**, fine-grained write locking. Drop-in `sync.Map` replacement. | General purpose, mixed R/W workloads.  |
| **`FlatMap[K, V]`** | **Seqlock-based**, open-addressing with inline storage.                          | Read-heavy, cache-sensitive scenarios. |

> **Note**: These components retain the core high-performance logic of `llxisdsh/pb` but are packaged here for lightweight integration. For comprehensive benchmarks and advanced architectural details, please refer to the [upstream repository](https://github.com/llxisdsh/pb).

### ⚡ OnceGroup

Generic, high-performance duplicate suppression (singleflight).

- **Generic**: `OnceGroup[K comparable, V any]`.
- **Robust**: Preserves `panic` and `Goexit` semantics (unlike `x/sync/singleflight`).
- **Fast**: ~20× faster than `singleflight` for same-key operations with near-zero allocations.

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
