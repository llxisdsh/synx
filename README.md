[![Go Reference](https://pkg.go.dev/badge/github.com/llxisdsh/synx.svg)](https://pkg.go.dev/github.com/llxisdsh/synx)

# synx

A lightweight, high-performance concurrency toolkit for Go.

## Install

`go get github.com/llxisdsh/synx`

## OnceGroup

Per-key duplicate suppression with generics. API-compatible with `golang.org/x/sync/singleflight`; preserves panic/Goexit semantics.

- Highlights
  - Drop-in API: `Do`, `DoChan`, `Forget`, `ForgetUnshared`.
  - Generic keys and values: `OnceGroup[K comparable, V any]`.
  - Panic/Goexit propagation compatible with singleflight.
  - Near-zero allocations under contention.

- Benchmarks (Windows/amd64, AMD Ryzen Threadripper 3970X)
  - Same key (`Do`): ~42 ns/op vs singleflight ~846 ns/op (~20× faster).
  - Many keys (`Do`): ~171 ns/op vs ~698 ns/op (~4× faster).
  - `DoChan`, same key: synx slower (~9401 ns/op vs ~1375 ns/op).
  - `DoChan`, many keys: synx slightly faster (~1762 ns/op vs ~2058 ns/op).
  - Heavy work, same key: ~44 ns/op vs ~904 ns/op (~20× faster).
  - Heavy work, many keys: ~203 ns/op vs ~778 ns/op (~3.8× faster).

Note: Results vary by workload and environment; see `benchmark/` for details.
