# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project follows Semantic Versioning.

## [Unreleased]

## [0.2.0] - 2026-08-25

### Added

- Authenticated cluster runtime with static-registry discovery, incarnation-safe
  remote references, bounded and ordered admission, cancellation, heartbeats,
  reconnect backoff, registry reload, runtime statistics, stress tests, fuzzing,
  and benchmarks.
- `actor/protowire` deterministic protobuf codecs for typed remote methods and
  notifications.
- `app` lifecycle coordinator for ordered startup, readiness, signal handling,
  rollback, and reverse-order shutdown.
- `gate` bounded framed TCP server for external client connections.
- `ClusterCounters.AcceptErrors` for listener failure observability.

### Fixed

- Reject remote Calls and Sends whose target incarnation does not match the
  connected or receiving node, preventing stale references from reaching a
  restarted System that reused an address and generation.
- Back off persistent Accept failures in cluster and gate without delaying
  shutdown, preventing resource-exhaustion errors from causing CPU spin.
- Close peer sender admissions before draining queued notifications so every
  successfully admitted notification is written or reported exactly once.
- Preserve NoInterleave service identity for call observation without allowing
  cooperative mailbox yields.
- Reject NoInterleave synchronous self-calls with `ErrCallCycle` before mailbox
  dispatch instead of timing out and executing the queued call later.
- Detect multi-hop local and remote NoInterleave call cycles with complete,
  node-aware paths while allowing same-named services on different nodes.
- Isolate actor-owned context state while preserving caller values and the
  explicitly propagated call path.

### Changed

- `NoYield` now enforces its critical section inside `NoInterleave` services.
  It previously saw no activation there and silently passed calls through, so a
  `Call` made from a guarded section may start returning `ErrYieldForbidden`.
  Route such calls outside the `NoYield` closure, or use `Send`.

## [0.1.0] - 2026-08-18

### Added

- Single-process actor services with named lookup and generation-safe references.
- Cooperative mailbox yielding and an opt-in non-interleaving mode.
- Length-prefixed framing, asynchronous TCP pooling, and synchronous request/response pooling.
- Context-aware logging seam backed by `log/slog`.
- Optional protobuf cloning adapter.
- Delay queue, timing wheel, timer engine, and optional Redis reliable queue.
- Actor call observers with call graph aggregation.
- Explicit wait-for graph cycle detection.
- Deterministic actor stress tests, TCP reconnect and backpressure scenarios,
  timing-wheel mutation stress, frame fuzz targets, and actor benchmarks.
- Shared pre-cancel stack capture and leak checks across stress packages, with
  locally light and CI-scaled actor workloads.
