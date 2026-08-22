# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project follows Semantic Versioning.

## [Unreleased]

### Fixed

- Preserve NoInterleave service identity for call observation without allowing
  cooperative mailbox yields.
- Reject NoInterleave synchronous self-calls with `ErrCallCycle` before mailbox
  dispatch instead of timing out and executing the queued call later.

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
