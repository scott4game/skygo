# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project follows Semantic Versioning.

## [Unreleased]

### Added

- Single-process actor services with named lookup and generation-safe references.
- Cooperative mailbox yielding and an opt-in non-interleaving mode.
- Length-prefixed framing, asynchronous TCP pooling, and synchronous request/response pooling.
- Context-aware logging seam backed by `log/slog`.
- Optional protobuf cloning adapter.
- Delay queue, timing wheel, timer engine, and optional Redis reliable queue.
- Actor call observers with call graph aggregation.
- Explicit wait-for graph cycle detection.
