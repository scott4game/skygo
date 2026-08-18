# skygo

skygo is a single-process, Skynet-inspired service runtime for Go with actor, timer, observability, and TCP transport primitives.

It is not a Skynet port, a distributed actor system, or an official Skynet project. References are process-local; skygo does not provide node discovery, remote references, or transparent cross-node calls.

## Packages

- `actor`: named services, generation-safe references, typed methods, mailbox serialization, and cooperative yielding.
- `frame`: bounded 4-byte big-endian length-prefixed framing.
- `tcppool`: asynchronous multi-connection TCP pool with reconnect and backoff.
- `tcpsync`: bounded synchronous request/response TCP pool.
- `skylog`: context-aware logging interface with a `log/slog` default.
- `actor/protoclone`: optional protobuf deep-cloning integration.
- `timer`: delay queues, a one-hour timing wheel, and process-safe ID generation.
- `timer/engine`: generic hot-window scheduling over a persistent queue interface.
- `timer/redisqueue`: optional Redis-backed reliable timer ledger.
- `observe/callgraph`: actor call aggregation through `actor.Observer`.
- `observe/waitgraph`: explicit wait-for cycle detection.

The core actor, timer, observation, framing, logging, and TCP packages have no third-party runtime dependencies. Optional protobuf cloning depends on `google.golang.org/protobuf`; `timer/redisqueue` depends on go-redis/v8.

## Requirements

Go 1.23 or newer.

```sh
go get github.com/scott4game/skygo
```

## Actor example

```go
system := actor.NewSystem(actor.SystemOptions{})
service, ref, err := system.Reserve("echo", actor.ServiceOptions{})
if err != nil {
    log.Fatal(err)
}

echo := actor.NewMethod[string, string]("echo")
if err := actor.Register(service, echo, func(_ context.Context, request string) (string, error) {
    return request, nil
}); err != nil {
    log.Fatal(err)
}
if err := service.Start(context.Background()); err != nil {
    log.Fatal(err)
}

response, err := echo.Call(context.Background(), ref, "hello")
```

A complete runnable version is in `examples/actor_echo`.

## Actor concurrency semantics

Each service processes one active mailbox segment at a time. By default, calling another service from a handler cooperatively yields the current service, allowing another queued message to run. This also permits self-calls and call cycles to make progress.

`ServiceOptions.NoInterleave` keeps a handler atomic across a nested call. It trades away cooperative progress: it causes head-of-line blocking, and a call back into the same service will deadlock until a configured timeout. Use it only when state invariants span a cross-service call.

Mutable typed method values need an explicit clone function, a suitable `Clone` method, or a registered clone provider. For protobuf messages, import the adapter once from the package that assembles the process:

```go
import _ "github.com/scott4game/skygo/actor/protoclone"
```

## Timers and observation

`timer.TimingWheel` runs callbacks with a bounded context. A callback that must
serialize with actor state should use `actor.Send` to enter the owning mailbox.
`timer/engine` adds hot-window promotion and reliable claim/complete processing.
`timer/redisqueue` supplies optional Redis ledger primitives; an application
adapter provides tenant discovery and implements the engine queue interface.

Set an `actor.Observer` in `actor.SystemOptions` to receive completed synchronous
call events. `observe/callgraph.Recorder` is a ready-to-use, concurrency-safe
observer that can export aggregated edges as JSON or JSONL.

## Framing contract

`frame.Read` and `frame.Write` enforce `frame.MaxPayload`. `WriteUnchecked` exists for compatibility with protocols that deliberately configure a larger peer-side read limit. Both write paths handle short writes and never report success for a truncated frame.

## Status

The public API is pre-1.0 and may change between minor releases. Production adopters should pin a tag. See [CHANGELOG.md](CHANGELOG.md) for changes.

## License

Apache License 2.0. See [LICENSE](LICENSE).
