# Testing skygo

The normal suite is suitable for every change:

```bash
go test -race ./...
go vet ./...
```

Stress tests are deterministic, bounded, and isolated behind a build tag:

```bash
go test -race -tags stress -timeout 20m ./...
```

Local runs default to `SKYGO_STRESS_SCALE=1`. Main-branch CI uses scale 25 to
increase randomized actor operations and NoInterleave DAG calls while retaining
the same deterministic seed:

```bash
SKYGO_STRESS_SCALE=25 go test -race -tags stress -timeout 20m ./...
```

To replay an actor call-graph failure, use the seed printed by the failed run:

```bash
SKYGO_STRESS_SEED=1234 go test -race -tags stress -run TestStressRandomCallGraph ./actor -v
```

Actor and TCP pool benchmarks report allocations:

```bash
go test -run='^$' -bench=. -benchmem ./actor ./tcppool
```

The frame fuzz targets can be run independently:

```bash
go test -run='^$' -fuzz='^FuzzRead$' -fuzztime=60s ./frame
go test -run='^$' -fuzz='^FuzzRoundTrip$' -fuzztime=60s ./frame
```
