package cluster

import (
	"bytes"
	"context"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/scott4game/skygo/actor"
	"github.com/scott4game/skygo/actor/protowire"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestClusterStress(t *testing.T) {
	duration := stressDuration("SKYGO_CLUSTER_STRESS_DURATION", 750*time.Millisecond)
	concurrency := stressInt("SKYGO_CLUSTER_STRESS_CONCURRENCY", 8)
	nodeCount := stressInt("SKYGO_CLUSTER_STRESS_NODES", 2)
	payloadBytes := stressInt("SKYGO_CLUSTER_STRESS_PAYLOAD_BYTES", 1024)
	seed := int64(stressInt("SKYGO_CLUSTER_STRESS_SEED", 759129))
	if nodeCount < 2 {
		nodeCount = 2
	}

	addresses := make([]string, nodeCount)
	endpoints := make([]Endpoint, nodeCount)
	for i := range addresses {
		addresses[i] = reserveAddress(t)
		endpoints[i] = Endpoint{NodeID: stressNodeID(i), Address: addresses[i]}
	}
	registry := NewStaticRegistry(endpoints...)
	systems := make([]*actor.System, nodeCount)
	nodes := make([]*Node, nodeCount)
	for i := range nodes {
		systems[i] = actor.NewSystem(actor.SystemOptions{})
		cfg := Config{
			NodeID: stressNodeID(i), Listen: addresses[i], Registry: registry, Secret: testSecret,
			SendQueue: 16, InboundQueue: 64, MaxInboundCalls: 32, CallTimeout: 100 * time.Millisecond,
			ConnectBackoffMin: 5 * time.Millisecond, ConnectBackoffMax: 20 * time.Millisecond,
		}
		nodes[i] = startTestNodeConfig(t, cfg, systems[i])
	}

	echo := protowire.NewMethod("stress.echo", func() *wrapperspb.BytesValue { return &wrapperspb.BytesValue{} }, func() *wrapperspb.BytesValue { return &wrapperspb.BytesValue{} })
	notify := protowire.NewNotification("stress.notify", func() *wrapperspb.BytesValue { return &wrapperspb.BytesValue{} })
	var notifications atomic.Uint64
	refs := make([]actor.Ref, 0, nodeCount-1)
	for i := 1; i < nodeCount; i++ {
		service, _, err := systems[i].Reserve("stress-service", actor.ServiceOptions{MailboxSize: 256})
		if err != nil {
			t.Fatal(err)
		}
		if err := actor.Register(service, echo, func(_ context.Context, value *wrapperspb.BytesValue) (*wrapperspb.BytesValue, error) {
			return value, nil
		}); err != nil {
			t.Fatal(err)
		}
		if err := actor.RegisterNotification(service, notify, func(context.Context, *wrapperspb.BytesValue) error { notifications.Add(1); return nil }); err != nil {
			t.Fatal(err)
		}
		if err := service.Start(context.Background()); err != nil {
			t.Fatal(err)
		}
		ref, err := nodes[0].Resolve(context.Background(), stressNodeID(i), "stress-service")
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, ref)
	}

	payload := wrapperspb.Bytes(bytes.Repeat([]byte{0x5a}, payloadBytes))
	deadline := time.Now().Add(duration)
	var successes atomic.Uint64
	latencySamples := make(chan time.Duration, 100000)
	var latencies []time.Duration
	var collector sync.WaitGroup
	collector.Add(1)
	go func() {
		defer collector.Done()
		for latency := range latencySamples {
			latencies = append(latencies, latency)
		}
	}()
	var workers sync.WaitGroup
	for worker := 0; worker < concurrency; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			rng := rand.New(rand.NewSource(seed + int64(worker)))
			for time.Now().Before(deadline) {
				ref := refs[rng.Intn(len(refs))]
				if rng.Intn(3) == 0 {
					_ = notify.TrySend(context.Background(), ref, payload)
					continue
				}
				ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
				if rng.Intn(7) == 0 {
					cancel()
				}
				started := time.Now()
				if response, err := echo.Call(ctx, ref, payload); err == nil && bytes.Equal(response.Value, payload.Value) {
					successes.Add(1)
					select {
					case latencySamples <- time.Since(started):
					default:
					}
				}
				cancel()
			}
		}(worker)
	}

	// Change one endpoint during traffic, then restore it. Calls and Sends may
	// fail explicitly during the outage, but the transport must not replay them.
	time.Sleep(duration / 4)
	bad := append([]Endpoint(nil), endpoints...)
	bad[1].Address = reserveAddress(t)
	registry.Replace(bad...)
	if err := nodes[0].Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(duration / 8)
	registry.Replace(endpoints...)
	if err := nodes[0].Reload(context.Background()); err != nil {
		t.Fatal(err)
	}
	workers.Wait()
	close(latencySamples)
	collector.Wait()

	if successes.Load() == 0 {
		t.Fatal("stress run completed without a successful call")
	}
	for i, ref := range refs {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		response, err := echo.Call(ctx, ref, payload)
		cancel()
		if err != nil || !bytes.Equal(response.GetValue(), payload.Value) {
			t.Fatalf("node %d did not recover: response=%v err=%v", i+1, response, err)
		}
	}
	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
	t.Logf("seed=%d duration=%s concurrency=%d nodes=%d payload=%d successful_calls=%d delivered_sends=%d p50=%s p95=%s p99=%s", seed, duration, concurrency, nodeCount, payloadBytes, successes.Load(), notifications.Load(), stressPercentile(latencies, 50), stressPercentile(latencies, 95), stressPercentile(latencies, 99))
}

func stressPercentile(values []time.Duration, percentile int) time.Duration {
	if len(values) == 0 {
		return 0
	}
	index := (len(values)*percentile + 99) / 100
	if index > 0 {
		index--
	}
	return values[index]
}

func stressNodeID(index int) string {
	if index == 0 {
		return "stress-client"
	}
	return "stress-server-" + strconv.Itoa(index)
}

func stressInt(name string, fallback int) int {
	if value, err := strconv.Atoi(os.Getenv(name)); err == nil && value > 0 {
		return value
	}
	return fallback
}

func stressDuration(name string, fallback time.Duration) time.Duration {
	if value, err := time.ParseDuration(os.Getenv(name)); err == nil && value > 0 {
		return value
	}
	return fallback
}
