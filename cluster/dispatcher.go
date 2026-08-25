package cluster

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/scott4game/skygo/actor"
	"github.com/scott4game/skygo/cluster/internal/wire"
)

type inboundJob struct {
	conn     *inboundConn
	envelope *wire.Envelope
	canceled atomic.Bool
}

type inboundDispatcher struct {
	node   *Node
	ctx    context.Context
	shards []chan *inboundJob
	calls  chan struct{}
	slots  chan struct{}
	queued atomic.Int64
}

func newInboundDispatcher(node *Node, ctx context.Context) *inboundDispatcher {
	d := &inboundDispatcher{node: node, ctx: ctx, calls: make(chan struct{}, node.cfg.MaxInboundCalls), slots: make(chan struct{}, node.cfg.InboundQueue)}
	d.shards = make([]chan *inboundJob, node.cfg.InboundShards)
	for index := range d.shards {
		// The global slots semaphore enforces the total bound while allowing one
		// hot actor shard to use otherwise idle capacity.
		d.shards[index] = make(chan *inboundJob, node.cfg.InboundQueue)
		node.wg.Add(1)
		go d.run(d.shards[index])
	}
	return d
}

func (d *inboundDispatcher) submit(job *inboundJob) bool {
	target := job.envelope.GetTarget()
	if target == nil || len(d.shards) == 0 {
		return false
	}
	queue := d.shards[target.GetAddress()%uint64(len(d.shards))]
	select {
	case d.slots <- struct{}{}:
	default:
		return false
	}
	select {
	case queue <- job:
		d.queued.Add(1)
		return true
	default:
		<-d.slots
		return false
	}
}

func (d *inboundDispatcher) run(queue <-chan *inboundJob) {
	defer d.node.wg.Done()
	for {
		select {
		case <-d.ctx.Done():
			return
		case job := <-queue:
			d.queued.Add(-1)
			<-d.slots
			d.dispatch(job)
		}
	}
}

func (d *inboundDispatcher) dispatch(job *inboundJob) {
	envelope := job.envelope
	if job.canceled.Load() {
		job.conn.forgetCall(envelope.GetRequestId())
		return
	}
	target := fromWireTarget(envelope.GetTarget())
	if err := d.node.validateInboundTarget(target); err != nil {
		if envelope.GetKind() == wire.Kind_KIND_SEND {
			d.node.counters.inboundRejected.Add(1)
			d.node.system.ReportAsyncError(actor.AsyncError{Service: target.Service, Protocol: envelope.GetProtocol(), RemoteNode: job.conn.remoteNode, Stage: actor.AsyncErrorTransportAdmission, Err: err})
			return
		}
		job.conn.forgetCall(envelope.GetRequestId())
		_ = job.conn.writeResponse(envelope.GetRequestId(), nil, err)
		return
	}
	if envelope.GetKind() == wire.Kind_KIND_CALL {
		select {
		case d.calls <- struct{}{}:
		default:
			d.node.counters.inboundRejected.Add(1)
			job.conn.forgetCall(envelope.GetRequestId())
			_ = job.conn.writeResponse(envelope.GetRequestId(), nil, actor.ErrTransportBackpressure)
			return
		}
	}
	baseCtx := job.conn.ctx
	if baseCtx == nil {
		baseCtx = context.Background()
	}
	ctx := actor.WithCallPath(baseCtx, fromWirePath(envelope.GetCallPath()))
	ctx = withPeer(ctx, job.conn.remoteNode)
	if envelope.GetTraceId() != "" {
		ctx = WithTraceID(ctx, envelope.GetTraceId())
	}
	// The deadline bounds admission and caller waiting. Actor handlers retain the
	// established detached-cancellation contract after admission.
	var cancel context.CancelFunc
	if deadline := envelope.GetDeadlineUnixNano(); deadline > 0 {
		ctx, cancel = context.WithDeadline(ctx, time.Unix(0, deadline))
	}
	future, err := d.node.system.DispatchRemoteAsync(ctx, target, envelope.GetProtocol(), envelope.GetFingerprint(), envelope.GetPayload(), envelope.GetKind() == wire.Kind_KIND_SEND)
	if envelope.GetKind() == wire.Kind_KIND_SEND {
		if cancel != nil {
			cancel()
		}
		if err != nil {
			d.node.counters.inboundRejected.Add(1)
			d.node.system.ReportAsyncError(actor.AsyncError{Service: envelope.GetTarget().GetService(), Protocol: envelope.GetProtocol(), RemoteNode: job.conn.remoteNode, Stage: actor.AsyncErrorTransportAdmission, Err: err})
		}
		return
	}
	if err != nil {
		if cancel != nil {
			cancel()
		}
		<-d.calls
		job.conn.forgetCall(envelope.GetRequestId())
		_ = job.conn.writeResponse(envelope.GetRequestId(), nil, err)
		return
	}
	d.node.wg.Add(1)
	go func() {
		defer d.node.wg.Done()
		defer func() { <-d.calls }()
		if cancel != nil {
			defer cancel()
		}
		var result actor.RemoteResult
		select {
		case result = <-future:
		case <-d.ctx.Done():
			job.canceled.Store(true)
			job.conn.forgetCall(envelope.GetRequestId())
			return
		}
		job.conn.forgetCall(envelope.GetRequestId())
		if !job.canceled.Load() {
			if writeErr := job.conn.writeResponse(envelope.GetRequestId(), result.Payload, result.Err); writeErr != nil && !errors.Is(writeErr, context.Canceled) {
				job.conn.close()
			}
		}
	}()
}

func (d *inboundDispatcher) stats() (queued, active int) {
	return int(d.queued.Load()), len(d.calls)
}
