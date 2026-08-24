package cluster

import "context"

type peerContextKey struct{}
type traceContextKey struct{}

// PeerFromContext reports the authenticated cluster node that sent a request.
func PeerFromContext(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	peer, ok := ctx.Value(peerContextKey{}).(string)
	return peer, ok && peer != ""
}

// WithTraceID associates application trace metadata with a cluster operation.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, traceContextKey{}, traceID)
}

// TraceIDFromContext returns trace metadata received from the caller.
func TraceIDFromContext(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	traceID, ok := ctx.Value(traceContextKey{}).(string)
	return traceID, ok && traceID != ""
}

func withPeer(ctx context.Context, peer string) context.Context {
	return context.WithValue(ctx, peerContextKey{}, peer)
}
