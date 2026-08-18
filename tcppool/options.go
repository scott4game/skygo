package tcppool

import (
	"net"
	"time"
)

// Options configures connection count, reconnect backoff, buffering, and TCP
// keepalive behavior.
type Options struct {
	// PoolSize is the number of managed connections. The default is 1.
	PoolSize int

	// DialTimeout bounds each dial. Zero leaves dialing unbounded.
	DialTimeout time.Duration

	// ReconnectInitial is the first reconnect delay. The default is 1 second.
	ReconnectInitial time.Duration

	// ReconnectMax caps reconnect backoff. The default is 10 seconds.
	ReconnectMax time.Duration

	// StableThreshold is how long a connection must survive before its backoff
	// is reset. The default is 5 seconds.
	StableThreshold time.Duration

	// JitterFraction adds random jitter in [0, JitterFraction) to reconnect
	// backoff. The default is 0.2.
	JitterFraction float64

	// TCPKeepalive enables operating-system TCP keepalive.
	TCPKeepalive bool

	// TCPKeepalivePeriod sets the keepalive probe interval. Zero uses the OS default.
	TCPKeepalivePeriod time.Duration

	// SendBufSize is each connection's send queue capacity. The default is 128.
	SendBufSize int

	// PreReady runs synchronously after dialing and before the connection becomes
	// visible to senders.
	PreReady func(conn net.Conn)

	// OnClose runs after a connection has been closed.
	OnClose func(conn net.Conn)
}

func (o *Options) setDefaults() {
	if o.PoolSize <= 0 {
		o.PoolSize = 1
	}
	if o.ReconnectInitial <= 0 {
		o.ReconnectInitial = time.Second
	}
	if o.ReconnectMax <= 0 {
		o.ReconnectMax = 10 * time.Second
	}
	if o.StableThreshold <= 0 {
		o.StableThreshold = 5 * time.Second
	}
	if o.JitterFraction <= 0 {
		o.JitterFraction = 0.2
	}
	if o.SendBufSize <= 0 {
		o.SendBufSize = 128
	}
}
