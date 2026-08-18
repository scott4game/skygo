package tcpsync

import (
	"net"
	"time"
)

// PingFunc is an application-layer heartbeat callback for validating an idle connection.
type PingFunc func(conn net.Conn, timeout time.Duration) error

// Options configures a synchronous request/response TCP pool.
type Options struct {
	// MaxSize is the maximum number of concurrent TCP connections to the backend.
	MaxSize int

	// DialTimeout is the timeout for establishing a new TCP connection. 0 or negative
	// values are replaced in setDefaults with the effective RequestTimeout so that
	// unconfigured pools always have a bounded dial phase.
	DialTimeout time.Duration

	// RequestTimeout is the deadline for a single write+read round-trip; 0 uses a default.
	RequestTimeout time.Duration

	// RetryCount is how many times to retry after a failed attempt (not counting ErrPoolExhausted).
	// Total attempts = 1 + RetryCount.
	RetryCount int

	// RetryDelays[i] is the sleep before attempt i+1 (after attempt i failed).
	// If shorter than RetryCount, remaining delays default to the last entry or 60ms.
	RetryDelays []time.Duration

	// HeartbeatInterval is how often to ping idle connections; 0 disables app-layer heartbeat.
	HeartbeatInterval time.Duration

	// HeartbeatRequestTimeout is the deadline for a single ping/pong round-trip.
	HeartbeatRequestTimeout time.Duration

	// Pinger implements application-layer heartbeat. Nil skips ping even if heartbeat is enabled.
	Pinger PingFunc

	// IdleTimeout closes idle connections that have been unused longer than this; 0 disables.
	IdleTimeout time.Duration

	// MaintenanceTick is the ticker interval for heartbeat + idle sweep; 0 picks a default.
	MaintenanceTick time.Duration

	// TCPKeepalive enables operating-system TCP keepalive.
	TCPKeepalive bool
	// TCPKeepalivePeriod sets the keepalive probe interval. Zero uses the OS default.
	TCPKeepalivePeriod time.Duration

	// OnClose runs after a connection has been closed.
	OnClose func(conn net.Conn)
}

func (o *Options) setDefaults() {
	if o.MaxSize <= 0 {
		o.MaxSize = 100
	}
	if o.RequestTimeout <= 0 {
		o.RequestTimeout = 5 * time.Second
	}
	if o.DialTimeout <= 0 {
		o.DialTimeout = o.RequestTimeout
	}
	if o.RetryCount < 0 {
		o.RetryCount = 0
	}
	if len(o.RetryDelays) == 0 {
		o.RetryDelays = []time.Duration{
			10 * time.Millisecond,
			30 * time.Millisecond,
			60 * time.Millisecond,
		}
	}
	if o.HeartbeatRequestTimeout <= 0 {
		o.HeartbeatRequestTimeout = 3 * time.Second
	}
	if o.MaintenanceTick <= 0 {
		if o.HeartbeatInterval > 0 {
			t := o.HeartbeatInterval / 2
			if t < time.Second {
				t = time.Second
			}
			o.MaintenanceTick = t
		} else if o.IdleTimeout > 0 {
			t := o.IdleTimeout / 10
			if t < time.Second {
				t = time.Second
			}
			o.MaintenanceTick = t
		} else {
			o.MaintenanceTick = time.Minute
		}
	}
}

func (o *Options) retryDelay(attemptIndex int) time.Duration {
	if len(o.RetryDelays) == 0 {
		return 60 * time.Millisecond
	}
	if attemptIndex < len(o.RetryDelays) {
		return o.RetryDelays[attemptIndex]
	}
	return o.RetryDelays[len(o.RetryDelays)-1]
}
