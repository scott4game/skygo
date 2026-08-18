package tcpsync

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/scott4game/skygo/skylog"
)

var (
	// ErrPoolExhausted is returned when MaxSize connections exist and none are idle.
	ErrPoolExhausted = errors.New("tcpsync: pool exhausted")

	// ErrClosed is returned after the pool is closed.
	ErrClosed = errors.New("tcpsync: pool closed")
)

// Pool is a bounded pool of TCP connections used for synchronous request/response.
type Pool struct {
	addr string
	opts Options

	idle   chan *Conn
	total  int64 // live connections (idle + in-use)
	mu     sync.Mutex
	closed chan struct{}
	once   sync.Once
}

// New creates a pool; connections are created on demand up to MaxSize.
func New(addr string, opts Options) (*Pool, error) {
	opts.setDefaults()
	if opts.MaxSize <= 0 {
		return nil, fmt.Errorf("tcpsync: MaxSize must be positive")
	}
	p := &Pool{
		addr:   addr,
		opts:   opts,
		idle:   make(chan *Conn, opts.MaxSize),
		closed: make(chan struct{}),
	}
	if opts.HeartbeatInterval > 0 || opts.IdleTimeout > 0 {
		if opts.HeartbeatInterval > 0 && opts.Pinger == nil {
			skylog.Warnf(context.Background(), "tcpsync: heartbeat enabled for %s but Pinger is nil; skipping app-layer ping\n", addr)
		} else {
			skylog.Infof(context.Background(), "tcpsync: heartbeat enabled for %s, Pinger is not nil, starting maintenance loop\n", addr)
		}
		skylog.Infof(context.Background(), "tcpsync: starting maintenance loop for %s", addr)
		go p.maintenanceLoop()
	} else {
		skylog.Infof(context.Background(), "tcpsync: heartbeat disabled for %s, starting maintenance loop\n", addr)
	}
	return p, nil
}

// Addr returns the remote address configured for the pool.
func (p *Pool) Addr() string {
	return p.addr
}

// Do sends req and returns one length-prefixed response frame. Retries on transport errors;
// ErrPoolExhausted is never retried.
func (p *Pool) Do(ctx context.Context, req []byte) ([]byte, error) {
	var lastErr error
	maxAttempts := 1 + p.opts.RetryCount
	for attempt := 0; attempt < maxAttempts; attempt++ {
		if attempt > 0 {
			if err := p.sleepRetry(ctx, attempt-1); err != nil {
				return nil, err
			}
		}
		c, err := p.acquire(ctx)
		if errors.Is(err, ErrPoolExhausted) {
			return nil, err
		}
		if err != nil {
			lastErr = err
			continue
		}
		resp, err := c.sendRecv(ctx, req, p.opts.RequestTimeout)
		p.release(c, err != nil)
		if err == nil {
			return resp, nil
		}
		lastErr = err
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("tcpsync: request failed after %d attempts", maxAttempts)
	}
	return nil, lastErr
}

func (p *Pool) sleepRetry(ctx context.Context, delayIndex int) error {
	d := p.opts.retryDelay(delayIndex)
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.closed:
		return ErrClosed
	case <-t.C:
		return nil
	}
}

func (p *Pool) acquire(ctx context.Context) (*Conn, error) {
	select {
	case <-p.closed:
		return nil, ErrClosed
	default:
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case c := <-p.idle:
		return c, nil
	default:
	}

	p.mu.Lock()
	select {
	case <-p.closed:
		p.mu.Unlock()
		return nil, ErrClosed
	default:
	}
	if p.total >= int64(p.opts.MaxSize) {
		p.mu.Unlock()
		return nil, ErrPoolExhausted
	}
	p.total++
	p.mu.Unlock()

	c, err := p.dialConn(ctx)
	if err != nil {
		p.mu.Lock()
		p.total--
		p.mu.Unlock()
		return nil, err
	}
	return c, nil
}

func (p *Pool) dialConn(ctx context.Context) (*Conn, error) {
	nc, err := (&net.Dialer{Timeout: p.opts.DialTimeout}).DialContext(ctx, "tcp", p.addr)
	if err != nil {
		return nil, err
	}
	applyTCPKeepalive(nc, p.opts)
	return &Conn{
		netConn:  nc,
		pool:     p,
		lastUsed: time.Now(),
	}, nil
}

func applyTCPKeepalive(nc net.Conn, opts Options) {
	if !opts.TCPKeepalive {
		return
	}
	tcp, ok := nc.(*net.TCPConn)
	if !ok {
		return
	}
	_ = tcp.SetKeepAlive(true)
	if opts.TCPKeepalivePeriod > 0 {
		_ = tcp.SetKeepAlivePeriod(opts.TCPKeepalivePeriod)
	}
}

func (p *Pool) release(c *Conn, broken bool) {
	if c == nil {
		return
	}
	if broken {
		c.close()
		p.mu.Lock()
		p.total--
		p.mu.Unlock()
		return
	}
	c.lastUsed = time.Now()
	select {
	case <-p.closed:
		c.close()
		p.mu.Lock()
		p.total--
		p.mu.Unlock()
		return
	case p.idle <- c:
	default:
		c.close()
		p.mu.Lock()
		p.total--
		p.mu.Unlock()
	}
}

// Close shuts down the pool and closes idle connections.
func (p *Pool) Close() {
	p.once.Do(func() {
		close(p.closed)
		for {
			select {
			case c := <-p.idle:
				c.close()
				p.mu.Lock()
				p.total--
				p.mu.Unlock()
			default:
				return
			}
		}
	})
}

func (p *Pool) maintenanceLoop() {
	tick := p.opts.MaintenanceTick
	if tick <= 0 {
		tick = time.Minute
	}
	ticker := time.NewTicker(tick)
	defer ticker.Stop()
	for {
		select {
		case <-p.closed:
			return
		case <-ticker.C:
			p.sweepIdle()
		}
	}
}

func (p *Pool) sweepIdle() {
	select {
	case <-p.closed:
		return
	default:
	}
	var batch []*Conn
	for {
		select {
		case c := <-p.idle:
			batch = append(batch, c)
		default:
			goto drained
		}
	}
drained:
	for _, c := range batch {
		if p.opts.IdleTimeout > 0 && time.Since(c.lastUsed) > p.opts.IdleTimeout {
			c.close()
			p.mu.Lock()
			p.total--
			p.mu.Unlock()
			continue
		}
		if p.opts.HeartbeatInterval > 0 && p.opts.Pinger != nil {
			lastActivity := c.lastUsed
			if c.lastPinged.After(lastActivity) {
				lastActivity = c.lastPinged
			}
			if time.Since(lastActivity) >= p.opts.HeartbeatInterval {
				if err := c.ping(p.opts.HeartbeatRequestTimeout); err != nil {
					c.close()
					p.mu.Lock()
					p.total--
					p.mu.Unlock()
					continue
				}
				c.lastPinged = time.Now()
			}
		}
		select {
		case <-p.closed:
			c.close()
			p.mu.Lock()
			p.total--
			p.mu.Unlock()
		case p.idle <- c:
		default:
			c.close()
			p.mu.Lock()
			p.total--
			p.mu.Unlock()
		}
	}
}
