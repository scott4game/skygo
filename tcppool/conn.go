package tcppool

import (
	"math/rand/v2"
	"net"
	"runtime"
	"sync"
	"time"
)

// WriteFunc writes one application message to an established connection.
// Implementations own framing and serialization. The pool serializes writes
// for each connection, so implementations do not need their own write lock.
type WriteFunc func(conn net.Conn, data []byte) error

// poolConn owns the lifecycle of one connection slot.
type poolConn struct {
	id      int
	pool    *Pool
	sendCh  chan []byte
	conn    net.Conn
	connMu  sync.RWMutex
	connCv  *sync.Cond // broadcasts connection readiness
	once    sync.Once
	stopped chan struct{}
}

func newPoolConn(id int, p *Pool) *poolConn {
	pc := &poolConn{
		id:      id,
		pool:    p,
		sendCh:  make(chan []byte, p.opts.SendBufSize),
		stopped: make(chan struct{}),
	}
	pc.connCv = sync.NewCond(&pc.connMu)
	return pc
}

func (pc *poolConn) start() {
	go pc.lifecycle()
	go pc.sendLoop()
}

func (pc *poolConn) stop() {
	pc.once.Do(func() {
		close(pc.stopped)
		pc.connMu.Lock()
		if pc.conn != nil {
			_ = pc.conn.Close()
			pc.conn = nil
		}
		pc.connCv.Broadcast()
		pc.connMu.Unlock()
	})
}

// send admits data without blocking. It checks stopped first so a send after
// Close does not randomly win against a writable send channel.
func (pc *poolConn) send(data []byte) bool {
	select {
	case <-pc.stopped:
		return false
	default:
	}
	select {
	case <-pc.stopped:
		return false
	case pc.sendCh <- data:
		return true
	default:
		return false
	}
}

func (pc *poolConn) lifecycle() {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 64<<10)
			n := runtime.Stack(buf, false)
			pc.pool.logf("[tcppool] conn[%d] lifecycle panic: %v\n%s", pc.id, r, buf[:n])
		}
	}()
	opts := &pc.pool.opts
	backoff := opts.ReconnectInitial

	for {
		select {
		case <-pc.stopped:
			return
		default:
		}

		var dialOpts []interface{}
		_ = dialOpts

		var conn net.Conn
		var err error
		if opts.DialTimeout > 0 {
			conn, err = net.DialTimeout("tcp", pc.pool.addr, opts.DialTimeout)
		} else {
			conn, err = net.Dial("tcp", pc.pool.addr)
		}

		if err != nil {
			pc.pool.logf("[tcppool] conn[%d] dial %s failed: %v (retry in %v)", pc.id, pc.pool.addr, err, backoff)
			if pc.sleepBackoff(backoff) {
				return
			}
			backoff = nextBackoff(backoff, opts.ReconnectMax, opts.JitterFraction)
			continue
		}

		pc.applyKeepalive(conn)

		if pc.pool.opts.PreReady != nil {
			pc.pool.opts.PreReady(conn)
		}

		pc.connMu.Lock()
		pc.conn = conn
		pc.connCv.Broadcast()
		pc.connMu.Unlock()

		pc.pool.logf("[tcppool] conn[%d] connected to %s", pc.id, pc.pool.addr)
		connAt := time.Now()

		// The read loop blocks until the connection fails.
		if pc.pool.readFn != nil {
			pc.pool.readFn(conn, pc.pool.dispatchOnMsg)
		} else {
			// A send-only pool waits for shutdown.
			<-pc.stopped
			return
		}

		pc.connMu.Lock()
		if pc.conn == conn {
			pc.conn = nil
			pc.connCv.Broadcast()
		}
		pc.connMu.Unlock()
		_ = conn.Close()
		if pc.pool.opts.OnClose != nil {
			pc.pool.opts.OnClose(conn)
		}
		if time.Since(connAt) >= opts.StableThreshold {
			backoff = opts.ReconnectInitial
		}

		select {
		case <-pc.stopped:
			return
		default:
		}

		pc.pool.logf("[tcppool] conn[%d] disconnected, reconnect in %v", pc.id, backoff)
		if pc.sleepBackoff(backoff) {
			return
		}
		backoff = nextBackoff(backoff, opts.ReconnectMax, opts.JitterFraction)
	}
}

func (pc *poolConn) sendLoop() {
	defer func() {
		if r := recover(); r != nil {
			buf := make([]byte, 64<<10)
			n := runtime.Stack(buf, false)
			pc.pool.logf("[tcppool] conn[%d] sendLoop panic: %v\n%s", pc.id, r, buf[:n])
		}
	}()
	for {
		select {
		case <-pc.stopped:
			return
		case data, ok := <-pc.sendCh:
			if !ok {
				return
			}
			conn := pc.waitConn()
			if conn == nil {
				return
			}
			if err := pc.pool.writeFn(conn, data); err != nil {
				pc.pool.logf("[tcppool] conn[%d] write failed: %v", pc.id, err)
				pc.breakConn(conn)
			}
		}
	}
}

// waitConn waits for connection readiness without polling.
func (pc *poolConn) waitConn() net.Conn {
	pc.connMu.Lock()
	defer pc.connMu.Unlock()
	for {
		select {
		case <-pc.stopped:
			return nil
		default:
		}
		if pc.conn != nil {
			return pc.conn
		}
		pc.connCv.Wait()
	}
}

func (pc *poolConn) getConn() net.Conn {
	pc.connMu.RLock()
	defer pc.connMu.RUnlock()
	return pc.conn
}

func (pc *poolConn) breakConn(conn net.Conn) {
	pc.connMu.Lock()
	defer pc.connMu.Unlock()
	if pc.conn == conn {
		_ = conn.Close()
		pc.conn = nil
		pc.connCv.Broadcast()
	}
}

func (pc *poolConn) applyKeepalive(conn net.Conn) {
	if !pc.pool.opts.TCPKeepalive {
		return
	}
	tcp, ok := conn.(*net.TCPConn)
	if !ok {
		return
	}
	_ = tcp.SetKeepAlive(true)
	if pc.pool.opts.TCPKeepalivePeriod > 0 {
		_ = tcp.SetKeepAlivePeriod(pc.pool.opts.TCPKeepalivePeriod)
	}
}

func (pc *poolConn) sleepBackoff(d time.Duration) (stopped bool) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-pc.stopped:
		return true
	case <-t.C:
		return false
	}
}

// nextBackoff doubles the delay and applies bounded positive jitter.
func nextBackoff(cur, max time.Duration, jitterFraction float64) time.Duration {
	next := cur * 2
	if next > max {
		next = max
	}
	if jitterFraction > 0 {
		jitter := time.Duration(float64(next) * jitterFraction * rand.Float64())
		next += jitter
		if next > max {
			next = max
		}
	}
	return next
}
