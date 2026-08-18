package tcppool

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
)

// ReadFunc reads messages and dispatches them through onMsg until the
// connection fails. The pool invokes it again after each reconnect.
type ReadFunc func(conn net.Conn, onMsg OnMsgFunc)

// OnMsgFunc handles one complete application message read by ReadFunc.
type OnMsgFunc func(data []byte) error

// Pool manages persistent TCP connections to one address with reconnect,
// jittered backoff, and round-robin sending.
type Pool struct {
	addr    string
	opts    Options
	conns   []*poolConn
	counter atomic.Uint64
	writeFn WriteFunc
	readFn  ReadFunc
	onMsg   OnMsgFunc
	logMu   sync.RWMutex
	logFn   func(format string, args ...interface{})
}

// New creates a pool and starts establishing its connections asynchronously.
//
// writeFn is required. A nil readFn creates a send-only pool; onMsg is ignored
// in that case.
func New(addr string, writeFn WriteFunc, readFn ReadFunc, onMsg OnMsgFunc, opts Options) (*Pool, error) {
	if writeFn == nil {
		return nil, fmt.Errorf("tcppool: writeFn is required")
	}
	opts.setDefaults()

	p := &Pool{
		addr:    addr,
		opts:    opts,
		conns:   make([]*poolConn, opts.PoolSize),
		writeFn: writeFn,
		readFn:  readFn,
		onMsg:   onMsg,
		logFn:   defaultLogFn,
	}

	for i := range p.conns {
		pc := newPoolConn(i, p)
		p.conns[i] = pc
		pc.start()
	}

	return p, nil
}

// SetLogger replaces the pool's printf-style logger.
func (p *Pool) SetLogger(fn func(format string, args ...interface{})) {
	if fn != nil {
		p.logMu.Lock()
		p.logFn = fn
		p.logMu.Unlock()
	}
}

// dispatchOnMsg wraps user onMsg to record recv metrics.
func (p *Pool) dispatchOnMsg(data []byte) error {
	if p.onMsg != nil {
		return p.onMsg(data)
	}
	return nil
}

// Send queues data on a round-robin connection and reports false when its
// queue is full or the connection has stopped.
func (p *Pool) Send(data []byte) bool {
	idx := p.counter.Add(1) % uint64(len(p.conns))
	return p.conns[idx].send(data)
}

// SendTo queues data on a specific connection slot for session affinity.
func (p *Pool) SendTo(slot int, data []byte) bool {
	idx := slot % len(p.conns)
	return p.conns[idx].send(data)
}

// Close closes all connections and stops the pool's goroutines.
func (p *Pool) Close() {
	for _, pc := range p.conns {
		pc.stop()
	}
}

// ConnCount returns the number of currently connected slots.
func (p *Pool) ConnCount() int {
	online := 0
	for _, pc := range p.conns {
		if pc.getConn() != nil {
			online++
		}
	}
	return online
}

func (p *Pool) logf(format string, args ...interface{}) {
	p.logMu.RLock()
	fn := p.logFn
	p.logMu.RUnlock()
	fn(format, args...)
}

func defaultLogFn(format string, args ...interface{}) {
	fmt.Printf(format+"\n", args...)
}
