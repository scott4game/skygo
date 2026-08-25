// Package gate implements a bounded length-prefixed TCP server for external
// client connections.
package gate

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/scott4game/skygo/frame"
	"github.com/scott4game/skygo/internal/netutil"
	"github.com/scott4game/skygo/skylog"
)

var (
	ErrClosed    = errors.New("gate: closed")
	ErrQueueFull = errors.New("gate: send queue full")
	ErrMaxConn   = errors.New("gate: maximum connections reached")
)

// Handler receives connection lifecycle and complete frame events.
type Handler interface {
	OnOpen(*Conn)
	OnMessage(context.Context, *Conn, []byte)
	OnClose(*Conn, error)
}

// HandlerFuncs adapts callbacks into a Handler.
type HandlerFuncs struct {
	Open    func(*Conn)
	Message func(context.Context, *Conn, []byte)
	Close   func(*Conn, error)
}

func (h HandlerFuncs) OnOpen(conn *Conn) {
	if h.Open != nil {
		h.Open(conn)
	}
}
func (h HandlerFuncs) OnMessage(ctx context.Context, conn *Conn, data []byte) {
	if h.Message != nil {
		h.Message(ctx, conn, data)
	}
}
func (h HandlerFuncs) OnClose(conn *Conn, err error) {
	if h.Close != nil {
		h.Close(conn, err)
	}
}

// Options controls listener and per-connection resource bounds.
type Options struct {
	Address        string
	MaxConnections int
	MaxPayload     uint32
	SendQueue      int
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	SendTimeout    time.Duration
	TCPKeepAlive   time.Duration
}

func (o *Options) defaults() {
	if o.Address == "" {
		o.Address = "127.0.0.1:0"
	}
	if o.MaxConnections <= 0 {
		o.MaxConnections = 1024
	}
	if o.MaxPayload == 0 {
		o.MaxPayload = frame.MaxPayload
	}
	if o.SendQueue <= 0 {
		o.SendQueue = 128
	}
	if o.SendTimeout <= 0 {
		o.SendTimeout = 5 * time.Millisecond
	}
}

// Server accepts and owns client connections.
type Server struct {
	opts        Options
	handler     Handler
	mu          sync.Mutex
	listener    net.Listener
	connections map[uint64]*Conn
	nextID      atomic.Uint64
	stopping    chan struct{}
	stopOnce    sync.Once
	wg          sync.WaitGroup
}

func New(opts Options, handler Handler) (*Server, error) {
	if handler == nil {
		return nil, fmt.Errorf("gate: handler is required")
	}
	opts.defaults()
	return &Server{opts: opts, handler: handler, connections: make(map[uint64]*Conn), stopping: make(chan struct{})}, nil
}

func (s *Server) Start(context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener != nil {
		return fmt.Errorf("gate: already started")
	}
	listener, err := net.Listen("tcp", s.opts.Address)
	if err != nil {
		return err
	}
	s.listener = listener
	s.wg.Add(1)
	go s.acceptLoop(listener)
	return nil
}

func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.listener == nil {
		return nil
	}
	return s.listener.Addr()
}

func (s *Server) acceptLoop(listener net.Listener) {
	defer s.wg.Done()
	for {
		raw, ok := netutil.Accept(listener, s.stopping, func(err error, delay time.Duration) {
			skylog.Errorf(context.Background(), "gate: accept: %v; retrying in %s", err, delay)
		})
		if !ok {
			return
		}
		s.mu.Lock()
		if len(s.connections) >= s.opts.MaxConnections {
			s.mu.Unlock()
			_ = raw.Close()
			continue
		}
		id := s.nextID.Add(1)
		conn := newConn(id, raw, s)
		s.connections[id] = conn
		s.mu.Unlock()
		if tcp, ok := raw.(*net.TCPConn); ok && s.opts.TCPKeepAlive > 0 {
			_ = tcp.SetKeepAlive(true)
			_ = tcp.SetKeepAlivePeriod(s.opts.TCPKeepAlive)
		}
		s.wg.Add(1)
		go func() { defer s.wg.Done(); conn.run() }()
	}
}

func (s *Server) remove(id uint64) { s.mu.Lock(); delete(s.connections, id); s.mu.Unlock() }

func (s *Server) Stop(ctx context.Context) error {
	s.stopOnce.Do(func() {
		close(s.stopping)
		s.mu.Lock()
		if s.listener != nil {
			_ = s.listener.Close()
		}
		for _, conn := range s.connections {
			conn.Close()
		}
		s.mu.Unlock()
	})
	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Conn is one framed client connection.
type Conn struct {
	id        uint64
	raw       net.Conn
	server    *Server
	send      chan []byte
	done      chan struct{}
	closeOnce sync.Once
}

func newConn(id uint64, raw net.Conn, server *Server) *Conn {
	return &Conn{id: id, raw: raw, server: server, send: make(chan []byte, server.opts.SendQueue), done: make(chan struct{})}
}

func (c *Conn) ID() uint64           { return c.id }
func (c *Conn) RemoteAddr() net.Addr { return c.raw.RemoteAddr() }
func (c *Conn) LocalAddr() net.Addr  { return c.raw.LocalAddr() }
func (c *Conn) PeerAddr() string     { return c.raw.RemoteAddr().String() }

func (c *Conn) Send(ctx context.Context, payload []byte) error {
	copyPayload := append([]byte(nil), payload...)
	timer := time.NewTimer(c.server.opts.SendTimeout)
	defer timer.Stop()
	select {
	case <-c.done:
		return ErrClosed
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ErrQueueFull
	case c.send <- copyPayload:
		return nil
	}
}

func (c *Conn) TrySend(payload []byte) error {
	copyPayload := append([]byte(nil), payload...)
	select {
	case <-c.done:
		return ErrClosed
	default:
	}
	select {
	case <-c.done:
		return ErrClosed
	case c.send <- copyPayload:
		return nil
	default:
		return ErrQueueFull
	}
}

func (c *Conn) Close() { c.closeOnce.Do(func() { close(c.done); _ = c.raw.Close() }) }

func (c *Conn) run() {
	c.server.handler.OnOpen(c)
	writeErr := make(chan error, 1)
	go func() {
		err := c.writeLoop()
		c.Close()
		writeErr <- err
	}()
	readErr := c.readLoop()
	c.Close()
	if readErr == nil {
		readErr = <-writeErr
	}
	c.server.remove(c.id)
	c.server.handler.OnClose(c, readErr)
}

func (c *Conn) readLoop() error {
	for {
		if c.server.opts.ReadTimeout > 0 {
			_ = c.raw.SetReadDeadline(time.Now().Add(c.server.opts.ReadTimeout))
		}
		payload, err := frame.ReadLimit(c.raw, c.server.opts.MaxPayload)
		if err != nil {
			return err
		}
		c.server.handler.OnMessage(context.Background(), c, payload)
	}
}

func (c *Conn) writeLoop() error {
	for {
		select {
		case <-c.done:
			return ErrClosed
		case payload := <-c.send:
			if c.server.opts.WriteTimeout > 0 {
				_ = c.raw.SetWriteDeadline(time.Now().Add(c.server.opts.WriteTimeout))
			}
			if err := frame.WriteLimit(c.raw, payload, c.server.opts.MaxPayload); err != nil {
				return err
			}
		}
	}
}
