package tcpsync

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/scott4game/skygo/frame"
	"github.com/scott4game/skygo/skylog"
)

// Conn is one leased TCP connection; not safe for concurrent use.
type Conn struct {
	netConn    net.Conn
	pool       *Pool
	lastUsed   time.Time
	lastPinged time.Time
	mu         sync.Mutex
}

func (c *Conn) sendRecv(ctx context.Context, req []byte, timeout time.Duration) ([]byte, error) {
	if c.netConn == nil {
		return nil, fmt.Errorf("tcpsync: closed connection")
	}
	deadline := time.Now().Add(timeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	if err := c.netConn.SetDeadline(deadline); err != nil {
		return nil, err
	}
	defer func() { _ = c.netConn.SetDeadline(time.Time{}) }()

	if err := frame.Write(c.netConn, req); err != nil {
		skylog.Errorf(ctx, "Conn.sendRecv: failed to write timeout:%v request: %v", timeout, err)
		return nil, err
	}
	skylog.Debugf(ctx, "Conn.sendRecv: wait read response, remoteAddr:%v, timeout:%v, localAddr:%+v", c.netConn.RemoteAddr(), timeout, c.netConn.LocalAddr().Network())
	return frame.Read(c.netConn)
}

func (c *Conn) ping(timeout time.Duration) error {
	if c.pool == nil || c.pool.opts.Pinger == nil {
		return nil
	}
	return c.pool.opts.Pinger(c.netConn, timeout)
}

func (c *Conn) close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.netConn != nil {
		nc := c.netConn
		_ = nc.Close()
		c.netConn = nil
		if c.pool != nil && c.pool.opts.OnClose != nil {
			c.pool.opts.OnClose(nc)
		}
	}
}
