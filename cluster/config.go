package cluster

import (
	"fmt"
	"time"

	"github.com/scott4game/skygo/frame"
)

const hardMaxPayload uint32 = 64 * 1024 * 1024

// Config defines one cluster node. The HMAC secret is required and protects
// identity only; operators must keep the listener on a trusted private network.
type Config struct {
	NodeID             string
	Listen             string
	Secret             []byte
	Registry           Registry
	MaxPayload         uint32
	SendQueue          int
	ConnectionsPerPeer int
	DialTimeout        time.Duration
	HandshakeTimeout   time.Duration
	CallTimeout        time.Duration
	SendTimeout        time.Duration
	WriteTimeout       time.Duration
	ClockSkew          time.Duration
	NonceTTL           time.Duration
}

func (c *Config) defaults() error {
	if c.NodeID == "" || c.Listen == "" || len(c.Secret) < 16 || c.Registry == nil {
		return fmt.Errorf("cluster: node ID, listen address, registry, and a 16-byte secret are required")
	}
	if c.MaxPayload == 0 {
		c.MaxPayload = 16 * 1024 * 1024
	}
	if c.MaxPayload < frame.MaxPayload {
		c.MaxPayload = frame.MaxPayload
	}
	if c.MaxPayload > hardMaxPayload {
		return fmt.Errorf("cluster: max payload exceeds %d", hardMaxPayload)
	}
	if c.SendQueue <= 0 {
		c.SendQueue = 1024
	}
	if c.ConnectionsPerPeer <= 0 {
		c.ConnectionsPerPeer = 1
	}
	if c.ConnectionsPerPeer > 16 {
		return fmt.Errorf("cluster: connections per peer exceeds 16")
	}
	if c.DialTimeout <= 0 {
		c.DialTimeout = 3 * time.Second
	}
	if c.HandshakeTimeout <= 0 {
		c.HandshakeTimeout = 5 * time.Second
	}
	if c.CallTimeout <= 0 {
		c.CallTimeout = 5 * time.Second
	}
	if c.SendTimeout <= 0 {
		c.SendTimeout = 5 * time.Millisecond
	}
	if c.WriteTimeout <= 0 {
		c.WriteTimeout = 5 * time.Second
	}
	if c.ClockSkew <= 0 {
		c.ClockSkew = 30 * time.Second
	}
	if c.NonceTTL <= 0 {
		c.NonceTTL = 5 * time.Minute
	}
	return nil
}
