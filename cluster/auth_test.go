package cluster

import (
	"strings"
	"testing"
	"time"

	"github.com/scott4game/skygo/cluster/internal/wire"
)

func signedHello(secret []byte, timestamp time.Time, nonce []byte) *wire.Envelope {
	envelope := &wire.Envelope{
		Version:            protocolVersion,
		Kind:               wire.Kind_KIND_HELLO,
		SourceNode:         "peer",
		Incarnation:        "peer-boot",
		TimestampUnixMilli: timestamp.UnixMilli(),
		Nonce:              nonce,
	}
	envelope.Mac = signHandshake(secret, envelope)
	return envelope
}

func TestVerifyHandshakeRejectsNonceReplay(t *testing.T) {
	secret := []byte("0123456789abcdef")
	node := &Node{
		cfg:    Config{Secret: secret, ClockSkew: time.Second, NonceTTL: time.Minute},
		nonces: make(map[string]time.Time),
	}
	hello := signedHello(secret, time.Now(), []byte("0123456789abcdef"))
	if err := node.verifyHandshake(hello, wire.Kind_KIND_HELLO); err != nil {
		t.Fatal(err)
	}
	if err := node.verifyHandshake(hello, wire.Kind_KIND_HELLO); err == nil || !strings.Contains(err.Error(), "nonce replay") {
		t.Fatalf("replay error=%v", err)
	}
}

func TestVerifyHandshakeRejectsClockSkew(t *testing.T) {
	secret := []byte("0123456789abcdef")
	node := &Node{
		cfg:    Config{Secret: secret, ClockSkew: time.Second, NonceTTL: time.Minute},
		nonces: make(map[string]time.Time),
	}
	hello := signedHello(secret, time.Now().Add(-2*time.Second), []byte("fedcba9876543210"))
	if err := node.verifyHandshake(hello, wire.Kind_KIND_HELLO); err == nil || !strings.Contains(err.Error(), "allowed skew") {
		t.Fatalf("clock-skew error=%v", err)
	}
}
