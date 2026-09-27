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

func TestAdminPeerSecretCannotImpersonateGamePeer(t *testing.T) {
	game := []byte("game-secret-0123456789012345")
	admin := []byte("admin-secret-01234567890123")
	node := &Node{cfg: Config{Secret: game, PeerSecrets: map[string][]byte{"admin-api": admin}, ClockSkew: time.Second, NonceTTL: time.Minute}, nonces: map[string]time.Time{}}
	for _, tc := range []struct {
		source  string
		key     []byte
		allowed bool
	}{{"admin-api", admin, true}, {"admin-api", game, false}, {"logic", admin, false}, {"logic", game, true}} {
		nonce, _ := newNonce(24)
		hello := signedHello(tc.key, time.Now(), nonce)
		hello.SourceNode = tc.source
		hello.Mac = signHandshake(tc.key, hello)
		err := node.verifyHandshake(hello, wire.Kind_KIND_HELLO)
		if (err == nil) != tc.allowed {
			t.Fatalf("source %s allowed=%v error=%v", tc.source, tc.allowed, err)
		}
	}
	ack, err := node.handshake(wire.Kind_KIND_HELLO_ACK, "admin-api")
	if err != nil {
		t.Fatal(err)
	}
	if string(ack.Mac) != string(signHandshake(admin, ack)) {
		t.Fatal("reply used game credential")
	}
}
