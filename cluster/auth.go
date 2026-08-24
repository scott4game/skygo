package cluster

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/scott4game/skygo/cluster/internal/wire"
)

const protocolVersion uint32 = 1

func newNonce(size int) ([]byte, error) {
	value := make([]byte, size)
	if _, err := rand.Read(value); err != nil {
		return nil, err
	}
	return value, nil
}

func newIncarnation() (string, error) {
	value, err := newNonce(16)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", value), nil
}

func signHandshake(secret []byte, envelope *wire.Envelope) []byte {
	mac := hmac.New(sha256.New, secret)
	var number [8]byte
	binary.BigEndian.PutUint32(number[:4], envelope.GetVersion())
	_, _ = mac.Write(number[:4])
	binary.BigEndian.PutUint32(number[:4], uint32(envelope.GetKind()))
	_, _ = mac.Write(number[:4])
	writeMACString(mac, envelope.GetSourceNode())
	writeMACString(mac, envelope.GetIncarnation())
	binary.BigEndian.PutUint64(number[:], uint64(envelope.GetTimestampUnixMilli()))
	_, _ = mac.Write(number[:])
	_, _ = mac.Write(envelope.GetNonce())
	return mac.Sum(nil)
}

type hashWriter interface{ Write([]byte) (int, error) }

func writeMACString(w hashWriter, value string) {
	var length [4]byte
	binary.BigEndian.PutUint32(length[:], uint32(len(value)))
	_, _ = w.Write(length[:])
	_, _ = w.Write([]byte(value))
}

func (n *Node) handshake(kind wire.Kind) (*wire.Envelope, error) {
	nonce, err := newNonce(24)
	if err != nil {
		return nil, err
	}
	envelope := &wire.Envelope{
		Version: protocolVersion, Kind: kind, SourceNode: n.cfg.NodeID,
		Incarnation: n.incarnation, TimestampUnixMilli: time.Now().UnixMilli(), Nonce: nonce,
	}
	envelope.Mac = signHandshake(n.cfg.Secret, envelope)
	return envelope, nil
}

func (n *Node) verifyHandshake(envelope *wire.Envelope, expectedKind wire.Kind) error {
	if envelope.GetVersion() != protocolVersion || envelope.GetKind() != expectedKind {
		return fmt.Errorf("cluster: unsupported handshake version or kind")
	}
	if envelope.GetSourceNode() == "" || envelope.GetIncarnation() == "" || len(envelope.GetNonce()) < 16 {
		return fmt.Errorf("cluster: incomplete handshake")
	}
	now := time.Now()
	timestamp := time.UnixMilli(envelope.GetTimestampUnixMilli())
	if timestamp.Before(now.Add(-n.cfg.ClockSkew)) || timestamp.After(now.Add(n.cfg.ClockSkew)) {
		return fmt.Errorf("cluster: handshake timestamp outside allowed skew")
	}
	want := signHandshake(n.cfg.Secret, envelope)
	if len(want) != len(envelope.GetMac()) || subtle.ConstantTimeCompare(want, envelope.GetMac()) != 1 {
		return fmt.Errorf("cluster: handshake authentication failed")
	}
	key := envelope.GetSourceNode() + ":" + fmt.Sprintf("%x", envelope.GetNonce())
	n.nonceMu.Lock()
	defer n.nonceMu.Unlock()
	for existing, expires := range n.nonces {
		if now.After(expires) {
			delete(n.nonces, existing)
		}
	}
	if _, exists := n.nonces[key]; exists {
		return fmt.Errorf("cluster: handshake nonce replay")
	}
	n.nonces[key] = now.Add(n.cfg.NonceTTL)
	return nil
}
