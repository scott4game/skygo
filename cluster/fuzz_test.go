package cluster

import (
	"bytes"
	"testing"

	"github.com/scott4game/skygo/cluster/internal/wire"
)

func FuzzReadEnvelope(f *testing.F) {
	for _, envelope := range []*wire.Envelope{
		{Version: protocolVersion, Kind: wire.Kind_KIND_PING, SourceNode: "a", RequestId: 1},
		{Version: protocolVersion, Kind: wire.Kind(255), SourceNode: "unknown"},
		{Version: protocolVersion, Kind: wire.Kind_KIND_CANCEL, RequestId: ^uint64(0)},
	} {
		var framed bytes.Buffer
		if err := writeEnvelope(&framed, envelope, 1<<20); err != nil {
			f.Fatal(err)
		}
		f.Add(framed.Bytes())
	}
	f.Add([]byte{0, 0, 0, 5, 0xff})
	f.Fuzz(func(t *testing.T, data []byte) {
		_, _ = readEnvelope(bytes.NewReader(data), 1<<20)
	})
}

func FuzzDecodeWireError(f *testing.F) {
	f.Add(int32(0), "")
	f.Add(int32(13), "transport_backpressure")
	f.Add(int32(255), "\x00\xffunknown")
	f.Fuzz(func(t *testing.T, code int32, message string) {
		_ = decodeError(&wire.RemoteError{Code: wire.ErrorCode(code), Message: message})
	})
}
