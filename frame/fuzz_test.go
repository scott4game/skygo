package frame

import (
	"bytes"
	"encoding/binary"
	"errors"
	"testing"

	"github.com/scott4game/skygo/skylog"
)

func FuzzRead(f *testing.F) {
	skylog.SetDefault(skylog.FuncLogger{})
	defer skylog.SetDefault(nil)
	f.Add([]byte(nil))
	f.Add([]byte{0, 0, 0})
	f.Add([]byte{0, 0, 0, 1})
	f.Add([]byte{0, 0, 0, 1, 42})
	f.Add([]byte{0, 64, 0, 1})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})
	f.Fuzz(func(t *testing.T, wire []byte) {
		payload, err := Read(bytes.NewReader(wire))
		if len(wire) < int(HeadLen) {
			if err == nil {
				t.Fatal("short header unexpectedly succeeded")
			}
			return
		}
		declared := binary.BigEndian.Uint32(wire[:HeadLen])
		if declared > MaxPayload {
			if !errors.Is(err, ErrPayloadTooLarge) {
				t.Fatalf("declared=%d error=%v, want ErrPayloadTooLarge", declared, err)
			}
			return
		}
		if uint64(len(wire)-int(HeadLen)) < uint64(declared) {
			if err == nil {
				t.Fatalf("declared=%d available=%d unexpectedly succeeded", declared, len(wire)-int(HeadLen))
			}
			return
		}
		if err != nil {
			t.Fatalf("valid frame error: %v", err)
		}
		if uint32(len(payload)) != declared {
			t.Fatalf("payload length=%d declared=%d", len(payload), declared)
		}
		if !bytes.Equal(payload, wire[HeadLen:uint32(HeadLen)+declared]) {
			t.Fatal("payload does not match declared frame body")
		}
	})
}

func FuzzRoundTrip(f *testing.F) {
	f.Add([]byte(nil))
	f.Add([]byte("skygo"))
	f.Add(bytes.Repeat([]byte{0xff}, 1024))
	f.Fuzz(func(t *testing.T, payload []byte) {
		if uint64(len(payload)) > uint64(MaxPayload) {
			t.Skip()
		}
		var wire bytes.Buffer
		if err := Write(&wire, payload); err != nil {
			t.Fatalf("Write: %v", err)
		}
		got, err := Read(&wire)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("round trip changed %d-byte payload", len(payload))
		}
	})
}
