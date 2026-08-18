// Package frame implements the 4-byte big-endian length-prefixed wire format
// used between the project's services.
//
// It depends only on the standard library and skylog, so it is usable from
// clients, tools and tests that do not link a network framework.
package frame

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/scott4game/skygo/skylog"
)

// HeadLen is the size in bytes of the length prefix preceding every payload.
const HeadLen uint32 = 4

// MaxPayload bounds a single frame's payload. A read whose declared length
// exceeds it is rejected before any allocation, so a malformed or hostile peer
// cannot force an oversized buffer.
const MaxPayload uint32 = 4 * 1024 * 1024

// ErrPayloadTooLarge reports a declared length above MaxPayload.
var ErrPayloadTooLarge = errors.New("too large length-prefixed packet received")

// Read consumes one frame from r and returns its payload.
func Read(r io.Reader) ([]byte, error) {
	ctx := context.Background()
	var dataLen uint32
	if err := binary.Read(r, binary.BigEndian, &dataLen); err != nil {
		skylog.Errorf(ctx, "frame.Read: failed to read length prefix: %v", err)
		return nil, err
	}
	if dataLen > MaxPayload {
		return nil, fmt.Errorf("%w: %d bytes", ErrPayloadTooLarge, dataLen)
	}
	buf := make([]byte, dataLen)
	if _, err := io.ReadFull(r, buf); err != nil {
		skylog.Errorf(ctx, "frame.Read: failed to read payload: %v", err)
		return nil, err
	}

	skylog.Debugf(ctx, "frame.Read: read length-prefixed payload of length %d", dataLen)
	return buf, nil
}

// Write emits payload as one frame. Payloads larger than MaxPayload are
// rejected so a peer using Read can always consume frames produced here.
func Write(w io.Writer, payload []byte) error {
	if uint64(len(payload)) > uint64(MaxPayload) {
		return fmt.Errorf("%w: %d bytes", ErrPayloadTooLarge, len(payload))
	}
	return WriteUnchecked(w, payload)
}

// WriteUnchecked emits payload as one frame without applying MaxPayload. It is
// provided for compatibility with protocols that intentionally use a larger
// read limit. Payloads must still fit in the wire format's uint32 length field.
func WriteUnchecked(w io.Writer, payload []byte) error {
	if uint64(len(payload)) > uint64(^uint32(0)) {
		return fmt.Errorf("%w: %d bytes exceeds uint32 framing", ErrPayloadTooLarge, len(payload))
	}
	var prefix [HeadLen]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(payload)))
	if err := writeFull(w, prefix[:]); err != nil {
		return err
	}
	return writeFull(w, payload)
}

func writeFull(w io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := w.Write(data)
		if n > 0 {
			data = data[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}
