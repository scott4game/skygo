package cluster

import (
	"io"

	"github.com/scott4game/skygo/cluster/internal/wire"
	"github.com/scott4game/skygo/frame"
	"google.golang.org/protobuf/proto"
)

func readEnvelope(reader io.Reader, max uint32) (*wire.Envelope, error) {
	payload, err := frame.ReadLimit(reader, max)
	if err != nil {
		return nil, err
	}
	envelope := &wire.Envelope{}
	if err := proto.Unmarshal(payload, envelope); err != nil {
		return nil, err
	}
	return envelope, nil
}

func writeEnvelope(writer io.Writer, envelope *wire.Envelope, max uint32) error {
	payload, err := (proto.MarshalOptions{Deterministic: true}).Marshal(envelope)
	if err != nil {
		return err
	}
	return frame.WriteLimit(writer, payload, max)
}
