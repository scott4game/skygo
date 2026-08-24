// Package protowire builds actor protocols with deterministic protobuf wire
// encoding for both local ownership transfer and remote cluster calls.
package protowire

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/scott4game/skygo/actor"
	"google.golang.org/protobuf/proto"
)

// NewMethod constructs a typed actor method that is safe to call through a
// remote Ref. Factories must return non-nil messages of the declared types.
func NewMethod[Req proto.Message, Resp proto.Message](name string, newRequest func() Req, newResponse func() Resp) actor.Method[Req, Resp] {
	codec := &methodCodec[Req, Resp]{
		fingerprint: fingerprint("call", name, messageName(newRequest), messageName(newResponse)),
		newRequest:  newRequest,
		newResponse: newResponse,
	}
	return actor.NewMethod[Req, Resp](name, actor.WithMethodWireCodec[Req, Resp](codec))
}

// NewNotification constructs a one-way typed actor protocol that is safe to
// send through a remote Ref.
func NewNotification[Req proto.Message](name string, newRequest func() Req) actor.Notification[Req] {
	codec := &notificationCodec[Req]{
		fingerprint: fingerprint("notification", name, messageName(newRequest), ""),
		newRequest:  newRequest,
	}
	return actor.NewNotification[Req](name, actor.WithNotificationWireCodec[Req](codec))
}

func fingerprint(kind, name, request, response string) string {
	digest := sha256.Sum256([]byte(kind + "\x00" + name + "\x00" + request + "\x00" + response))
	return hex.EncodeToString(digest[:])
}

func messageName[T proto.Message](factory func() T) string {
	if factory == nil {
		return "<nil>"
	}
	message := factory()
	if isNilMessage(message) {
		return "<nil>"
	}
	return string(message.ProtoReflect().Descriptor().FullName())
}

func isNilMessage(message proto.Message) bool {
	return message == nil || !message.ProtoReflect().IsValid()
}

func marshal(message proto.Message) ([]byte, error) {
	if isNilMessage(message) {
		return nil, fmt.Errorf("protowire: nil protobuf message")
	}
	return (proto.MarshalOptions{Deterministic: true}).Marshal(message)
}

type methodCodec[Req proto.Message, Resp proto.Message] struct {
	fingerprint string
	newRequest  func() Req
	newResponse func() Resp
}

func (c *methodCodec[Req, Resp]) Fingerprint() string { return c.fingerprint }

func (c *methodCodec[Req, Resp]) PackRequest(args []any) (actor.Message, error) {
	request, err := one[Req](args)
	if err != nil {
		return actor.Message{}, err
	}
	return actor.NewMessage(proto.Clone(request).(Req)), nil
}

func (c *methodCodec[Req, Resp]) UnpackRequest(message actor.Message) ([]any, error) {
	request, ok := actor.MessageValue(message).(Req)
	if !ok {
		return nil, fmt.Errorf("protowire: request payload %T", actor.MessageValue(message))
	}
	return []any{request}, nil
}

func (c *methodCodec[Req, Resp]) PackResponse(value any) (actor.Message, error) {
	response, ok := value.(Resp)
	if !ok || isNilMessage(response) {
		return actor.Message{}, fmt.Errorf("protowire: response payload %T", value)
	}
	return actor.NewMessage(proto.Clone(response).(Resp)), nil
}

func (c *methodCodec[Req, Resp]) UnpackResponse(message actor.Message) (any, error) {
	response, ok := actor.MessageValue(message).(Resp)
	if !ok {
		return nil, fmt.Errorf("protowire: response payload %T", actor.MessageValue(message))
	}
	return response, nil
}

func (c *methodCodec[Req, Resp]) MarshalRequest(args []any) ([]byte, error) {
	request, err := one[Req](args)
	if err != nil {
		return nil, err
	}
	return marshal(request)
}

func (c *methodCodec[Req, Resp]) UnmarshalRequest(data []byte) ([]any, error) {
	if c.newRequest == nil {
		return nil, fmt.Errorf("protowire: request factory is nil")
	}
	request := c.newRequest()
	if isNilMessage(request) {
		return nil, fmt.Errorf("protowire: request factory returned nil")
	}
	if err := proto.Unmarshal(data, request); err != nil {
		return nil, err
	}
	return []any{request}, nil
}

func (c *methodCodec[Req, Resp]) MarshalResponse(value any) ([]byte, error) {
	response, ok := value.(Resp)
	if !ok {
		return nil, fmt.Errorf("protowire: response payload %T", value)
	}
	return marshal(response)
}

func (c *methodCodec[Req, Resp]) UnmarshalResponse(data []byte) (any, error) {
	if c.newResponse == nil {
		return nil, fmt.Errorf("protowire: response factory is nil")
	}
	response := c.newResponse()
	if isNilMessage(response) {
		return nil, fmt.Errorf("protowire: response factory returned nil")
	}
	if err := proto.Unmarshal(data, response); err != nil {
		return nil, err
	}
	return response, nil
}

type notificationCodec[Req proto.Message] struct {
	fingerprint string
	newRequest  func() Req
}

func (c *notificationCodec[Req]) Fingerprint() string { return c.fingerprint }

func (c *notificationCodec[Req]) PackRequest(args []any) (actor.Message, error) {
	request, err := one[Req](args)
	if err != nil {
		return actor.Message{}, err
	}
	return actor.NewMessage(proto.Clone(request).(Req)), nil
}

func (c *notificationCodec[Req]) UnpackRequest(message actor.Message) ([]any, error) {
	request, ok := actor.MessageValue(message).(Req)
	if !ok {
		return nil, fmt.Errorf("protowire: request payload %T", actor.MessageValue(message))
	}
	return []any{request}, nil
}

func (c *notificationCodec[Req]) PackResponse(value any) (actor.Message, error) {
	return actor.NewMessage(struct{}{}), nil
}

func (c *notificationCodec[Req]) UnpackResponse(actor.Message) (any, error) {
	return struct{}{}, nil
}

func (c *notificationCodec[Req]) MarshalRequest(args []any) ([]byte, error) {
	request, err := one[Req](args)
	if err != nil {
		return nil, err
	}
	return marshal(request)
}

func (c *notificationCodec[Req]) UnmarshalRequest(data []byte) ([]any, error) {
	if c.newRequest == nil {
		return nil, fmt.Errorf("protowire: request factory is nil")
	}
	request := c.newRequest()
	if isNilMessage(request) {
		return nil, fmt.Errorf("protowire: request factory returned nil")
	}
	if err := proto.Unmarshal(data, request); err != nil {
		return nil, err
	}
	return []any{request}, nil
}

func (c *notificationCodec[Req]) MarshalResponse(any) ([]byte, error)   { return nil, nil }
func (c *notificationCodec[Req]) UnmarshalResponse([]byte) (any, error) { return struct{}{}, nil }

func one[T proto.Message](args []any) (T, error) {
	var zero T
	if len(args) != 1 {
		return zero, fmt.Errorf("protowire: got %d arguments, want 1", len(args))
	}
	value, ok := args[0].(T)
	if !ok || isNilMessage(value) {
		return zero, fmt.Errorf("protowire: request payload %T", args[0])
	}
	return value, nil
}
