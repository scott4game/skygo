package actor

import "fmt"

// Message is an opaque ownership-transfer container produced by a protocol Codec.
type Message struct{ payload any }

// Codec defines the pack/unpack boundary for one protocol. PackRequest and
// PackResponse must detach all mutable data from the source actor.
type Codec interface {
	PackRequest([]any) (Message, error)
	UnpackRequest(Message) ([]any, error)
	PackResponse(any) (Message, error)
	UnpackResponse(Message) (any, error)
}

// CodecFuncs is the low-level adapter used by protocol packages to implement
// explicit request and response ownership copying.
type CodecFuncs struct {
	PackRequestFunc    func([]any) (Message, error)
	UnpackRequestFunc  func(Message) ([]any, error)
	PackResponseFunc   func(any) (Message, error)
	UnpackResponseFunc func(Message) (any, error)
}

func (c CodecFuncs) PackRequest(v []any) (Message, error) {
	if c.PackRequestFunc == nil {
		return Message{}, fmt.Errorf("%w: PackRequest not implemented", ErrCodec)
	}
	return c.PackRequestFunc(v)
}

func (c CodecFuncs) UnpackRequest(v Message) ([]any, error) {
	if c.UnpackRequestFunc == nil {
		return nil, fmt.Errorf("%w: UnpackRequest not implemented", ErrCodec)
	}
	return c.UnpackRequestFunc(v)
}

func (c CodecFuncs) PackResponse(v any) (Message, error) {
	if c.PackResponseFunc == nil {
		return Message{}, fmt.Errorf("%w: PackResponse not implemented", ErrCodec)
	}
	return c.PackResponseFunc(v)
}

func (c CodecFuncs) UnpackResponse(v Message) (any, error) {
	if c.UnpackResponseFunc == nil {
		return nil, fmt.Errorf("%w: UnpackResponse not implemented", ErrCodec)
	}
	return c.UnpackResponseFunc(v)
}

// CloneCodec builds a Codec from explicit cloning functions. The unpack side
// transfers ownership of the already-cloned message payload to its recipient.
func CloneCodec(
	cloneRequest func([]any) ([]any, error),
	cloneResponse func(any) (any, error),
) Codec {
	return CodecFuncs{
		PackRequestFunc: func(v []any) (Message, error) {
			if cloneRequest == nil {
				return Message{}, fmt.Errorf("%w: request clone missing", ErrCodec)
			}
			cloned, err := cloneRequest(v)
			return Message{payload: cloned}, err
		},
		UnpackRequestFunc: func(v Message) ([]any, error) {
			args, ok := v.payload.([]any)
			if !ok {
				return nil, fmt.Errorf("%w: invalid request payload %T", ErrCodec, v.payload)
			}
			return args, nil
		},
		PackResponseFunc: func(v any) (Message, error) {
			if cloneResponse == nil {
				return Message{}, fmt.Errorf("%w: response clone missing", ErrCodec)
			}
			cloned, err := cloneResponse(v)
			return Message{payload: cloned}, err
		},
		UnpackResponseFunc: func(v Message) (any, error) { return v.payload, nil },
	}
}
