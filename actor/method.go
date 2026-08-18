package actor

import (
	"context"
	"fmt"
	"reflect"
)

type methodKind uint8

const (
	methodCall methodKind = iota
	methodNotification
)

type methodToken struct {
	name     string
	kind     methodKind
	request  reflect.Type
	response reflect.Type
}

// CloneFunc detaches a value from its current owner.
type CloneFunc[T any] func(T) (T, error)

type methodConfig[Req, Resp any] struct {
	cloneRequest  CloneFunc[Req]
	cloneResponse CloneFunc[Resp]
}

// MethodOption customizes ownership copying for one request/response method.
type MethodOption[Req, Resp any] func(*methodConfig[Req, Resp])

// WithMethodRequestClone sets the request ownership-copying function.
func WithMethodRequestClone[Req, Resp any](fn CloneFunc[Req]) MethodOption[Req, Resp] {
	return func(cfg *methodConfig[Req, Resp]) { cfg.cloneRequest = fn }
}

// WithMethodResponseClone sets the response ownership-copying function.
func WithMethodResponseClone[Req, Resp any](fn CloneFunc[Resp]) MethodOption[Req, Resp] {
	return func(cfg *methodConfig[Req, Resp]) { cfg.cloneResponse = fn }
}

// Method is the canonical typed descriptor for one request/response protocol.
// Its private token prevents a separately constructed same-name descriptor from
// silently calling a handler registered with another type contract.
type Method[Req, Resp any] struct {
	name          string
	token         *methodToken
	cloneRequest  CloneFunc[Req]
	cloneResponse CloneFunc[Resp]
}

// NewMethod constructs a typed request/response protocol descriptor.
func NewMethod[Req, Resp any](name string, options ...MethodOption[Req, Resp]) Method[Req, Resp] {
	cfg := methodConfig[Req, Resp]{}
	for _, option := range options {
		if option != nil {
			option(&cfg)
		}
	}
	return Method[Req, Resp]{
		name: name,
		token: &methodToken{
			name:     name,
			kind:     methodCall,
			request:  typeOf[Req](),
			response: typeOf[Resp](),
		},
		cloneRequest:  cfg.cloneRequest,
		cloneResponse: cfg.cloneResponse,
	}
}

// Name returns the registered protocol name.
func (m Method[Req, Resp]) Name() string { return m.name }

type notificationConfig[Req any] struct {
	cloneRequest CloneFunc[Req]
}

// NotificationOption customizes ownership copying for a notification.
type NotificationOption[Req any] func(*notificationConfig[Req])

// WithNotificationClone sets the notification request cloning function.
func WithNotificationClone[Req any](fn CloneFunc[Req]) NotificationOption[Req] {
	return func(cfg *notificationConfig[Req]) { cfg.cloneRequest = fn }
}

// Notification is the canonical typed descriptor for one fire-and-forget
// mailbox protocol. Notifications cannot be called synchronously.
type Notification[Req any] struct {
	name         string
	token        *methodToken
	cloneRequest CloneFunc[Req]
}

// NewNotification constructs a typed fire-and-forget protocol descriptor.
func NewNotification[Req any](name string, options ...NotificationOption[Req]) Notification[Req] {
	cfg := notificationConfig[Req]{}
	for _, option := range options {
		if option != nil {
			option(&cfg)
		}
	}
	return Notification[Req]{
		name: name,
		token: &methodToken{
			name:    name,
			kind:    methodNotification,
			request: typeOf[Req](),
		},
		cloneRequest: cfg.cloneRequest,
	}
}

// Name returns the registered protocol name.
func (n Notification[Req]) Name() string { return n.name }

// Register installs a typed method handler on a reserved service.
func Register[Req, Resp any](svc *Service, method Method[Req, Resp], fn func(context.Context, Req) (Resp, error)) error {
	if fn == nil || method.token == nil || method.name == "" || method.token.kind != methodCall {
		return fmt.Errorf("%w: invalid typed method registration", ErrInvalidArgs)
	}
	requestClone, err := resolveClone(method.cloneRequest)
	if err != nil {
		return fmt.Errorf("%w: protocol=%s request: %v", ErrCodec, method.name, err)
	}
	responseClone, err := resolveClone(method.cloneResponse)
	if err != nil {
		return fmt.Errorf("%w: protocol=%s response: %v", ErrCodec, method.name, err)
	}
	codec := typedCodec[Req, Resp]{cloneRequest: requestClone, cloneResponse: responseClone}
	return svc.handle(method.name, HandlerOptions{Codec: codec}, func(ctx context.Context, args []any) (any, error) {
		request, ok := singleArg[Req](args)
		if !ok {
			return nil, fmt.Errorf("%w: typed request for %s", ErrInvalidArgs, method.name)
		}
		return fn(ctx, request)
	}, method.token)
}

// RegisterNotification installs a typed notification handler on a reserved service.
func RegisterNotification[Req any](svc *Service, notification Notification[Req], fn func(context.Context, Req) error) error {
	if fn == nil || notification.token == nil || notification.name == "" || notification.token.kind != methodNotification {
		return fmt.Errorf("%w: invalid typed notification registration", ErrInvalidArgs)
	}
	requestClone, err := resolveClone(notification.cloneRequest)
	if err != nil {
		return fmt.Errorf("%w: protocol=%s request: %v", ErrCodec, notification.name, err)
	}
	responseClone, _ := resolveClone[struct{}](nil)
	codec := typedCodec[Req, struct{}]{cloneRequest: requestClone, cloneResponse: responseClone}
	return svc.handle(notification.name, HandlerOptions{Codec: codec}, func(ctx context.Context, args []any) (any, error) {
		request, ok := singleArg[Req](args)
		if !ok {
			return nil, fmt.Errorf("%w: typed request for %s", ErrInvalidArgs, notification.name)
		}
		return struct{}{}, fn(ctx, request)
	}, notification.token)
}

// Call invokes the typed method and returns its typed response.
func (m Method[Req, Resp]) Call(ctx context.Context, ref Ref, request Req) (Resp, error) {
	var zero Resp
	if err := validateDescriptor(ref, m.name, m.token); err != nil {
		return zero, err
	}
	value, err := Call(ctx, ref, m.name, request)
	if err != nil {
		return zero, err
	}
	response, ok := value.(Resp)
	if !ok {
		return zero, fmt.Errorf("%w: protocol=%s response got=%T want=%v", ErrProtocolTypeMismatch, m.name, value, typeOf[Resp]())
	}
	return response, nil
}

// Send admits this notification to the target service mailbox.
func (n Notification[Req]) Send(ctx context.Context, ref Ref, request Req) error {
	if err := validateDescriptor(ref, n.name, n.token); err != nil {
		return err
	}
	return Send(ctx, ref, n.name, request)
}

// TrySend performs non-blocking mailbox admission for a notification.
func (n Notification[Req]) TrySend(ctx context.Context, ref Ref, request Req) error {
	if err := validateDescriptor(ref, n.name, n.token); err != nil {
		return err
	}
	return TrySend(ctx, ref, n.name, request)
}

func validateDescriptor(ref Ref, name string, token *methodToken) error {
	if token == nil || name == "" {
		return fmt.Errorf("%w: empty descriptor", ErrProtocolTypeMismatch)
	}
	if !ref.valid() {
		return ErrStaleRef
	}
	svc, err := ref.system.service(ref)
	if err != nil {
		return err
	}
	handler, err := svc.handler(name)
	if err != nil {
		return err
	}
	if handler.descriptor != token {
		return fmt.Errorf("%w: service=%s protocol=%s request=%v response=%v", ErrProtocolTypeMismatch, svc.name, name, token.request, token.response)
	}
	return nil
}

type typedCodec[Req, Resp any] struct {
	cloneRequest  CloneFunc[Req]
	cloneResponse CloneFunc[Resp]
}

func (c typedCodec[Req, Resp]) PackRequest(args []any) (Message, error) {
	request, ok := singleArg[Req](args)
	if !ok {
		return Message{}, fmt.Errorf("%w: request got %d arguments", ErrCodec, len(args))
	}
	cloned, err := c.cloneRequest(request)
	return Message{payload: cloned}, err
}

func (c typedCodec[Req, Resp]) UnpackRequest(message Message) ([]any, error) {
	request, ok := message.payload.(Req)
	if !ok {
		return nil, fmt.Errorf("%w: request payload %T", ErrCodec, message.payload)
	}
	return []any{request}, nil
}

func (c typedCodec[Req, Resp]) PackResponse(value any) (Message, error) {
	response, ok := value.(Resp)
	if !ok {
		return Message{}, fmt.Errorf("%w: response payload %T", ErrCodec, value)
	}
	cloned, err := c.cloneResponse(response)
	return Message{payload: cloned}, err
}

func (c typedCodec[Req, Resp]) UnpackResponse(message Message) (any, error) {
	response, ok := message.payload.(Resp)
	if !ok {
		return nil, fmt.Errorf("%w: response payload %T", ErrCodec, message.payload)
	}
	return response, nil
}

func singleArg[T any](args []any) (T, bool) {
	var zero T
	if len(args) != 1 {
		return zero, false
	}
	value, ok := args[0].(T)
	return value, ok
}

func typeOf[T any]() reflect.Type { return reflect.TypeOf((*T)(nil)).Elem() }

func resolveClone[T any](explicit CloneFunc[T]) (CloneFunc[T], error) {
	if explicit != nil {
		return explicit, nil
	}
	if clone, ok := providerClone[T](); ok {
		return clone, nil
	}
	cloneType := reflect.TypeOf((*interface{ Clone() T })(nil)).Elem()
	if typeOf[T]().Implements(cloneType) {
		return func(value T) (T, error) {
			var zero T
			if isNil(value) {
				return zero, nil
			}
			return any(value).(interface{ Clone() T }).Clone(), nil
		}, nil
	}
	if transitivelyImmutable(typeOf[T](), make(map[reflect.Type]bool)) {
		return func(value T) (T, error) { return value, nil }, nil
	}
	return nil, fmt.Errorf(
		"type %v is mutable and has no typed clone: give the method an explicit clone option, "+
			"implement Clone() %[1]v, or import a clone provider such as actor/protoclone",
		typeOf[T]())
}

func isNil[T any](value T) bool {
	rv := reflect.ValueOf(value)
	if !rv.IsValid() {
		return true
	}
	switch rv.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return rv.IsNil()
	default:
		return false
	}
}

func transitivelyImmutable(t reflect.Type, visiting map[reflect.Type]bool) bool {
	if t == nil {
		return false
	}
	if visiting[t] {
		return true
	}
	visiting[t] = true
	defer delete(visiting, t)
	switch t.Kind() {
	case reflect.Bool,
		reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128,
		reflect.String:
		return true
	case reflect.Array:
		return transitivelyImmutable(t.Elem(), visiting)
	case reflect.Struct:
		for i := 0; i < t.NumField(); i++ {
			if !transitivelyImmutable(t.Field(i).Type, visiting) {
				return false
			}
		}
		return true
	default:
		return false
	}
}
