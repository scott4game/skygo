package actor

import (
	"reflect"
	"sync"
)

// CloneProvider supplies ownership-detaching clone functions for types the core
// does not know how to copy on its own. It reports whether it handles t; when
// it does, the returned function must deep-copy a non-nil value of that type.
//
// Providers exist so the actor core stays free of serialization dependencies.
// Support for a message library is added by importing an adapter package, such
// as actor/protoclone for google.golang.org/protobuf.
type CloneProvider func(t reflect.Type) (func(any) (any, error), bool)

var (
	cloneProviderMu sync.RWMutex
	cloneProviders  []CloneProvider
)

// RegisterCloneProvider appends a provider to the resolution chain. Providers
// are consulted in registration order, before the Clone() and immutability
// fallbacks, and only for methods that did not supply an explicit clone.
//
// Call it from an adapter package's init. Registration must happen before the
// first Register or RegisterNotification call; descriptors built by NewMethod
// resolve their clones lazily at handler-registration time, not at construction.
func RegisterCloneProvider(provider CloneProvider) {
	if provider == nil {
		return
	}
	cloneProviderMu.Lock()
	defer cloneProviderMu.Unlock()
	cloneProviders = append(cloneProviders, provider)
}

// providerClone finds the first registered provider handling T and adapts its
// untyped clone function back to CloneFunc[T].
func providerClone[T any]() (CloneFunc[T], bool) {
	cloneProviderMu.RLock()
	providers := cloneProviders
	cloneProviderMu.RUnlock()

	target := typeOf[T]()
	for _, provider := range providers {
		clone, ok := provider(target)
		if !ok || clone == nil {
			continue
		}
		return func(value T) (T, error) {
			var zero T
			if isNil(value) {
				return zero, nil
			}
			cloned, err := clone(value)
			if err != nil {
				return zero, err
			}
			typed, ok := cloned.(T)
			if !ok {
				return zero, newCloneTypeError(cloned, target)
			}
			return typed, nil
		}, true
	}
	return nil, false
}

func newCloneTypeError(got any, want reflect.Type) error {
	return &cloneTypeError{got: reflect.TypeOf(got), want: want}
}

type cloneTypeError struct {
	got  reflect.Type
	want reflect.Type
}

func (e *cloneTypeError) Error() string {
	return "actor: clone provider returned " + typeName(e.got) + ", want " + typeName(e.want)
}

func typeName(t reflect.Type) string {
	if t == nil {
		return "<nil>"
	}
	return t.String()
}
