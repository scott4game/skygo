// Package protoclone teaches the actor core how to detach protobuf messages.
//
// Importing it for side effects registers a clone provider that deep-copies any
// method request or response type implementing proto.Message, which is what the
// core did built-in before clone resolution became pluggable:
//
//	import _ "github.com/scott4game/skygo/actor/protoclone"
//
// Import it once per binary, from the package that assembles services. The core
// resolves clones when a handler is registered, not when the method descriptor
// is built, so a blank import anywhere in the binary takes effect in time.
package protoclone

import (
	"fmt"
	"reflect"

	"github.com/scott4game/skygo/actor"

	"google.golang.org/protobuf/proto"
)

var messageType = reflect.TypeOf((*proto.Message)(nil)).Elem()

func init() { actor.RegisterCloneProvider(Provider) }

// Provider reports whether t is a protobuf message and, if so, returns a clone
// function for it. It is registered automatically by importing this package;
// exported so an embedder can control ordering in the provider chain.
func Provider(t reflect.Type) (func(any) (any, error), bool) {
	if t == nil || !t.Implements(messageType) {
		return nil, false
	}
	return func(value any) (any, error) {
		message, ok := value.(proto.Message)
		if !ok {
			return nil, fmt.Errorf("protoclone: value %T does not implement proto.Message", value)
		}
		return proto.Clone(message), nil
	}, true
}
