package actor

import "fmt"

// RequireArgs verifies the dynamic protocol arity before values are decoded.
func RequireArgs(args []any, count int) error {
	if len(args) != count {
		return fmt.Errorf("%w: got %d arguments, want %d", ErrInvalidArgs, len(args), count)
	}
	return nil
}

// Arg decodes one dynamically-dispatched protocol argument without panicking.
func Arg[T any](args []any, index int) (T, error) {
	var zero T
	if index < 0 || index >= len(args) {
		return zero, fmt.Errorf("%w: argument %d is missing", ErrInvalidArgs, index)
	}
	value, ok := args[index].(T)
	if !ok {
		return zero, fmt.Errorf("%w: argument %d has type %T", ErrInvalidArgs, index, args[index])
	}
	return value, nil
}
