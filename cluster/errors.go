package cluster

import (
	"errors"
	"fmt"

	"github.com/scott4game/skygo/actor"
	"github.com/scott4game/skygo/cluster/internal/wire"
)

// RemoteError preserves a remote handler message while supporting errors.Is
// for stable actor runtime errors.
type RemoteError struct {
	Code    int32
	Message string
	cause   error
}

func (e *RemoteError) Error() string {
	return fmt.Sprintf("cluster: remote error %d: %s", e.Code, e.Message)
}
func (e *RemoteError) Unwrap() error { return e.cause }

func encodeError(err error) *wire.RemoteError {
	if err == nil {
		return nil
	}
	code := wire.ErrorCode_ERROR_CODE_HANDLER
	switch {
	case errors.Is(err, actor.ErrRemoteUnavailable):
		code = wire.ErrorCode_ERROR_CODE_REMOTE_UNAVAILABLE
	case errors.Is(err, actor.ErrStaleRef):
		code = wire.ErrorCode_ERROR_CODE_STALE_REF
	case errors.Is(err, actor.ErrServiceNotFound):
		code = wire.ErrorCode_ERROR_CODE_SERVICE_NOT_FOUND
	case errors.Is(err, actor.ErrServiceNotReady):
		code = wire.ErrorCode_ERROR_CODE_SERVICE_NOT_READY
	case errors.Is(err, actor.ErrServiceStopping):
		code = wire.ErrorCode_ERROR_CODE_SERVICE_STOPPING
	case errors.Is(err, actor.ErrProtocolNotFound):
		code = wire.ErrorCode_ERROR_CODE_PROTOCOL_NOT_FOUND
	case errors.Is(err, actor.ErrProtocolTypeMismatch):
		code = wire.ErrorCode_ERROR_CODE_PROTOCOL_TYPE_MISMATCH
	case errors.Is(err, actor.ErrCodec):
		code = wire.ErrorCode_ERROR_CODE_CODEC
	case errors.Is(err, actor.ErrMailboxTimeout):
		code = wire.ErrorCode_ERROR_CODE_MAILBOX_TIMEOUT
	case errors.Is(err, actor.ErrMailboxFull):
		code = wire.ErrorCode_ERROR_CODE_MAILBOX_FULL
	case errors.Is(err, actor.ErrCallTimeout):
		code = wire.ErrorCode_ERROR_CODE_CALL_TIMEOUT
	case errors.Is(err, actor.ErrCallCycle):
		code = wire.ErrorCode_ERROR_CODE_CALL_CYCLE
	case errors.Is(err, actor.ErrTransportBackpressure):
		code = wire.ErrorCode_ERROR_CODE_BACKPRESSURE
	}
	return &wire.RemoteError{Code: code, Message: err.Error()}
}

func decodeError(remote *wire.RemoteError) error {
	if remote == nil || remote.GetCode() == wire.ErrorCode_ERROR_CODE_OK {
		return nil
	}
	var cause error
	switch remote.GetCode() {
	case wire.ErrorCode_ERROR_CODE_REMOTE_UNAVAILABLE:
		cause = actor.ErrRemoteUnavailable
	case wire.ErrorCode_ERROR_CODE_STALE_REF:
		cause = actor.ErrStaleRef
	case wire.ErrorCode_ERROR_CODE_SERVICE_NOT_FOUND:
		cause = actor.ErrServiceNotFound
	case wire.ErrorCode_ERROR_CODE_SERVICE_NOT_READY:
		cause = actor.ErrServiceNotReady
	case wire.ErrorCode_ERROR_CODE_SERVICE_STOPPING:
		cause = actor.ErrServiceStopping
	case wire.ErrorCode_ERROR_CODE_PROTOCOL_NOT_FOUND:
		cause = actor.ErrProtocolNotFound
	case wire.ErrorCode_ERROR_CODE_PROTOCOL_TYPE_MISMATCH:
		cause = actor.ErrProtocolTypeMismatch
	case wire.ErrorCode_ERROR_CODE_CODEC:
		cause = actor.ErrCodec
	case wire.ErrorCode_ERROR_CODE_MAILBOX_TIMEOUT:
		cause = actor.ErrMailboxTimeout
	case wire.ErrorCode_ERROR_CODE_MAILBOX_FULL:
		cause = actor.ErrMailboxFull
	case wire.ErrorCode_ERROR_CODE_CALL_TIMEOUT:
		cause = actor.ErrCallTimeout
	case wire.ErrorCode_ERROR_CODE_CALL_CYCLE:
		cause = actor.ErrCallCycle
	case wire.ErrorCode_ERROR_CODE_BACKPRESSURE:
		cause = actor.ErrTransportBackpressure
	}
	return &RemoteError{Code: int32(remote.GetCode()), Message: remote.GetMessage(), cause: cause}
}
