package workflow

import (
	"errors"
	"fmt"
)

var (
	ErrRecordNotFound       = errors.New("record not found")
	ErrTimeoutNotFound      = errors.New("timeout not found")
	ErrWorkflowInProgress   = errors.New("current workflow still in progress - retry once complete")
	ErrOutboxRecordNotFound = errors.New("outbox record not found")
	ErrInvalidTransition    = errors.New("invalid transition")

	// ErrRunCancelled is returned by Await when the awaited workflow run has been moved into the
	// RunStateCancelled terminal state before reaching the awaited status. It is distinct from a context
	// cancellation which is a client side cancellation of the Await call itself.
	ErrRunCancelled = errors.New("workflow run cancelled")
	// ErrRunDeadlineExceeded is returned by Await when the awaited workflow run has been cancelled as a
	// result of its record level deadline elapsing. It wraps ErrRunCancelled so callers can either handle
	// all cancellations or only deadline timeouts specifically.
	ErrRunDeadlineExceeded = fmt.Errorf("workflow run deadline exceeded: %w", ErrRunCancelled)
)

// DeadlineExceededReason is the RunStateReason recorded when a record is cancelled because its record
// level deadline has elapsed. It distinguishes deadline driven cancellations from per-stage timeout
// handling and manual cancellations.
const DeadlineExceededReason = "record deadline exceeded"

// ErrorCounter defines an interface for counting errors keyed by stable labels.
// At least one label is required — labels should identify the process and run (e.g. processName, runID).
type ErrorCounter interface {
	Add(label string, extras ...string) int
	Count(label string, extras ...string) int
	Clear(label string, extras ...string)
}
