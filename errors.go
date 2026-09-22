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
	// ErrRecordVersionConflict is returned by VersionedRecordStore.StoreIfVersion when the stored record's
	// version no longer matches the version the mutation was based on, meaning another writer won the race.
	ErrRecordVersionConflict = errors.New("record version conflict")

	// ErrRunCancelled is returned by Await when the awaited workflow run has reached RunStateCancelled before it
	// reached the awaited status. Inspect the wrapped error or use errors.Is to determine the cause.
	ErrRunCancelled = errors.New("workflow run cancelled")
	// ErrRunDeadlineExceeded is returned by Await when the awaited workflow run was cancelled because its
	// record-level deadline elapsed. It wraps ErrRunCancelled.
	ErrRunDeadlineExceeded = fmt.Errorf("%w: workflow run deadline exceeded", ErrRunCancelled)
)

// RunStateDeadlineExceededReason is the RunStateReason recorded when a record is cancelled because its
// record-level deadline elapsed. It distinguishes deadline cancellations from per-stage timeout handling and
// from explicit user cancellations.
const RunStateDeadlineExceededReason = "record deadline exceeded"

// ErrorCounter defines an interface for counting errors keyed by stable labels.
// At least one label is required — labels should identify the process and run (e.g. processName, runID).
type ErrorCounter interface {
	Add(label string, extras ...string) int
	Count(label string, extras ...string) int
	Clear(label string, extras ...string)
}
