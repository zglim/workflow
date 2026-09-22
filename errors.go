package workflow

import "errors"

var (
	ErrRecordNotFound       = errors.New("record not found")
	ErrTimeoutNotFound      = errors.New("timeout not found")
	ErrWorkflowInProgress   = errors.New("current workflow still in progress - retry once complete")
	ErrOutboxRecordNotFound = errors.New("outbox record not found")
	ErrInvalidTransition    = errors.New("invalid transition")

	// ErrRecordVersionConflict is returned when a record update is rejected because the record was modified
	// since the caller loaded it. RecordStore implementations must return this error (wrapped) from Store when
	// the record's Meta.Version is not exactly one greater than the currently stored version. Callers that
	// race with another writer (e.g. Pause vs a timeout poll commit) can use errors.Is to detect the conflict
	// and reload the record instead of clobbering the winning write.
	ErrRecordVersionConflict = errors.New("record version conflict")
)

// ErrorCounter defines an interface for counting errors keyed by stable labels.
// At least one label is required — labels should identify the process and run (e.g. processName, runID).
type ErrorCounter interface {
	Add(label string, extras ...string) int
	Count(label string, extras ...string) int
	Clear(label string, extras ...string)
}
