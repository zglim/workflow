package workflow

import (
	"context"
	"fmt"
	"slices"

	"k8s.io/utils/clock"

	"github.com/luno/workflow/internal/graph"
	"github.com/luno/workflow/internal/metrics"
)

type (
	lookupFunc func(ctx context.Context, runID string) (*Record, error)
	storeFunc  func(ctx context.Context, record *Record) error

	updater[Type any, Status StatusType] func(ctx context.Context, current Status, next Status, update *Run[Type, Status], workingVersion uint) error
)

func newUpdater[Type any, Status StatusType](
	lookup lookupFunc,
	store storeFunc,
	graph *graph.Graph,
	clock clock.Clock,
) updater[Type, Status] {
	return func(ctx context.Context, current Status, next Status, record *Run[Type, Status], workingVersion uint) error {
		object, err := Marshal(&record.Object)
		if err != nil {
			return err
		}

		runState := RunStateRunning
		isEnd := graph.IsTerminal(int(next))
		if isEnd {
			runState = RunStateCompleted
		}

		updatedRecord := &Record{
			WorkflowName: record.WorkflowName,
			ForeignID:    record.ForeignID,
			RunID:        record.RunID,
			RunState:     runState,
			Status:       int(next),
			Object:       object,
			CreatedAt:    record.CreatedAt,
			UpdatedAt:    clock.Now(),
			Meta:         record.Meta,
		}

		latest, err := lookup(ctx, updatedRecord.RunID)
		if err != nil {
			return err
		}

		if latest.RunState.Finished() {
			return fmt.Errorf("cannot update record as it is already finished: run_id=%s, run_state=%s", latest.RunID, latest.RunState.String())
		}

		// A business status transition may only overwrite a live, processable record.
		// If the run was paused (or otherwise stopped) after this update loaded its
		// working snapshot, abandon the stale write. The resume event re-delivers the
		// record for processing from its persisted position. This is reported as an
		// optimistic-lock conflict so callers handle it identically to a version clash.
		if latest.RunState.Stopped() && latest.RunState.Valid() {
			return fmt.Errorf("%w: record is %s: run_id=%s", ErrOptimisticLock, latest.RunState.String(), latest.RunID)
		}

		// Ensure that the record still has the intended status. If not then another consumer will be processing this
		// record.
		if latest.Meta.Version != workingVersion {
			return fmt.Errorf("record was modified since it was loaded: run_id=%s, expected_version=%d, actual_version=%d", latest.RunID, workingVersion, latest.Meta.Version)
		}

		// Save and repeat skips transition validation and keeps the current status.
		if isSaveAndRepeat(next) {
			updatedRecord.Status = int(current)
			return storeBusinessUpdate(ctx, store, updatedRecord, current.String())
		}

		err = validateTransition(current, next, graph)
		if err != nil {
			return err
		}

		return storeBusinessUpdate(ctx, store, updatedRecord, next.String())
	}
}

// storeBusinessUpdate commits a status update. The run's lifecycle state only changes
// when a terminal status is reached (Running -> Completed); every other business status
// update keeps the run Running, which is not a run state transition and therefore must
// not be passed through ValidRunStateTransition.
func storeBusinessUpdate(ctx context.Context, store storeFunc, record *Record, statusDescription string) error {
	if record.RunState == RunStateRunning {
		record.Meta.StatusDescription = statusDescription
		record.Meta.Version++
		metrics.RunStateChanges.WithLabelValues(record.WorkflowName, RunStateRunning.String(), RunStateRunning.String()).Inc()

		return store(ctx, record)
	}

	return updateRecord(ctx, store, record, RunStateRunning, statusDescription)
}

func validateTransition[Status StatusType](current, next Status, graph *graph.Graph) error {
	// Lookup all available transitions from the current status
	nodes := graph.Transitions(int(current))
	if len(nodes) == 0 {
		return fmt.Errorf("current status not defined in graph: current=%s", current)
	}

	var found bool
	// Attempt to find the next status amongst the list of valid transitions
	if slices.Contains(nodes, int(next)) {
		found = true
	}

	// If no valid transition matches that of the next status then error.
	if !found {
		return fmt.Errorf("current status not defined in graph: current=%s, next=%s", current, next)
	}

	return nil
}

func updateRecord(
	ctx context.Context,
	store storeFunc,
	record *Record,
	previousRunState RunState,
	statusDescription string,
) error {
	// Every RunState change must be legal per the run state machine. RunStateUnknown is
	// reserved for the initial Trigger insert which creates the record in RunStateInitiated.
	if previousRunState != RunStateUnknown && !ValidRunStateTransition(previousRunState, record.RunState) {
		return fmt.Errorf("%w: from %s | to %s", ErrInvalidTransition, previousRunState, record.RunState)
	}

	// Push run state changes for observability
	metrics.RunStateChanges.WithLabelValues(record.WorkflowName, previousRunState.String(), record.RunState.String()).
		Inc()

	record.Meta.StatusDescription = statusDescription
	// Increment the version by 1.
	record.Meta.Version++

	return store(ctx, record)
}
