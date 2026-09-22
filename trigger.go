package workflow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"

	"github.com/luno/workflow/internal/stack"
	"github.com/luno/workflow/internal/util"
)

// deadlineTimeoutStatus is the synthetic status used when persisting a record-level deadline in the TimeoutStore.
// It never collides with a user defined status because those are registered in the status graph while the deadline
// poller is the only consumer querying this value.
const deadlineTimeoutStatus = -100

func (w *Workflow[Type, Status]) Trigger(
	ctx context.Context,
	foreignID string,
	opts ...TriggerOption[Type, Status],
) (runID string, err error) {
	return trigger(ctx, w, w.recordStore.Latest, w.recordStore.Store, w.timeoutStore, foreignID, opts...)
}

func trigger[Type any, Status StatusType](
	ctx context.Context,
	w *Workflow[Type, Status],
	lookup latestLookup,
	store storeFunc,
	timeoutStore TimeoutStore,
	foreignID string,
	opts ...TriggerOption[Type, Status],
) (runID string, err error) {
	if !w.calledRun {
		return "", fmt.Errorf("trigger failed: workflow is not running")
	}

	var o triggerOpts[Type, Status]
	for _, fn := range opts {
		fn(&o)
	}

	startingStatus := w.defaultStartingPoint
	if o.startingPoint != Status(0) {
		startingStatus = o.startingPoint
	}

	var deadline time.Time
	if o.deadline != nil {
		deadline = *o.deadline
		if !deadline.IsZero() && deadline.Before(w.clock.Now()) {
			return "", fmt.Errorf("trigger failed: deadline must be in the future: %s", deadline)
		}

		if !deadline.IsZero() && timeoutStore == nil {
			return "", fmt.Errorf("trigger failed: a TimeoutStore is required when setting a run deadline")
		}
	}

	if !w.statusGraph.IsValid(int(startingStatus)) {
		w.logger.Debug(
			w.ctx,
			fmt.Sprintf("ensure %v is configured for workflow: %v", startingStatus, w.Name()),
			map[string]string{},
		)

		return "", fmt.Errorf("trigger failed: status provided is not configured for workflow: %s", startingStatus)
	}

	var t Type
	if o.initialValue != nil {
		t = *o.initialValue
	}

	object, err := Marshal(&t)
	if err != nil {
		return "", err
	}

	lastRecord, err := lookup(ctx, w.Name(), foreignID)
	if errors.Is(err, ErrRecordNotFound) {
		lastRecord = &Record{}
	} else if err != nil {
		return "", err
	}

	// Check that the last run has completed before triggering a new run.
	if lastRecord.RunState.Valid() && !lastRecord.RunState.Finished() {
		// Cannot trigger a new run for this foreignID if there is a workflow in progress.
		return "", ErrWorkflowInProgress
	}

	meta := Meta{
		StatusDescription: util.CamelCaseToSpacing(startingStatus.String()),
		TraceOrigin:       stack.Trace(),
	}

	uid, err := uuid.NewRandom()
	if err != nil {
		return "", err
	}

	runID = uid.String()
	wr := &Record{
		WorkflowName: w.Name(),
		ForeignID:    foreignID,
		RunID:        runID,
		RunState:     RunStateInitiated,
		Status:       int(startingStatus),
		Object:       object,
		CreatedAt:    w.clock.Now(),
		UpdatedAt:    w.clock.Now(),
		Deadline:     deadline,
		Meta:         meta,
	}

	err = updateRecord(ctx, store, wr, RunStateUnknown, startingStatus.String())
	if err != nil {
		return "", err
	}

	if !deadline.IsZero() {
		// Persist the deadline through the existing TimeoutStore so that the deadline poller can reuse the same
		// polling machinery as per-stage timeouts. The record-level deadline is keyed by the synthetic
		// deadlineTimeoutStatus to keep it separate from stage timeouts.
		err = timeoutStore.Create(ctx, w.Name(), foreignID, runID, deadlineTimeoutStatus, deadline)
		if err != nil {
			return "", err
		}
	}

	return runID, nil
}

type triggerOpts[Type any, Status StatusType] struct {
	startingPoint Status
	initialValue  *Type
	deadline      *time.Time
}

type TriggerOption[Type any, Status StatusType] func(o *triggerOpts[Type, Status])

func WithStartingPoint[Type any, Status StatusType](startingStatus Status) TriggerOption[Type, Status] {
	return func(o *triggerOpts[Type, Status]) {
		o.startingPoint = startingStatus
	}
}

func WithInitialValue[Type any, Status StatusType](t *Type) TriggerOption[Type, Status] {
	return func(o *triggerOpts[Type, Status]) {
		o.initialValue = t
	}
}

// WithDeadline sets an absolute record-level deadline for the run. Once the deadline elapses the record is
// moved to RunStateCancelled (reason: record deadline exceeded) regardless of the status it has reached and
// regardless of whether it is Initiated, Running, or Paused. Unlike a per-stage timeout (AddTimeout) the
// deadline spans the entire lifecycle of the record; when both are configured whichever fires first takes
// effect. A TimeoutStore must be configured on the workflow when using this option.
func WithDeadline[Type any, Status StatusType](deadline time.Time) TriggerOption[Type, Status] {
	return func(o *triggerOpts[Type, Status]) {
		o.deadline = &deadline
	}
}
