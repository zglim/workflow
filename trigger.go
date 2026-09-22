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

func (w *Workflow[Type, Status]) Trigger(
	ctx context.Context,
	foreignID string,
	opts ...TriggerOption[Type, Status],
) (runID string, err error) {
	return trigger(ctx, w, w.recordStore.Latest, w.recordStore.Store, foreignID, opts...)
}

func trigger[Type any, Status StatusType](
	ctx context.Context,
	w *Workflow[Type, Status],
	lookup latestLookup,
	store storeFunc,
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

	// A record level deadline is enforced through the same TimeoutStore and polling machinery as per-stage
	// timeouts. It is an explicit configuration error to set a deadline without a TimeoutStore as the
	// deadline would otherwise never fire.
	if !o.deadline.IsZero() && w.timeoutStore == nil {
		return "", fmt.Errorf("trigger failed: record level deadline requires a TimeoutStore to be configured for workflow: %s", w.Name())
	}

	meta := Meta{
		StatusDescription: util.CamelCaseToSpacing(startingStatus.String()),
		TraceOrigin:       stack.Trace(),
		Deadline:          o.deadline,
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
		Meta:         meta,
	}

	err = updateRecord(ctx, store, wr, RunStateUnknown, startingStatus.String())
	if err != nil {
		return "", err
	}

	if !o.deadline.IsZero() {
		// Register the record level deadline with the timeout store. The single deadline row is valid for
		// the entire lifecycle of the run: it is not tied to a stage and is consumed by the workflow wide
		// deadline poller rather than the per-stage timeout consumers.
		err = w.timeoutStore.Create(ctx, w.Name(), foreignID, runID, deadlineTimeoutStatus, o.deadline)
		if err != nil {
			return "", err
		}
	}

	return runID, nil
}

type triggerOpts[Type any, Status StatusType] struct {
	startingPoint Status
	initialValue  *Type
	deadline      time.Time
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

// WithDeadline sets the absolute record level deadline for the triggered run. When the deadline elapses the
// record is transitioned to RunStateCancelled with RunStateReason set to DeadlineExceededReason regardless of
// the status it is currently at, including when it is paused or has not yet been consumed. A deadline covers
// the entire lifecycle of the run and is independent of any per-stage timeouts; when both are configured the
// earliest one to elapse takes effect. Using WithDeadline requires the workflow to be built with a
// TimeoutStore.
func WithDeadline[Type any, Status StatusType](deadline time.Time) TriggerOption[Type, Status] {
	return func(o *triggerOpts[Type, Status]) {
		o.deadline = deadline
	}
}
