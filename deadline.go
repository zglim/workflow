package workflow

import (
	"context"
	"errors"

	"github.com/luno/workflow/internal/metrics"
)

// deadlineTimeoutStatus is the reserved TimeoutRecord.Status used for record level deadlines. It is a
// negative sentinel value that can never collide with a user defined workflow status or with the internal
// skip types and ensures per-stage timeout pollers (which query for their own status) never pick up
// deadline rows. A single deadline row is created for the whole lifecycle of a run and is consumed by the
// workflow wide deadlinePoller.
const deadlineTimeoutStatus = -100

// deadlineConflictRetries bounds how many times the deadline poller will re-attempt an idempotent
// cancellation when a concurrent transition (e.g. a consumer update or an automatic resume of a paused
// record) races with the deadline cancellation. Any remaining conflict is retried on the next poll as the
// deadline row stays incomplete until cancellation is confirmed.
const deadlineConflictRetries = 3

func deadlinePoller[Type any, Status StatusType](w *Workflow[Type, Status]) {
	role := makeRole(w.Name(), "deadline", "poller")
	processName := makeRole("deadline", "poller")

	w.run(role, processName, func(ctx context.Context) error {
		return pollDeadlines(ctx, w, processName)
	}, w.defaultOpts.errBackOff)
}

func pollDeadlines[Type any, Status StatusType](
	ctx context.Context,
	w *Workflow[Type, Status],
	processName string,
) error {
	store := w.recordStore.Store

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		// Reuse the existing timeout store and polling component: deadline rows are plain timeout rows with
		// the reserved deadlineTimeoutStatus. No separate polling line / store is introduced.
		expired, err := w.timeoutStore.ListValid(ctx, w.Name(), deadlineTimeoutStatus, w.clock.Now())
		if err != nil {
			return err
		}

		for _, expiredDeadline := range expired {
			t0 := w.clock.Now()
			err = processDeadline(ctx, w, expiredDeadline, store)
			metrics.ProcessLatency.WithLabelValues(w.Name(), processName).Observe(w.clock.Since(t0).Seconds())
			if err != nil {
				return err
			}
		}

		err = wait(ctx, w.defaultOpts.pollingFrequency)
		if err != nil {
			return err
		}
	}
}

// processDeadline idempotently transitions the expired record to RunStateCancelled with the deadline
// reason. It is safe to invoke concurrently with consumers and with per-stage timeout handling: only one
// cancellation is persisted as a terminal state, a record that has already finished silently completes the
// deadline row, and concurrent state transitions that overwrite the cancellation are re-attempted.
func processDeadline[Type any, Status StatusType](
	ctx context.Context,
	w *Workflow[Type, Status],
	deadline TimeoutRecord,
	store storeFunc,
) error {
	for attempt := 0; attempt < deadlineConflictRetries; attempt++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		record, err := w.recordStore.Lookup(ctx, deadline.RunID)
		if errors.Is(err, ErrRecordNotFound) {
			// Stale deadline row for a record that no longer exists. Silently complete it.
			return w.timeoutStore.Complete(ctx, deadline.ID)
		} else if err != nil {
			return err
		}

		// Records that have already reached a terminal state must never regress. The deadline has lost the
		// race against a stage transition, a manual cancellation, or another deadline trigger. Complete the
		// row silently so it is not processed again.
		if record.RunState.Finished() {
			return w.timeoutStore.Complete(ctx, deadline.ID)
		}

		// A row without a corresponding persisted deadline (e.g. an old record that predates the feature)
		// is invalid and is discarded.
		recordDeadline := record.Meta.Deadline
		if recordDeadline.IsZero() {
			return w.timeoutStore.Complete(ctx, deadline.ID)
		}

		// The persisted deadline has not elapsed (defensive check against stale rows, e.g. if the deadline
		// was replaced). Leave the row incomplete so it is re-evaluated on the next poll without touching
		// the record.
		if w.clock.Now().Before(recordDeadline) {
			return nil
		}

		// Paused records are not exempt from the deadline. Initiated, Running and Paused all allow the
		// transition to Cancelled, which goes through the standard RunState state machine validation.
		controller := NewRunStateController(store, record)
		err = controller.Cancel(ctx, DeadlineExceededReason)
		if err != nil {
			// A concurrent transition may have moved the record between Lookup and Store. Reload and
			// re-evaluate instead of persisting a potentially stale state.
			continue
		}

		// Confirm that the cancellation is still the persisted state. A consumer update or an automatic
		// resume of a paused record could have raced and overwritten it; in that case cancel again.
		latest, err := w.recordStore.Lookup(ctx, deadline.RunID)
		if err != nil {
			return err
		}

		if latest.RunState == RunStateCancelled && latest.Meta.RunStateReason == DeadlineExceededReason {
			return w.timeoutStore.Complete(ctx, deadline.ID)
		}

		if latest.RunState.Finished() {
			return w.timeoutStore.Complete(ctx, deadline.ID)
		}
	}

	// Leave the deadline row incomplete and rely on the next poll to retry. Returning an error would abort
	// processing of any further expired rows in the current batch.
	return nil
}
