package workflow

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/luno/workflow/internal/metrics"
)

type TimeoutRecord struct {
	ID           int64
	WorkflowName string
	ForeignID    string
	RunID        string
	Status       int
	Completed    bool
	ExpireAt     time.Time
	CreatedAt    time.Time
}

// pollTimeouts attempts to find the very next expired timeout and execute it
func pollTimeouts[Type any, Status StatusType](
	ctx context.Context,
	w *Workflow[Type, Status],
	status Status,
	timeouts timeouts[Type, Status],
	processName string,
	pollingFrequency time.Duration,
	pauseAfterErrCount int,
) error {
	updateFn := newUpdater[Type, Status](w.recordStore.Lookup, w.recordStore.Store, w.statusGraph, w.clock)
	store := w.recordStore.Store

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		expiredTimeouts, err := w.timeoutStore.ListValid(ctx, w.Name(), int(status), w.clock.Now())
		if err != nil {
			return err
		}

		for _, expiredTimeout := range expiredTimeouts {
			r, err := w.recordStore.Latest(ctx, expiredTimeout.WorkflowName, expiredTimeout.ForeignID)
			if err != nil {
				return err
			}

			if r.Status != int(status) || r.RunState.Finished() {
				// Object has been updated already. Mark timeout as cancelled as it is no longer valid.
				err = w.timeoutStore.Cancel(ctx, expiredTimeout.ID)
				if err != nil {
					return err
				}

				// Continue to next expired timeout
				continue
			}

			if r.RunState.Stopped() {
				w.logger.Debug(ctx, "Skipping processing of timeout of stopped workflow record", map[string]string{
					"workflow":       r.WorkflowName,
					"run_id":         r.RunID,
					"foreign_id":     r.ForeignID,
					"process_name":   processName,
					"current_status": strconv.FormatInt(int64(r.Status), 10),
					"run_state":      r.RunState.String(),
				})

				// Continue to next expired timeout
				continue
			}

			for _, config := range timeouts.transitions {
				t0 := w.clock.Now()
				err = processTimeout(
					ctx,
					w,
					config,
					r,
					expiredTimeout,
					w.timeoutStore.Complete,
					store,
					updateFn,
					processName,
					pauseAfterErrCount,
				)
				if err != nil {
					metrics.ProcessLatency.WithLabelValues(w.Name(), processName).Observe(w.clock.Since(t0).Seconds())
					return err
				}

				metrics.ProcessLatency.WithLabelValues(w.Name(), processName).Observe(w.clock.Since(t0).Seconds())
			}
		}

		err = wait(ctx, pollingFrequency)
		if err != nil {
			return err
		}
	}
}

type completeFunc func(ctx context.Context, id int64) error

// deadlinePoller polls for expired record-level deadlines. It reuses the TimeoutStore (the deadline is persisted
// as a timeout record keyed by the synthetic deadlineTimeoutStatus) so that deadlines do not require a separate
// polling line from the per-stage timeout mechanism.
func deadlinePoller[Type any, Status StatusType](w *Workflow[Type, Status]) {
	role := makeRole(w.Name(), "deadline-consumer")
	processName := makeRole("deadline-consumer")

	pollingFrequency := w.defaultOpts.pollingFrequency
	errBackOff := w.defaultOpts.errBackOff

	w.run(role, processName, func(ctx context.Context) error {
		for {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			expired, err := w.timeoutStore.ListValid(ctx, w.Name(), deadlineTimeoutStatus, w.clock.Now())
			if err != nil {
				return err
			}

			for _, dl := range expired {
				err = processDeadline(ctx, w, dl)
				if err != nil {
					return err
				}
			}

			err = wait(ctx, pollingFrequency)
			if err != nil {
				return err
			}
		}
	}, errBackOff)
}

// processDeadline idempotently moves an expired deadline's record to RunStateCancelled. It is safe to call
// repeatedly: a record that has already reached a terminal state results in the deadline record being completed
// without any further state change. When the store implements VersionedRecordStore the cancellation is a single
// atomic compare-and-swap on the record version so a racing deadline poller or stage consumer can never produce
// a duplicate transition or roll a state back; the loser returns ErrRecordVersionConflict and leaves the
// deadline record uncompleted for the next poll cycle.
func processDeadline[Type any, Status StatusType](
	ctx context.Context,
	w *Workflow[Type, Status],
	dl TimeoutRecord,
) error {
	latest, err := w.recordStore.Lookup(ctx, dl.RunID)
	if errors.Is(err, ErrRecordNotFound) {
		// Record no longer exists - nothing to cancel. Mark the deadline as processed so it isn't retried.
		return w.timeoutStore.Complete(ctx, dl.ID)
	} else if err != nil {
		return err
	}

	if latest.RunState.Finished() {
		// The record has already reached a terminal state (completed, cancelled, or deleted). The deadline must
		// not roll the state back, so mark it as processed.
		return w.timeoutStore.Complete(ctx, dl.ID)
	}

	expectedVersion := latest.Meta.Version

	// Mutate a copy of the record into the cancelled state and validate the run state transition without
	// bypassing the state machine.
	cancelled := *latest
	err = (&runStateControllerImpl{record: &cancelled}).transition(RunStateCancelled, RunStateDeadlineExceededReason)
	if err != nil {
		return fmt.Errorf("deadline cancel error: %w", err)
	}

	// updateRecord increments the version and stamps the status description without persisting.
	cancelled.Meta.Version = expectedVersion
	err = updateRecord(ctx, func(ctx context.Context, r *Record) error {
		versioned, ok := w.recordStore.(VersionedRecordStore)
		if ok {
			return versioned.StoreIfVersion(ctx, r, expectedVersion)
		}

		return w.recordStore.Store(ctx, r)
	}, &cancelled, latest.RunState, latest.Meta.StatusDescription)
	if errors.Is(err, ErrRecordVersionConflict) {
		w.logger.Debug(ctx, "deadline cancellation lost concurrent record update race", map[string]string{
			"workflow":   latest.WorkflowName,
			"run_id":     latest.RunID,
			"foreign_id": latest.ForeignID,
		})

		// Another writer (a stage consumer or an earlier deadline cancellation) won. The deadline record is left
		// uncompleted; the next poll will observe the new state and either complete silently (terminal) or retry
		// the cancellation against the fresh version.
		return nil
	} else if err != nil {
		return err
	}

	return w.timeoutStore.Complete(ctx, dl.ID)
}

func processTimeout[Type any, Status StatusType](
	ctx context.Context,
	w *Workflow[Type, Status],
	config timeout[Type, Status],
	record *Record,
	timeout TimeoutRecord,
	completeFn completeFunc,
	store storeFunc,
	updater updater[Type, Status],
	processName string,
	pauseAfterErrCount int,
) error {
	run, err := buildRun[Type, Status](w.newRunObj(), store, record)
	if err != nil {
		return err
	}

	// Ensure the run is returned to the pool when we're done
	defer w.releaseRun(run)

	next, err := config.TimeoutFunc(ctx, run, w.clock.Now())
	if err != nil {
		_, err := maybePause(ctx, pauseAfterErrCount, w.errorCounter, processName, run, w.logger)
		if err != nil {
			return fmt.Errorf("pause error: %v, meta: %v", err, map[string]string{
				"run_id":     record.RunID,
				"foreign_id": record.ForeignID,
			})
		}

		return nil
	}

	if skipUpdate(next) {
		w.logger.Debug(ctx, "skipping update", map[string]string{
			"description":   skipUpdateDescription(next),
			"workflow_name": w.Name(),
			"foreign_id":    run.ForeignID,
			"run_id":        run.RunID,
			"run_state":     run.RunState.String(),
			"record_status": run.Status.String(),
		})

		metrics.ProcessSkippedEvents.WithLabelValues(w.Name(), processName, "next value specified skip").Inc()
		return nil
	}

	err = updater(ctx, Status(timeout.Status), next, run, record.Meta.Version)
	if err != nil {
		return err
	}

	// Mark timeout as having been executed (aka completed) only in the case that true is returned.
	return completeFn(ctx, timeout.ID)
}

type timeouts[Type any, Status StatusType] struct {
	pollingFrequency   time.Duration
	errBackOff         time.Duration
	lagAlert           time.Duration
	pauseAfterErrCount int
	transitions        []timeout[Type, Status]
}

type timeout[Type any, Status StatusType] struct {
	TimerFunc   TimerFunc[Type, Status]
	TimeoutFunc TimeoutFunc[Type, Status]
}

func timeoutPoller[Type any, Status StatusType](
	w *Workflow[Type, Status],
	status Status,
	timeouts timeouts[Type, Status],
) {
	role := makeRole(w.Name(), strconv.FormatInt(int64(status), 10), "timeout-consumer")
	// readableRole can change in value if the string value of the status enum is changed. It should not be used for
	// storing in the record store, event streamer, timeout store, or offset store.
	processName := makeRole(status.String(), "timeout-consumer")

	errBackOff := w.defaultOpts.errBackOff
	if timeouts.errBackOff > 0 {
		errBackOff = timeouts.errBackOff
	}

	pollingFrequency := w.defaultOpts.pollingFrequency
	if timeouts.pollingFrequency > 0 {
		pollingFrequency = timeouts.pollingFrequency
	}

	pauseAfterErrCount := w.defaultOpts.pauseAfterErrCount
	if timeouts.pauseAfterErrCount != 0 {
		pauseAfterErrCount = timeouts.pauseAfterErrCount
	}

	w.run(role, processName, func(ctx context.Context) error {
		err := pollTimeouts(ctx, w, status, timeouts, processName, pollingFrequency, pauseAfterErrCount)
		if err != nil {
			return err
		}

		return nil
	}, errBackOff)
}

func timeoutAutoInserterConsumer[Type any, Status StatusType](
	w *Workflow[Type, Status],
	status Status,
	timeouts timeouts[Type, Status],
) {
	role := makeRole(w.Name(), strconv.FormatInt(int64(status), 10), "timeout-auto-inserter-consumer")
	processName := makeRole(status.String(), "timeout-auto-inserter-consumer")

	pauseAfterErrCount := w.defaultOpts.pauseAfterErrCount
	if timeouts.pauseAfterErrCount != 0 {
		pauseAfterErrCount = timeouts.pauseAfterErrCount
	}

	errBackOff := w.defaultOpts.errBackOff
	if timeouts.errBackOff > 0 {
		errBackOff = timeouts.errBackOff
	}

	pollingFrequency := w.defaultOpts.pollingFrequency
	if timeouts.pollingFrequency > 0 {
		pollingFrequency = timeouts.pollingFrequency
	}

	lagAlert := w.defaultOpts.lagAlert
	if timeouts.lagAlert > 0 {
		lagAlert = timeouts.lagAlert
	}

	w.run(role, processName, func(ctx context.Context) error {
		consumerFunc := func(ctx context.Context, r *Run[Type, Status]) (Status, error) {
			for _, config := range timeouts.transitions {
				expireAt, err := config.TimerFunc(ctx, r, w.clock.Now())
				if err != nil {
					return 0, err
				}

				if expireAt.IsZero() {
					// Ignore and evaluate the next
					continue
				}

				err = w.timeoutStore.Create(ctx, r.WorkflowName, r.ForeignID, r.RunID, int(status), expireAt)
				if err != nil {
					return 0, err
				}
			}

			// Never update status even when successful
			return 0, nil
		}

		topic := Topic(w.Name(), int(status))
		stream, err := w.eventStreamer.NewReceiver(
			ctx,
			topic,
			role,
			WithReceiverPollFrequency(pollingFrequency),
		)
		if err != nil {
			return err
		}
		defer stream.Close()

		updater := newUpdater[Type, Status](w.recordStore.Lookup, w.recordStore.Store, w.statusGraph, w.clock)
		return consume(
			ctx,
			w.Name(),
			processName,
			stream,
			stepConsumer(
				w.Name(),
				processName,
				consumerFunc,
				status,
				w.recordStore.Lookup,
				w.recordStore.Store,
				w.logger,
				updater,
				pauseAfterErrCount,
				w.errorCounter,
				w.newRunObj(),
				w.releaseRun,
			),
			w.clock,
			0,
			lagAlert,
		)
	}, errBackOff)
}

// TimerFunc exists to allow the specification of when the timeout should expire dynamically. If not time is set then a
// timeout will not be created and the event will be skipped. If the time is set then a timeout will be created and
// once expired TimeoutFunc will be called. Any non-nil error will be retried with backoff.
type TimerFunc[Type any, Status StatusType] func(ctx context.Context, r *Run[Type, Status], now time.Time) (time.Time, error)

// TimeoutFunc runs once the timeout has expired which is set by TimerFunc. If false is returned with a nil error
// then the timeout is skipped and not retried at a later date. If a non-nil error is returned the TimeoutFunc will be
// called again until a nil error is returned. If true is returned with a nil error then the provided record and any
// modifications made to it will be stored and the status updated - continuing the workflow.
type TimeoutFunc[Type any, Status StatusType] func(ctx context.Context, r *Run[Type, Status], now time.Time) (Status, error)
