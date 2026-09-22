package workflow

import (
	"context"
	"errors"
	"strconv"
	"time"
)

func (w *Workflow[Type, Status]) Await(
	ctx context.Context,
	foreignID, runID string,
	status Status,
	opts ...AwaitOption,
) (*Run[Type, Status], error) {
	var opt awaitOpts
	for _, option := range opts {
		option(&opt)
	}

	pollFrequency := w.defaultOpts.pollingFrequency
	if opt.pollFrequency > 0 {
		pollFrequency = opt.pollFrequency
	}

	role := makeRole("await", w.Name(), strconv.FormatInt(int64(status), 10), foreignID)
	return awaitWorkflowStatusByForeignID[Type, Status](ctx, w, status, foreignID, runID, role, pollFrequency)
}

func awaitWorkflowStatusByForeignID[Type any, Status StatusType](
	ctx context.Context,
	w *Workflow[Type, Status],
	status Status,
	foreignID, runID string,
	role string,
	pollFrequency time.Duration,
) (*Run[Type, Status], error) {
	topic := Topic(w.Name(), int(status))
	// Terminal statuses result in the RunState changing to Completed and are stored in the RunStateChangeTopic
	// as it is a key event in the Workflow Run's lifecycle.
	if w.statusGraph.IsTerminal(int(status)) {
		topic = RunStateChangeTopic(w.Name())
	}

	stream, err := w.eventStreamer.NewReceiver(
		ctx,
		topic,
		role,
		WithReceiverPollFrequency(pollFrequency),
		StreamFromLatest(),
	)
	if err != nil {
		return nil, err
	}
	defer stream.Close()

	timer := w.clock.NewTimer(pollFrequency)
	defer timer.Stop()

	// runChanges receives events from the blocking stream receiver so the loop can also periodically poll
	// the record and observe cancellation or deadline expiry without relying solely on an event matching the
	// awaited status (non-terminal cancellations are published to the run state change topic).
	type streamResult struct {
		e   *Event
		ack func() error
		err error
	}
	recv := make(chan streamResult, 1)
	go func() {
		for {
			e, ack, err := stream.Recv(ctx)
			select {
			case recv <- streamResult{e: e, ack: ack, err: err}:
			case <-ctx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	}()

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C():
			timer.Reset(pollFrequency)
			// Periodically check the run state directly so that a deadline driven cancellation (or any
			// cancellation) unblocks Await even when the awaited status is non-terminal and its status
			// topic never receives another event.
			r, err := w.recordStore.Lookup(ctx, runID)
			if errors.Is(err, ErrRecordNotFound) {
				continue
			} else if err != nil {
				return nil, err
			}

			if err := runAwaitStateError(r); err != nil {
				return nil, err
			}
		case result := <-recv:
			if result.err != nil {
				return nil, result.err
			}

			e, ack := result.e, result.ack
			shouldFilter := FilterUsing(e,
				filterByForeignID(foreignID),
				filterByRunID(runID),
			)
			if shouldFilter {
				err := ack()
				if err != nil {
					return nil, err
				}

				continue
			}

			r, err := w.recordStore.Lookup(ctx, e.ForeignID)
			if errors.Is(err, ErrRecordNotFound) {
				err = ack()
				if err != nil {
					return nil, err
				}

				continue
			} else if err != nil {
				return nil, err
			}

			// A run may be cancelled by its record level deadline concurrently with the event arriving.
			// Never return a cancelled run as a successful status match.
			if err := runAwaitStateError(r); err != nil {
				_ = ack()
				return nil, err
			}

			var t Type
			err = Unmarshal(r.Object, &t)
			if err != nil {
				return nil, err
			}

			return &Run[Type, Status]{
				TypedRecord: TypedRecord[Type, Status]{
					Record: *r,
					Status: Status(r.Status),
					Object: &t,
				},
				controller: NewRunStateController(w.recordStore.Store, r),
			}, ack()
		}
	}
}

// runAwaitStateError returns the error that Await must surface when the awaited run has entered a terminal
// cancelled state. Deadline driven cancellations return the distinct ErrRunDeadlineExceeded error so they
// can be told apart from ordinary cancellations and from client side context cancellation.
func runAwaitStateError(r *Record) error {
	if r.RunState != RunStateCancelled {
		return nil
	}

	if r.Meta.RunStateReason == DeadlineExceededReason {
		return ErrRunDeadlineExceeded
	}

	return ErrRunCancelled
}

type awaitOpts struct {
	pollFrequency time.Duration
}

type AwaitOption func(o *awaitOpts)

func WithAwaitPollingFrequency(d time.Duration) AwaitOption {
	return func(o *awaitOpts) {
		o.pollFrequency = d
	}
}
