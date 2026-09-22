package workflow

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"k8s.io/utils/clock"
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
	terminalAwait := w.statusGraph.IsTerminal(int(status))
	if terminalAwait {
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

	// cancelledErr inspects the current record and returns a typed error if the awaited run has been cancelled.
	// A deadline-driven cancellation returns ErrRunDeadlineExceeded (which wraps ErrRunCancelled) while any other
	// cancellation returns ErrRunCancelled wrapped with the recorded reason.
	cancelledErr := func() error {
		r, lerr := w.recordStore.Lookup(ctx, runID)
		if lerr != nil {
			return nil
		}

		if r.RunState != RunStateCancelled {
			return nil
		}

		if r.Meta.RunStateReason == RunStateDeadlineExceededReason {
			return ErrRunDeadlineExceeded
		}

		return fmt.Errorf("%w: %s", ErrRunCancelled, r.Meta.RunStateReason)
	}

	// Non-terminal awaits listen on the status topic, but cancellation events are always published to the
	// RunStateChangeTopic. The timer therefore polls the record periodically so that a deadline elapsing while
	// the run is paused (or sitting on any non-terminal status) still unblocks the await promptly.
	var timer clock.Timer
	if !terminalAwait {
		timer = w.clock.NewTimer(pollFrequency)
		defer timer.Stop()

		if err := cancelledErr(); err != nil {
			return nil, err
		}
	}

	// One long-lived receiver goroutine drives the blocking stream.Recv calls. Every event it delivers must be
	// consumed (acked or returned) before nextCh is signalled, at which point it receives the following event.
	// A poll tick never discards an event: if the timer fires first the main loop simply re-enters the wait.
	type recvResult struct {
		e   *Event
		ack Ack
		err error
	}

	eventsCh := make(chan recvResult)
	nextCh := make(chan struct{}, 1)
	go func() {
		for {
			e, ack, rerr := stream.Recv(ctx)
			select {
			case eventsCh <- recvResult{e: e, ack: ack, err: rerr}:
			case <-ctx.Done():
				return
			}

			select {
			case <-nextCh:
			case <-ctx.Done():
				return
			}
		}
	}()

	for {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		var result recvResult
		if terminalAwait {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case result = <-eventsCh:
			}
		} else {
			// Re-arm the poll timer for this wait.
			if !timer.Stop() {
				select {
				case <-timer.C():
				default:
				}
			}
			timer.Reset(pollFrequency)

			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-timer.C():
				if cerr := cancelledErr(); cerr != nil {
					return nil, cerr
				}

				// No event was consumed on this iteration; loop and wait again.
				continue
			case result = <-eventsCh:
			}
		}

		if result.err != nil {
			return nil, result.err
		}

		// Allow the receiver goroutine to fetch the next event once this event has been processed.
		allowNext := func() {
			select {
			case nextCh <- struct{}{}:
			default:
			}
		}

		// Terminal awaits observe run state changes directly. A cancellation arriving on this topic means the
		// awaited terminal status will never be reached.
		if terminalAwait {
			if cerr := cancelledErr(); cerr != nil {
				_ = result.ack()
				allowNext()
				return nil, cerr
			}
		}

		e := result.e
		ack := result.ack

		shouldFilter := FilterUsing(e,
			filterByForeignID(foreignID),
			filterByRunID(runID),
		)
		if shouldFilter {
			if err := ack(); err != nil {
				return nil, err
			}

			allowNext()
			continue
		}

		r, err := w.recordStore.Lookup(ctx, e.ForeignID)
		if errors.Is(err, ErrRecordNotFound) {
			if err := ack(); err != nil {
				return nil, err
			}

			allowNext()
			continue
		} else if err != nil {
			return nil, err
		}

		var t Type
		if err := Unmarshal(r.Object, &t); err != nil {
			return nil, err
		}

		if err := ack(); err != nil {
			return nil, err
		}

		allowNext()

		return &Run[Type, Status]{
			TypedRecord: TypedRecord[Type, Status]{
				Record: *r,
				Status: Status(r.Status),
				Object: &t,
			},
			controller: NewRunStateController(w.recordStore.Store, r),
		}, nil
	}
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
