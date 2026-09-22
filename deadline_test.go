package workflow_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/utils/clock"
	clock_testing "k8s.io/utils/clock/testing"

	"github.com/luno/workflow"
	"github.com/luno/workflow/adapters/memrecordstore"
	"github.com/luno/workflow/adapters/memrolescheduler"
	"github.com/luno/workflow/adapters/memstreamer"
	"github.com/luno/workflow/adapters/memtimeoutstore"
	"github.com/luno/workflow/internal/logger"
)

func newDeadlineWorkflow(
	t *testing.T,
	cl clock.Clock,
	firstStep func(ctx context.Context, r *workflow.Run[string, status]) (status, error),
) (*workflow.Workflow[string, status], *memrecordstore.Store) {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	eventStreamer := memstreamer.New()
	recordStore := memrecordstore.New(memrecordstore.WithOutbox(ctx, eventStreamer, logger.New(io.Discard)))

	b := workflow.NewBuilder[string, status]("deadline workflow")
	b.AddStep(StatusStart, firstStep, StatusMiddle)
	b.AddStep(StatusMiddle, func(ctx context.Context, r *workflow.Run[string, status]) (status, error) {
		return StatusEnd, nil
	}, StatusEnd)

	wf := b.Build(
		eventStreamer,
		recordStore,
		memrolescheduler.New(),
		workflow.WithTimeoutStore(memtimeoutstore.New()),
		workflow.WithClock(cl),
		workflow.WithoutOutbox(),
		workflow.WithDefaultOptions(workflow.PollingFrequency(time.Millisecond)),
	)

	wf.Run(ctx)
	t.Cleanup(wf.Stop)

	return wf, recordStore
}

func waitForRunState(t *testing.T, recordStore *memrecordstore.Store, wf *workflow.Workflow[string, status], foreignID string, want workflow.RunState) {
	t.Helper()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r, err := recordStore.Latest(context.Background(), wf.Name(), foreignID)
		if err == nil && r.RunState == want {
			return
		}

		time.Sleep(5 * time.Millisecond)
	}

	r, err := recordStore.Latest(context.Background(), wf.Name(), foreignID)
	require.NoError(t, err)
	require.Equal(t, want.String(), r.RunState.String())
}

// TestDeadline_cancelsRunningRecord verifies a running record that never reaches a terminal status is cancelled
// with the deadline reason once the absolute deadline elapses.
func TestDeadline_cancelsRunningRecord(t *testing.T) {
	start := time.Date(2024, time.January, 1, 12, 0, 0, 0, time.UTC)
	clock := clock_testing.NewFakeClock(start)

	stepStarted := make(chan struct{})
	release := make(chan struct{})

	wf, recordStore := newDeadlineWorkflow(t, clock, func(ctx context.Context, r *workflow.Run[string, status]) (status, error) {
		select {
		case stepStarted <- struct{}{}:
		default:
		}

		<-release
		return StatusMiddle, nil
	})

	fid := "deadline-running"
	runID, err := wf.Trigger(context.Background(), fid,
		workflow.WithStartingPoint[string, status](StatusStart),
		workflow.WithDeadline[string, status](start.Add(time.Minute)),
	)
	require.NoError(t, err)

	<-stepStarted

	// Persisted deadline is set on the record.
	r, err := recordStore.Lookup(context.Background(), runID)
	require.NoError(t, err)
	require.Equal(t, start.Add(time.Minute), r.Deadline)

	clock.Step(time.Minute + time.Second)

	waitForRunState(t, recordStore, wf, fid, workflow.RunStateCancelled)

	r, err = recordStore.Lookup(context.Background(), runID)
	require.NoError(t, err)
	require.Equal(t, workflow.RunStateCancelled, r.RunState)
	require.Equal(t, workflow.RunStateDeadlineExceededReason, r.Meta.RunStateReason)

	close(release)
}

// TestDeadline_cancelsPausedRecord verifies pausing does not exempt a record from its deadline.
func TestDeadline_cancelsPausedRecord(t *testing.T) {
	start := time.Date(2024, time.January, 2, 12, 0, 0, 0, time.UTC)
	clock := clock_testing.NewFakeClock(start)

	wf, recordStore := newDeadlineWorkflow(t, clock, func(ctx context.Context, r *workflow.Run[string, status]) (status, error) {
		// Pause immediately and never transition.
		return r.Pause(ctx, "manual pause")
	})

	fid := "deadline-paused"
	runID, err := wf.Trigger(context.Background(), fid,
		workflow.WithStartingPoint[string, status](StatusStart),
		workflow.WithDeadline[string, status](start.Add(30*time.Second)),
	)
	require.NoError(t, err)

	waitForRunState(t, recordStore, wf, fid, workflow.RunStatePaused)

	clock.Step(time.Minute)

	waitForRunState(t, recordStore, wf, fid, workflow.RunStateCancelled)

	r, err := recordStore.Lookup(context.Background(), runID)
	require.NoError(t, err)
	require.Equal(t, workflow.RunStateCancelled, r.RunState)
	require.Equal(t, workflow.RunStateDeadlineExceededReason, r.Meta.RunStateReason)
}

// TestDeadline_cancelsInitiatedRecord verifies a record that has not yet been consumed is cancelled straight
// from Initiated.
func TestDeadline_cancelsInitiatedRecord(t *testing.T) {
	start := time.Date(2024, time.January, 3, 12, 0, 0, 0, time.UTC)
	clock := clock_testing.NewFakeClock(start)

	wf, recordStore := newDeadlineWorkflow(t, clock, func(ctx context.Context, r *workflow.Run[string, status]) (status, error) {
		return StatusMiddle, nil
	})

	fid := "deadline-initiated"
	runID, err := wf.Trigger(context.Background(), fid,
		workflow.WithStartingPoint[string, status](StatusStart),
		workflow.WithDeadline[string, status](start.Add(time.Second)),
	)
	require.NoError(t, err)

	clock.Step(2 * time.Second)

	waitForRunState(t, recordStore, wf, fid, workflow.RunStateCancelled)

	r, err := recordStore.Lookup(context.Background(), runID)
	require.NoError(t, err)
	require.Equal(t, workflow.RunStateDeadlineExceededReason, r.Meta.RunStateReason)
}

// TestDeadline_completedRecordUnaffected verifies the deadline does not roll back a completed record.
func TestDeadline_completedRecordUnaffected(t *testing.T) {
	start := time.Date(2024, time.January, 4, 12, 0, 0, 0, time.UTC)
	clock := clock_testing.NewFakeClock(start)

	wf, recordStore := newDeadlineWorkflow(t, clock, func(ctx context.Context, r *workflow.Run[string, status]) (status, error) {
		return StatusMiddle, nil
	})

	fid := "deadline-completed"
	runID, err := wf.Trigger(context.Background(), fid,
		workflow.WithStartingPoint[string, status](StatusStart),
		workflow.WithDeadline[string, status](start.Add(time.Hour)),
	)
	require.NoError(t, err)

	_, err = wf.Await(context.Background(), fid, runID, StatusEnd)
	require.NoError(t, err)

	clock.Step(2 * time.Hour)

	// Give the deadline poller a chance to run.
	time.Sleep(50 * time.Millisecond)

	r, err := recordStore.Lookup(context.Background(), runID)
	require.NoError(t, err)
	require.Equal(t, workflow.RunStateCompleted, r.RunState)
	require.Equal(t, int(StatusEnd), r.Status)
}

// TestDeadline_awaitReturnsDeadlineError verifies an awaiting caller receives the typed deadline error rather
// than a generic cancellation or context error.
func TestDeadline_awaitReturnsDeadlineError(t *testing.T) {
	wf, _ := newDeadlineWorkflow(t, clock.RealClock{}, func(ctx context.Context, r *workflow.Run[string, status]) (status, error) {
		// Block forever so the record never leaves StatusStart.
		<-ctx.Done()
		return 0, ctx.Err()
	})

	fid := "deadline-await"
	runID, err := wf.Trigger(context.Background(), fid,
		workflow.WithStartingPoint[string, status](StatusStart),
		workflow.WithDeadline[string, status](time.Now().Add(50*time.Millisecond)),
	)
	require.NoError(t, err)

	awaitCtx, awaitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer awaitCancel()

	awaitErr := make(chan error, 1)
	go func() {
		_, err := wf.Await(awaitCtx, fid, runID, StatusMiddle,
			workflow.WithAwaitPollingFrequency(5*time.Millisecond),
		)
		awaitErr <- err
	}()

	select {
	case err := <-awaitErr:
		require.Error(t, err)
		require.True(t, errors.Is(err, workflow.ErrRunDeadlineExceeded), "expected deadline error, got: %v", err)
		require.True(t, errors.Is(err, workflow.ErrRunCancelled))
	case <-awaitCtx.Done():
		t.Fatal("await did not return after deadline")
	}
}

// TestDeadline_requiresTimeoutStore verifies configuration errors are surfaced early.
func TestDeadline_requiresTimeoutStore(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	b := workflow.NewBuilder[string, status]("no timeout store")
	b.AddStep(StatusStart, func(ctx context.Context, r *workflow.Run[string, status]) (status, error) {
		return StatusMiddle, nil
	}, StatusMiddle)

	wf := b.Build(
		memstreamer.New(),
		memrecordstore.New(),
		memrolescheduler.New(),
	)
	wf.Run(ctx)
	t.Cleanup(wf.Stop)

	_, err := wf.Trigger(ctx, "x",
		workflow.WithStartingPoint[string, status](StatusStart),
		workflow.WithDeadline[string, status](time.Now().Add(time.Hour)),
	)
	require.ErrorContains(t, err, "TimeoutStore is required")
}

// TestDeadline_noDeadlineBehavesAsBefore verifies backwards compatibility: runs without a deadline carry the
// zero deadline value and complete normally.
func TestDeadline_noDeadlineBehavesAsBefore(t *testing.T) {
	start := time.Date(2024, time.January, 6, 12, 0, 0, 0, time.UTC)
	clock := clock_testing.NewFakeClock(start)

	wf, recordStore := newDeadlineWorkflow(t, clock, func(ctx context.Context, r *workflow.Run[string, status]) (status, error) {
		return StatusMiddle, nil
	})

	fid := "no-deadline"
	runID, err := wf.Trigger(context.Background(), fid,
		workflow.WithStartingPoint[string, status](StatusStart),
	)
	require.NoError(t, err)

	r, err := recordStore.Lookup(context.Background(), runID)
	require.NoError(t, err)
	require.True(t, r.Deadline.IsZero())

	_, err = wf.Await(context.Background(), fid, runID, StatusEnd)
	require.NoError(t, err)

	clock.Step(24 * time.Hour)
	time.Sleep(50 * time.Millisecond)

	r, err = recordStore.Lookup(context.Background(), runID)
	require.NoError(t, err)
	require.Equal(t, workflow.RunStateCompleted, r.RunState)
}
