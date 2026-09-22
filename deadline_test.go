package workflow_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clock_testing "k8s.io/utils/clock/testing"

	"github.com/luno/workflow"
	"github.com/luno/workflow/adapters/memrecordstore"
	"github.com/luno/workflow/adapters/memrolescheduler"
	"github.com/luno/workflow/adapters/memstreamer"
	"github.com/luno/workflow/adapters/memtimeoutstore"
)

func newDeadlineWorkflow(
	t *testing.T,
	clk *clock_testing.FakeClock,
	step func(ctx context.Context, r *workflow.Run[string, status]) (status, error),
) (workflow.API[string, status], *memrecordstore.Store) {
	t.Helper()

	b := workflow.NewBuilder[string, status]("deadline test")
	b.AddStep(StatusStart, step, StatusEnd)

	recordStore := memrecordstore.New(memrecordstore.WithClock(clk))
	wf := b.Build(
		memstreamer.New(),
		recordStore,
		memrolescheduler.New(),
		workflow.WithTimeoutStore(memtimeoutstore.New(memtimeoutstore.WithClock(clk))),
		workflow.WithClock(clk),
		workflow.WithDefaultOptions(
			workflow.PollingFrequency(time.Millisecond),
		),
	)

	return wf, recordStore
}

func waitFor(t *testing.T, timeout time.Duration, fn func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if fn() {
			return
		}

		time.Sleep(time.Millisecond)
	}

	require.True(t, fn(), "condition not met within %s", timeout)
}

// TestDeadline_cancelsRunningRecord verifies that a record mid-stage is moved to Cancelled with the
// deadline reason once the absolute deadline elapses.
func TestDeadline_cancelsRunningRecord(t *testing.T) {
	now := time.Date(2024, time.January, 1, 12, 0, 0, 0, time.UTC)
	clk := clock_testing.NewFakeClock(now)

	started := make(chan struct{})
	release := make(chan struct{})

	wf, recordStore := newDeadlineWorkflow(t, clk, func(ctx context.Context, r *workflow.Run[string, status]) (status, error) {
		close(started)
		<-release
		return StatusEnd, nil
	})

	ctx := context.Background()
	wf.Run(ctx)
	t.Cleanup(wf.Stop)

	deadline := now.Add(10 * time.Second)
	runID, err := wf.Trigger(ctx, "fid-1", workflow.WithDeadline[string, status](deadline))
	require.NoError(t, err)

	<-started

	clk.Step(11 * time.Second)

	waitFor(t, 5*time.Second, func() bool {
		r, err := recordStore.Lookup(context.Background(), runID)
		return err == nil && r.RunState == workflow.RunStateCancelled
	})

	r, err := recordStore.Lookup(context.Background(), runID)
	require.NoError(t, err)
	require.Equal(t, workflow.RunStateCancelled, r.RunState)
	require.Equal(t, workflow.DeadlineExceededReason, r.Meta.RunStateReason)
	require.Equal(t, deadline, r.Meta.Deadline)
	require.Equal(t, int(StatusStart), r.Status, "deadline must not move the record to a new stage")

	// Unblock the step once the record is already terminal. The updater must refuse the late stage
	// transition: the terminal state must not regress.
	close(release)
	time.Sleep(50 * time.Millisecond)

	r, err = recordStore.Lookup(context.Background(), runID)
	require.NoError(t, err)
	require.Equal(t, workflow.RunStateCancelled, r.RunState)
	require.Equal(t, int(StatusStart), r.Status)
}

// TestDeadline_cancelsPausedRecord verifies pausing does not exempt a record from its deadline.
func TestDeadline_cancelsPausedRecord(t *testing.T) {
	now := time.Date(2024, time.January, 1, 12, 0, 0, 0, time.UTC)
	clk := clock_testing.NewFakeClock(now)

	paused := make(chan struct{})

	wf, recordStore := newDeadlineWorkflow(t, clk, func(ctx context.Context, r *workflow.Run[string, status]) (status, error) {
		s, err := r.Pause(ctx, "manual pause")
		close(paused)
		return s, err
	})

	ctx := context.Background()
	wf.Run(ctx)
	t.Cleanup(wf.Stop)

	deadline := now.Add(time.Minute)
	runID, err := wf.Trigger(ctx, "fid-paused", workflow.WithDeadline[string, status](deadline))
	require.NoError(t, err)

	<-paused

	waitFor(t, 5*time.Second, func() bool {
		r, err := recordStore.Lookup(context.Background(), runID)
		return err == nil && r.RunState == workflow.RunStatePaused
	})

	// Advance well past the auto resume interval and the deadline.
	clk.Step(2 * time.Hour)

	waitFor(t, 5*time.Second, func() bool {
		r, err := recordStore.Lookup(context.Background(), runID)
		return err == nil && r.RunState == workflow.RunStateCancelled
	})

	r, err := recordStore.Lookup(context.Background(), runID)
	require.NoError(t, err)
	require.Equal(t, workflow.RunStateCancelled, r.RunState)
	require.Equal(t, workflow.DeadlineExceededReason, r.Meta.RunStateReason)
}

// TestDeadline_awaitReturnsDistinctError verifies Await surfaces a distinct timeout error rather than a
// generic cancellation error.
func TestDeadline_awaitReturnsDistinctError(t *testing.T) {
	now := time.Date(2024, time.January, 1, 12, 0, 0, 0, time.UTC)
	clk := clock_testing.NewFakeClock(now)

	release := make(chan struct{})
	started := make(chan struct{})

	wf, _ := newDeadlineWorkflow(t, clk, func(ctx context.Context, r *workflow.Run[string, status]) (status, error) {
		close(started)
		<-release
		return StatusEnd, nil
	})

	ctx := context.Background()
	wf.Run(ctx)
	t.Cleanup(wf.Stop)

	deadline := now.Add(5 * time.Second)
	runID, err := wf.Trigger(ctx, "fid-await", workflow.WithDeadline[string, status](deadline))
	require.NoError(t, err)

	<-started

	awaitErr := make(chan error, 1)
	go func() {
		_, err := wf.Await(ctx, "fid-await", runID, StatusEnd, workflow.WithAwaitPollingFrequency(time.Millisecond))
		awaitErr <- err
	}()

	clk.Step(6 * time.Second)

	select {
	case err := <-awaitErr:
		require.ErrorIs(t, err, workflow.ErrRunDeadlineExceeded)
		require.ErrorIs(t, err, workflow.ErrRunCancelled)
		require.NotErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("await did not return after deadline")
	}

	close(release)
}

// TestDeadline_notSetBehavesAsBefore verifies backward compatibility: records without a deadline are never
// cancelled by the deadline poller.
func TestDeadline_notSetBehavesAsBefore(t *testing.T) {
	now := time.Date(2024, time.January, 1, 12, 0, 0, 0, time.UTC)
	clk := clock_testing.NewFakeClock(now)

	release := make(chan struct{})
	started := make(chan struct{})

	wf, recordStore := newDeadlineWorkflow(t, clk, func(ctx context.Context, r *workflow.Run[string, status]) (status, error) {
		close(started)
		<-release
		return StatusEnd, nil
	})

	ctx := context.Background()
	wf.Run(ctx)
	t.Cleanup(wf.Stop)

	runID, err := wf.Trigger(ctx, "fid-nodeadline")
	require.NoError(t, err)

	<-started

	clk.Step(24 * time.Hour)
	time.Sleep(50 * time.Millisecond)

	r, err := recordStore.Lookup(context.Background(), runID)
	require.NoError(t, err)
	require.NotEqual(t, workflow.RunStateCancelled, r.RunState)

	close(release)

	waitFor(t, 5*time.Second, func() bool {
		r, err := recordStore.Lookup(context.Background(), runID)
		return err == nil && r.RunState == workflow.RunStateCompleted
	})
}

// TestDeadline_requiresTimeoutStore verifies the configuration error when a deadline is used without a
// TimeoutStore.
func TestDeadline_requiresTimeoutStore(t *testing.T) {
	b := workflow.NewBuilder[string, status]("no timeout store")
	b.AddStep(StatusStart, func(ctx context.Context, r *workflow.Run[string, status]) (status, error) {
		return StatusEnd, nil
	}, StatusEnd)

	wf := b.Build(
		memstreamer.New(),
		memrecordstore.New(),
		memrolescheduler.New(),
	)

	ctx := context.Background()
	wf.Run(ctx)
	t.Cleanup(wf.Stop)

	_, err := wf.Trigger(ctx, "fid-err", workflow.WithDeadline[string, status](time.Now().Add(time.Minute)))
	require.Error(t, err)
}

// TestDeadline_completedRunIsIdempotent verifies that when the record finishes before the deadline, the
// expired deadline row is completed silently and the completed state never regresses.
func TestDeadline_completedRunIsIdempotent(t *testing.T) {
	now := time.Date(2024, time.January, 1, 12, 0, 0, 0, time.UTC)
	clk := clock_testing.NewFakeClock(now)

	wf, recordStore := newDeadlineWorkflow(t, clk, func(ctx context.Context, r *workflow.Run[string, status]) (status, error) {
		return StatusEnd, nil
	})

	ctx := context.Background()
	wf.Run(ctx)
	t.Cleanup(wf.Stop)

	deadline := now.Add(time.Minute)
	runID, err := wf.Trigger(ctx, "fid-done", workflow.WithDeadline[string, status](deadline))
	require.NoError(t, err)

	waitFor(t, 5*time.Second, func() bool {
		r, err := recordStore.Lookup(context.Background(), runID)
		return err == nil && r.RunState == workflow.RunStateCompleted
	})

	// Deadline elapses long after completion.
	clk.Step(2 * time.Minute)
	time.Sleep(100 * time.Millisecond)

	r, err := recordStore.Lookup(context.Background(), runID)
	require.NoError(t, err)
	require.Equal(t, workflow.RunStateCompleted, r.RunState)
}
