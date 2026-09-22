package workflow_test

import (
	"context"
	"sync/atomic"
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

// TestPausedRecordNotTimeoutProcessed verifies the pause semantics of the timeout poller:
//
//  1. A paused record is not selected for timeout processing even when its timeout has expired.
//  2. The timeout's deadline keeps elapsing while paused (wall clock) - it is not extended.
//  3. Once resumed, the run continues from its pre-pause position and the expired timeout fires
//     exactly once - no status regression and no repeated execution of completed stages.
func TestPausedRecordNotTimeoutProcessed(t *testing.T) {
	var stepCalls, timeoutCalls atomic.Int32

	b := workflow.NewBuilder[string, status]("example")
	b.AddStep(
		StatusStart,
		func(ctx context.Context, r *workflow.Run[string, status]) (status, error) {
			stepCalls.Add(1)
			return StatusMiddle, nil
		},
		StatusMiddle,
	)
	b.AddTimeout(
		StatusMiddle,
		workflow.DurationTimerFunc[string, status](time.Hour),
		func(ctx context.Context, r *workflow.Run[string, status], now time.Time) (status, error) {
			timeoutCalls.Add(1)
			return StatusEnd, nil
		},
		StatusEnd,
	)

	clock := clock_testing.NewFakeClock(time.Now())
	recordStore := memrecordstore.New()
	timeoutStore := memtimeoutstore.New()
	w := b.Build(
		memstreamer.New(),
		recordStore,
		memrolescheduler.New(),
		workflow.WithTimeoutStore(timeoutStore),
		workflow.WithClock(clock),
		workflow.WithDefaultOptions(workflow.PollingFrequency(10*time.Millisecond)),
		workflow.DisablePauseRetry(),
	)

	ctx := t.Context()
	w.Run(ctx)
	t.Cleanup(w.Stop)

	fid := "pause-vs-timeout"
	_, err := w.Trigger(ctx, fid)
	require.NoError(t, err)

	// Wait for the run to reach StatusMiddle where the timeout is armed.
	workflow.Require(t, w, fid, StatusMiddle, "")
	require.Equal(t, int32(1), stepCalls.Load())

	// Wait for the timeout to be armed so that the deadline is fixed before the pause.
	var armedAt time.Time
	require.Eventually(t, func() bool {
		ts, err := timeoutStore.List(ctx, "example")
		require.NoError(t, err)
		if len(ts) == 0 {
			return false
		}
		armedAt = ts[0].ExpireAt
		return true
	}, 5*time.Second, 10*time.Millisecond)

	// The deadline is evaluated against the wall clock from when the status was entered.
	require.Equal(t, clock.Now().Add(time.Hour).UTC(), armedAt.UTC())

	// Pause the run before the timeout expires.
	latest, err := recordStore.Latest(ctx, "example", fid)
	require.NoError(t, err)
	err = workflow.NewRunStateController(recordStore.Store, latest).Pause(ctx, "test pause")
	require.NoError(t, err)

	// Let the deadline lapse while paused. The timeout must not fire.
	clock.Step(2 * time.Hour)
	time.Sleep(500 * time.Millisecond)

	require.Equal(t, int32(0), timeoutCalls.Load())
	latest, err = recordStore.Latest(ctx, "example", fid)
	require.NoError(t, err)
	require.Equal(t, workflow.RunStatePaused, latest.RunState)
	require.Equal(t, int(StatusMiddle), latest.Status)

	// Resume - the already-expired timeout fires and the run completes from its pre-pause position.
	err = workflow.NewRunStateController(recordStore.Store, latest).Resume(ctx)
	require.NoError(t, err)

	workflow.Require(t, w, fid, StatusEnd, "")

	require.Equal(t, int32(1), stepCalls.Load())
	require.Equal(t, int32(1), timeoutCalls.Load())

	latest, err = recordStore.Latest(ctx, "example", fid)
	require.NoError(t, err)
	require.Equal(t, workflow.RunStateCompleted, latest.RunState)
	require.Equal(t, int(StatusEnd), latest.Status)
}
