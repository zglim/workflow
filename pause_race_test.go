package workflow_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/luno/workflow"
	"github.com/luno/workflow/adapters/memrecordstore"
	"github.com/luno/workflow/adapters/memrolescheduler"
	"github.com/luno/workflow/adapters/memstreamer"
	"github.com/luno/workflow/adapters/memtimeoutstore"
)

type raceObj struct {
	TimeoutCalls int
}

// TestPausedRecordNotSelectedByTimeoutPoll reproduces the race between an external
// Pause and the timeout poller:
//
//  1. Timeout poller lists an already-expired timeout and loads the record snapshot
//     while the record is still Running.
//  2. Pause commits (Running -> Paused) before the poller commits its timeout update.
//  3. The stale timeout update must be rejected by the optimistic-lock commit and the
//     record must remain Paused; the timeout must not transition the status.
//
// The window is forced by blocking the TimeoutFunc on a gate that is released only
// after Pause has been committed.
func TestPausedRecordNotSelectedByTimeoutPoll(t *testing.T) {
	t.Parallel()

	const (
		raceStatusWaiting status = 900
		raceStatusDone    status = 901
	)

	b := workflow.NewBuilder[raceObj, status]("pause-timeout-race")

	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once

	b.AddTimeout(
		raceStatusWaiting,
		workflow.DurationTimerFunc[raceObj, status](time.Millisecond),
		func(ctx context.Context, r *workflow.Run[raceObj, status], now time.Time) (status, error) {
			once.Do(func() { close(entered) })
			<-release
			r.Object.TimeoutCalls++
			return raceStatusDone, nil
		},
		raceStatusDone,
	).WithOptions(
		workflow.PollingFrequency(5 * time.Millisecond),
	)

	recordStore := memrecordstore.New()
	timeoutStore := memtimeoutstore.New()
	w := b.Build(
		memstreamer.New(),
		recordStore,
		memrolescheduler.New(),
		workflow.WithTimeoutStore(timeoutStore),
		workflow.DisablePauseRetry(),
	)

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	w.Run(ctx)
	t.Cleanup(w.Stop)

	foreignID := "paused-timeout-race"
	var api workflow.API[raceObj, status] = w
	runID, err := api.Trigger(ctx, foreignID, workflow.WithStartingPoint[raceObj, status](raceStatusWaiting))
	require.NoError(t, err)

	workflow.AwaitTimeoutInsert(t, w, foreignID, runID, raceStatusWaiting)

	// Wait until the poller has loaded its Running snapshot and entered the timeout
	// logic, then pause the record while it is blocked mid-processing.
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout func never entered")
	}

	rec, err := recordStore.Lookup(ctx, runID)
	require.NoError(t, err)

	controller := workflow.NewRunStateController(
		workflow.NewOptimisticStoreFunc(recordStore), rec,
	)
	require.NoError(t, controller.Pause(ctx, "race test pause"))

	close(release)

	// Give the poller ample time to attempt (and fail) its stale commit.
	time.Sleep(200 * time.Millisecond)

	latest, err := recordStore.Lookup(ctx, runID)
	require.NoError(t, err)
	require.Equal(t, workflow.RunStatePaused, latest.RunState, "paused record must not be processed by the timeout poller")
	require.Equal(t, int(raceStatusWaiting), latest.Status, "stale timeout update must not transition the status")

	timeouts, err := timeoutStore.List(ctx, w.Name())
	require.NoError(t, err)

	var found bool
	for _, tr := range timeouts {
		if tr.RunID == runID {
			found = true
			require.False(t, tr.Completed, "timeout must not be completed while the record is paused")
		}
	}
	require.True(t, found, "expired timeout remains valid while paused (deadlines advance in wall-clock time)")
}

// TestResumeContinuesFromPersistedPosition reproduces the race between Resume and a
// stale timeout/consumer commit:
//
//  1. A consumer loads the record at status A (version N) and starts work.
//  2. Pause commits (version N+1), then Resume commits (version N+2); the resume
//     event redelivers status A.
//  3. The stale worker attempts to commit its version-N snapshot after Resume.
//
// The stale commit must lose the optimistic-lock arbitration: no status regression,
// no repeated transition of an already-advanced stage.
func TestResumeContinuesFromPersistedPosition(t *testing.T) {
	t.Parallel()

	const (
		raceStatusA status = 20
		raceStatusB status = 21
	)

	recordStore := memrecordstore.New()

	rec := &workflow.Record{
		WorkflowName: "resume-race",
		ForeignID:    "fid",
		RunID:        "run-1",
		RunState:     workflow.RunStateRunning,
		Status:       int(raceStatusA),
		Object:       []byte(`{}`),
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}
	require.NoError(t, recordStore.Store(context.Background(), rec))

	storeFunc := workflow.NewOptimisticStoreFunc(recordStore)

	// commit mutates the record in place and commits it with an incremented version.
	// The version-checked store rejects writes based on the persisted version.
	commit := func(r *workflow.Record) error {
		r.Meta.Version++
		return storeFunc(context.Background(), r)
	}

	// Stale worker snapshot from before Pause/Resume.
	stale, err := recordStore.Lookup(context.Background(), rec.RunID)
	require.NoError(t, err)

	// Pause then Resume commit on top of the snapshot.
	current, err := recordStore.Lookup(context.Background(), rec.RunID)
	require.NoError(t, err)
	require.NoError(t, workflow.NewRunStateController(storeFunc, current).Pause(context.Background(), "pause"))

	current, err = recordStore.Lookup(context.Background(), rec.RunID)
	require.NoError(t, err)
	require.NoError(t, workflow.NewRunStateController(storeFunc, current).Resume(context.Background()))

	// A different (resumed) worker advances A -> B first.
	current, err = recordStore.Lookup(context.Background(), rec.RunID)
	require.NoError(t, err)
	current.Status = int(raceStatusB)
	require.NoError(t, commit(current))

	// The stale worker now attempts its version-N commit and must be rejected.
	stale.RunState = workflow.RunStateRunning
	stale.Status = int(raceStatusB)
	err = commit(stale)
	require.ErrorIs(t, err, workflow.ErrOptimisticLock)

	latest, err := recordStore.Lookup(context.Background(), rec.RunID)
	require.NoError(t, err)
	require.Equal(t, int(raceStatusB), latest.Status)
	require.Equal(t, workflow.RunStateRunning, latest.RunState)
}
