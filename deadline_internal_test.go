package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clock_testing "k8s.io/utils/clock/testing"
)

type fakeDeadlineRecordStore struct {
	records    map[string]*Record
	storeCalls int
}

func newFakeDeadlineRecordStore(records ...*Record) *fakeDeadlineRecordStore {
	s := &fakeDeadlineRecordStore{records: make(map[string]*Record)}
	for _, r := range records {
		clone := *r
		s.records[r.RunID] = &clone
	}

	return s
}

func (s *fakeDeadlineRecordStore) Lookup(_ context.Context, runID string) (*Record, error) {
	r, ok := s.records[runID]
	if !ok {
		return nil, ErrRecordNotFound
	}

	clone := *r
	return &clone, nil
}

func (s *fakeDeadlineRecordStore) Store(_ context.Context, record *Record) error {
	s.storeCalls++

	clone := *record
	s.records[record.RunID] = &clone
	return nil
}

type fakeDeadlineTimeoutStore struct {
	completed []int64
}

func (s *fakeDeadlineTimeoutStore) Create(context.Context, string, string, string, int, time.Time) error {
	return nil
}

func (s *fakeDeadlineTimeoutStore) Complete(_ context.Context, id int64) error {
	s.completed = append(s.completed, id)
	return nil
}

func (s *fakeDeadlineTimeoutStore) Cancel(context.Context, int64) error { return nil }
func (s *fakeDeadlineTimeoutStore) List(context.Context, string) ([]TimeoutRecord, error) {
	return nil, nil
}
func (s *fakeDeadlineTimeoutStore) ListValid(context.Context, string, int, time.Time) ([]TimeoutRecord, error) {
	return nil, nil
}

func newDeadlineTestWorkflow(clk *clock_testing.FakeClock, records ...*Record) (*Workflow[string, testStatus], *fakeDeadlineRecordStore, *fakeDeadlineTimeoutStore) {
	rs := newFakeDeadlineRecordStore(records...)
	ts := &fakeDeadlineTimeoutStore{}
	w := &Workflow[string, testStatus]{
		name:         "example",
		clock:        clk,
		recordStore:  &deadlineStoreHolder{rs},
		timeoutStore: ts,
	}

	return w, rs, ts
}

type deadlineStoreHolder struct {
	*fakeDeadlineRecordStore
}

func (h *deadlineStoreHolder) Latest(context.Context, string, string) (*Record, error) {
	return nil, ErrRecordNotFound
}

func (h *deadlineStoreHolder) List(context.Context, string, int64, int, OrderType, ...RecordFilter) ([]Record, error) {
	return nil, nil
}

func (h *deadlineStoreHolder) ListOutboxEvents(context.Context, string, int64) ([]OutboxEvent, error) {
	return nil, nil
}

func (h *deadlineStoreHolder) DeleteOutboxEvent(context.Context, string) error { return nil }

func deadlineRecord(state RunState, deadlineAt time.Time, reason string) *Record {
	return &Record{
		WorkflowName: "example",
		ForeignID:    "fid",
		RunID:        "run-1",
		RunState:     state,
		Status:       int(statusStart),
		Meta: Meta{
			Deadline:       deadlineAt,
			RunStateReason: reason,
		},
	}
}

func TestProcessDeadline(t *testing.T) {
	now := time.Date(2024, time.May, 1, 0, 0, 0, 0, time.UTC)
	row := TimeoutRecord{ID: 7, WorkflowName: "example", ForeignID: "fid", RunID: "run-1", Status: deadlineTimeoutStatus}

	t.Run("cancels initiated record and completes row", func(t *testing.T) {
		clk := clock_testing.NewFakeClock(now)
		w, rs, ts := newDeadlineTestWorkflow(clk, deadlineRecord(RunStateInitiated, now.Add(-time.Second), ""))

		err := processDeadline(t.Context(), w, row, rs.Store)
		require.NoError(t, err)

		r, err := rs.Lookup(t.Context(), "run-1")
		require.NoError(t, err)
		require.Equal(t, RunStateCancelled, r.RunState)
		require.Equal(t, DeadlineExceededReason, r.Meta.RunStateReason)
		require.Equal(t, []int64{7}, ts.completed)
	})

	t.Run("cancels paused record", func(t *testing.T) {
		clk := clock_testing.NewFakeClock(now)
		w, rs, _ := newDeadlineTestWorkflow(clk, deadlineRecord(RunStatePaused, now.Add(-time.Second), "manual pause"))

		err := processDeadline(t.Context(), w, row, rs.Store)
		require.NoError(t, err)

		r, err := rs.Lookup(t.Context(), "run-1")
		require.NoError(t, err)
		require.Equal(t, RunStateCancelled, r.RunState)
		require.Equal(t, DeadlineExceededReason, r.Meta.RunStateReason)
	})

	t.Run("already cancelled is idempotent and row completed once", func(t *testing.T) {
		clk := clock_testing.NewFakeClock(now)
		w, rs, ts := newDeadlineTestWorkflow(clk, deadlineRecord(RunStateCancelled, now.Add(-time.Second), DeadlineExceededReason))

		err := processDeadline(t.Context(), w, row, rs.Store)
		require.NoError(t, err)
		err = processDeadline(t.Context(), w, row, rs.Store)
		require.NoError(t, err)

		require.Equal(t, 0, rs.storeCalls, "no state must be rewritten for a finished record")
		require.Equal(t, []int64{7, 7}, ts.completed)

		r, err := rs.Lookup(t.Context(), "run-1")
		require.NoError(t, err)
		require.Equal(t, RunStateCancelled, r.RunState)
	})

	t.Run("completed record never regresses", func(t *testing.T) {
		clk := clock_testing.NewFakeClock(now)
		w, rs, _ := newDeadlineTestWorkflow(clk, deadlineRecord(RunStateCompleted, now.Add(-time.Second), ""))

		err := processDeadline(t.Context(), w, row, rs.Store)
		require.NoError(t, err)

		r, err := rs.Lookup(t.Context(), "run-1")
		require.NoError(t, err)
		require.Equal(t, RunStateCompleted, r.RunState)
		require.Equal(t, 0, rs.storeCalls)
	})

	t.Run("record without deadline silently completes row", func(t *testing.T) {
		clk := clock_testing.NewFakeClock(now)
		w, rs, _ := newDeadlineTestWorkflow(clk, deadlineRecord(RunStateRunning, time.Time{}, ""))

		err := processDeadline(t.Context(), w, row, rs.Store)
		require.NoError(t, err)

		r, err := rs.Lookup(t.Context(), "run-1")
		require.NoError(t, err)
		require.Equal(t, RunStateRunning, r.RunState)
	})

	t.Run("record deadline newer than expired row leaves the row for a later poll", func(t *testing.T) {
		// ListValid only returns rows past their expire_at, but the persisted record could theoretically
		// carry a later deadline than the row (defensive consistency check). It must not be cancelled on
		// the basis of a stale row and the row must remain incomplete.
		clk := clock_testing.NewFakeClock(now)
		w, rs, ts := newDeadlineTestWorkflow(clk, deadlineRecord(RunStateRunning, now.Add(time.Minute), ""))

		err := processDeadline(t.Context(), w, row, rs.Store)
		require.NoError(t, err)

		r, err := rs.Lookup(t.Context(), "run-1")
		require.NoError(t, err)
		require.Equal(t, RunStateRunning, r.RunState)
		require.Empty(t, ts.completed, "row must remain incomplete until the record deadline truly elapses")
	})

	t.Run("concurrent overwrite is re-attempted", func(t *testing.T) {
		clk := clock_testing.NewFakeClock(now)
		rec := deadlineRecord(RunStatePaused, now.Add(-time.Second), "manual pause")
		w, rs, _ := newDeadlineTestWorkflow(clk, rec)

		// Make the first cancel store look like it was overwritten concurrently with a resume by swapping
		// the persisted state after Store via a wrapper.
		baseStore := rs
		storeCalls := 0
		wrappingStore := func(ctx context.Context, record *Record) error {
			err := baseStore.Store(ctx, record)
			if err != nil {
				return err
			}

			storeCalls++
			if storeCalls == 1 {
				// Simulate an automatic resume racing and overwriting the cancellation.
				racing := *record
				racing.RunState = RunStateRunning
				racing.Meta.RunStateReason = ""
				baseStore.records[record.RunID] = &racing
			}

			return nil
		}

		err := processDeadline(t.Context(), w, row, wrappingStore)
		require.NoError(t, err)

		r, err := rs.Lookup(t.Context(), "run-1")
		require.NoError(t, err)
		require.Equal(t, RunStateCancelled, r.RunState)
		require.Equal(t, DeadlineExceededReason, r.Meta.RunStateReason)
		require.Equal(t, 2, storeCalls, "deadline must retry after the concurrent overwrite")
	})

	t.Run("missing record completes stale row", func(t *testing.T) {
		clk := clock_testing.NewFakeClock(now)
		w, rs, ts := newDeadlineTestWorkflow(clk)

		err := processDeadline(t.Context(), w, row, rs.Store)
		require.NoError(t, err)
		require.Equal(t, []int64{7}, ts.completed)
	})
}

func TestRunAwaitStateError(t *testing.T) {
	require.ErrorIs(t, runAwaitStateError(&Record{RunState: RunStateCancelled, Meta: Meta{RunStateReason: DeadlineExceededReason}}), ErrRunDeadlineExceeded)
	require.ErrorIs(t, runAwaitStateError(&Record{RunState: RunStateCancelled, Meta: Meta{RunStateReason: "manual"}}), ErrRunCancelled)
	require.NoError(t, runAwaitStateError(&Record{RunState: RunStateRunning}))
	require.NoError(t, runAwaitStateError(&Record{RunState: RunStateCompleted}))
}
