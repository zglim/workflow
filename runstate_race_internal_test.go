package workflow

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	clock_testing "k8s.io/utils/clock/testing"

	"github.com/luno/workflow/internal/errorcounter"
	"github.com/luno/workflow/internal/graph"
)

// versionedRecordStore is a minimal in-memory RecordStore test double that enforces the optimistic locking
// contract required of RecordStore implementations: an update to an existing record is rejected with
// ErrRecordVersionConflict unless its Meta.Version is exactly one greater than the stored version.
type versionedRecordStore struct {
	mu     sync.Mutex
	record *Record

	// onLookup and beforeStore are one-shot hooks that allow tests to deterministically interleave a
	// concurrent write between a reader's snapshot and its subsequent commit.
	onLookup    func()
	beforeStore func()
}

func newVersionedRecordStore(record *Record) *versionedRecordStore {
	cp := *record
	return &versionedRecordStore{record: &cp}
}

func (s *versionedRecordStore) Lookup(ctx context.Context, runID string) (*Record, error) {
	if hook := s.onLookup; hook != nil {
		s.onLookup = nil
		hook()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.record == nil {
		return nil, ErrRecordNotFound
	}

	cp := *s.record
	return &cp, nil
}

func (s *versionedRecordStore) Latest(ctx context.Context, workflowName, foreignID string) (*Record, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.record == nil {
		return nil, ErrRecordNotFound
	}

	cp := *s.record
	return &cp, nil
}

func (s *versionedRecordStore) Store(ctx context.Context, record *Record) error {
	if hook := s.beforeStore; hook != nil {
		s.beforeStore = nil
		hook()
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.record != nil && record.Meta.Version != s.record.Meta.Version+1 {
		return fmt.Errorf(
			"expected version %d, got %d: %w",
			s.record.Meta.Version+1,
			record.Meta.Version,
			ErrRecordVersionConflict,
		)
	}

	cp := *record
	s.record = &cp
	return nil
}

func (s *versionedRecordStore) List(ctx context.Context, workflowName string, offsetID int64, limit int, order OrderType, filters ...RecordFilter) ([]Record, error) {
	return nil, nil
}

func (s *versionedRecordStore) ListOutboxEvents(ctx context.Context, workflowName string, limit int64) ([]OutboxEvent, error) {
	return nil, nil
}

func (s *versionedRecordStore) DeleteOutboxEvent(ctx context.Context, id string) error {
	return nil
}

// timeoutStoreDouble is a minimal TimeoutStore test double that records Complete and Cancel calls.
type timeoutStoreDouble struct {
	valid     []TimeoutRecord
	completed []int64
	cancelled []int64
}

func (s *timeoutStoreDouble) Create(ctx context.Context, workflowName, foreignID, runID string, status int, expireAt time.Time) error {
	return nil
}

func (s *timeoutStoreDouble) Complete(ctx context.Context, id int64) error {
	s.completed = append(s.completed, id)
	return nil
}

func (s *timeoutStoreDouble) Cancel(ctx context.Context, id int64) error {
	s.cancelled = append(s.cancelled, id)
	return nil
}

func (s *timeoutStoreDouble) List(ctx context.Context, workflowName string) ([]TimeoutRecord, error) {
	return s.valid, nil
}

func (s *timeoutStoreDouble) ListValid(ctx context.Context, workflowName string, status int, now time.Time) ([]TimeoutRecord, error) {
	return s.valid, nil
}

func seedRecord() *Record {
	return &Record{
		WorkflowName: "example",
		ForeignID:    "andrew",
		RunID:        "run-1",
		RunState:     RunStateRunning,
		Status:       int(statusStart),
		Object:       []byte(`"seed"`),
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
		Meta:         Meta{Version: 1},
	}
}

func testGraph() *graph.Graph {
	g := graph.New()
	g.AddTransition(int(statusStart), int(statusMiddle))
	g.AddTransition(int(statusMiddle), int(statusEnd))
	return g
}

func runFromRecord(record *Record) *Run[string, testStatus] {
	var obj string
	return &Run[string, testStatus]{
		TypedRecord: TypedRecord[string, testStatus]{
			Record: *record,
			Status: testStatus(record.Status),
			Object: &obj,
		},
	}
}

// Interleaving 1 (Pause wins):
//
//	poller: Latest()                      -> snapshot v1 (Running, statusStart)
//	actor:  Pause commits                 -> v2 (Paused, statusStart)
//	poller: updater commit with version 1 -> must be rejected; record stays Paused at statusStart
func TestRacePauseCommitVsTimeoutPollCommit(t *testing.T) {
	ctx := t.Context()
	clock := clock_testing.NewFakeClock(time.Now())
	store := newVersionedRecordStore(seedRecord())

	snapshot, err := store.Latest(ctx, "example", "andrew")
	require.NoError(t, err)

	// Pause lands after the poller read its snapshot.
	pauser, err := store.Lookup(ctx, snapshot.RunID)
	require.NoError(t, err)
	err = NewRunStateController(store.Store, pauser).Pause(ctx, "manual pause")
	require.NoError(t, err)

	// The poller's commit, based on its stale snapshot, must lose the race.
	updater := newUpdater[string, testStatus](store.Lookup, store.Store, testGraph(), clock)
	err = updater(ctx, statusStart, statusMiddle, runFromRecord(snapshot), snapshot.Meta.Version)
	require.ErrorIs(t, err, ErrRecordVersionConflict)

	latest, err := store.Lookup(ctx, snapshot.RunID)
	require.NoError(t, err)
	require.Equal(t, RunStatePaused, latest.RunState)
	require.Equal(t, int(statusStart), latest.Status)
	require.Equal(t, uint(2), latest.Meta.Version)
}

// Interleaving 2 (timeout poll commit wins):
//
//	poller: Latest()                       -> snapshot v1 (Running, statusStart)
//	poller: updater commit                 -> v2 (Running, statusMiddle)
//	actor:  Pause from stale v1 snapshot   -> must be rejected
//	actor:  Pause from fresh snapshot      -> succeeds; status must remain statusMiddle (no regression)
func TestRaceTimeoutPollCommitVsPauseCommit(t *testing.T) {
	ctx := t.Context()
	clock := clock_testing.NewFakeClock(time.Now())
	store := newVersionedRecordStore(seedRecord())

	snapshot, err := store.Latest(ctx, "example", "andrew")
	require.NoError(t, err)

	updater := newUpdater[string, testStatus](store.Lookup, store.Store, testGraph(), clock)
	err = updater(ctx, statusStart, statusMiddle, runFromRecord(snapshot), snapshot.Meta.Version)
	require.NoError(t, err)

	// A Pause issued from the stale snapshot must lose the race and leave the record untouched.
	err = NewRunStateController(store.Store, snapshot).Pause(ctx, "stale pause")
	require.ErrorIs(t, err, ErrRecordVersionConflict)

	latest, err := store.Lookup(ctx, snapshot.RunID)
	require.NoError(t, err)
	require.Equal(t, RunStateRunning, latest.RunState)
	require.Equal(t, int(statusMiddle), latest.Status)
	require.Equal(t, uint(2), latest.Meta.Version)

	// A Pause issued from a fresh snapshot succeeds and must not regress the status.
	err = NewRunStateController(store.Store, latest).Pause(ctx, "fresh pause")
	require.NoError(t, err)

	latest, err = store.Lookup(ctx, snapshot.RunID)
	require.NoError(t, err)
	require.Equal(t, RunStatePaused, latest.RunState)
	require.Equal(t, int(statusMiddle), latest.Status)
	require.Equal(t, uint(3), latest.Meta.Version)
}

// Interleaving 3 (Resume vs timeout poll commit):
//
//	actor:  Resume commits                  -> v3 (Running, statusMiddle)
//	poller: updater commit with version 2   -> must be rejected; run resumes from its pre-pause position
func TestRaceResumeCommitVsTimeoutPollCommit(t *testing.T) {
	ctx := t.Context()
	clock := clock_testing.NewFakeClock(time.Now())

	paused := seedRecord()
	paused.RunState = RunStatePaused
	paused.Meta.Version = 2
	store := newVersionedRecordStore(paused)

	latest, err := store.Lookup(ctx, paused.RunID)
	require.NoError(t, err)
	err = NewRunStateController(store.Store, latest).Resume(ctx)
	require.NoError(t, err)

	// A timeout poll that loaded the record while it was paused at version 2 must lose the race.
	staleSnapshot := *paused
	updater := newUpdater[string, testStatus](store.Lookup, store.Store, testGraph(), clock)
	err = updater(ctx, statusStart, statusMiddle, runFromRecord(&staleSnapshot), staleSnapshot.Meta.Version)
	require.ErrorIs(t, err, ErrRecordVersionConflict)

	latest, err = store.Lookup(ctx, paused.RunID)
	require.NoError(t, err)
	require.Equal(t, RunStateRunning, latest.RunState)
	require.Equal(t, int(statusStart), latest.Status)
	require.Equal(t, uint(3), latest.Meta.Version)
}

// Interleaving 4 (duplicate Resume - manual resume racing the paused records retry consumer):
//
//	consumer: Lookup()            -> snapshot v2 (Paused)
//	actor:    Resume commits      -> v3 (Running)
//	consumer: Resume from v2      -> must be rejected and treated as a no-op
func TestRaceDuplicateResumeCommit(t *testing.T) {
	ctx := t.Context()
	clock := clock_testing.NewFakeClock(time.Now())

	paused := seedRecord()
	paused.RunState = RunStatePaused
	paused.Meta.Version = 2
	paused.UpdatedAt = clock.Now().Add(-time.Hour)
	store := newVersionedRecordStore(paused)

	// A manual resume commits after the auto retry consumer loaded its snapshot but before its
	// own resume commit lands.
	store.beforeStore = func() {
		latest, err := store.Latest(ctx, "example", "andrew")
		require.NoError(t, err)
		err = NewRunStateController(store.Store, latest).Resume(ctx)
		require.NoError(t, err)
	}

	// The auto retry consumer's resume, based on its stale snapshot, is rejected and swallowed.
	err := autoRetryConsumer(store.Lookup, store.Store, clock, time.Minute)(ctx, &Event{})
	require.NoError(t, err)

	latest, err := store.Lookup(ctx, paused.RunID)
	require.NoError(t, err)
	require.Equal(t, RunStateRunning, latest.RunState)
	require.Equal(t, uint(3), latest.Meta.Version)
}

// Interleaving 5 (Pause lands between the timeout poller's snapshot and its commit):
//
//	poller: ListValid + Latest -> snapshot (Running, statusStart)
//	actor:  Pause commits      -> Paused
//	poller: processTimeout     -> conflict; poller must skip the record and keep polling
//
// The paused record must not be timeout-processed and the timeout must remain valid so that it can
// be executed once the record is resumed.
func TestPollTimeoutsSkipsConcurrentlyPausedRecord(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	clock := clock_testing.NewFakeClock(time.Now())
	store := newVersionedRecordStore(seedRecord())

	// Simulate a Pause committing after the poller's snapshot (Latest) but before the updater's
	// commit-time Lookup.
	store.onLookup = func() {
		latest, err := store.Latest(ctx, "example", "andrew")
		require.NoError(t, err)
		err = NewRunStateController(store.Store, latest).Pause(ctx, "manual pause")
		require.NoError(t, err)

		// Unblock the poller after the interleaving has played out.
		cancel()
	}

	timeoutStore := &timeoutStoreDouble{
		valid: []TimeoutRecord{{
			ID:           1,
			WorkflowName: "example",
			ForeignID:    "andrew",
			RunID:        "run-1",
			Status:       int(statusStart),
		}},
	}

	var timeoutFuncCalls int
	w := &Workflow[string, testStatus]{
		name:         "example",
		ctx:          ctx,
		clock:        clock,
		recordStore:  store,
		timeoutStore: timeoutStore,
		statusGraph:  testGraph(),
		errorCounter: errorcounter.New(),
		logger:       &logger{},
		runPool:      newRunPool[string, testStatus](),
	}

	timeouts := timeouts[string, testStatus]{
		transitions: []timeout[string, testStatus]{
			{
				TimeoutFunc: func(ctx context.Context, r *Run[string, testStatus], now time.Time) (testStatus, error) {
					timeoutFuncCalls++
					return statusMiddle, nil
				},
			},
		},
	}

	err := pollTimeouts(ctx, w, statusStart, timeouts, "test", time.Millisecond, 0)
	require.ErrorIs(t, err, context.Canceled)

	latest, err := store.Lookup(ctx, "run-1")
	require.NoError(t, err)
	require.Equal(t, RunStatePaused, latest.RunState)
	require.Equal(t, int(statusStart), latest.Status)

	// The timeout must not be completed or cancelled - it remains valid and will be processed after resume.
	require.Empty(t, timeoutStore.completed)
	require.Empty(t, timeoutStore.cancelled)
}

// A record that is already paused when the timeout poller selects it must not be selected for
// timeout processing at all.
func TestPollTimeoutsSkipsAlreadyPausedRecord(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	clock := clock_testing.NewFakeClock(time.Now())

	paused := seedRecord()
	paused.RunState = RunStatePaused
	store := newVersionedRecordStore(paused)

	timeoutStore := &timeoutStoreDouble{
		valid: []TimeoutRecord{{
			ID:           1,
			WorkflowName: "example",
			ForeignID:    "andrew",
			RunID:        "run-1",
			Status:       int(statusStart),
		}},
	}

	var timeoutFuncCalls int
	w := &Workflow[string, testStatus]{
		name:         "example",
		ctx:          ctx,
		clock:        clock,
		recordStore:  store,
		timeoutStore: timeoutStore,
		statusGraph:  testGraph(),
		errorCounter: errorcounter.New(),
		logger:       &logger{},
		runPool:      newRunPool[string, testStatus](),
	}

	timeouts := timeouts[string, testStatus]{
		transitions: []timeout[string, testStatus]{
			{
				TimeoutFunc: func(ctx context.Context, r *Run[string, testStatus], now time.Time) (testStatus, error) {
					timeoutFuncCalls++
					return statusMiddle, nil
				},
			},
		},
	}

	time.AfterFunc(50*time.Millisecond, cancel)
	err := pollTimeouts(ctx, w, statusStart, timeouts, "test", time.Millisecond, 0)
	require.ErrorIs(t, err, context.Canceled)

	require.Zero(t, timeoutFuncCalls)
	require.Empty(t, timeoutStore.completed)
	require.Empty(t, timeoutStore.cancelled)
}
