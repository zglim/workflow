package component

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type testComponent struct {
	name    string
	run     func(ctx context.Context, ready func()) error
	onReady func()
}

func (c *testComponent) Name() string { return c.name }

func (c *testComponent) Run(ctx context.Context, ready func()) error {
	ready()
	if c.onReady != nil {
		c.onReady()
	}

	return c.run(ctx, ready)
}

func TestManager_StartsAndWaitsForAllComponents(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	m := New(ctx)

	var ran atomic.Int32
	for _, name := range []string{"a", "b", "c"} {
		c := &testComponent{
			name: name,
			run: func(ctx context.Context, _ func()) error {
				<-ctx.Done()
				ran.Add(1)
				return nil
			},
		}
		m.Start(c)
	}

	m.WaitLaunched()
	require.Equal(t, []string{"a", "b", "c"}, m.Names())

	require.NoError(t, m.Wait())
	require.Equal(t, int32(3), ran.Load())
}

func TestManager_AggregatesEscapedErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	errBoom := errors.New("boom")
	m := New(ctx)
	m.Start(&testComponent{
		name: "failing",
		run:  func(context.Context, func()) error { return errBoom },
	})
	m.Start(&testComponent{
		name: "clean",
		run:  func(ctx context.Context, _ func()) error { <-ctx.Done(); return nil },
	})

	err := m.Wait()
	require.Error(t, err)
	require.ErrorIs(t, err, errBoom)
	require.Contains(t, err.Error(), "failing")
}

func TestManager_PanicIsIsolatedAndAggregated(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	m := New(ctx)
	m.Start(&testComponent{
		name: "panicker",
		run: func(context.Context, func()) error {
			panic("kaboom")
		},
	})

	var waited atomic.Int32
	m.Start(&testComponent{
		name: "healthy",
		run: func(ctx context.Context, _ func()) error {
			<-ctx.Done()
			waited.Add(1)
			return nil
		},
	})

	err := m.Wait()
	require.Error(t, err)
	require.Contains(t, err.Error(), `component "panicker" panicked`)
	require.Contains(t, err.Error(), "kaboom")
	require.Equal(t, int32(1), waited.Load())
}

func TestManager_ShutdownTimeout(t *testing.T) {
	ctx := t.Context()

	m := New(ctx, WithShutdownTimeout(10*time.Millisecond))
	m.Start(&testComponent{
		name: "stuck",
		run: func(context.Context, func()) error {
			select {}
		},
	})

	done := make(chan error, 1)
	go func() { done <- m.Wait() }()

	select {
	case err := <-done:
		// The stuck goroutine leaks for the lifetime of the test process; assert
		// the timeout error is surfaced and Wait returned promptly.
		require.ErrorIs(t, err, ErrShutdownTimeout)
	case <-time.After(time.Second):
		t.Fatal("Wait did not return after shutdown timeout")
	}
}

func TestManager_StartAfterSealIsNoop(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	m := New(ctx)

	require.NoError(t, m.Wait())

	m.Start(&testComponent{
		name: "late",
		run:  func(context.Context, func()) error { t.Fatal("late component must not run"); return nil },
	})

	require.Empty(t, m.Names())
}

func TestManager_DuplicateNamePanics(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	m := New(ctx)
	mk := func() *testComponent {
		return &testComponent{
			name: "dup",
			run:  func(ctx context.Context, _ func()) error { <-ctx.Done(); return nil },
		}
	}
	m.Start(mk())
	require.PanicsWithValue(t, "component: duplicate component name: dup", func() {
		m.Start(mk())
	})
}

func TestManager_ErrorHookReceivesErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	var mu sync.Mutex
	var got []string
	m := New(ctx, WithErrorHook(func(name string, err error) {
		mu.Lock()
		got = append(got, fmt.Sprintf("%s:%v", name, err))
		mu.Unlock()
	}))

	m.Start(&testComponent{
		name: "hooked",
		run:  func(context.Context, func()) error { return errors.New("nope") },
	})

	_ = m.Wait()

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, got, 1)
	require.Equal(t, "hooked:nope", got[0])
}
