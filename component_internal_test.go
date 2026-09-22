package workflow

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestShardSpecs(t *testing.T) {
	makeSpec := func(shard, totalShards int) componentSpec {
		return componentSpec{
			processName: makeRole("process", string(rune('0'+shard)), "of", string(rune('0'+totalShards))),
		}
	}

	t.Run("parallel count below 2 resolves to a single unsharded component", func(t *testing.T) {
		for _, count := range []int{0, 1} {
			specs := shardSpecs(count, makeSpec)
			require.Len(t, specs, 1)
			require.Equal(t, "process-1-of-1", specs[0].processName)
		}
	})

	t.Run("parallel count expands into one component per shard", func(t *testing.T) {
		specs := shardSpecs(3, makeSpec)
		require.Len(t, specs, 3)
		require.Equal(t, "process-1-of-3", specs[0].processName)
		require.Equal(t, "process-2-of-3", specs[1].processName)
		require.Equal(t, "process-3-of-3", specs[2].processName)
	})
}

func TestComponentSupervisor_waitForShutdown(t *testing.T) {
	t.Run("returns once all processes have shut down", func(t *testing.T) {
		var state atomic.Int32
		state.Store(int32(StateRunning))
		supervisor := &componentSupervisor{
			states: func() map[string]State {
				return map[string]State{"process": State(state.Load())}
			},
		}

		go func() {
			time.Sleep(50 * time.Millisecond)
			state.Store(int32(StateShutdown))
		}()

		supervisor.waitForShutdown(0)
	})

	t.Run("returns after the timeout even if processes are still running", func(t *testing.T) {
		supervisor := &componentSupervisor{
			states: func() map[string]State {
				return map[string]State{"process": StateRunning}
			},
		}

		start := time.Now()
		supervisor.waitForShutdown(50 * time.Millisecond)
		require.Less(t, time.Since(start), 5*time.Second)
	})
}

func TestComponentSupervisor_errorAggregation(t *testing.T) {
	supervisor := &componentSupervisor{}
	require.NoError(t, supervisor.errors())

	supervisor.recordError(context.DeadlineExceeded)
	supervisor.recordError(context.DeadlineExceeded)

	err := supervisor.errors()
	require.Error(t, err)
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestLaunch_panicIsolation(t *testing.T) {
	b := NewBuilder[string, testStatus]("panic-test")
	b.AddStep(statusStart, func(ctx context.Context, r *Run[string, testStatus]) (testStatus, error) {
		return statusEnd, nil
	}, statusEnd)

	wf := b.Build(
		&noopEventStreamer{},
		&noopRecordStore{},
		&noopScheduler{},
		WithoutOutbox(),
	)

	wf.Run(t.Context())
	t.Cleanup(wf.Stop)

	wf.launch(componentSpec{
		role:        "panic-role",
		processName: "panic-process",
		process: func(ctx context.Context) error {
			panic("boom")
		},
	})

	require.Eventually(t, func() bool {
		return wf.supervisor.errors() != nil
	}, 5*time.Second, time.Millisecond)

	require.Contains(t, wf.supervisor.errors().Error(), "boom")
	require.Equal(t, StateShutdown, wf.States()["panic-process"])
}

func TestLaunch_aggregatesTerminalErrors(t *testing.T) {
	b := NewBuilder[string, testStatus]("terminal-error-test")
	b.AddStep(statusStart, func(ctx context.Context, r *Run[string, testStatus]) (testStatus, error) {
		return statusEnd, nil
	}, statusEnd)

	wf := b.Build(
		&noopEventStreamer{},
		&noopRecordStore{},
		&noopScheduler{},
		WithoutOutbox(),
	)

	wf.Run(t.Context())
	t.Cleanup(wf.Stop)

	wf.launch(componentSpec{
		role:        "deadline-role",
		processName: "deadline-process",
		process: func(ctx context.Context) error {
			return context.DeadlineExceeded
		},
	})

	require.Eventually(t, func() bool {
		return wf.supervisor.errors() != nil
	}, 5*time.Second, time.Millisecond)

	require.ErrorIs(t, wf.supervisor.errors(), context.DeadlineExceeded)
}
