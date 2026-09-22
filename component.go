package workflow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

// componentSpec describes a single background process that the workflow
// supervises. Every long-running process (outbox consumer, step consumers,
// timeout pollers and inserters, connector consumers, run state change hooks,
// the delete consumer and the paused records retry consumer) is expressed as
// a componentSpec so that Run only assembles the list of specs and the
// supervisor drives their lifecycle uniformly.
type componentSpec struct {
	// role is the unique identity of the process used for role scheduling.
	role string
	// processName is the human-readable name used for state tracking,
	// metrics and logging.
	processName string
	// errBackOff is the duration to wait before restarting the process
	// after a non-terminal error.
	errBackOff time.Duration
	// process is the blocking unit of work. It must return when the
	// provided context is cancelled.
	process func(ctx context.Context) error
}

// shardSpecs expands a component factory into one spec per shard. A parallel
// count below 2 resolves to a single unsharded component.
func shardSpecs(parallelCount int, makeSpec func(shard, totalShards int) componentSpec) []componentSpec {
	if parallelCount < 2 {
		return []componentSpec{makeSpec(1, 1)}
	}

	specs := make([]componentSpec, 0, parallelCount)
	for shard := 1; shard <= parallelCount; shard++ {
		specs = append(specs, makeSpec(shard, parallelCount))
	}

	return specs
}

// componentSupervisor manages the lifecycle of a workflow's background
// components: ordered startup against a shared context, graceful shutdown
// with an optional timeout, terminal error aggregation and panic isolation.
type componentSupervisor struct {
	// states returns the live state of every process keyed by process name.
	states func() map[string]State

	mu   sync.Mutex
	errs []error
}

// recordError aggregates a terminal component error. Terminal errors cannot
// be returned to any caller and are collected here for inspection.
func (s *componentSupervisor) recordError(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.errs = append(s.errs, err)
}

// errors returns all terminal component errors joined into a single error.
func (s *componentSupervisor) errors() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	return errors.Join(s.errs...)
}

// waitForShutdown blocks until every process has reached a terminal state.
// A timeout <= 0 waits indefinitely, preserving the workflow's historic
// shutdown behaviour.
func (s *componentSupervisor) waitForShutdown(timeout time.Duration) {
	var deadline <-chan time.Time
	if timeout > 0 {
		timer := time.NewTimer(timeout)
		defer timer.Stop()
		deadline = timer.C
	}

	for {
		var runningProcesses int
		for _, state := range s.states() {
			switch state {
			case StateUnknown, StateShutdown:
				continue
			default:
				runningProcesses++
			}
		}

		// Once all processes have exited then return
		if runningProcesses == 0 {
			return
		}

		select {
		case <-deadline:
			return
		case <-time.After(10 * time.Millisecond):
		}
	}
}

// components assembles all the background component specs in startup order.
// The shutdown order is the inverse of the actual dependencies: all
// components share the same context and shut down together when it is
// cancelled, so assembly order only affects startup.
func (w *Workflow[Type, Status]) components() []componentSpec {
	var specs []componentSpec

	if !w.outboxConfig.disabled {
		specs = append(specs, outboxComponent(w, w.outboxConfig))
	}

	for currentStatus, config := range w.consumers {
		specs = append(specs, stepConsumerComponents(w, currentStatus, config)...)
	}

	// Only start timeout consumers if the timeout store is provided. This allows for the timeout store to
	// be optional for workflows where the timeout feature is not needed.
	if w.timeoutStore != nil {
		for status, timeouts := range w.timeouts {
			specs = append(specs, timeoutComponents(w, status, timeouts)...)
		}
	}

	for _, config := range w.connectorConfigs {
		specs = append(specs, connectorComponents(w, config)...)
	}

	for state, hook := range w.runStateChangeHooks {
		specs = append(specs, runStateChangeHookComponent(w, state, hook))
	}

	specs = append(specs, deleteComponent(w))

	// Only start the paused record retry consumer if enabled.
	if w.pausedRecordsRetry.enabled {
		specs = append(specs, pausedRecordsRetryComponent(w))
	}

	return specs
}

// launch starts a component in its own goroutine against the workflow's
// shared context. Panics are isolated to the component, recorded and logged
// so that a single faulty component cannot take down the whole process, and
// terminal errors are aggregated on the supervisor.
func (w *Workflow[Type, Status]) launch(spec componentSpec) {
	w.launching.Add(1)
	go func() {
		defer func() {
			if r := recover(); r != nil {
				err := fmt.Errorf("panic in process [process=%s]: %v", spec.processName, r)
				w.supervisor.recordError(err)
				w.logger.Error(w.ctx, err)
			}
		}()

		err := w.run(spec.role, spec.processName, spec.process, spec.errBackOff)
		if err != nil && !errors.Is(err, context.Canceled) {
			w.supervisor.recordError(fmt.Errorf("process exited [process=%s]: %w", spec.processName, err))
		}
	}()
}
