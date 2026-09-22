package workflow

import (
	"context"
	"errors"
	"fmt"
	"io"
	"slices"
	"sync"
	"time"

	"k8s.io/utils/clock"

	"github.com/luno/workflow/internal/component"
	"github.com/luno/workflow/internal/graph"
	"github.com/luno/workflow/internal/metrics"
)

type API[Type any, Status StatusType] interface {
	// Name returns the name of the implemented workflow.
	Name() string

	// Trigger will kickstart a workflow Run for the provided foreignID starting from the default entrypoint to
	// the workflow which is the first "from" status added via the builder
	// (e.g. builder.AddStep(FromStatus, func{}, ToStatus). There is no limitation as to where you start the workflow
	// from and can do so via the WithStartAt trigger option. WithInitialValue should be used when you need data to be
	// present in the workflow Run before it starts. This can be used to reduce the need for duplicating reads.
	//
	// foreignID should not be random and should be deterministic for the thing that you are running the workflow for.
	// This especially helps when connecting other workflows as the foreignID is the only way to connect the streams. The
	// same goes for Callback as you will need the foreignID to connect the callback back to the workflow instance that
	// was run.
	Trigger(
		ctx context.Context,
		foreignID string,
		opts ...TriggerOption[Type, Status],
	) (runID string, err error)

	// Schedule takes a cron spec and will call Trigger at the specified intervals. Schedule is a blocking call and all
	// schedule errors will be retried indefinitely. The same options are available for Schedule as they are
	// for Trigger.
	Schedule(foreignID string, spec string, opts ...ScheduleOption[Type, Status]) error

	// Await is a blocking call that returns the typed Run when the workflow of the specified run ID reaches the
	// specified status.
	Await(ctx context.Context, foreignID, runID string, status Status, opts ...AwaitOption) (*Run[Type, Status], error)

	// Callback can be used if Builder.AddCallback has been defined for the provided status. The data in the reader
	// will be passed to the CallbackFunc that you specify and so the serialisation and deserialisation is in the
	// hands of the user.
	Callback(ctx context.Context, foreignID string, status Status, payload io.Reader) error

	// Run must be called in order to start up all the background consumers / consumers required to run the workflow. Run
	// only needs to be called once. Any subsequent calls to run are safe and are noop.
	Run(ctx context.Context)

	// Stop tells the workflow to shut down gracefully.
	Stop()
}

type Workflow[Type any, Status StatusType] struct {
	name      string
	ctx       context.Context
	mu        sync.Mutex
	cancel    context.CancelFunc
	clock     clock.Clock
	calledRun bool
	once      sync.Once
	logger    *logger

	eventStreamer EventStreamer
	recordStore   RecordStore
	timeoutStore  TimeoutStore
	scheduler     RoleScheduler

	consumers        map[Status]consumerConfig[Type, Status]
	callback         map[Status][]callback[Type, Status]
	timeouts         map[Status]timeouts[Type, Status]
	connectorConfigs []*connectorConfig[Type, Status]

	defaultOpts         options
	outboxConfig        outboxConfig
	pausedRecordsRetry  pausedRecordsRetry
	customDelete        customDelete
	runStateChangeHooks map[RunState]RunStateChangeHookFunc[Type, Status]

	internalStateMu sync.Mutex
	// internalState holds the State of all expected consumers and timeout go routines using their role names
	// as the key.
	internalState map[string]State
	// components owns the lifecycle (start, shared context, graceful shutdown,
	// error aggregation and panic isolation) of all background processes.
	components *component.Manager

	// runPool pools Run objects to reduce allocations
	runPool *sync.Pool

	// defaultStartingPoint defines that status that the workflow run will start on when Trigger is called.
	defaultStartingPoint Status
	statusGraph          *graph.Graph
	// errorCounter keeps a central in-mem state of errors from consumers and timeouts in order to implement
	// PauseAfterstatusGraphErrCount. The tracking of errors is done in a way where errors need to be unique per process
	// (consumer / timeout).
	errorCounter ErrorCounter
}

func (w *Workflow[Type, Status]) Name() string {
	return w.name
}

func (w *Workflow[Type, Status]) Run(ctx context.Context) {
	// Ensure that the background consumers are only initialized once
	w.once.Do(func() {
		ctx, cancel := context.WithCancel(ctx)

		func() {
			w.mu.Lock()
			defer w.mu.Unlock()
			w.ctx = ctx
			w.cancel = cancel
			w.calledRun = true

			w.components = component.New(ctx, component.WithErrorHook(func(_ string, err error) {
				w.logger.Error(ctx, err)
			}))
		}()

		// Assemble the managed components in their historical dependency order.
		// Every component shares the workflow context and is driven by the
		// component manager for start-up, shutdown and error handling.
		w.registerComponents()
	})

	// Block until every component has recorded its initial state, preserving
	// Run's guarantee that all processes are tracked by the time it returns.
	w.mu.Lock()
	components := w.components
	w.mu.Unlock()
	components.WaitLaunched()
}

// startComponent registers a single managed background process with the
// workflow's component manager.
func (w *Workflow[Type, Status]) startComponent(c component.Component) {
	w.components.Start(c)
}

// registerComponents assembles all background processes in their historical
// dependency order: outbox, step consumers, timeout pollers and auto-inserters,
// connector consumers, run-state-change hooks, delete consumer and finally the
// paused-records retry consumer. Status- and run-state-keyed processes are
// sorted for deterministic registration (map iteration was previously random).
func (w *Workflow[Type, Status]) registerComponents() {
	if !w.outboxConfig.disabled {
		w.startComponent(newOutboxComponent(w, w.outboxConfig))
	}

	for _, currentStatus := range sortedKeys(w.consumers) {
		config := w.consumers[currentStatus]

		parallelCount := w.defaultOpts.parallelCount
		if config.parallelCount != 0 {
			parallelCount = config.parallelCount
		}

		if parallelCount < 2 {
			w.startComponent(newStepConsumerComponent(w, currentStatus, config, 1, 1))
			continue
		}

		for i := 1; i <= parallelCount; i++ {
			w.startComponent(newStepConsumerComponent(w, currentStatus, config, i, parallelCount))
		}
	}

	// Only start timeout consumers if the timeout store is provided. This allows
	// for the timeout store to be optional for workflows where timeouts are not
	// used.
	if w.timeoutStore != nil {
		for _, status := range sortedKeys(w.timeouts) {
			w.startComponent(newTimeoutPollerComponent(w, status, w.timeouts[status]))
			w.startComponent(newTimeoutAutoInserterComponent(w, status, w.timeouts[status]))
		}
	}

	for _, config := range w.connectorConfigs {
		parallelCount := w.defaultOpts.parallelCount
		if config.parallelCount != 0 {
			parallelCount = config.parallelCount
		}

		if parallelCount < 2 {
			w.startComponent(newConnectorConsumerComponent(w, config, 1, 1))
			continue
		}

		for i := 1; i <= parallelCount; i++ {
			w.startComponent(newConnectorConsumerComponent(w, config, i, parallelCount))
		}
	}

	for _, state := range sortedKeys(w.runStateChangeHooks) {
		w.startComponent(newRunStateChangeHookComponent(w, state, w.runStateChangeHooks[state]))
	}

	w.startComponent(newDeleteConsumerComponent(w))

	// Only start the paused record retry consumer if enabled.
	if w.pausedRecordsRetry.enabled {
		w.startComponent(newPausedRecordsRetryComponent(w))
	}
}

// sortedKeys returns the keys of m in ascending numeric order.
func sortedKeys[K ~int | ~int32 | ~int64, V any](m map[K]V) []K {
	keys := make([]K, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	return keys
}

// run is a standardise way of running blocking calls with a built-in retry mechanism.
func (w *Workflow[Type, Status]) run(
	role string,
	processName string,
	process func(ctx context.Context) error,
	errBackOff time.Duration,
) component.Component {
	return newProcessComponent(role, processName, process, errBackOff, w)
}

// runProcess drives the retry loop for a single managed process. It keeps the
// historical state transitions (Idle -> Running -> Shutdown), role scheduling,
// error logging/metrics and backoff behaviour unchanged.
func (w *Workflow[Type, Status]) runProcess(
	ctx context.Context,
	role string,
	processName string,
	process func(ctx context.Context) error,
	errBackOff time.Duration,
	ready func(),
) error {
	w.updateState(processName, StateIdle)
	// The initial state is recorded: Run may now return and Stop can observe
	// the process through States().
	ready()
	defer w.updateState(processName, StateShutdown)

	for {
		err := runOnce(
			ctx,
			w.Name(),
			role,
			processName,
			w.updateState,
			w.scheduler.Await,
			process,
			w.logger,
			w.clock,
			errBackOff,
		)
		if err != nil {
			w.logger.Debug(w.ctx, "shutting down process", map[string]string{
				"role":         role,
				"process_name": processName,
			})

			return nil
		}
	}
}

type (
	updateStateFn func(processName string, s State)
	awaitRoleFn   func(ctx context.Context, role string) (context.Context, context.CancelFunc, error)
)

func runOnce(
	ctx context.Context,
	workflowName string,
	role string,
	processName string,
	updateState updateStateFn,
	awaitRole awaitRoleFn,
	process func(ctx context.Context) error,
	logger *logger,
	clock clock.Clock,
	errBackOff time.Duration,
) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}

	updateState(processName, StateIdle)

	ctx, cancel, err := awaitRole(ctx, role)
	if errors.Is(err, context.Canceled) {
		// Exit cleanly if error returned is cancellation of context
		return err
	} else if errors.Is(err, context.DeadlineExceeded) {
		// Exit cleanly if context's deadline was exceeded
		return err
	} else if err != nil {
		logger.Error(ctx, fmt.Errorf("run error [role=%s], [process=%s]: %v", role, processName, err))

		// Return nil to try again
		return nil
	}
	defer cancel()

	updateState(processName, StateRunning)

	err = process(ctx)
	if errors.Is(err, context.Canceled) {
		// Context can be cancelled by the role scheduler and thus return nil to attempt to gain the role again
		// and if the parent context was cancelled then that will exit safely.
		return nil
	} else if errors.Is(err, context.DeadlineExceeded) {
		// Exit cleanly if context's deadline was exceeded
		return err
	} else if err != nil {
		logger.Error(ctx, fmt.Errorf("run error [role=%s], [process=%s]: %v", role, processName, err))
		metrics.ProcessErrors.WithLabelValues(workflowName, processName).Inc()

		timer := clock.NewTimer(errBackOff)
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C():
			// Return nil to try again
			return nil
		}
	}

	return nil
}

// Stop cancels the context provided to all the background processes that the workflow launched and waits for all of
// them to shut down gracefully.
func (w *Workflow[Type, Status]) Stop() {
	w.mu.Lock()
	cancel := w.cancel
	components := w.components
	w.mu.Unlock()

	if cancel == nil {
		return
	}

	// Cancel the parent context of the workflow to gracefully shutdown.
	cancel()

	// Wait for every managed component to exit. The manager owns the shutdown
	// wait (unbounded by default) and aggregates any escaped errors.
	if err := components.Wait(); err != nil {
		w.logger.Error(w.ctx, err)
	}
}
