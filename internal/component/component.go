// Package component provides a small managed-component lifecycle model used by
// workflow.Run to start and shut down its background processes uniformly.
//
// Design constraints:
//   - Components share a single parent context and are started concurrently
//     in registration order.
//   - Shutdown is driven by cancelling the shared context; the manager then
//     waits for every component to return. An optional shutdown timeout bounds
//     the wait. By default the wait is unbounded which preserves workflow's
//     historical Stop() semantics.
//   - Each component runs in its own goroutine with panic recovery so a single
//     faulty process cannot tear down the whole workflow.
//   - Errors that escape a component (including recovered panics and shutdown
//     timeouts) are aggregated and queryable via Err.
package component

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
	"time"
)

// ErrShutdownTimeout is returned when one or more components have not exited
// within the configured shutdown timeout.
var ErrShutdownTimeout = errors.New("component shutdown timed out")

// Component is a managed background process. Name must be unique within a
// Manager. Run blocks until the component has shut down. The provided ready
// callback must be invoked once the component has completed any synchronous
// startup work (e.g. recording its initial state); Manager.WaitLaunched blocks
// until every started component has called it. Run receives the shared parent
// context: returning when ctx is cancelled is the graceful-shutdown contract.
type Component interface {
	Name() string
	Run(ctx context.Context, ready func()) error
}

// ErrorHook is invoked with every error that escapes a component, including
// recovered panics. It is purely observational: the error is aggregated
// regardless of what the hook does.
type ErrorHook func(name string, err error)

// Option configures a Manager.
type Option func(*Manager)

// WithShutdownTimeout bounds how long Wait blocks for components to exit after
// the shared context is cancelled. A zero or negative value (the default)
// waits indefinitely.
func WithShutdownTimeout(d time.Duration) Option {
	return func(m *Manager) {
		m.shutdownTimeout = d
	}
}

// WithErrorHook installs an observational hook for component errors.
func WithErrorHook(h ErrorHook) Option {
	return func(m *Manager) {
		m.onError = h
	}
}

// Manager owns the lifecycle of a set of components. Its zero value is not
// usable; construct one with New.
type Manager struct {
	shutdownTimeout time.Duration
	onError         ErrorHook

	startMu sync.Mutex
	sealed  bool

	mu       sync.Mutex
	names    []string
	registry map[string]struct{}
	errs     []error

	live       sync.WaitGroup
	launch     sync.WaitGroup
	parentCtx  context.Context
	cancel     context.CancelFunc
	parentOnce sync.Once
}

// New returns a new Manager. Components started by the manager share the
// provided context; cancelling it triggers graceful shutdown.
func New(parentCtx context.Context, opts ...Option) *Manager {
	ctx, cancel := context.WithCancel(parentCtx)
	m := &Manager{
		registry:  make(map[string]struct{}),
		parentCtx: ctx,
		cancel:    cancel,
	}
	for _, opt := range opts {
		opt(m)
	}

	return m
}

// Cancel cancels the context shared by all components. It is safe to call
// concurrently and any number of times.
func (m *Manager) Cancel() {
	m.cancel()
}

// Start launches c in its own goroutine. Components may be registered up until
// the manager is shut down; calls made while or after shutdown are no-ops
// because the shared context is already cancelled. Ready is signalled by the
// component itself from within Run.
func (m *Manager) Start(c Component) {
	m.startMu.Lock()
	defer m.startMu.Unlock()

	if m.sealed {
		return
	}

	m.mu.Lock()
	if _, ok := m.registry[c.Name()]; ok {
		m.mu.Unlock()
		panic("component: duplicate component name: " + c.Name())
	}
	m.registry[c.Name()] = struct{}{}
	m.names = append(m.names, c.Name())
	m.mu.Unlock()

	m.launch.Add(1)
	m.live.Add(1)
	go m.runGuarded(c)
}

// WaitLaunched blocks until every started component has signalled ready.
func (m *Manager) WaitLaunched() {
	m.launch.Wait()
}

// Wait seals the manager so no further components can be started, cancels the
// shared context and blocks until every component has exited. If a shutdown
// timeout is configured it bounds the wait and aggregates ErrShutdownTimeout
// when it elapses. The aggregated result of all component runs is returned.
func (m *Manager) Wait() error {
	m.seal()
	m.Cancel()

	if m.shutdownTimeout <= 0 {
		m.live.Wait()
		return m.Err()
	}

	done := make(chan struct{})
	go func() {
		m.live.Wait()
		close(done)
	}()

	timer := time.NewTimer(m.shutdownTimeout)
	defer timer.Stop()

	select {
	case <-done:
		return m.Err()
	case <-timer.C:
		m.recordError("", ErrShutdownTimeout)
		return m.Err()
	}
}

// Err returns an aggregation of all errors that escaped components. It returns
// nil when every component exited cleanly.
func (m *Manager) Err() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	return errors.Join(m.errs...)
}

// Names returns the component names in registration order.
func (m *Manager) Names() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	names := make([]string, len(m.names))
	copy(names, m.names)

	return names
}

func (m *Manager) seal() {
	m.parentOnce.Do(func() {
		m.startMu.Lock()
		m.sealed = true
		m.startMu.Unlock()
	})
}

func (m *Manager) runGuarded(c Component) {
	defer m.live.Done()

	var readyOnce sync.Once
	ready := func() {
		readyOnce.Do(m.launch.Done)
	}
	defer ready()

	defer func() {
		if r := recover(); r != nil {
			err := fmt.Errorf("component %q panicked: %v\n%s", c.Name(), r, debug.Stack())
			m.recordError(c.Name(), err)
		}
	}()

	if err := c.Run(m.parentCtx, ready); err != nil {
		m.recordError(c.Name(), err)
	}
}

func (m *Manager) recordError(name string, err error) {
	m.mu.Lock()
	m.errs = append(m.errs, fmt.Errorf("[%s]: %w", name, err))
	m.mu.Unlock()

	if m.onError != nil {
		m.onError(name, err)
	}
}
