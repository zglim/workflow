package workflow

import (
	"context"
	"time"

	"github.com/luno/workflow/internal/component"
)

// processComponent adapts a workflow background process (consumer, poller or
// inserter) to the managed component model. All process-specific behaviour -
// role scheduling, state transitions, error metrics and backoff retries -
// remains inside Workflow.runProcess; the component only supplies its name and
// the shared context.
type processComponent[Type any, Status StatusType] struct {
	w           *Workflow[Type, Status]
	role        string
	processName string
	process     func(ctx context.Context) error
	errBackOff  time.Duration
}

func newProcessComponent[Type any, Status StatusType](
	role string,
	processName string,
	process func(ctx context.Context) error,
	errBackOff time.Duration,
	w *Workflow[Type, Status],
) *processComponent[Type, Status] {
	return &processComponent[Type, Status]{
		w:           w,
		role:        role,
		processName: processName,
		process:     process,
		errBackOff:  errBackOff,
	}
}

func (c *processComponent[Type, Status]) Name() string { return c.processName }

func (c *processComponent[Type, Status]) Run(ctx context.Context, ready func()) error {
	return c.w.runProcess(ctx, c.role, c.processName, c.process, c.errBackOff, ready)
}

type processComponentStatus int

func (processComponentStatus) String() string { return "" }

var _ component.Component = (*processComponent[struct{}, processComponentStatus])(nil)
