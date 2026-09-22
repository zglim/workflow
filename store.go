package workflow

import (
	"context"
	"time"
)

// RecordStore implementations should all be tested with adaptertest.TestRecordStore. The underlying implementation of
// store must support transactions or the ability to commit the record and an outbox event in a single call as well as
// being able to obtain an ID for the record before it is created.
type RecordStore interface {
	// Store should create or update a record depending on whether the underlying store is mutable or append only. Store
	// must implement transactions and a separate outbox store to store the outbox record (that should be
	// generated using MakeOutboxEventData) which can be retrieved when calling ListOutboxEvents and can be
	// deleted when DeleteOutboxEvent is called.
	//
	// Store must implement optimistic locking: when updating an existing record (same RunID), the record's
	// Meta.Version must be exactly one greater than the currently stored version. If it is not, Store must
	// reject the write with ErrRecordVersionConflict (wrapped) and must not modify the record or append an
	// outbox event. The version check and the write must be atomic (e.g. guarded by a lock or a row lock in
	// the same transaction). Creating a new record is not version constrained. This is the compare-and-swap
	// commit path that prevents concurrent writers - such as Pause/Resume racing a consumer or timeout poll
	// commit - from clobbering each other.
	Store(ctx context.Context, record *Record) error
	Lookup(ctx context.Context, runID string) (*Record, error)
	Latest(ctx context.Context, workflowName, foreignID string) (*Record, error)

	// List provides a slice of Record where the total items will be equal or less than the limit depending
	// on the offset provided and how many records remain after that ID.
	List(ctx context.Context, workflowName string, offsetID int64, limit int, order OrderType, filters ...RecordFilter) ([]Record, error)

	// ListOutboxEvents lists all events that are yet to be published to the event streamer. A requirement for
	// implementation of the RecordStore is to support a Transactional Outbox that has Event's written to it when
	// Store is called.
	ListOutboxEvents(ctx context.Context, workflowName string, limit int64) ([]OutboxEvent, error)
	// DeleteOutboxEvent will expect an Event's ID field and will remove the event from the outbox store when the
	// event has successfully been published to the event streamer.
	DeleteOutboxEvent(ctx context.Context, id string) error
}

type TestingRecordStore interface {
	RecordStore

	Snapshots(workflowName, foreignID, runID string) []*Record
}

// TimeoutStore implementations should all be tested with adaptertest.TestTimeoutStore
type TimeoutStore interface {
	Create(ctx context.Context, workflowName, foreignID, runID string, status int, expireAt time.Time) error
	Complete(ctx context.Context, id int64) error
	Cancel(ctx context.Context, id int64) error
	List(ctx context.Context, workflowName string) ([]TimeoutRecord, error)
	ListValid(ctx context.Context, workflowName string, status int, now time.Time) ([]TimeoutRecord, error)
}
