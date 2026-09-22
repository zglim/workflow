package workflow

import "context"

// optimisticRecordStore is an optional extension of RecordStore that commits record
// updates atomically on the record version. Stores that support transactions or
// compare-and-swap should implement it so that a commit whose expected version no
// longer matches the persisted version is rejected with ErrOptimisticLock instead of
// blindly overwriting the record.
//
// StoreWithVersion must, in a single atomic operation (transaction):
//  1. Reject the write with ErrOptimisticLock if a record with runID already exists
//     but its Meta.Version differs from expectedVersion.
//  2. Create the record if it does not exist and expectedVersion is 0.
//  3. Otherwise persist record (with its already incremented Meta.Version) together
//     with the outbox event generated via MakeOutboxEventData.
type optimisticRecordStore interface {
	StoreWithVersion(ctx context.Context, record *Record, expectedVersion uint) error
}

// OptimisticRecordStore is the public form of the optional atomic version-checked
// commit extension implemented by RecordStore adapters that support transactions or
// compare-and-swap.
type OptimisticRecordStore = optimisticRecordStore

// NewOptimisticStoreFunc wraps a RecordStore so that commits are guarded by the
// record version (optimistic locking). If the store implements
// OptimisticRecordStore, the returned func rejects stale writes atomically with
// ErrOptimisticLock; otherwise it falls back to the store's plain Store method.
// External code that drives a RunStateController (for example a HTTP API that
// pauses/resumes runs) should pass the returned func to NewRunStateController so
// that Pause/Resume/Cancel participate in the same conflict arbitration.
func NewOptimisticStoreFunc(rs RecordStore) func(ctx context.Context, record *Record) error {
	return casStore(rs)
}

// casStore returns a storeFunc that commits records via atomic optimistic locking
// when the RecordStore supports it. Stores without native version checks fall back
// to the plain Store call, which cannot close the read/modify/write race; production
// stores should implement optimisticRecordStore.
func casStore(rs RecordStore) storeFunc {
	versioned, ok := rs.(optimisticRecordStore)
	if !ok {
		return rs.Store
	}

	return func(ctx context.Context, record *Record) error {
		// updateRecord increments Meta.Version before committing, so the version that
		// the record must still have at commit time is one less than the new version.
		expectedVersion := uint(0)
		if record.Meta.Version > 0 {
			expectedVersion = record.Meta.Version - 1
		}

		return versioned.StoreWithVersion(ctx, record, expectedVersion)
	}
}
