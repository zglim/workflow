package workflow

import (
	"context"
)

func deleteComponent[Type any, Status StatusType](w *Workflow[Type, Status]) componentSpec {
	role := makeRole(
		w.Name(),
		"delete",
		"consumer",
	)

	processName := makeRole("delete", "consumer")
	return componentSpec{
		role:        role,
		processName: processName,
		errBackOff:  w.defaultOpts.errBackOff,
		process: func(ctx context.Context) error {
			topic := DeleteTopic(w.Name())
			stream, err := w.eventStreamer.NewReceiver(
				ctx,
				topic,
				role,
				WithReceiverPollFrequency(w.defaultOpts.pollingFrequency),
			)
			if err != nil {
				return err
			}
			defer stream.Close()

			return consume(
				ctx,
				w.Name(),
				processName,
				stream,
				runDelete(
					w.recordStore.Store,
					w.recordStore.Lookup,
					w.customDelete,
				),
				w.clock,
				0,
				w.defaultOpts.lagAlert,
			)
		},
	}
}

func runDelete(
	store storeFunc,
	lookup lookupFunc,
	customDeleteFn customDelete,
) func(ctx context.Context, e *Event) error {
	return func(ctx context.Context, e *Event) error {
		record, err := lookup(ctx, e.ForeignID)
		if err != nil {
			return err
		}

		replacementData := []byte(`{"result":"deleted"}`)
		// If a custom delete has been configured then use the custom delete
		if customDeleteFn != nil {
			bytes, err := customDeleteFn(record)
			if err != nil {
				return err
			}

			replacementData = bytes
		}

		record.Object = replacementData
		record.RunState = RunStateDataDeleted
		return updateRecord(
			ctx,
			store,
			record,
			RunStateRequestedDataDeleted,
			record.Meta.StatusDescription,
		)
	}
}
