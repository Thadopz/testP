package applier

import (
	"context"
	"fmt"

	"testP/internal/cluster/ownership"
	"testP/internal/eventlog"
	"testP/internal/model"
	"testP/internal/orderstate"
)

type OwnershipReader interface {
	OwnerOf(shardID int) (ownership.Ownership, bool, error)
}

type EventApplier struct {
	codec           eventlog.EventCodec
	nodeID          int
	ownershipReader OwnershipReader
	orderStore      orderstate.Store
}

func NewEventApplier(codec eventlog.EventCodec) *EventApplier {
	return &EventApplier{codec: codec}
}

func NewEventApplierWithOrderStore(codec eventlog.EventCodec, orderStore orderstate.Store) *EventApplier {
	applier := NewEventApplier(codec)
	applier.orderStore = orderStore
	return applier
}

func NewFencedEventApplier(
	codec eventlog.EventCodec,
	nodeID int,
	reader OwnershipReader,
	orderStore orderstate.Store,
) *EventApplier {
	applier := NewEventApplier(codec)
	applier.nodeID = nodeID
	applier.ownershipReader = reader
	applier.orderStore = orderStore
	return applier
}

func (a *EventApplier) Apply(ctx context.Context, event model.Event) error {
	switch event.Type {
	case model.EventOrderCreated:
		return a.applyOrderCreated(ctx, event)
	case model.EventOrderCancelled:
		return a.applyOrderCancelled(ctx, event)
	case model.EventOrderRetryRequest:
		return a.applyOrderRetryRequest(ctx, event)
	case model.EventOrderMatched:
		return a.applyOrderMatched(ctx, event)
	case model.EventOrderMissed:
		return a.applyOrderMissed(ctx, event)
	default:
		return fmt.Errorf("unsupported event type: %s", event.Type)
	}
}

func (a *EventApplier) ApplyWithFence(ctx context.Context, event model.Event, currentOwnership ownership.Ownership) error {
	if err := a.checkFence(event, currentOwnership); err != nil {
		return err
	}

	if err := a.Apply(ctx, event); err != nil {
		return err
	}

	return a.checkFence(event, currentOwnership)
}

func (a *EventApplier) checkFence(event model.Event, currentOwnership ownership.Ownership) error {
	if a.ownershipReader == nil {
		return nil
	}
	if a.nodeID != 0 && currentOwnership.NodeID != a.nodeID {
		return ownership.ErrOwnershipFenceLost
	}
	if event.ShardID != currentOwnership.ShardID {
		return fmt.Errorf("event shard %d does not match ownership shard %d", event.ShardID, currentOwnership.ShardID)
	}

	current, ok, err := a.ownershipReader.OwnerOf(currentOwnership.ShardID)
	if err != nil {
		return err
	}
	if !ok {
		return ownership.ErrOwnershipFenceLost
	}
	if current.NodeID != currentOwnership.NodeID || current.Epoch != currentOwnership.Epoch {
		return ownership.ErrOwnershipFenceLost
	}

	return nil
}

func (a *EventApplier) applyOrderCreated(ctx context.Context, event model.Event) error {
	payload := model.OrderCreated{}
	if err := a.codec.DecodePayload(event.Payload, &payload); err != nil {
		return err
	}

	state, found, err := a.prepareOrderState(ctx, event, payload.OrderID)
	if err != nil {
		return err
	}
	if found && orderAlreadyQueuedOrFinished(state.Status) {
		return nil
	}

	state.Status = orderstate.StatusCreated
	state.X = payload.X
	state.Y = payload.Y
	return a.saveOrderState(ctx, state)
}

func (a *EventApplier) applyOrderCancelled(ctx context.Context, event model.Event) error {
	payload := model.OrderCancelled{}
	if err := a.codec.DecodePayload(event.Payload, &payload); err != nil {
		return err
	}

	state, _, err := a.prepareOrderState(ctx, event, payload.OrderID)
	if err != nil {
		return err
	}
	state.Status = orderstate.StatusCancelled
	state.CancelReason = payload.Reason
	return a.saveOrderState(ctx, state)
}

func (a *EventApplier) applyOrderRetryRequest(ctx context.Context, event model.Event) error {
	payload := model.OrderRetryRequest{}
	if err := a.codec.DecodePayload(event.Payload, &payload); err != nil {
		return err
	}

	state, found, err := a.prepareOrderState(ctx, event, payload.OrderID)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("retry order %d before creation", payload.OrderID)
	}
	if state.Status == orderstate.StatusCancelled || state.Status == orderstate.StatusMatched {
		return nil
	}

	state.Status = orderstate.StatusRetryRequested
	state.Attempt = payload.Attempt
	state.RetryReason = payload.Reason
	return a.saveOrderState(ctx, state)
}

func (a *EventApplier) applyOrderMatched(ctx context.Context, event model.Event) error {
	payload := model.OrderMatched{}
	if err := a.codec.DecodePayload(event.Payload, &payload); err != nil {
		return err
	}

	state, _, err := a.prepareOrderState(ctx, event, payload.OrderID)
	if err != nil {
		return err
	}
	state.Status = orderstate.StatusMatched
	state.RiderID = payload.RiderID
	state.Score = payload.Score
	return a.saveOrderState(ctx, state)
}

func (a *EventApplier) applyOrderMissed(ctx context.Context, event model.Event) error {
	payload := model.OrderMissed{}
	if err := a.codec.DecodePayload(event.Payload, &payload); err != nil {
		return err
	}

	state, _, err := a.prepareOrderState(ctx, event, payload.OrderID)
	if err != nil {
		return err
	}
	if state.Status == orderstate.StatusMatched || state.Status == orderstate.StatusCancelled {
		return nil
	}

	state.Status = orderstate.StatusMissed
	state.MissReason = payload.Reason
	return a.saveOrderState(ctx, state)
}

func (a *EventApplier) prepareOrderState(
	ctx context.Context,
	event model.Event,
	orderID int64,
) (orderstate.State, bool, error) {
	state := orderstate.State{
		OrderID: orderID,
		ShardID: event.ShardID,
	}
	found := false

	if a.orderStore != nil {
		loadedState, loaded, err := a.orderStore.Load(ctx, orderID)
		if err != nil {
			return orderstate.State{}, false, err
		}
		if loaded {
			state = loadedState
			found = true
		}
	}

	state.LastEventID = event.ID
	state.UpdatedAt = event.OccurredAt
	return state, found, nil
}

func (a *EventApplier) saveOrderState(ctx context.Context, state orderstate.State) error {
	if a.orderStore == nil {
		return nil
	}
	return a.orderStore.Save(ctx, state)
}

func orderAlreadyQueuedOrFinished(status orderstate.Status) bool {
	return status == orderstate.StatusSubmitted ||
		status == orderstate.StatusMatchPending ||
		status == orderstate.StatusMatched ||
		status == orderstate.StatusMissed ||
		status == orderstate.StatusCancelled
}
