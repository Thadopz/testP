package applier

import (
	"context"
	"fmt"
	"testP/internal/cluster/ownership"
	"testP/internal/engine"
	"testP/internal/eventlog"
	"testP/internal/model"
	"testP/internal/orderstate"
)

type OwnershipReader interface {
	OwnerOf(shardID int) (ownership.Ownership, bool, error)
}

type EventApplier struct {
	//解码编码事件
	codec eventlog.EventCodec
	//执行层
	engine engine.Engine
	//持有这个applier的node，用来认主
	nodeID int
	//拆出小接口用来测试
	ownershipReader OwnershipReader
	//有限的幂等实现
	orderStore orderstate.Store
}

func NewEventApplier(codec eventlog.EventCodec, engine engine.Engine) *EventApplier {
	return &EventApplier{
		codec:  codec,
		engine: engine,
	}
}

func NewEventApplierWithOrderStore(codec eventlog.EventCodec, engine engine.Engine, orderStore orderstate.Store) *EventApplier {
	applier := NewEventApplier(codec, engine)
	applier.orderStore = orderStore
	return applier
}

func NewFencedEventApplier(
	codec eventlog.EventCodec,
	engine engine.Engine,
	nodeID int,
	reader OwnershipReader,
	orderStore orderstate.Store,
) *EventApplier {
	applier := NewEventApplier(codec, engine)
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
	case model.EventRiderOnline:
		return a.riderEventFunc(event)
	case model.EventRiderMoved:
		return a.riderEventFunc(event)
	case model.EventRiderOffline:
		return a.riderEventFunc(event)
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

	if found {
		if orderAlreadySubmitted(state.Status) {
			return nil
		}
		if state.X == 0 && state.Y == 0 {
			state.X = payload.X
			state.Y = payload.Y
		}
	} else {
		state.Status = orderstate.StatusCreated
		state.X = payload.X
		state.Y = payload.Y
		if a.orderStore != nil {
			if err := a.orderStore.Save(ctx, state); err != nil {
				return err
			}
		}
	}
	//还是没有把batch换成真正的批处理，还在构思中，主要是批处理在这种情况下会存在大批订单积攒后集体失效的场景
	//感觉比较复杂
	if err := a.submitOrder(ctx, state.OrderID, state.X, state.Y); err != nil {
		return err
	}

	if a.orderStore != nil {
		state.Status = orderstate.StatusSubmitted
		return a.orderStore.Save(ctx, state)
	}
	return nil
}

func (a *EventApplier) applyOrderCancelled(ctx context.Context, event model.Event) error {
	payload := model.OrderCancelled{}
	if err := a.codec.DecodePayload(event.Payload, &payload); err != nil {
		return err
	}
	if a.orderStore == nil {
		return nil
	}

	state, _, err := a.prepareOrderState(ctx, event, payload.OrderID)
	if err != nil {
		return err
	}
	state.Status = orderstate.StatusCancelled
	state.CancelReason = payload.Reason
	return a.orderStore.Save(ctx, state)
}

func (a *EventApplier) applyOrderRetryRequest(ctx context.Context, event model.Event) error {
	payload := model.OrderRetryRequest{}
	if err := a.codec.DecodePayload(event.Payload, &payload); err != nil {
		return err
	}
	if a.orderStore == nil {
		return nil
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
	if err := a.orderStore.Save(ctx, state); err != nil {
		return err
	}

	if err := a.submitOrder(ctx, state.OrderID, state.X, state.Y); err != nil {
		return err
	}

	state.Status = orderstate.StatusSubmitted
	return a.orderStore.Save(ctx, state)
}

func (a *EventApplier) applyOrderMatched(ctx context.Context, event model.Event) error {
	payload := model.OrderMatched{}
	if err := a.codec.DecodePayload(event.Payload, &payload); err != nil {
		return err
	}
	if a.orderStore == nil {
		return nil
	}

	state, _, err := a.prepareOrderState(ctx, event, payload.OrderID)
	if err != nil {
		return err
	}
	state.Status = orderstate.StatusMatched
	state.RiderID = payload.RiderID
	state.Score = payload.Score
	return a.orderStore.Save(ctx, state)
}

func (a *EventApplier) applyOrderMissed(ctx context.Context, event model.Event) error {
	payload := model.OrderMissed{}
	if err := a.codec.DecodePayload(event.Payload, &payload); err != nil {
		return err
	}
	if a.orderStore == nil {
		return nil
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
	return a.orderStore.Save(ctx, state)
}

// 统一做orderstate初始化，算是还之前的结构债了
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

func (a *EventApplier) submitOrder(ctx context.Context, orderID int64, x int, y int) error {
	return a.engine.SubmitBatch(ctx, model.OrderBatch{
		Orders: []model.Order{
			{
				ID: orderID,
				X:  x,
				Y:  y,
			},
		},
	})
}

func orderAlreadySubmitted(status orderstate.Status) bool {
	return status == orderstate.StatusSubmitted ||
		status == orderstate.StatusMatched ||
		status == orderstate.StatusMissed ||
		status == orderstate.StatusCancelled
}

func (a *EventApplier) riderEventFunc(event model.Event) error {
	payload := model.RiderEvent{}
	err := a.codec.DecodePayload(event.Payload, &payload)
	if err != nil {
		return err
	}
	riderEvent := model.RiderEvent{
		Type: payload.Type,
		UID:  payload.UID,
		X:    payload.X,
		Y:    payload.Y,
	}
	a.engine.ApplyRiderEvent(riderEvent)
	return err
}
