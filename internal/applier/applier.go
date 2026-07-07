package applier

import (
	"context"
	"fmt"
	"testP/internal/engine"
	"testP/internal/eventlog"
	"testP/internal/model"
)

type EventApplier struct {
	codec  eventlog.EventCodec
	engine engine.Engine
}

func NewEventApplier(codec eventlog.EventCodec, engine engine.Engine) *EventApplier {
	return &EventApplier{
		codec:  codec,
		engine: engine,
	}
}

func (a *EventApplier) Apply(ctx context.Context, event model.Event) error {
	eventType := event.Type
	switch eventType {
	case model.EventOrderCreated:
		payload := model.OrderCreated{}
		err := a.codec.DecodePayload(event.Payload, &payload)
		if err != nil {
			return err
		}
		order := model.Order{
			ID: payload.OrderID,
			X:  payload.X,
			Y:  payload.Y,
		}
		//暂时这么写吧，四点了该睡觉了 TODO:批处理适配
		if err = a.engine.SubmitBatch(ctx, model.OrderBatch{
			Orders: []model.Order{order},
		}); err != nil {
			return err
		}
	case model.EventRiderOnline:
		return a.riderEventFunc(event)
	case model.EventRiderMoved:
		return a.riderEventFunc(event)
	case model.EventRiderOffline:
		return a.riderEventFunc(event)
	default:
		return fmt.Errorf("unsupported event type: %s", event.Type)
	}
	return nil
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
