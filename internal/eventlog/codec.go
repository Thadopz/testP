package eventlog

import (
	"encoding/json"
	"fmt"
	"testP/internal/model"
)

type EventCodec interface {
	EncodeEvent(model.Event) ([]byte, error)
	DecodeEvent([]byte) (model.Event, error)

	EncodePayload(value any) ([]byte, error)
	DecodePayload(data []byte, target any) error
}

type JSONEventCodec struct{}

func (*JSONEventCodec) EncodeEvent(e model.Event) ([]byte, error) {
	if e.ID == "" || e.Type == "" {
		return nil, fmt.Errorf("Event does not exist")
	}
	return json.Marshal(e)
}

func (*JSONEventCodec) DecodeEvent(b []byte) (model.Event, error) {
	e := model.Event{}
	if b == nil {
		return e, fmt.Errorf("payload is empty")
	}
	if err := json.Unmarshal(b, &e); err != nil {
		return e, err
	}
	if e.Type == "" {
		return e, fmt.Errorf("Type is empty")
	}
	return e, nil
}

func (*JSONEventCodec) EncodePayload(value any) ([]byte, error) {
	return json.Marshal(value)
}

func (*JSONEventCodec) DecodePayload(data []byte, target any) error {
	return json.Unmarshal(data, target)
}
