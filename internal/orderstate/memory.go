package orderstate

import (
	"context"
	"sync"
)

type MemoryStore struct {
	mu     sync.Mutex
	states map[int64]State
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		states: make(map[int64]State),
	}
}

func (s *MemoryStore) Load(ctx context.Context, orderID int64) (State, bool, error) {
	if err := ctx.Err(); err != nil {
		return State{}, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	state, ok := s.states[orderID]
	return state, ok, nil
}

func (s *MemoryStore) Save(ctx context.Context, state State) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.states == nil {
		s.states = make(map[int64]State)
	}

	s.states[state.OrderID] = state
	return nil
}
