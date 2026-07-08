package orderstate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

type FileStore struct {
	mu   sync.Mutex
	path string
}

func NewFileStore(dir string) *FileStore {
	return &FileStore{
		path: filepath.Join(dir, "orders.json"),
	}
}

func (s *FileStore) Load(ctx context.Context, orderID int64) (State, bool, error) {
	if err := ctx.Err(); err != nil {
		return State{}, false, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	states, err := s.load()
	if err != nil {
		return State{}, false, err
	}

	state, ok := states[orderID]
	return state, ok, nil
}

func (s *FileStore) Save(ctx context.Context, state State) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if state.OrderID <= 0 {
		return fmt.Errorf("order id must be > 0: %d", state.OrderID)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	states, err := s.load()
	if err != nil {
		return err
	}

	states[state.OrderID] = state
	return s.save(states)
}

func (s *FileStore) load() (map[int64]State, error) {
	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return make(map[int64]State), nil
	}
	if err != nil {
		return nil, fmt.Errorf("read order state file: %w", err)
	}

	statesByTextID := make(map[string]State)
	if err := json.Unmarshal(data, &statesByTextID); err != nil {
		return nil, fmt.Errorf("decode order state file: %w", err)
	}

	states := make(map[int64]State, len(statesByTextID))
	for textOrderID, state := range statesByTextID {
		orderID, err := strconv.ParseInt(textOrderID, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse order id %q: %w", textOrderID, err)
		}
		state.OrderID = orderID
		states[orderID] = state
	}

	return states, nil
}

func (s *FileStore) save(states map[int64]State) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0755); err != nil {
		return fmt.Errorf("create order state dir: %w", err)
	}

	statesByTextID := make(map[string]State, len(states))
	for orderID, state := range states {
		statesByTextID[fmt.Sprintf("%d", orderID)] = state
	}

	data, err := json.MarshalIndent(statesByTextID, "", "  ")
	if err != nil {
		return fmt.Errorf("encode order state file: %w", err)
	}

	tempPath := s.path + ".tmp"
	file, err := os.OpenFile(tempPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("open temp order state file: %w", err)
	}
	defer os.Remove(tempPath)

	if _, err := file.Write(data); err != nil {
		file.Close()
		return fmt.Errorf("write temp order state file: %w", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return fmt.Errorf("sync temp order state file: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close temp order state file: %w", err)
	}

	if err := os.Rename(tempPath, s.path); err != nil {
		return fmt.Errorf("replace order state file: %w", err)
	}

	return nil
}
