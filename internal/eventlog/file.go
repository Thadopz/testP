package eventlog

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testP/internal/model"
	"time"
)

const defaultTailPollInterval = 200 * time.Millisecond

type FileEventLog struct {
	mu           sync.Mutex
	dir          string
	codec        EventCodec
	pollInterval time.Duration
}

func NewFileEventLog(dir string, codec EventCodec) *FileEventLog {
	if codec == nil {
		codec = &JSONEventCodec{}
	}

	return &FileEventLog{
		dir:          dir,
		codec:        codec,
		pollInterval: defaultTailPollInterval,
	}
}

func (l *FileEventLog) SetPollInterval(interval time.Duration) {
	if interval <= 0 {
		return
	}
	l.pollInterval = interval
}

func (l *FileEventLog) Append(ctx context.Context, event model.Event) (Position, error) {
	if err := ctx.Err(); err != nil {
		return Position{}, err
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	if err := os.MkdirAll(l.dir, 0755); err != nil {
		return Position{}, fmt.Errorf("create eventlog dir: %w", err)
	}

	path := l.shardPath(event.ShardID)
	offset, err := countLines(path)
	if err != nil {
		return Position{}, fmt.Errorf("count eventlog lines: %w", err)
	}

	record := Record{
		Position: Position{
			ShardID: event.ShardID,
			Offset:  offset,
		},
		Event: event,
	}

	data, err := json.Marshal(record)
	if err != nil {
		return record.Position, fmt.Errorf("encode eventlog record: %w", err)
	}
	data = append(data, '\n')

	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return record.Position, fmt.Errorf("open eventlog file: %w", err)
	}

	if _, err := file.Write(data); err != nil {
		file.Close()
		return record.Position, fmt.Errorf("write eventlog file: %w", err)
	}
	if err := file.Sync(); err != nil {
		file.Close()
		return record.Position, fmt.Errorf("sync eventlog file: %w", err)
	}
	if err := file.Close(); err != nil {
		return record.Position, fmt.Errorf("close eventlog file: %w", err)
	}

	return record.Position, nil
}

func (l *FileEventLog) ReadFrom(ctx context.Context, position Position) (<-chan Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	l.mu.Lock()
	records, err := l.readRecordsFromFileLocked(position)
	l.mu.Unlock()
	if err != nil {
		return nil, err
	}

	recordCh := make(chan Record)
	go func() {
		defer close(recordCh)

		for _, record := range records {
			select {
			case <-ctx.Done():
				return
			case recordCh <- record:
			}
		}
	}()

	return recordCh, nil
}

func (l *FileEventLog) TailFrom(ctx context.Context, position Position) (<-chan Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	recordCh := make(chan Record)

	go func() {
		defer close(recordCh)

		nextOffset := position.Offset
		ticker := time.NewTicker(l.pollInterval)
		defer ticker.Stop()

		for {
			records, err := l.readRecordsFromPosition(position.ShardID, nextOffset)
			if err != nil {
				return
			}

			for _, record := range records {
				select {
				case <-ctx.Done():
					return
				case recordCh <- record:
					nextOffset = record.Position.Offset + 1
				}
			}

			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	return recordCh, nil
}

func (l *FileEventLog) readRecordsFromPosition(shardID int, offset int64) ([]Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.readRecordsFromFileLocked(Position{
		ShardID: shardID,
		Offset:  offset,
	})
}

func (l *FileEventLog) readRecordsFromFileLocked(position Position) ([]Record, error) {
	path := l.shardPath(position.ShardID)

	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("open eventlog file: %w", err)
	}
	defer file.Close()

	records := make([]Record, 0)
	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		var record Record
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			return nil, fmt.Errorf("decode eventlog record: %w", err)
		}

		if record.Position.Offset < position.Offset {
			continue
		}

		records = append(records, record)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan eventlog file: %w", err)
	}

	return records, nil
}

func (l *FileEventLog) shardPath(shardID int) string {
	fileName := fmt.Sprintf("shard-%d.log", shardID)
	return filepath.Join(l.dir, fileName)
}

func countLines(path string) (int64, error) {
	file, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var count int64
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		count++
	}
	if err := scanner.Err(); err != nil {
		return 0, err
	}

	return count, nil
}
