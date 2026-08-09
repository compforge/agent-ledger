package agentledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"strconv"
	"sync"
	"time"
)

var (
	ErrStreamConflict       = errors.New("event stream version conflict")
	ErrIdempotencyViolation = errors.New("append id reused with different content")
	ErrDuplicateEvent       = errors.New("duplicate event id")
)

type MemoryEventStore struct {
	mu       sync.Mutex
	streams  map[string][]StoredEvent
	sessions map[string][]StoredEvent
	receipts map[string]CommitReceipt
	eventIDs map[string]struct{}
}

func NewMemoryEventStore() *MemoryEventStore {
	return &MemoryEventStore{
		streams:  make(map[string][]StoredEvent),
		sessions: make(map[string][]StoredEvent),
		receipts: make(map[string]CommitReceipt),
		eventIDs: make(map[string]struct{}),
	}
}

func (s *MemoryEventStore) Append(
	ctx context.Context,
	stream EventStream,
	expectedVersion int64,
	appendID string,
	events ...ProposedEvent,
) (CommitReceipt, error) {
	if err := ctx.Err(); err != nil {
		return CommitReceipt{}, err
	}
	if len(events) == 0 {
		return CommitReceipt{}, errors.New("append requires at least one event")
	}
	batch, err := clone(events)
	if err != nil {
		return CommitReceipt{}, fmt.Errorf("snapshot append batch: %w", err)
	}
	batchEventIDs := make(map[string]struct{}, len(batch))
	for _, event := range batch {
		if event.SessionID != stream.SessionID {
			return CommitReceipt{}, errors.New("all events must belong to the target stream's session")
		}
		if _, duplicate := batchEventIDs[event.EventID]; duplicate {
			return CommitReceipt{}, fmt.Errorf("%w: %s", ErrDuplicateEvent, event.EventID)
		}
		batchEventIDs[event.EventID] = struct{}{}
	}
	digest, err := CanonicalAppendDigest(batch)
	if err != nil {
		return CommitReceipt{}, err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	receiptKey := streamKey(stream) + "\x00" + appendID
	if receipt, ok := s.receipts[receiptKey]; ok {
		if receipt.Digest != digest {
			return CommitReceipt{}, ErrIdempotencyViolation
		}
		return cloneOne(receipt)
	}
	key := streamKey(stream)
	current := s.streams[key]
	if int64(len(current))-1 != expectedVersion {
		return CommitReceipt{}, fmt.Errorf("%w: expected %d, actual %d", ErrStreamConflict, expectedVersion, len(current)-1)
	}
	for _, event := range batch {
		if _, exists := s.eventIDs[stream.SessionID+"\x00"+event.EventID]; exists {
			return CommitReceipt{}, fmt.Errorf("%w: %s", ErrDuplicateEvent, event.EventID)
		}
	}

	committedAt := time.Now().UTC().Format(time.RFC3339Nano)
	session := s.sessions[stream.SessionID]
	stored := make([]StoredEvent, 0, len(batch))
	for index, event := range batch {
		stored = append(stored, StoredEvent{
			ProposedEvent: event,
			StreamID:      stream.StreamID,
			StreamVersion: expectedVersion + int64(index) + 1,
			CommitCursor:  strconv.Itoa(len(session) + index),
			CommittedAt:   committedAt,
		})
	}
	receipt := CommitReceipt{
		Stream:       stream,
		AppendID:     appendID,
		Digest:       digest,
		FirstVersion: stored[0].StreamVersion,
		LastVersion:  stored[len(stored)-1].StreamVersion,
		FirstCursor:  stored[0].CommitCursor,
		LastCursor:   stored[len(stored)-1].CommitCursor,
		CommittedAt:  committedAt,
	}
	for _, event := range stored {
		receipt.EventIDs = append(receipt.EventIDs, event.EventID)
		s.eventIDs[stream.SessionID+"\x00"+event.EventID] = struct{}{}
	}
	s.streams[key] = append(current, stored...)
	s.sessions[stream.SessionID] = append(session, stored...)
	s.receipts[receiptKey] = receipt
	return cloneOne(receipt)
}

func (s *MemoryEventStore) Load(ctx context.Context, stream EventStream, afterVersion int64) iter.Seq2[StoredEvent, error] {
	return func(yield func(StoredEvent, error) bool) {
		s.mu.Lock()
		snapshot := append([]StoredEvent(nil), s.streams[streamKey(stream)]...)
		s.mu.Unlock()
		events, err := clone(snapshot)
		if err != nil {
			yield(StoredEvent{}, err)
			return
		}
		for _, event := range events {
			if err := ctx.Err(); err != nil {
				yield(StoredEvent{}, err)
				return
			}
			if event.StreamVersion > afterVersion && !yield(event, nil) {
				return
			}
		}
	}
}

func (s *MemoryEventStore) ScanSession(ctx context.Context, sessionID, afterCursor string) iter.Seq2[StoredEvent, error] {
	return func(yield func(StoredEvent, error) bool) {
		cursor := -1
		var err error
		if afterCursor != "" {
			cursor, err = strconv.Atoi(afterCursor)
		}
		if err != nil || cursor < -1 {
			yield(StoredEvent{}, fmt.Errorf("invalid cursor %q", afterCursor))
			return
		}
		s.mu.Lock()
		snapshot := append([]StoredEvent(nil), s.sessions[sessionID]...)
		s.mu.Unlock()
		events, cloneErr := clone(snapshot)
		if cloneErr != nil {
			yield(StoredEvent{}, cloneErr)
			return
		}
		for _, event := range events {
			if err := ctx.Err(); err != nil {
				yield(StoredEvent{}, err)
				return
			}
			position, _ := strconv.Atoi(event.CommitCursor)
			if position > cursor && !yield(event, nil) {
				return
			}
		}
	}
}

func streamKey(stream EventStream) string {
	return stream.SessionID + "\x00" + stream.StreamID
}

func clone[T any](value T) (T, error) {
	var result T
	encoded, err := json.Marshal(value)
	if err != nil {
		return result, err
	}
	if err := json.Unmarshal(encoded, &result); err != nil {
		return result, err
	}
	return result, nil
}

func cloneOne[T any](value T) (T, error) {
	return clone(value)
}
