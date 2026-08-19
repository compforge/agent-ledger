package boltstore

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"strconv"
	"time"

	agentledger "github.com/compforge/agent-ledger/go"
	bolt "go.etcd.io/bbolt"
)

var (
	streamsBucket  = []byte("streams")
	sessionsBucket = []byte("sessions")
	receiptsBucket = []byte("receipts")
	eventIDsBucket = []byte("event_ids")
)

// Store persists Agent Ledger streams in one Bolt database. Bolt serializes
// writers, so the EventStore append contract and its optimistic version check
// are committed in the same transaction.
type Store struct {
	db *bolt.DB
}

func Open(path string, timeout time.Duration) (*Store, error) {
	if timeout <= 0 {
		return nil, errors.New("open bolt event store: timeout must be positive")
	}
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: timeout})
	if err != nil {
		return nil, fmt.Errorf("open bolt event store %q: %w", path, err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	if err := s.db.Close(); err != nil {
		return fmt.Errorf("close bolt event store: %w", err)
	}
	return nil
}

func (s *Store) Append(
	ctx context.Context,
	stream agentledger.EventStream,
	expectedVersion int64,
	appendID string,
	events ...agentledger.ProposedEvent,
) (agentledger.CommitReceipt, error) {
	if err := ctx.Err(); err != nil {
		return agentledger.CommitReceipt{}, err
	}
	if len(events) == 0 {
		return agentledger.CommitReceipt{}, errors.New("append requires at least one event")
	}
	batch, err := clone(events)
	if err != nil {
		return agentledger.CommitReceipt{}, fmt.Errorf("snapshot append batch: %w", err)
	}
	seen := make(map[string]struct{}, len(batch))
	for _, event := range batch {
		if event.SessionID != stream.SessionID {
			return agentledger.CommitReceipt{}, errors.New("all events must belong to the target stream's session")
		}
		if _, duplicate := seen[event.EventID]; duplicate {
			return agentledger.CommitReceipt{}, fmt.Errorf("%w: %s", agentledger.ErrDuplicateEvent, event.EventID)
		}
		seen[event.EventID] = struct{}{}
	}
	digest, err := agentledger.CanonicalAppendDigest(batch)
	if err != nil {
		return agentledger.CommitReceipt{}, err
	}

	var receipt agentledger.CommitReceipt
	err = s.db.Update(func(tx *bolt.Tx) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		streams, err := tx.CreateBucketIfNotExists(streamsBucket)
		if err != nil {
			return err
		}
		sessions, err := tx.CreateBucketIfNotExists(sessionsBucket)
		if err != nil {
			return err
		}
		receipts, err := tx.CreateBucketIfNotExists(receiptsBucket)
		if err != nil {
			return err
		}
		eventIDs, err := tx.CreateBucketIfNotExists(eventIDsBucket)
		if err != nil {
			return err
		}

		receiptKey := composite(stream.SessionID, stream.StreamID, appendID)
		if encoded := receipts.Get(receiptKey); encoded != nil {
			if err := json.Unmarshal(encoded, &receipt); err != nil {
				return fmt.Errorf("decode append receipt: %w", err)
			}
			if receipt.Digest != digest {
				return agentledger.ErrIdempotencyViolation
			}
			return nil
		}

		streamBucket, err := streams.CreateBucketIfNotExists(composite(stream.SessionID, stream.StreamID))
		if err != nil {
			return err
		}
		currentVersion := int64(streamBucket.Sequence()) - 1
		if currentVersion != expectedVersion {
			return fmt.Errorf("%w: expected %d, actual %d", agentledger.ErrStreamConflict, expectedVersion, currentVersion)
		}
		for _, event := range batch {
			key := composite(stream.SessionID, event.EventID)
			if eventIDs.Get(key) != nil {
				return fmt.Errorf("%w: %s", agentledger.ErrDuplicateEvent, event.EventID)
			}
		}

		sessionBucket, err := sessions.CreateBucketIfNotExists([]byte(stream.SessionID))
		if err != nil {
			return err
		}
		committedAt := time.Now().UTC().Format(time.RFC3339Nano)
		stored := make([]agentledger.StoredEvent, 0, len(batch))
		for _, event := range batch {
			streamSequence, err := streamBucket.NextSequence()
			if err != nil {
				return err
			}
			sessionSequence, err := sessionBucket.NextSequence()
			if err != nil {
				return err
			}
			item := agentledger.StoredEvent{
				ProposedEvent: event,
				StreamID:      stream.StreamID,
				StreamVersion: int64(streamSequence) - 1,
				CommitCursor:  strconv.FormatUint(sessionSequence-1, 10),
				CommittedAt:   committedAt,
			}
			encoded, err := json.Marshal(item)
			if err != nil {
				return fmt.Errorf("encode stored event: %w", err)
			}
			if err := streamBucket.Put(sequenceKey(streamSequence-1), encoded); err != nil {
				return err
			}
			if err := sessionBucket.Put(sequenceKey(sessionSequence-1), encoded); err != nil {
				return err
			}
			if err := eventIDs.Put(composite(stream.SessionID, event.EventID), []byte{1}); err != nil {
				return err
			}
			stored = append(stored, item)
		}

		receipt = agentledger.CommitReceipt{
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
		}
		encoded, err := json.Marshal(receipt)
		if err != nil {
			return fmt.Errorf("encode append receipt: %w", err)
		}
		return receipts.Put(receiptKey, encoded)
	})
	if err != nil {
		return agentledger.CommitReceipt{}, fmt.Errorf("append bolt event batch: %w", err)
	}
	return clone(receipt)
}

func (s *Store) Load(ctx context.Context, stream agentledger.EventStream, afterVersion int64) iter.Seq2[agentledger.StoredEvent, error] {
	return s.read(ctx, streamsBucket, composite(stream.SessionID, stream.StreamID), afterVersion)
}

func (s *Store) ScanSession(ctx context.Context, sessionID, afterCursor string) iter.Seq2[agentledger.StoredEvent, error] {
	after := int64(-1)
	if afterCursor != "" {
		value, err := strconv.ParseInt(afterCursor, 10, 64)
		if err != nil || value < 0 {
			return errorSequence(fmt.Errorf("invalid cursor %q", afterCursor))
		}
		after = value
	}
	return s.read(ctx, sessionsBucket, []byte(sessionID), after)
}

func (s *Store) read(ctx context.Context, rootName, childName []byte, after int64) iter.Seq2[agentledger.StoredEvent, error] {
	return func(yield func(agentledger.StoredEvent, error) bool) {
		var encodedEvents [][]byte
		err := s.db.View(func(tx *bolt.Tx) error {
			root := tx.Bucket(rootName)
			if root == nil {
				return nil
			}
			child := root.Bucket(childName)
			if child == nil {
				return nil
			}
			cursor := child.Cursor()
			for key, value := cursor.Seek(sequenceKey(uint64(after + 1))); key != nil; key, value = cursor.Next() {
				encodedEvents = append(encodedEvents, append([]byte(nil), value...))
			}
			return nil
		})
		if err != nil {
			yield(agentledger.StoredEvent{}, fmt.Errorf("read bolt event stream: %w", err))
			return
		}
		for _, encoded := range encodedEvents {
			if err := ctx.Err(); err != nil {
				yield(agentledger.StoredEvent{}, err)
				return
			}
			var event agentledger.StoredEvent
			if err := json.Unmarshal(encoded, &event); err != nil {
				yield(agentledger.StoredEvent{}, fmt.Errorf("decode stored event: %w", err))
				return
			}
			if !yield(event, nil) {
				return
			}
		}
	}
}

func composite(parts ...string) []byte {
	var result []byte
	for index, part := range parts {
		if index > 0 {
			result = append(result, 0)
		}
		result = append(result, part...)
	}
	return result
}

func sequenceKey(value uint64) []byte {
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, value)
	return key
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

func errorSequence(err error) iter.Seq2[agentledger.StoredEvent, error] {
	return func(yield func(agentledger.StoredEvent, error) bool) {
		yield(agentledger.StoredEvent{}, err)
	}
}
