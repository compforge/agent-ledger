package agentledger

import (
	"context"
	"iter"
)

type EventStore interface {
	Append(ctx context.Context, stream EventStream, expectedVersion int64, appendID string, events ...ProposedEvent) (CommitReceipt, error)
	Load(ctx context.Context, stream EventStream, afterVersion int64) iter.Seq2[StoredEvent, error]
	ScanSession(ctx context.Context, sessionID, afterCursor string) iter.Seq2[StoredEvent, error]
}
