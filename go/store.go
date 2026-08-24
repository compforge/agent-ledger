package agentledger

import (
	"context"
	"iter"
)

type ActorStore interface {
	CreateActor(context.Context, Actor) error
	GetActor(context.Context, string) (Actor, bool, error)
	GetActorByKey(context.Context, string) (Actor, bool, error)
	EnsureActor(context.Context, Actor) (Actor, error)
}

type ArtifactStore interface {
	CreateArtifact(context.Context, Artifact) error
	GetArtifact(context.Context, string) (Artifact, bool, error)
	GetArtifactByKey(context.Context, string, string) (Artifact, bool, error)
	EnsureArtifact(context.Context, Artifact) (Artifact, error)
}

type EventStore interface {
	ActorStore
	ArtifactStore
	CreateLane(context.Context, Lane) error
	GetLane(context.Context, string) (Lane, bool, error)
	FindLane(context.Context, string, string, string) (Lane, bool, error)
	CreateTurn(context.Context, Turn) error
	GetTurn(context.Context, string) (Turn, bool, error)
	CreateAction(context.Context, Action) error
	GetAction(context.Context, string) (Action, bool, error)
	CreateAttempt(context.Context, Attempt) error
	GetAttempt(context.Context, string) (Attempt, bool, error)
	Append(context.Context, string, int64, string, ...ProposedEvent) (AppendReceipt, error)
	LoadLane(context.Context, string, int64) iter.Seq2[StoredEvent, error]
	LoadLanePage(context.Context, string, int64, int) (EventPage, error)
	LoadSession(context.Context, string) (SessionView, error)
	LoadRun(context.Context, string, string) (RunView, error)
}

// EventPage is a bounded slice of one Lane's append-only sequence.
type EventPage struct {
	Events  []StoredEvent
	HasMore bool
}

type CheckpointStore interface {
	ActorStore
	ArtifactStore
	SaveCheckpoint(context.Context, int64, ProposedCheckpoint) (Checkpoint, error)
	GetCheckpoint(context.Context, string) (Checkpoint, bool, error)
	LoadLatestCheckpoint(context.Context, string) (Checkpoint, bool, error)
}
