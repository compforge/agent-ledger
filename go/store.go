package agentledger

import (
	"context"
	"iter"
)

type EventStore interface {
	CreateActor(context.Context, Actor) error
	GetActor(context.Context, string) (Actor, bool, error)
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
	LoadSession(context.Context, string) (SessionView, error)
}
