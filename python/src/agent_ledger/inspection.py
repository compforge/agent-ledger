from __future__ import annotations

from collections import defaultdict
from typing import Any

from pydantic import BaseModel, ConfigDict

from agent_ledger.models import EventType, RunView, SessionView, StoredEvent


class LaneGap(BaseModel):
    model_config = ConfigDict(frozen=True)

    lane_id: str
    expected_seq: int
    actual_seq: int


class UnresolvedAttempt(BaseModel):
    model_config = ConfigDict(frozen=True)

    run_id: str
    lane_id: str
    turn_id: str
    action_id: str
    action_type: str
    attempt_id: str
    attempt_no: int
    requested_event_id: str


class RunEdge(BaseModel):
    model_config = ConfigDict(frozen=True)

    parent_run_id: str
    child_run_id: str
    causation_id: str


class LinkedCheckpoint(BaseModel):
    model_config = ConfigDict(frozen=True)

    event: StoredEvent
    checkpoint_id: str | None
    profile: str | None
    profile_version: str | None
    metadata: dict[str, Any]


class RunInspection(BaseModel):
    model_config = ConfigDict(frozen=True)

    run_id: str
    terminal_events: tuple[StoredEvent, ...]
    linked_checkpoints: tuple[LinkedCheckpoint, ...]
    unresolved_attempts: tuple[UnresolvedAttempt, ...]


class SessionInspection(BaseModel):
    model_config = ConfigDict(frozen=True)

    timeline: tuple[StoredEvent, ...]
    lane_gaps: tuple[LaneGap, ...]
    unresolved_attempts: tuple[UnresolvedAttempt, ...]
    run_edges: tuple[RunEdge, ...]


def inspect_session(view: SessionView) -> SessionInspection:
    lanes = {lane.id: lane for lane in view.lanes}
    expected: dict[str, int] = defaultdict(lambda: 1)
    gaps: list[LaneGap] = []
    edges: dict[tuple[str, str], RunEdge] = {}

    for event in sorted(view.events, key=lambda item: (item.lane_id, item.seq)):
        if event.seq != expected[event.lane_id]:
            gaps.append(
                LaneGap(
                    lane_id=event.lane_id,
                    expected_seq=expected[event.lane_id],
                    actual_seq=event.seq,
                )
            )
        expected[event.lane_id] = event.seq + 1

    for event in view.events:
        lane = lanes[event.lane_id]
        if event.event_type == EventType.RUN_STARTED:
            parent_run_id = event.payload.get("parent_run_id")
            if isinstance(parent_run_id, str) and event.causation_id is not None:
                edges[(parent_run_id, lane.run_id)] = RunEdge(
                    parent_run_id=parent_run_id,
                    child_run_id=lane.run_id,
                    causation_id=event.causation_id,
                )
    return SessionInspection(
        timeline=view.events,
        lane_gaps=tuple(gaps),
        unresolved_attempts=_unresolved_attempts(view),
        run_edges=tuple(edges.values()),
    )


def inspect_run(view: RunView) -> RunInspection:
    terminal_types = {
        EventType.RUN_COMPLETED,
        EventType.RUN_FAILED,
        EventType.RUN_CANCELLED,
    }
    terminal_events = tuple(
        event
        for event in view.events
        if event.event_type in terminal_types and event.subject_id == view.run_id
    )
    linked_checkpoints: list[LinkedCheckpoint] = []
    for event in view.events:
        if event.event_type != EventType.LANE_FRAMEWORK_CHECKPOINT_LINKED:
            continue
        checkpoint_id = event.payload.get("checkpoint_id")
        profile = event.payload.get("profile")
        profile_version = event.payload.get("profile_version")
        metadata = event.payload.get("metadata")
        linked_checkpoints.append(
            LinkedCheckpoint(
                event=event,
                checkpoint_id=checkpoint_id if isinstance(checkpoint_id, str) else None,
                profile=profile if isinstance(profile, str) else None,
                profile_version=profile_version if isinstance(profile_version, str) else None,
                metadata=metadata if isinstance(metadata, dict) else {},
            )
        )
    return RunInspection(
        run_id=view.run_id,
        terminal_events=terminal_events,
        linked_checkpoints=tuple(linked_checkpoints),
        unresolved_attempts=_unresolved_attempts(view),
    )


def _unresolved_attempts(view: SessionView | RunView) -> tuple[UnresolvedAttempt, ...]:
    lanes = {lane.id: lane for lane in view.lanes}
    turns = {turn.id: turn for turn in view.turns}
    actions = {action.id: action for action in view.actions}
    attempts = {attempt.id: attempt for attempt in view.attempts}
    open_attempts: dict[str, UnresolvedAttempt] = {}
    for event in view.events:
        if event.subject_kind != "attempt":
            continue
        attempt = attempts.get(event.subject_id)
        action = actions.get(attempt.action_id) if attempt is not None else None
        turn = turns.get(action.turn_id) if action is not None else None
        lane = lanes.get(turn.lane_id) if turn is not None else None
        if attempt is None or action is None or turn is None or lane is None:
            continue
        if event.event_type == EventType.ATTEMPT_REQUESTED:
            open_attempts[attempt.id] = UnresolvedAttempt(
                run_id=lane.run_id,
                lane_id=lane.id,
                turn_id=turn.id,
                action_id=action.id,
                action_type=action.type,
                attempt_id=attempt.id,
                attempt_no=attempt.attempt_no,
                requested_event_id=event.id,
            )
        elif event.event_type in {EventType.ATTEMPT_COMPLETED, EventType.ATTEMPT_FAILED}:
            open_attempts.pop(attempt.id, None)
    return tuple(open_attempts.values())
