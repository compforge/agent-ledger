from __future__ import annotations

from collections import defaultdict

from pydantic import BaseModel, ConfigDict

from agent_ledger.models import EventType, SessionView, StoredEvent


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


class SessionInspection(BaseModel):
    model_config = ConfigDict(frozen=True)

    timeline: tuple[StoredEvent, ...]
    lane_gaps: tuple[LaneGap, ...]
    unresolved_attempts: tuple[UnresolvedAttempt, ...]
    run_edges: tuple[RunEdge, ...]


def inspect_session(view: SessionView) -> SessionInspection:
    lanes = {lane.id: lane for lane in view.lanes}
    turns = {turn.id: turn for turn in view.turns}
    actions = {action.id: action for action in view.actions}
    attempts = {attempt.id: attempt for attempt in view.attempts}
    expected: dict[str, int] = defaultdict(lambda: 1)
    gaps: list[LaneGap] = []
    open_attempts: dict[str, UnresolvedAttempt] = {}
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
        if event.subject_kind != "attempt":
            continue
        attempt = attempts[event.subject_id]
        action = actions[attempt.action_id]
        turn = turns[action.turn_id]
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

    return SessionInspection(
        timeline=view.events,
        lane_gaps=tuple(gaps),
        unresolved_attempts=tuple(open_attempts.values()),
        run_edges=tuple(edges.values()),
    )
