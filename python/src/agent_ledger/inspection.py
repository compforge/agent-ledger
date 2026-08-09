from __future__ import annotations

from collections import defaultdict

from pydantic import BaseModel, ConfigDict

from agent_ledger.models import EventType, StoredEvent


class StreamGap(BaseModel):
    model_config = ConfigDict(frozen=True)

    stream_id: str
    expected_version: int
    actual_version: int


class UnresolvedAttempt(BaseModel):
    model_config = ConfigDict(frozen=True)

    run_id: str
    step_id: str | None
    attempt_id: str
    kind: str
    requested_event_id: str


class RunEdge(BaseModel):
    model_config = ConfigDict(frozen=True)

    parent_run_id: str
    child_run_id: str
    caused_by_event_id: str


class SessionInspection(BaseModel):
    model_config = ConfigDict(frozen=True)

    timeline: tuple[StoredEvent, ...]
    stream_gaps: tuple[StreamGap, ...]
    unresolved_attempts: tuple[UnresolvedAttempt, ...]
    run_edges: tuple[RunEdge, ...]


def inspect_session(events: list[StoredEvent]) -> SessionInspection:
    versions: dict[str, int] = defaultdict(lambda: -1)
    gaps: list[StreamGap] = []
    open_attempts: dict[tuple[str, str], UnresolvedAttempt] = {}
    edges: dict[tuple[str, str], RunEdge] = {}

    for event in events:
        expected = versions[event.stream_id] + 1
        if event.stream_version != expected:
            gaps.append(
                StreamGap(
                    stream_id=event.stream_id,
                    expected_version=expected,
                    actual_version=event.stream_version,
                )
            )
        versions[event.stream_id] = event.stream_version

        if event.parent_run_id is not None and event.caused_by_event_id is not None:
            edges[(event.parent_run_id, event.run_id)] = RunEdge(
                parent_run_id=event.parent_run_id,
                child_run_id=event.run_id,
                caused_by_event_id=event.caused_by_event_id,
            )

        if event.attempt_id is None:
            continue
        key = (event.run_id, event.attempt_id)
        if event.event_type in {EventType.MODEL_REQUESTED, EventType.TOOL_REQUESTED}:
            open_attempts[key] = UnresolvedAttempt(
                run_id=event.run_id,
                step_id=event.step_id,
                attempt_id=event.attempt_id,
                kind="model" if event.event_type == EventType.MODEL_REQUESTED else "tool",
                requested_event_id=event.event_id,
            )
        elif event.event_type in {
            EventType.MODEL_COMPLETED,
            EventType.MODEL_FAILED,
            EventType.TOOL_COMPLETED,
            EventType.TOOL_FAILED,
        }:
            open_attempts.pop(key, None)

    return SessionInspection(
        timeline=tuple(events),
        stream_gaps=tuple(gaps),
        unresolved_attempts=tuple(open_attempts.values()),
        run_edges=tuple(edges.values()),
    )
