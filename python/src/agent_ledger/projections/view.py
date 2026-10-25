"""Read-only Ledger → View projection; no Store or recovery decisions."""

from __future__ import annotations

from typing import Any

from agent_ledger.common import CallRequest, CallResponse, call_details
from agent_ledger.models import RunView, StoredEvent
from agent_ledger.view.models import (
    Document,
    Lane,
    ModelCall,
    Node,
    Operation,
    Run,
    SourceRef,
    Status,
    ToolCall,
    Turn,
)

_TERMINALS: dict[str, Status] = {
    "completed": "completed",
    "failed": "failed",
    "cancelled": "cancelled",
    "outcome_unknown": "outcome_unknown",
}


def project_view(source: RunView) -> Document:
    """Preserve each physical call, Lane order and missing/conflicting outcomes."""
    run = Run(
        id=f"run:{source.session_id}/{source.run_id}",
        session_id=source.session_id,
        attributes={"run_id": source.run_id},
    )
    issues: list[str] = []
    turns: dict[str, Turn] = {}
    calls: dict[str, ModelCall | ToolCall | Operation] = {}
    actions = {value.id: value for value in source.actions}
    attempts = {value.id: value for value in source.attempts}
    turn_rows = {value.id: value for value in source.turns}
    terminals: dict[str, list[StoredEvent]] = {}

    def observe(node: Node, event: StoredEvent) -> None:
        node.evidence.append(SourceRef(source="ledger", id=event.id))
        suffix = event.event_type.rsplit(".", 1)[-1]
        if suffix in {"started", "requested"} and node.started_at is None:
            node.started_at = event.occurred_at.isoformat()
        if suffix in _TERMINALS:
            observations = terminals.setdefault(node.id, [])
            observations.append(event)
            node.finished_at = event.occurred_at.isoformat()
            states = {_TERMINALS[e.event_type.rsplit(".", 1)[-1]] for e in observations}
            node.status = _TERMINALS[suffix] if len(states) == 1 else "unknown"
            if len(observations) > 1:
                node.attributes["terminal_observations"] = [
                    e.model_dump(mode="json") for e in observations
                ]
                issues.append(f"node {node.id}: multiple terminal observations")
        if event.extensions:
            node.attributes.setdefault("event_extensions", {})[event.id] = event.extensions

    def turn_for(turn_id: str) -> Turn:
        if turn_id not in turns:
            row = turn_rows[turn_id]
            turn = Turn(id=f"turn:{row.id}", lane_id=row.lane_id)
            turns[turn_id] = turn
            run.items.append(turn)
        return turns[turn_id]

    known_lanes = {lane.id for lane in source.lanes}
    for event in source.events:
        if event.lane_id not in known_lanes:
            raise ValueError(f"event {event.id}: missing Lane {event.lane_id}")
    for lane in source.lanes:
        run.lanes.append(Lane(id=lane.id, name=lane.name, parent_id=lane.parent_lane_id))
        events = sorted((e for e in source.events if e.lane_id == lane.id), key=lambda e: e.seq)
        for event in events:
            if event.event_type.startswith("run.") and event.subject_id == source.run_id:
                observe(run, event)
                # Run payloads and open event vocabularies remain inspectable as operations.
            elif event.event_type.startswith("turn.") and event.subject_id in turn_rows:
                observe(turn_for(event.subject_id), event)
            elif event.event_type.startswith("attempt."):
                attempt = attempts.get(event.subject_id)
                action = actions.get(attempt.action_id) if attempt else None
                if attempt is None or action is None or action.turn_id not in turn_rows:
                    raise ValueError(f"event {event.id}: missing Attempt/Action/Turn ownership")
                if turn_rows[action.turn_id].lane_id != event.lane_id:
                    raise ValueError(f"event {event.id}: Attempt belongs to another Lane")
                call = calls.get(attempt.id)
                if call is None:
                    facts: dict[str, Any] = dict(
                        id=f"call:{attempt.id}",
                        lane_id=lane.id,
                        attributes={
                            "action_id": action.id,
                            "attempt_id": attempt.id,
                            "attempt_no": attempt.attempt_no,
                        },
                    )
                    if action.type == "model_call":
                        call = ModelCall(**facts)
                    elif action.type == "tool_call":
                        call = ToolCall(**facts, tool_call_id=action.key)
                    else:
                        call = Operation(**facts, operation=action.type)
                    calls[attempt.id] = call
                    turn_for(action.turn_id).items.append(call)
                observe(call, event)
                if isinstance(call, (ModelCall, ToolCall)):
                    if event.event_type == "attempt.requested":
                        call.request = CallRequest.from_payload(event.payload)
                    elif event.event_type.rsplit(".", 1)[-1] in _TERMINALS:
                        # Preserve conflicting observations without choosing a result.
                        if len(terminals[call.id]) == 1:
                            call.response = CallResponse.from_payload(event.payload)
                        else:
                            call.response = None
                    else:
                        call.attributes.setdefault("events", []).append(
                            event.model_dump(mode="json")
                        )
                    extra = call_details(event.payload)
                    if extra:
                        call.attributes.setdefault("event_details", {})[event.id] = extra
                else:
                    call.items.append(
                        Operation(
                            id=f"event:{event.id}",
                            operation=event.event_type,
                            data=event.payload,
                            evidence=[SourceRef(source="ledger", id=event.id)],
                        )
                    )
                continue
            op = Operation(
                id=f"event:{event.id}",
                lane_id=lane.id,
                operation=event.event_type,
                started_at=event.occurred_at.isoformat(),
                data=event.payload,
                evidence=[SourceRef(source="ledger", id=event.id)],
                attributes={"extensions": event.extensions},
            )
            if event.event_type.startswith("turn.") and event.subject_id in turn_rows:
                turn_for(event.subject_id).items.append(op)
            else:
                run.items.append(op)
    for row in source.turns:
        turn_for(row.id)
    for attempt in source.attempts:
        if attempt.id not in calls:
            issues.append(f"attempt {attempt.id}: no recorded call events")
    for call in calls.values():
        if call.started_at is None:
            issues.append(f"node {call.id}: request missing")
        if call.id not in terminals:
            issues.append(f"node {call.id}: result missing")
    return Document(runs=[run], issues=issues).model_copy(deep=True)
