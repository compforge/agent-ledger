from __future__ import annotations

import json
from collections import defaultdict
from typing import Any

from agent_ledger.models import ActionType, EventType, SessionView, StoredEvent


def project_atif(
    view: SessionView,
    *,
    agent_name: str = "agent-ledger",
    agent_version: str = "unknown",
) -> list[dict[str, Any]]:
    """Project a Session view into one ATIF-v1.7 trajectory per root Run."""

    lanes = {lane.id: lane for lane in view.lanes}
    turns = {turn.id: turn for turn in view.turns}
    actions = {action.id: action for action in view.actions}
    attempts = {attempt.id: attempt for attempt in view.attempts}
    by_run: dict[str, list[StoredEvent]] = defaultdict(list)
    parents: dict[str, str] = {}
    for event in view.events:
        run_id = lanes[event.lane_id].run_id
        by_run[run_id].append(event)
        if event.event_type == EventType.RUN_STARTED:
            parent = event.payload.get("parent_run_id")
            if isinstance(parent, str):
                parents[run_id] = parent

    children: dict[str, list[str]] = defaultdict(list)
    for child, parent in parents.items():
        children[parent].append(child)
    roots = [run_id for run_id in by_run if run_id not in parents or parents[run_id] not in by_run]
    return [
        _project_run(
            run_id,
            by_run,
            children,
            turns,
            actions,
            attempts,
            session_id=view.session_id,
            agent_name=agent_name,
            agent_version=agent_version,
            is_root=True,
        )
        for run_id in roots
    ]


def _project_run(
    run_id: str,
    by_run: dict[str, list[StoredEvent]],
    children: dict[str, list[str]],
    turns: dict[str, Any],
    actions: dict[str, Any],
    attempts: dict[str, Any],
    *,
    session_id: str,
    agent_name: str,
    agent_version: str,
    is_root: bool,
) -> dict[str, Any]:
    events = by_run[run_id]
    steps: list[dict[str, Any]] = []
    attempt_requests: dict[str, StoredEvent] = {}
    tool_steps: dict[str, dict[str, Any]] = {}
    model_name = ""

    for event in events:
        if event.event_type == EventType.RUN_STARTED:
            for message in event.payload.get("messages", []):
                if isinstance(message, dict):
                    steps.append(_message_step(len(steps), event, run_id, message))
            continue
        if event.subject_kind != "attempt":
            continue
        attempt = attempts[event.subject_id]
        action = actions[attempt.action_id]
        turn = turns[action.turn_id]
        if event.event_type == EventType.ATTEMPT_REQUESTED:
            attempt_requests[attempt.id] = event
            if action.type == ActionType.MODEL_CALL:
                model_name = str(event.payload.get("model", model_name))
            elif action.type == ActionType.TOOL_CALL:
                target = _last_agent_step(steps, event, run_id, turn.id)
                target.setdefault("tool_calls", []).append(
                    {
                        "tool_call_id": str(event.payload.get("tool_call_id", attempt.id)),
                        "function_name": str(
                            event.payload.get("tool_name", event.payload.get("name", "unknown"))
                        ),
                        "arguments": _arguments(
                            event.payload.get("arguments", event.payload.get("input", {}))
                        ),
                    }
                )
                tool_steps[attempt.id] = target
        elif (
            event.event_type == EventType.ATTEMPT_COMPLETED and action.type == ActionType.MODEL_CALL
        ):
            steps.append(_model_step(len(steps), event, run_id, turn.id, attempt.id))
        elif (
            event.event_type in {EventType.ATTEMPT_COMPLETED, EventType.ATTEMPT_FAILED}
            and action.type == ActionType.TOOL_CALL
        ):
            target = tool_steps.get(attempt.id) or _last_agent_step(steps, event, run_id, turn.id)
            observation = target.setdefault("observation", {"results": []})
            result = event.payload.get("result", event.payload.get("error", ""))
            request = attempt_requests.get(attempt.id)
            tool_call_id = (
                request.payload.get("tool_call_id", attempt.id) if request else attempt.id
            )
            observation["results"].append(
                {
                    "source_call_id": str(tool_call_id),
                    "content": _content(result),
                    "extra": {
                        "ok": event.event_type == EventType.ATTEMPT_COMPLETED,
                        "event_id": event.id,
                        "attempt_id": attempt.id,
                    },
                }
            )

    trajectory: dict[str, Any] = {
        "schema_version": "ATIF-v1.7",
        "trajectory_id": run_id,
        "agent": {"name": agent_name, "version": agent_version},
        "steps": steps,
        "final_metrics": {
            "total_prompt_tokens": sum(
                int(step.get("metrics", {}).get("prompt_tokens", 0)) for step in steps
            ),
            "total_completion_tokens": sum(
                int(step.get("metrics", {}).get("completion_tokens", 0)) for step in steps
            ),
            "total_cached_tokens": sum(
                int(step.get("metrics", {}).get("cached_tokens", 0)) for step in steps
            ),
            "total_steps": len(steps),
        },
        "extra": {"run_id": run_id},
    }
    if is_root:
        trajectory["session_id"] = session_id
    if model_name:
        trajectory["agent"]["model_name"] = model_name
    subagents = [
        _project_run(
            child,
            by_run,
            children,
            turns,
            actions,
            attempts,
            session_id=session_id,
            agent_name=agent_name,
            agent_version=agent_version,
            is_root=False,
        )
        for child in children.get(run_id, [])
    ]
    if subagents:
        trajectory["subagent_trajectories"] = subagents
    return trajectory


def _message_step(
    index: int,
    event: StoredEvent,
    run_id: str,
    message: dict[str, Any],
) -> dict[str, Any]:
    role = str(message.get("role", "user"))
    source = "agent" if role == "assistant" else role
    if source not in {"system", "user", "agent"}:
        source = "user"
    return {
        "step_id": index,
        "timestamp": event.occurred_at.isoformat(),
        "source": source,
        "message": message.get("content", ""),
        "extra": {"event_id": event.id, "run_id": run_id},
    }


def _model_step(
    index: int,
    event: StoredEvent,
    run_id: str,
    turn_id: str,
    attempt_id: str,
) -> dict[str, Any]:
    message = event.payload.get("message", event.payload.get("output", ""))
    content = message.get("content", "") if isinstance(message, dict) else message
    usage = event.payload.get("usage", {})
    step: dict[str, Any] = {
        "step_id": index,
        "timestamp": event.occurred_at.isoformat(),
        "source": "agent",
        "message": content,
        "llm_call_count": 1,
        "extra": {
            "event_id": event.id,
            "run_id": run_id,
            "turn_id": turn_id,
            "attempt_id": attempt_id,
        },
    }
    if isinstance(usage, dict):
        step["metrics"] = {
            "prompt_tokens": int(usage.get("prompt_tokens", 0)),
            "completion_tokens": int(usage.get("completion_tokens", 0)),
            "cached_tokens": int(usage.get("cached_tokens", 0)),
        }
    if "model" in event.payload:
        step["model_name"] = str(event.payload["model"])
    return step


def _last_agent_step(
    steps: list[dict[str, Any]],
    event: StoredEvent,
    run_id: str,
    turn_id: str,
) -> dict[str, Any]:
    for candidate in reversed(steps):
        if candidate["source"] == "agent":
            return candidate
    created: dict[str, Any] = {
        "step_id": len(steps),
        "timestamp": event.occurred_at.isoformat(),
        "source": "agent",
        "message": "",
        "extra": {"run_id": run_id, "turn_id": turn_id},
    }
    steps.append(created)
    return created


def _arguments(value: Any) -> dict[str, Any]:
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {"value": value}
        if isinstance(parsed, dict):
            return parsed
    return {"value": value}


def _content(value: Any) -> str:
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True)
