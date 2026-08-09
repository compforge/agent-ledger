from __future__ import annotations

import json
from collections import defaultdict
from collections.abc import Sequence
from typing import Any

from agent_ledger.models import EventType, StoredEvent


def project_atif(
    events: Sequence[StoredEvent],
    *,
    agent_name: str = "agent-ledger",
    agent_version: str = "unknown",
) -> list[dict[str, Any]]:
    """Project a session ledger into one ATIF-v1.7 trajectory per root run."""

    by_run: dict[str, list[StoredEvent]] = defaultdict(list)
    parents: dict[str, str] = {}
    for event in events:
        by_run[event.run_id].append(event)
        if event.parent_run_id is not None:
            parents[event.run_id] = event.parent_run_id

    children: dict[str, list[str]] = defaultdict(list)
    for child, parent in parents.items():
        children[parent].append(child)

    roots = [run_id for run_id in by_run if run_id not in parents or parents[run_id] not in by_run]
    return [
        _project_run(
            run_id,
            by_run,
            children,
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
    *,
    agent_name: str,
    agent_version: str,
    is_root: bool,
) -> dict[str, Any]:
    events = sorted(by_run[run_id], key=lambda event: event.stream_version)
    session_id = events[0].session_id
    steps: list[dict[str, Any]] = []
    attempts: dict[str, dict[str, Any]] = {}
    model_name = ""

    for event in events:
        if event.event_type == EventType.RUN_STARTED:
            for message in event.payload.get("messages", []):
                if isinstance(message, dict):
                    steps.append(_message_step(len(steps), event, message))
        elif event.event_type == EventType.MODEL_REQUESTED:
            if event.attempt_id is not None:
                attempts[event.attempt_id] = {"request": event}
            model_name = str(event.payload.get("model", model_name))
        elif event.event_type == EventType.MODEL_COMPLETED:
            steps.append(_model_step(len(steps), event))
        elif event.event_type == EventType.TOOL_REQUESTED and event.attempt_id is not None:
            attempts[event.attempt_id] = {"request": event}
            target = _last_agent_step(steps, event)
            target.setdefault("tool_calls", []).append(
                {
                    "tool_call_id": str(event.payload.get("tool_call_id", event.attempt_id)),
                    "function_name": str(event.payload.get("name", "unknown")),
                    "arguments": _arguments(event.payload.get("arguments", {})),
                }
            )
            attempts[event.attempt_id]["step"] = target
        elif event.event_type in {EventType.TOOL_COMPLETED, EventType.TOOL_FAILED}:
            if event.attempt_id is None:
                continue
            attempt = attempts.get(event.attempt_id, {})
            target = attempt.get("step") or _last_agent_step(steps, event)
            observation = target.setdefault("observation", {"results": []})
            result = event.payload.get("result", event.payload.get("error", ""))
            observation["results"].append(
                {
                    "source_call_id": str(event.payload.get("tool_call_id", event.attempt_id)),
                    "content": _content(result),
                    "extra": {
                        "ok": event.event_type == EventType.TOOL_COMPLETED,
                        "event_id": event.event_id,
                        "attempt_id": event.attempt_id,
                    },
                }
            )

    trajectory: dict[str, Any] = {
        "schema_version": "ATIF-v1.7",
        "trajectory_id": run_id,
        "agent": {
            "name": agent_name,
            "version": agent_version,
        },
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
            agent_name=agent_name,
            agent_version=agent_version,
            is_root=False,
        )
        for child in children.get(run_id, [])
    ]
    if subagents:
        trajectory["subagent_trajectories"] = subagents
    return trajectory


def _message_step(index: int, event: StoredEvent, message: dict[str, Any]) -> dict[str, Any]:
    role = str(message.get("role", "user"))
    source = "agent" if role == "assistant" else role
    if source not in {"system", "user", "agent"}:
        source = "user"
    return {
        "step_id": index,
        "timestamp": event.occurred_at.isoformat(),
        "source": source,
        "message": message.get("content", ""),
        "extra": {"event_id": event.event_id, "run_id": event.run_id},
    }


def _model_step(index: int, event: StoredEvent) -> dict[str, Any]:
    message = event.payload.get("message", event.payload.get("output", ""))
    if isinstance(message, dict):
        content = message.get("content", "")
    else:
        content = message
    usage = event.payload.get("usage", {})
    step: dict[str, Any] = {
        "step_id": index,
        "timestamp": event.occurred_at.isoformat(),
        "source": "agent",
        "message": content,
        "llm_call_count": 1,
        "extra": {
            "event_id": event.event_id,
            "run_id": event.run_id,
            "attempt_id": event.attempt_id,
            "logical_step_id": event.step_id,
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


def _last_agent_step(steps: list[dict[str, Any]], event: StoredEvent) -> dict[str, Any]:
    for candidate in reversed(steps):
        if candidate["source"] == "agent":
            return candidate
    created: dict[str, Any] = {
        "step_id": len(steps),
        "timestamp": event.occurred_at.isoformat(),
        "source": "agent",
        "message": "",
        "extra": {"run_id": event.run_id, "logical_step_id": event.step_id},
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
