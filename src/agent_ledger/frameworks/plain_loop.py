from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from agent_ledger.inspection import UnresolvedAttempt, inspect_session
from agent_ledger.models import EventType, StoredEvent
from agent_ledger.recorder import SessionRecorder


class PlainLoopContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    messages: list[dict[str, Any]] = Field(default_factory=list)
    completed_steps: list[str] = Field(default_factory=list)
    state: dict[str, Any] = Field(default_factory=dict)


class PlainLoopRecovery(BaseModel):
    model_config = ConfigDict(frozen=True)

    context: PlainLoopContext
    unresolved_attempts: tuple[UnresolvedAttempt, ...]
    restored_through_version: int


class PlainLoopProfile:
    """Example profile for a loop whose context is messages plus JSON state."""

    profile_id = "plain-loop"
    profile_version = "1"

    async def save(
        self,
        recorder: SessionRecorder,
        context: PlainLoopContext,
    ) -> StoredEvent:
        return await recorder.save_snapshot(
            self.profile_id,
            context.model_dump(mode="json"),
            profile_version=self.profile_version,
        )

    def restore(self, events: Sequence[StoredEvent]) -> PlainLoopContext:
        return self.recover(events).context

    def recover(self, events: Sequence[StoredEvent]) -> PlainLoopRecovery:
        context = PlainLoopContext()
        replay_from = 0

        for index, event in enumerate(events):
            if event.event_type != EventType.FRAMEWORK_SNAPSHOT_SAVED:
                continue
            if event.payload.get("profile") != self.profile_id:
                continue
            if event.payload.get("profile_version") != self.profile_version:
                continue
            context = PlainLoopContext.model_validate(event.payload["snapshot"])
            replay_from = index + 1

        for event in events[replay_from:]:
            _apply_event(context, event)

        unresolved = inspect_session(list(events)).unresolved_attempts
        last_version = events[-1].stream_version if events else -1
        return PlainLoopRecovery(
            context=context,
            unresolved_attempts=unresolved,
            restored_through_version=last_version,
        )


def _apply_event(context: PlainLoopContext, event: StoredEvent) -> None:
    if event.event_type == EventType.MODEL_COMPLETED:
        model_message = event.payload.get("message")
        if isinstance(model_message, dict):
            context.messages.append(model_message)
        elif model_message is not None:
            context.messages.append({"role": "assistant", "content": model_message})
    elif event.event_type == EventType.TOOL_COMPLETED:
        result = event.payload.get("result")
        if result is not None:
            tool_message: dict[str, Any] = {"role": "tool", "content": result}
            if "tool_call_id" in event.payload:
                tool_message["tool_call_id"] = event.payload["tool_call_id"]
            context.messages.append(tool_message)
    elif event.event_type == EventType.STEP_COMPLETED and event.step_id is not None:
        if event.step_id not in context.completed_steps:
            context.completed_steps.append(event.step_id)
