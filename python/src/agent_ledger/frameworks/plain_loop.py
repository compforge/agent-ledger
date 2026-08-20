from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from agent_ledger.adapters import (
    AdapterCapabilities,
    AdapterDescriptor,
    RecordingGuarantee,
    RecoveryMode,
)
from agent_ledger.inspection import UnresolvedAttempt, inspect_session
from agent_ledger.models import EventType, SessionView, StoredEvent
from agent_ledger.recorder import LaneRecorder


class PlainLoopContext(BaseModel):
    model_config = ConfigDict(extra="forbid")

    messages: list[dict[str, Any]] = Field(default_factory=list)
    completed_turns: list[str] = Field(default_factory=list)
    state: dict[str, Any] = Field(default_factory=dict)


class PlainLoopRecovery(BaseModel):
    model_config = ConfigDict(frozen=True)

    context: PlainLoopContext
    unresolved_attempts: tuple[UnresolvedAttempt, ...]
    restored_through_seq: int


class PlainLoopProfile:
    """Example profile for a loop whose context is messages plus JSON state."""

    profile_id = "plain-loop"
    profile_version = "1"
    descriptor = AdapterDescriptor(
        adapter_id=profile_id,
        adapter_version=profile_version,
        framework="plain-loop",
        framework_version="*",
        capabilities=AdapterCapabilities(
            model_prewrite=RecordingGuarantee.STRICT,
            tool_prewrite=RecordingGuarantee.STRICT,
            outcome_gate=RecordingGuarantee.STRICT,
            recovery=RecoveryMode.SNAPSHOT,
            preserves_native_state=True,
        ),
    )

    async def save(
        self,
        recorder: LaneRecorder,
        context: PlainLoopContext,
    ) -> StoredEvent:
        return await recorder.save_snapshot(
            self.profile_id,
            context.model_dump(mode="json"),
            profile_version=self.profile_version,
        )

    def restore(self, view: SessionView, lane_id: str) -> PlainLoopContext:
        return self.recover(view, lane_id).context

    def recover(self, view: SessionView, lane_id: str) -> PlainLoopRecovery:
        events = sorted(
            (event for event in view.events if event.lane_id == lane_id),
            key=lambda event: event.seq,
        )
        actions = {action.id: action for action in view.actions}
        attempts = {attempt.id: attempt for attempt in view.attempts}
        context = PlainLoopContext()
        replay_from = 0

        for index, event in enumerate(events):
            if event.event_type != EventType.LANE_FRAMEWORK_SNAPSHOT_SAVED:
                continue
            if event.payload.get("profile") != self.profile_id:
                continue
            if event.payload.get("profile_version") != self.profile_version:
                continue
            context = PlainLoopContext.model_validate(event.payload["snapshot"])
            replay_from = index + 1

        for event in events[replay_from:]:
            _apply_event(context, event, actions, attempts)

        inspection = inspect_session(view)
        unresolved = tuple(
            attempt for attempt in inspection.unresolved_attempts if attempt.lane_id == lane_id
        )
        return PlainLoopRecovery(
            context=context,
            unresolved_attempts=unresolved,
            restored_through_seq=events[-1].seq if events else 0,
        )


def _apply_event(
    context: PlainLoopContext,
    event: StoredEvent,
    actions: dict[str, Any],
    attempts: dict[str, Any],
) -> None:
    if event.event_type == EventType.TURN_COMPLETED:
        if event.subject_id not in context.completed_turns:
            context.completed_turns.append(event.subject_id)
        return
    if event.event_type != EventType.ATTEMPT_COMPLETED:
        return
    attempt = attempts.get(event.subject_id)
    action = actions.get(attempt.action_id) if attempt is not None else None
    if action is None:
        return
    if action.type == "model_call":
        model_message = event.payload.get("message")
        if isinstance(model_message, dict):
            context.messages.append(model_message)
        elif model_message is not None:
            context.messages.append({"role": "assistant", "content": model_message})
    elif action.type == "tool_call":
        result = event.payload.get("result")
        if result is not None:
            tool_message: dict[str, Any] = {"role": "tool", "content": result}
            if "tool_call_id" in event.payload:
                tool_message["tool_call_id"] = event.payload["tool_call_id"]
            context.messages.append(tool_message)
