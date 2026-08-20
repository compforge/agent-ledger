from datetime import UTC, datetime

import pytest
from pydantic import ValidationError

from agent_ledger import (
    Action,
    Actor,
    Attempt,
    Checkpoint,
    Lane,
    ProposedCheckpoint,
    ProposedEvent,
    Turn,
    new_id,
)
from agent_ledger.models import utc_now


def test_ledger_owned_ids_are_uuid7() -> None:
    lane = Lane(session_id="session", run_id="run")
    turn = Turn(lane_id=lane.id)
    action = Action(turn_id=turn.id, type="compact")
    attempt = Attempt(action_id=action.id, attempt_no=1)
    event = ProposedEvent(
        lane_id=lane.id,
        subject_id=attempt.id,
        event_type="attempt.requested",
        actor_id=Actor(type="harness").id,
    )

    assert all(
        value.split("-")[2].startswith("7")
        for value in (lane.id, turn.id, action.id, attempt.id, event.id, new_id())
    )


def test_event_type_prefix_identifies_subject_kind() -> None:
    lane = Lane(session_id="session", run_id="run")
    event = ProposedEvent(
        lane_id=lane.id,
        subject_id=lane.id,
        event_type="lane.framework.test.recorded",
        actor_id=Actor(type="harness").id,
    )

    assert event.subject_kind == "lane"
    with pytest.raises(ValidationError):
        ProposedEvent(
            lane_id=lane.id,
            subject_id=lane.id,
            event_type="framework.test.recorded",
            actor_id=Actor(type="harness").id,
        )


def test_timestamps_require_timezone() -> None:
    with pytest.raises(ValidationError):
        Lane(session_id="session", run_id="run", created_at=datetime(2026, 1, 2))

    Lane(session_id="session", run_id="run", created_at=datetime(2026, 1, 2, tzinfo=UTC))


def test_actor_is_extensible() -> None:
    actor = Actor(type="custom-runtime")
    assert actor.type == "custom-runtime"


def test_checkpoint_requires_exactly_one_state_source() -> None:
    actor = Actor(type="harness")
    with pytest.raises(ValidationError):
        ProposedCheckpoint(checkpoint_key="key", actor_id=actor.id, format="application/json")
    with pytest.raises(ValidationError):
        ProposedCheckpoint(
            checkpoint_key="key",
            actor_id=actor.id,
            format="application/json",
            state={},
            artifact_ref={
                "uri": "memory://state",
                "sha256": "0" * 64,
                "size": 2,
                "content_type": "application/json",
            },
        )


def test_checkpoint_id_is_uuid7() -> None:
    proposed = ProposedCheckpoint(
        checkpoint_key="key",
        actor_id=Actor(type="harness").id,
        format="application/json",
        state={},
    )
    stored = Checkpoint.from_proposed(proposed, revision=1, created_at=utc_now())
    assert stored.id.split("-")[2].startswith("7")
