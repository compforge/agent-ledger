-- Reference MySQL 8 persistence schema for Agent Ledger.
-- Relationships are logical references validated by the Store; no database foreign keys are used.

CREATE TABLE ledger_actors (
    id CHAR(36) NOT NULL,
    type VARCHAR(64) NOT NULL,
    actor_key VARCHAR(191) NULL COMMENT 'Stable producer key supplied by the upstream system',
    framework VARCHAR(191) NULL,
    created_at TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_ledger_actors_key (actor_key)
);

CREATE TABLE ledger_lanes (
    id CHAR(36) NOT NULL,
    session_id VARCHAR(191) NOT NULL,
    run_id VARCHAR(191) NOT NULL,
    name VARCHAR(191) NOT NULL,
    parent_lane_id CHAR(36) NULL COMMENT 'Logical reference to ledger_lanes.id',
    last_seq BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_ledger_lanes_owner_name (session_id, run_id, name),
    KEY ix_ledger_lanes_owner (session_id, run_id),
    KEY ix_ledger_lanes_parent (parent_lane_id)
);

CREATE TABLE ledger_turns (
    id CHAR(36) NOT NULL,
    lane_id CHAR(36) NOT NULL COMMENT 'Logical reference to ledger_lanes.id',
    created_at TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (id),
    KEY ix_ledger_turns_lane (lane_id)
);

CREATE TABLE ledger_actions (
    id CHAR(36) NOT NULL,
    turn_id CHAR(36) NOT NULL COMMENT 'Logical reference to ledger_turns.id',
    type VARCHAR(191) NOT NULL,
    parent_action_id CHAR(36) NULL COMMENT 'Logical reference to ledger_actions.id',
    effect_kind VARCHAR(32) NOT NULL DEFAULT 'unknown',
    effect_idempotency VARCHAR(32) NOT NULL DEFAULT 'unknown',
    created_at TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (id),
    KEY ix_ledger_actions_turn (turn_id),
    KEY ix_ledger_actions_parent (parent_action_id)
);

CREATE TABLE ledger_attempts (
    id CHAR(36) NOT NULL,
    action_id CHAR(36) NOT NULL COMMENT 'Logical reference to ledger_actions.id',
    attempt_no INT NOT NULL,
    created_at TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_ledger_attempts_action_no (action_id, attempt_no),
    KEY ix_ledger_attempts_action (action_id)
);

CREATE TABLE ledger_events (
    id CHAR(36) NOT NULL,
    schema_version VARCHAR(16) NOT NULL,
    lane_id CHAR(36) NOT NULL COMMENT 'Logical reference to ledger_lanes.id',
    seq BIGINT NOT NULL,
    subject_id VARCHAR(191) NOT NULL COMMENT 'Logical reference inferred from event_type prefix',
    event_type VARCHAR(191) NOT NULL,
    actor_id CHAR(36) NOT NULL COMMENT 'Logical reference to ledger_actors.id',
    causation_id CHAR(36) NULL COMMENT 'Logical reference to ledger_events.id',
    occurred_at TIMESTAMP(6) NOT NULL,
    committed_at TIMESTAMP(6) NOT NULL,
    payload JSON NOT NULL,
    extensions JSON NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_ledger_events_lane_seq (lane_id, seq),
    KEY ix_ledger_events_subject (subject_id),
    KEY ix_ledger_events_actor (actor_id),
    KEY ix_ledger_events_lane_type (lane_id, event_type),
    KEY ix_ledger_events_committed (committed_at, lane_id, seq, id),
    KEY ix_ledger_events_causation (causation_id)
);

CREATE TABLE ledger_appends (
    id CHAR(36) NOT NULL COMMENT 'Append idempotency key',
    lane_id CHAR(36) NOT NULL COMMENT 'Logical reference to ledger_lanes.id',
    digest CHAR(64) NOT NULL,
    first_seq BIGINT NOT NULL,
    last_seq BIGINT NOT NULL,
    committed_at TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (id),
    KEY ix_ledger_appends_lane_range (lane_id, first_seq, last_seq)
);

CREATE TABLE ledger_checkpoints (
    id CHAR(36) NOT NULL COMMENT 'Checkpoint save idempotency key',
    schema_version VARCHAR(16) NOT NULL,
    checkpoint_key VARCHAR(191) NOT NULL,
    revision BIGINT NOT NULL,
    actor_id CHAR(36) NOT NULL COMMENT 'Logical reference to ledger_actors.id',
    format VARCHAR(255) NOT NULL COMMENT 'Opaque format interpreted by the Harness Adapter',
    state JSON NULL,
    artifact_ref JSON NULL,
    lane_id CHAR(36) NULL COMMENT 'Optional logical reference to ledger_lanes.id',
    last_applied_seq BIGINT NULL,
    last_applied_event_id CHAR(36) NULL COMMENT 'Optional logical reference to ledger_events.id',
    extensions JSON NOT NULL,
    created_at TIMESTAMP(6) NOT NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uq_ledger_checkpoints_key_revision (checkpoint_key, revision),
    KEY ix_ledger_checkpoints_actor (actor_id),
    KEY ix_ledger_checkpoints_latest (checkpoint_key, revision),
    KEY ix_ledger_checkpoints_lane_seq (lane_id, last_applied_seq),
    KEY ix_ledger_checkpoints_event (last_applied_event_id)
);
