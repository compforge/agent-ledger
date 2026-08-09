from enum import StrEnum
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field


class RecordingGuarantee(StrEnum):
    STRICT = "strict"
    BEST_EFFORT = "best_effort"
    UNSUPPORTED = "unsupported"


class RecoveryMode(StrEnum):
    NATIVE_STORE = "native_store"
    SNAPSHOT = "snapshot"
    CHECKPOINT = "checkpoint"
    UNSUPPORTED = "unsupported"


class AdapterCapabilities(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    model_prewrite: RecordingGuarantee
    tool_prewrite: RecordingGuarantee
    outcome_gate: RecordingGuarantee
    recovery: RecoveryMode
    preserves_native_state: bool


class AdapterDescriptor(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")

    schema_version: Literal["1.0"] = "1.0"
    adapter_id: str = Field(min_length=1)
    adapter_version: str = Field(min_length=1)
    framework: str = Field(min_length=1)
    framework_version: str = Field(min_length=1)
    capabilities: AdapterCapabilities
