"""Shared call facts; no storage, recovery or rendering dependencies."""

from typing import Any, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    model_serializer,
    model_validator,
)


class Fact(BaseModel):
    model_config = ConfigDict(extra="forbid")

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> Self:
        return cls.model_validate({k: v for k, v in payload.items() if k in cls.model_fields})

    @model_serializer(mode="wrap")
    def serialize(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        values = handler(self)
        # Omit absent metadata, but preserve explicit native JSON null content.
        return {
            key: value
            for key, value in values.items()
            if value is not None
            or (
                type(self).model_fields[key].json_schema_extra == {"preserve_null": True}
                and key in self.model_fields_set
            )
        }


class Model(Fact):
    id: str = Field(min_length=1)
    provider: str | None = Field(default=None, min_length=1)


class Usage(Fact):
    input_tokens: int | None = Field(default=None, ge=0)
    output_tokens: int | None = Field(default=None, ge=0)
    cache_read_input_tokens: int | None = Field(default=None, ge=0)
    cache_write_input_tokens: int | None = Field(default=None, ge=0)
    total_tokens: int | None = Field(default=None, ge=0)


class CallError(Fact):
    type: str = Field(min_length=1)
    message: str
    code: str | None = Field(default=None, min_length=1)
    retryable: bool | None = None


class CallRequest(Fact):
    # Native payloads remain structured and ordered; no transcript reconstruction.
    input: Any = Field(default=None, json_schema_extra={"preserve_null": True})
    input_artifact_id: str | None = None
    model: Model | None = None
    tool_name: str | None = None
    client_request_id: str | None = None

    @model_validator(mode="after")
    def check_content(self) -> Self:
        if "input" in self.model_fields_set and self.input_artifact_id is not None:
            raise ValueError("request cannot contain both input and input_artifact_id")
        return self


class CallResponse(Fact):
    output: Any = Field(default=None, json_schema_extra={"preserve_null": True})
    output_artifact_id: str | None = None
    model: Model | None = None
    usage: Usage | None = None
    error: CallError | None = None
    finish_reason: str | None = None
    provider_request_id: str | None = None
    external_operation_id: str | None = None

    @model_validator(mode="after")
    def check_content(self) -> Self:
        if "output" in self.model_fields_set and self.output_artifact_id is not None:
            raise ValueError("response cannot contain both output and output_artifact_id")
        return self


def call_details(payload: dict[str, Any]) -> dict[str, Any]:
    """Return source details outside the shared call contract."""
    return {
        k: v
        for k, v in payload.items()
        if k not in CallRequest.model_fields and k not in CallResponse.model_fields
    }
