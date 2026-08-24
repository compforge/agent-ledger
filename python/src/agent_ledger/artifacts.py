from __future__ import annotations

import asyncio
import hashlib
from typing import Protocol
from uuid import uuid4

from agent_ledger.models import Artifact, new_id, utc_now


class ArtifactContentStore(Protocol):
    async def put(
        self,
        key: str,
        version: str,
        data: bytes,
        content_type: str,
    ) -> Artifact: ...

    async def get(self, artifact: Artifact) -> bytes: ...


class MemoryArtifactContentStore:
    def __init__(self) -> None:
        self._artifacts: dict[str, bytes] = {}
        self._lock = asyncio.Lock()

    async def put(self, key: str, version: str, data: bytes, content_type: str) -> Artifact:
        digest = hashlib.sha256(data).hexdigest()
        uri = f"memory://{key}/{version}/{uuid4()}"
        async with self._lock:
            self._artifacts[uri] = data
        return Artifact(
            id=new_id(),
            key=key,
            version=version,
            uri=uri,
            sha256=digest,
            size=len(data),
            content_type=content_type,
            created_at=utc_now(),
        )

    async def get(self, artifact: Artifact) -> bytes:
        async with self._lock:
            data = self._artifacts[artifact.uri]
        if hashlib.sha256(data).hexdigest() != artifact.sha256:
            raise ValueError(f"artifact digest mismatch for {artifact.uri}")
        return data
