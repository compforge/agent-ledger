from __future__ import annotations

import asyncio
import hashlib
from typing import Protocol
from uuid import uuid4

from agent_ledger.models import ArtifactRef


class ArtifactStore(Protocol):
    async def put(
        self,
        session_id: str,
        data: bytes,
        content_type: str,
    ) -> ArtifactRef: ...

    async def get(self, ref: ArtifactRef) -> bytes: ...


class MemoryArtifactStore:
    def __init__(self) -> None:
        self._artifacts: dict[str, bytes] = {}
        self._lock = asyncio.Lock()

    async def put(self, session_id: str, data: bytes, content_type: str) -> ArtifactRef:
        digest = hashlib.sha256(data).hexdigest()
        uri = f"memory://{session_id}/{uuid4()}"
        async with self._lock:
            self._artifacts[uri] = data
        return ArtifactRef(uri=uri, sha256=digest, size=len(data), content_type=content_type)

    async def get(self, ref: ArtifactRef) -> bytes:
        async with self._lock:
            data = self._artifacts[ref.uri]
        if hashlib.sha256(data).hexdigest() != ref.sha256:
            raise ValueError(f"artifact digest mismatch for {ref.uri}")
        return data
