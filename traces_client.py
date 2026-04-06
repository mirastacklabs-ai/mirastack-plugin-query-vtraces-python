"""VictoriaTraces / Jaeger HTTP client."""

from __future__ import annotations

import httpx
from typing import Any


# VictoriaTraces exposes the Jaeger API under /select/jaeger/api/.
# This prefix is required — plain /api/* only works with standalone Jaeger.
_API_PREFIX = "/select/jaeger/api"


class TracesClient:
    """Client for VictoriaTraces/Jaeger trace query APIs."""

    def __init__(self, base_url: str, timeout: float = 30.0):
        self.base_url = base_url.rstrip("/")
        self._client = httpx.AsyncClient(base_url=self.base_url, timeout=timeout)

    async def search(
        self,
        service: str | None = None,
        operation: str | None = None,
        tags: dict[str, str] | None = None,
        start: str | None = None,
        end: str | None = None,
        limit: int = 20,
        min_duration: str | None = None,
        max_duration: str | None = None,
    ) -> list[dict[str, Any]]:
        """Search for traces matching criteria (Jaeger API)."""
        params: dict[str, Any] = {"limit": limit}
        if service:
            params["service"] = service
        if operation:
            params["operation"] = operation
        if start:
            params["start"] = start
        if end:
            params["end"] = end
        if min_duration:
            params["minDuration"] = min_duration
        if max_duration:
            params["maxDuration"] = max_duration
        if tags:
            import json
            params["tags"] = json.dumps(tags)

        resp = await self._client.get(f"{_API_PREFIX}/traces", params=params)
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", [])

    async def trace_by_id(self, trace_id: str) -> dict[str, Any]:
        """Get a specific trace by ID."""
        resp = await self._client.get(f"{_API_PREFIX}/traces/{trace_id}")
        resp.raise_for_status()
        data = resp.json()
        traces = data.get("data", [])
        return traces[0] if traces else {}

    async def services(self) -> list[str]:
        """List all known service names."""
        resp = await self._client.get(f"{_API_PREFIX}/services")
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", [])

    async def operations(self, service: str) -> list[str]:
        """List operations for a service."""
        resp = await self._client.get(f"{_API_PREFIX}/services/{service}/operations")
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", [])

    async def dependencies(self, end_ts: str | None = None) -> list[dict[str, Any]]:
        """Get service dependency graph."""
        params = {}
        if end_ts:
            params["endTs"] = end_ts
        resp = await self._client.get(f"{_API_PREFIX}/dependencies", params=params)
        resp.raise_for_status()
        data = resp.json()
        return data.get("data", [])

    async def close(self):
        await self._client.aclose()
