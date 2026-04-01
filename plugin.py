"""MIRASTACK query_traces plugin — queries VictoriaTraces/Jaeger."""

from __future__ import annotations

import asyncio
import json

from mirastack_sdk import (
    Plugin,
    PluginInfo,
    PluginSchema,
    SchemaParam,
    EngineContext,
    Permission,
    DevOpsStage,
    ExecutionRequest,
    ExecutionResponse,
    serve,
)
from traces_client import TracesClient


class QueryTracesPlugin(Plugin):
    """Plugin for querying VictoriaTraces/Jaeger trace stores."""

    def __init__(self):
        self._client: TracesClient | None = None

    def info(self) -> PluginInfo:
        return PluginInfo(
            name="query_traces",
            version="0.1.0",
            description="Query VictoriaTraces/Jaeger for distributed traces",
            permission=Permission.READ,
            devops_stages=[DevOpsStage.OBSERVE],
        )

    def schema(self) -> PluginSchema:
        return PluginSchema(
            params=[
                SchemaParam(name="action", type="string", required=True,
                           description="One of: search, trace_by_id, services, operations, dependencies"),
                SchemaParam(name="trace_id", type="string", required=False,
                           description="Trace ID for trace_by_id action"),
                SchemaParam(name="service", type="string", required=False,
                           description="Service name filter"),
                SchemaParam(name="operation", type="string", required=False,
                           description="Operation name filter"),
                SchemaParam(name="start", type="string", required=False,
                           description="Start time"),
                SchemaParam(name="end", type="string", required=False,
                           description="End time"),
                SchemaParam(name="limit", type="string", required=False,
                           description="Max results (default 20)"),
                SchemaParam(name="min_duration", type="string", required=False,
                           description="Minimum span duration (e.g., 100ms)"),
                SchemaParam(name="max_duration", type="string", required=False,
                           description="Maximum span duration"),
                SchemaParam(name="tags", type="string", required=False,
                           description="JSON object of tag filters"),
            ],
        )

    async def execute(self, ctx: EngineContext, req: ExecutionRequest) -> ExecutionResponse:
        if self._client is None:
            config = await ctx.get_config()
            base_url = config.get("traces_url", "http://localhost:9411")
            self._client = TracesClient(base_url)

        action = req.params.get("action", "")
        try:
            result = await self._dispatch(action, req.params)
            return ExecutionResponse(
                output={"result": json.dumps(result, default=str)},
            )
        except Exception as e:
            return ExecutionResponse(
                output={"error": str(e)},
                error=str(e),
            )

    async def _dispatch(self, action: str, params: dict) -> dict | list:
        match action:
            case "search":
                tags = None
                if tags_str := params.get("tags"):
                    tags = json.loads(tags_str)
                return await self._client.search(
                    service=params.get("service"),
                    operation=params.get("operation"),
                    tags=tags,
                    start=params.get("start"),
                    end=params.get("end"),
                    limit=int(params.get("limit", "20")),
                    min_duration=params.get("min_duration"),
                    max_duration=params.get("max_duration"),
                )
            case "trace_by_id":
                return await self._client.trace_by_id(params["trace_id"])
            case "services":
                return await self._client.services()
            case "operations":
                return await self._client.operations(params["service"])
            case "dependencies":
                return await self._client.dependencies(params.get("end"))
            case _:
                raise ValueError(f"Unknown action: {action}")

    async def health_check(self) -> bool:
        if self._client is None:
            return False
        try:
            await self._client.services()
            return True
        except Exception:
            return False

    async def config_updated(self, config: dict):
        if "traces_url" in config:
            if self._client:
                await self._client.close()
            self._client = TracesClient(config["traces_url"])


def main():
    plugin = QueryTracesPlugin()
    asyncio.run(serve(plugin))


if __name__ == "__main__":
    main()
