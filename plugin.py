"""MIRASTACK query_traces plugin — queries VictoriaTraces/Jaeger."""

from __future__ import annotations

import json
import os

from mirastack_sdk import (
    ConfigParam,
    Plugin,
    PluginInfo,
    PluginSchema,
    ParamSchema,
    Permission,
    DevOpsStage,
    ExecuteRequest,
    ExecuteResponse,
    serve,
)
from mirastack_sdk.datetimeutils import format_epoch_micros, format_epoch_millis, format_lookback_millis
from mirastack_sdk.plugin import TimeRange
from traces_client import TracesClient


class QueryTracesPlugin(Plugin):
    """Plugin for querying VictoriaTraces/Jaeger trace stores."""

    def __init__(self):
        self._client: TracesClient | None = None
        # Bootstrap from env var; engine pushes runtime config via config_updated()
        url = os.environ.get("MIRASTACK_TRACES_URL", "")
        if url:
            self._client = TracesClient(url)

    def info(self) -> PluginInfo:
        return PluginInfo(
            name="query_traces",
            version="0.1.0",
            description="Query VictoriaTraces/Jaeger for distributed traces",
            permissions=[Permission.READ],
            devops_stages=[DevOpsStage.OBSERVE],
            config_params=[
                ConfigParam(key="traces_url", type="string", required=True, description="VictoriaTraces base URL (e.g. http://victoriatraces:9411)"),
            ],
        )

    def schema(self) -> PluginSchema:
        return PluginSchema(
            input_params=[
                ParamSchema(name="action", type="string", required=True,
                            description="One of: search, trace_by_id, services, operations, dependencies"),
                ParamSchema(name="trace_id", type="string", required=False,
                            description="Trace ID for trace_by_id action"),
                ParamSchema(name="service", type="string", required=False,
                            description="Service name filter"),
                ParamSchema(name="operation", type="string", required=False,
                            description="Operation name filter"),
                ParamSchema(name="start", type="string", required=False,
                            description="Start time"),
                ParamSchema(name="end", type="string", required=False,
                            description="End time"),
                ParamSchema(name="limit", type="string", required=False,
                            description="Max results (default 20)"),
                ParamSchema(name="min_duration", type="string", required=False,
                            description="Minimum span duration (e.g., 100ms)"),
                ParamSchema(name="max_duration", type="string", required=False,
                            description="Maximum span duration"),
                ParamSchema(name="tags", type="string", required=False,
                            description="JSON object of tag filters"),
            ],
            output_params=[
                ParamSchema(name="result", type="json", required=True,
                            description="Query result as JSON"),
            ],
        )

    async def execute(self, req: ExecuteRequest) -> ExecuteResponse:
        if self._client is None:
            return ExecuteResponse(
                output={"error": "traces_url not configured — set MIRASTACK_TRACES_URL or push config via engine"},
                logs=["ERROR: no traces client configured"],
            )

        action = req.params.get("action", "")
        try:
            result = await self._dispatch(action, req.params, req.time_range)
            return ExecuteResponse(
                output={"result": json.dumps(result, default=str)},
            )
        except Exception as e:
            return ExecuteResponse(
                output={"error": str(e)},
                logs=[f"ERROR: {e}"],
            )

    async def _dispatch(self, action: str, params: dict, tr: TimeRange | None = None) -> dict | list:
        match action:
            case "search":
                tags = None
                if tags_str := params.get("tags"):
                    tags = json.loads(tags_str)
                # Prefer engine-parsed TimeRange for start/end
                if tr and tr.start_epoch_ms > 0:
                    start = format_epoch_micros(tr.start_epoch_ms)
                    end = format_epoch_micros(tr.end_epoch_ms)
                else:
                    start = params.get("start")
                    end = params.get("end")
                return await self._client.search(
                    service=params.get("service"),
                    operation=params.get("operation"),
                    tags=tags,
                    start=start,
                    end=end,
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
                if tr and tr.end_epoch_ms > 0:
                    end_ts = format_epoch_millis(tr.end_epoch_ms)
                    lookback = format_lookback_millis(tr.start_epoch_ms, tr.end_epoch_ms)
                    return await self._client.dependencies(end_ts, lookback)
                return await self._client.dependencies(params.get("end"))
            case _:
                raise ValueError(f"Unknown action: {action}")

    async def health_check(self) -> None:
        # Pull config from engine (cached 15s in SDK)
        ec = getattr(self, "_engine_context", None)
        if ec is not None:
            try:
                config = await ec.get_config()
                await self._apply_config(config)
            except Exception:
                pass
        if self._client is None:
            raise RuntimeError("traces_url not configured")
        await self._client.services()

    async def config_updated(self, config: dict[str, str]) -> None:
        await self._apply_config(config)

    async def _apply_config(self, config: dict[str, str]) -> None:
        if "traces_url" in config:
            if self._client:
                await self._client.close()
            self._client = TracesClient(config["traces_url"])


def main():
    plugin = QueryTracesPlugin()
    serve(plugin)


if __name__ == "__main__":
    main()
