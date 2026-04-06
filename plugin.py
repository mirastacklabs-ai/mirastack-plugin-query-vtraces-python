"""MIRASTACK query_traces plugin — queries VictoriaTraces/Jaeger."""

from __future__ import annotations

import json
import os

from mirastack_sdk import (
    Action,
    ConfigParam,
    Plugin,
    PluginInfo,
    PluginSchema,
    ParamSchema,
    Permission,
    DevOpsStage,
    ExecuteRequest,
    ExecuteResponse,
    respond_map,
    respond_error,
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
            actions=[
                Action(
                    id="search",
                    description="Search for traces by service, operation, tags, and time range",
                    permission=Permission.READ,
                    stages=[DevOpsStage.OBSERVE],
                    input_params=[
                        ParamSchema(name="service", type="string", required=False, description="Service name filter"),
                        ParamSchema(name="operation", type="string", required=False, description="Operation name filter"),
                        ParamSchema(name="start", type="string", required=False, description="Start time"),
                        ParamSchema(name="end", type="string", required=False, description="End time"),
                        ParamSchema(name="limit", type="string", required=False, description="Max results (default 20)"),
                        ParamSchema(name="min_duration", type="string", required=False, description="Minimum span duration (e.g., 100ms)"),
                        ParamSchema(name="max_duration", type="string", required=False, description="Maximum span duration"),
                        ParamSchema(name="tags", type="string", required=False, description="JSON object of tag filters"),
                    ],
                    output_params=[ParamSchema(name="result", type="json", required=True, description="Matching traces")],
                ),
                Action(
                    id="trace_by_id",
                    description="Retrieve a single trace by its trace ID",
                    permission=Permission.READ,
                    stages=[DevOpsStage.OBSERVE],
                    input_params=[
                        ParamSchema(name="trace_id", type="string", required=True, description="Trace ID"),
                    ],
                    output_params=[ParamSchema(name="result", type="json", required=True, description="Full trace")],
                ),
                Action(
                    id="services",
                    description="List all services emitting traces",
                    permission=Permission.READ,
                    stages=[DevOpsStage.OBSERVE],
                    output_params=[ParamSchema(name="result", type="json", required=True, description="Service names")],
                ),
                Action(
                    id="operations",
                    description="List operations for a specific service",
                    permission=Permission.READ,
                    stages=[DevOpsStage.OBSERVE],
                    input_params=[
                        ParamSchema(name="service", type="string", required=True, description="Service name"),
                    ],
                    output_params=[ParamSchema(name="result", type="json", required=True, description="Operation names")],
                ),
                Action(
                    id="dependencies",
                    description="Get service dependency graph from traces",
                    permission=Permission.READ,
                    stages=[DevOpsStage.OBSERVE],
                    input_params=[
                        ParamSchema(name="end", type="string", required=False, description="End time"),
                    ],
                    output_params=[ParamSchema(name="result", type="json", required=True, description="Service dependencies")],
                ),
            ],
            config_params=[
                ConfigParam(key="traces_url", type="string", required=True, description="VictoriaTraces base URL (e.g. http://victoriatraces:9411)"),
            ],
        )

    def schema(self) -> PluginSchema:
        info = self.info()
        return PluginSchema(actions=info.actions)

    async def execute(self, req: ExecuteRequest) -> ExecuteResponse:
        if self._client is None:
            resp = respond_error("traces_url not configured — set MIRASTACK_TRACES_URL or push config via engine")
            resp.logs = ["ERROR: no traces client configured"]
            return resp

        action = req.action_id or req.params.get("action", "")
        try:
            result = await self._dispatch(action, req.params, req.time_range)
            return respond_map({"result": result})
        except Exception as e:
            resp = respond_error(str(e))
            resp.logs = [f"ERROR: {e}"]
            return resp

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
