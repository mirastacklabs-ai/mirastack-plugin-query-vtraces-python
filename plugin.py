"""MIRASTACK query_traces plugin — queries VictoriaTraces/Jaeger."""

from __future__ import annotations

import json
import os

from mirastack_sdk import (
    Action,
    ConfigParam,
    IntentPattern,
    Plugin,
    PluginInfo,
    PluginSchema,
    ParamSchema,
    Permission,
    PromptTemplate,
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
from output import enrich_traces_output


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
            version="0.2.0",
            description=(
                "Search and retrieve distributed traces from VictoriaTraces via the Jaeger-compatible API. "
                "Use this plugin to find traces by service, operation, tags, or duration; retrieve full span waterfalls; "
                "discover instrumented services and operations; and analyze service dependency graphs. "
                "Start with services to discover what is instrumented, then search for specific traces."
            ),
            permissions=[Permission.READ],
            devops_stages=[DevOpsStage.OBSERVE],
            intents=[
                IntentPattern(pattern="search traces", description="Search distributed traces", priority=10),
                IntentPattern(pattern="get trace", description="Retrieve trace by ID", priority=9),
                IntentPattern(pattern="find slow traces", description="Find traces with high latency", priority=8),
                IntentPattern(pattern="trace dependencies", description="Show service dependencies from traces", priority=7),
                IntentPattern(pattern="distributed tracing", description="Work with distributed trace data", priority=7),
                IntentPattern(pattern="span details", description="View trace span details", priority=6),
                IntentPattern(pattern="trace latency", description="Analyze request latency via traces", priority=6),
            ],
            actions=[
                Action(
                    id="search",
                    description=(
                        "Search distributed traces by service, operation, tags, and duration. "
                        "Use this for finding slow requests, error traces, or traces matching specific criteria. "
                        "Returns trace summaries with span counts and durations."
                    ),
                    permission=Permission.READ,
                    stages=[DevOpsStage.OBSERVE],
                    intents=[
                        IntentPattern(pattern="search traces", description="Search distributed traces", priority=10),
                        IntentPattern(pattern="find slow traces", description="Find traces with high latency", priority=9),
                        IntentPattern(pattern="find error traces", description="Find traces containing errors", priority=9),
                        IntentPattern(pattern="traces for service", description="Search traces for a specific service", priority=8),
                    ],
                    input_params=[
                        ParamSchema(name="service", type="string", required=False, description="Service name to filter traces by"),
                        ParamSchema(name="operation", type="string", required=False, description="Operation/endpoint name to filter (e.g., 'GET /api/orders')"),
                        ParamSchema(name="start", type="string", required=False, description="Start time"),
                        ParamSchema(name="end", type="string", required=False, description="End time"),
                        ParamSchema(name="limit", type="string", required=False, description="Maximum number of traces to return (default: 20)"),
                        ParamSchema(name="min_duration", type="string", required=False, description="Minimum trace duration (e.g., 100ms, 1s)"),
                        ParamSchema(name="max_duration", type="string", required=False, description="Maximum trace duration (e.g., 5s, 10s)"),
                        ParamSchema(name="tags", type="string", required=False, description="JSON object of tag filters (e.g., '{\"http.status_code\":\"500\"}')"),
                    ],
                    output_params=[ParamSchema(name="result", type="json", required=True, description="Traces in Jaeger API response format")],
                ),
                Action(
                    id="trace_by_id",
                    description=(
                        "Retrieve a specific trace by its trace ID including the full span waterfall. "
                        "Use this after identifying a trace of interest from search results "
                        "to see all spans, timing, parent-child relationships, and tag details."
                    ),
                    permission=Permission.READ,
                    stages=[DevOpsStage.OBSERVE],
                    intents=[
                        IntentPattern(pattern="get trace", description="Retrieve a trace by ID", priority=10),
                        IntentPattern(pattern="trace details", description="Show full span details for a trace", priority=9),
                        IntentPattern(pattern="span waterfall", description="Show the span waterfall for a trace", priority=8),
                    ],
                    input_params=[
                        ParamSchema(name="trace_id", type="string", required=True, description="Specific trace ID to retrieve (hex string, e.g., 'abc123def456')"),
                    ],
                    output_params=[ParamSchema(name="result", type="json", required=True, description="Full trace data with all spans in Jaeger format")],
                ),
                Action(
                    id="services",
                    description=(
                        "List all services reporting distributed traces. "
                        "Use this for discovery — to find which services are instrumented with tracing. "
                        "This is typically the first call before searching traces."
                    ),
                    permission=Permission.READ,
                    stages=[DevOpsStage.OBSERVE],
                    intents=[
                        IntentPattern(pattern="traced services", description="List services with tracing enabled", priority=9),
                        IntentPattern(pattern="which services have traces", description="Discover instrumented services", priority=8),
                        IntentPattern(pattern="tracing coverage", description="Check which services report traces", priority=7),
                    ],
                    output_params=[ParamSchema(name="result", type="json", required=True, description="Array of service names")],
                ),
                Action(
                    id="operations",
                    description=(
                        "List all operations (endpoints, methods) for a specific service. "
                        "Use this to discover what API endpoints or internal operations a service exposes. "
                        "Useful before filtering trace search to a specific operation."
                    ),
                    permission=Permission.READ,
                    stages=[DevOpsStage.OBSERVE],
                    intents=[
                        IntentPattern(pattern="service operations", description="List operations for a traced service", priority=9),
                        IntentPattern(pattern="endpoints for service", description="Find API endpoints of a service", priority=8),
                        IntentPattern(pattern="what operations does", description="Discover operations a service handles", priority=7),
                    ],
                    input_params=[
                        ParamSchema(name="service", type="string", required=True, description="Service name to list operations for"),
                    ],
                    output_params=[ParamSchema(name="result", type="json", required=True, description="Array of operation names")],
                ),
                Action(
                    id="dependencies",
                    description=(
                        "Analyze service dependency graph derived from trace data. "
                        "Shows which services call which other services and the call volume. "
                        "Use this for understanding service topology and identifying critical paths."
                    ),
                    permission=Permission.READ,
                    stages=[DevOpsStage.OBSERVE],
                    intents=[
                        IntentPattern(pattern="trace dependencies", description="Show service dependencies from trace data", priority=9),
                        IntentPattern(pattern="service call graph", description="Visualize service-to-service call relationships", priority=8),
                        IntentPattern(pattern="which services call", description="Find upstream/downstream service dependencies", priority=7),
                    ],
                    input_params=[
                        ParamSchema(name="end", type="string", required=False, description="End time"),
                    ],
                    output_params=[ParamSchema(name="result", type="json", required=True, description="Service dependency graph with call counts")],
                ),
            ],
            prompt_templates=[
                PromptTemplate(
                    name="query_traces_guide",
                    description="Best practices for using VictoriaTraces distributed tracing tools",
                    content=(
                        "You have access to VictoriaTraces distributed tracing tools. Follow these guidelines:\n\n"
                        "1. DISCOVERY FIRST: Use services action to find instrumented services. Then operations to find endpoints.\n"
                        "2. SEARCH STRATEGY: Start broad (service only), then narrow with operation, tags, and duration filters.\n"
                        "3. LATENCY ANALYSIS: Use min_duration filter to find slow traces (e.g., min_duration=1s).\n"
                        "4. ERROR INVESTIGATION: Filter by tags like http.status_code=500 or error=true.\n"
                        "5. TRACE DEEP DIVE: After search, use trace_by_id to get the full span waterfall for a specific trace.\n"
                        "6. DEPENDENCIES: Use dependencies action to understand service topology before investigating issues.\n"
                        "7. TAG FILTERING: Tags use JSON format. Common tags: http.method, http.status_code, error, db.type.\n"
                        "8. LIMIT results initially: start with limit=10, increase to 50+ for broader analysis.\n"
                        "9. INTERPRETATION:\n"
                        "   - Short traces with errors = fast failure (connection refused, auth failure)\n"
                        "   - Long traces with many spans = cascading slowness\n"
                        "   - Missing spans = instrumentation gaps\n"
                        "   - Fan-out patterns = potential N+1 query issues"
                    ),
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
            enriched = enrich_traces_output(action, result)
            return respond_map(enriched)
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
