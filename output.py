"""Output enrichment helpers for the query_traces plugin."""

from __future__ import annotations

import json
from typing import Any

MAX_RESULT_LEN = 32000


def enrich_traces_output(action: str, result: Any) -> dict[str, str]:
    """Wrap raw trace result with metadata for LLM consumption."""
    raw = result if isinstance(result, str) else json.dumps(result, default=str)

    output: dict[str, str] = {
        "action": action,
        "result": result if isinstance(result, str) else json.dumps(result, default=str),
    }

    if len(raw) > MAX_RESULT_LEN:
        output["result"] = raw[:MAX_RESULT_LEN]
        output["truncated"] = "true"

    # Jaeger API wraps results in {"data": [...]}
    parsed = result if isinstance(result, dict) else _try_parse(raw)
    if isinstance(parsed, dict) and "data" in parsed:
        data = parsed["data"]
        if isinstance(data, list):
            output["result_count"] = str(len(data))
            # For search results, extract unique service names.
            if action == "search":
                services: set[str] = set()
                for trace in data:
                    if isinstance(trace, dict) and "processes" in trace:
                        procs = trace["processes"]
                        if isinstance(procs, dict):
                            for proc in procs.values():
                                if isinstance(proc, dict):
                                    sn = proc.get("serviceName")
                                    if isinstance(sn, str):
                                        services.add(sn)
                if services:
                    # Serialize as comma-separated sorted string so the value stays a plain string.
                    output["services_found"] = ",".join(sorted(services))

    return output


def _try_parse(raw: str) -> dict | None:
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, TypeError):
        return None
