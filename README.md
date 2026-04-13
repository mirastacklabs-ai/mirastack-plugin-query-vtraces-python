# MIRASTACK Plugin: Query Traces

Python plugin for querying **VictoriaTraces / Jaeger** from MIRASTACK workflows. Part of the core observability plugin suite.

## Capabilities

| Action | Description |
|--------|-------------|
| `search` | Search traces by service, operation, tags, duration |
| `trace_by_id` | Retrieve a full trace by trace ID |
| `services` | List all discovered services |
| `operations` | List operations for a service |
| `dependencies` | Get service dependency graph |

## Configuration

Configure the VictoriaTraces URL via MIRASTACK settings:

```bash
miractl config set victoriatraces.url http://victoriatraces:10428
```

## Example Workflow Step

```yaml
- id: find-slow-traces
  type: plugin
  plugin: query_traces
  params:
    action: search
    service: "api-gateway"
    start: "-1h"
    end: "now"
    min_duration: "500ms"
    limit: "10"
```

## Development

```bash
pip install -e .
python -m mirastack_plugin_query_traces
```

## Requirements

- Python 3.12+
- httpx
- mirastack-sdk

## License

AGPL v3 — see [LICENSE](LICENSE).
