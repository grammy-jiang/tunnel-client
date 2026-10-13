# Debugging MCP response delivery without capturing payloads

When an MCP tool finishes locally but its caller does not continue, identify
which delivery stages were observed before blaming either the MCP server or
hosted workflow. This guide uses the released client's built-in DEBUG logs,
without a custom binary or MCP server change.

## Bounded structured DEBUG logging

In a private YAML profile:

    log:
      level: debug
      format: json

For a running client, the local health/admin endpoint supports GET and PUT
on /api/log-level. A PUT with JSON {"level":"debug"} takes effect without
restarting; another PUT with {"level":"info"} restores the usual level.
Keep the health listener on loopback and restrict access to logs. DEBUG can
increase log volume significantly.

Never enable log.http-raw-unsafe just for timing: it can expose credentials,
tool arguments, headers and response bodies. Do not publish raw command IDs,
shard tokens, tunnel IDs, private endpoints or chat content.

## Per-request lifecycle evidence

Use the existing request identifiers within one Tunnel instance to correlate
events; do not treat connector-local identifiers as global ChatGPT IDs.

| DEBUG/INFO event | Evidence | Important limitation |
| --- | --- | --- |
| control-plane command received | Poll decoded a command; polled_at records receipt; valid optional response_timeout may be present | MCP handler has not necessarily started |
| dispatcher received response from MCP server | Matching terminal JSON-RPC response observed | Whole SSE stream need not have closed |
| posted response to control-plane | Response POST returned HTTP success | Hosted model consumption not established |
| dispatcher delivered response to control plane | Dispatcher completed terminal delivery | Does not prove another hosted command followed |
| dispatcher forwarded command to MCP server | Normal forwarding lifecycle completion | Despite wording, not an MCP execution start timestamp |

The optional per-command response_timeout bounds downstream connect/write/read
and outgoing response delivery, anchored at poll receipt. It is distinct from
the long-poll timeout. A response-deadline drop can occur after local MCP work
produced output; such a drop does not imply the OS job was cancelled.

An upstream 502 with upstream_response_received=false indicates no usable MCP
upstream HTTP response was observed. Compare service restarts, retries and
healthy adjacent requests before assigning a root cause. Long-poll timeouts
alone are not a fault signal.

## Read-only health and metric snapshots

Available private loopback endpoints include:
- /health?details=true: bounded component statuses and runtime lifecycle.
- /health/control-plane: polling and consecutive failures.
- /health/response-delivery: completed responses, retries and failures.
- /health/dispatcher and /health/queue: concurrency and backpressure.
- /health/mcp: bounded MCP discovery evidence, when authoritative.
- /metrics: aggregate histograms, not per-request events.

With Streamable HTTP, /health/mcp can remain unknown because same-child
discovery evidence is not available, even while the independent main-channel
startup probe is healthy. Preserve unknown rather than relabeling it failed
or inventing remote server capabilities.

Built-in v0.0.16 DEBUG logs identify receipt, terminal MCP response and
accepted delivery, but have no explicit start marker for each outgoing
response POST attempt. Do not alter deadlines, retries or payloads for
diagnosis. If hosted workflow continuation stops, report redacted logs
and send exact IDs to support privately.

Related references: troubleshooting.md, health.md and configuration.md.
