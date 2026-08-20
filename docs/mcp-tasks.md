# MCP background tasks

`clio-relay mcp-server` is a native FastMCP 4 server. It implements the
[`io.modelcontextprotocol/tasks`](https://modelcontextprotocol.io/seps/2663-tasks-extension)
extension so an MCP client can detach from a long-running relay operation and
reattach through the standardized task methods.

The protocol adapter does not create another execution system. A task is a
projection of an existing durable relay job:

- the MCP `taskId` is the relay `job_id`;
- relay and clio-core job state remains authoritative;
- relay routing, leasing, workers, JARVIS execution, artifacts, events,
  recovery, and cancellation are unchanged;
- task projections retain the exact tool, arguments, catalog revision, initial
  receipt, and input rounds needed for reconnect. The SEP-2663 create response
  never carries a result or error -- only a status claim -- regardless of
  whether the underlying job already finished before the claim was minted; a
  client always resolves the final result through `tasks/get`. The projection
  records that result as `completed_result` once known: immediately, if the
  job was already terminal when the task was created, or on the first
  `tasks/get` that observes it turn terminal otherwise; and
- a new MCP client can reconstruct a `ToolTask` from its retained task handle
  while the same relay state remains available.

FastMCP's stock `TasksExtension` is not enabled. The server has no Docket
instance, Docket worker, Redis task queue, or second scheduler. The
`fastmcp-tasks` package supplies the standard wire models, client support, and
the currently required claimed-result serialization hook. Its Docket-backed
execution path is installed as a dependency but remains inactive.

## server transports

Stdio remains the default:

```powershell
clio-relay mcp-server --profile user
```

Authenticated Streamable HTTP uses the existing relay API token:

```powershell
$env:CLIO_RELAY_API_TOKEN = "<random-secret>"
clio-relay mcp-server --profile user --transport http --host 127.0.0.1 --port 8766 --path /mcp
```

HTTP startup fails when `CLIO_RELAY_API_TOKEN` is absent. Clients send
`Authorization: Bearer <token>`. Binding beyond loopback changes network
exposure and should be combined with the deployment's existing private
transport and access controls.

The tasks extension is available only on the modern MCP `2026-07-28` protocol.
Older handshake connections continue to receive ordinary foreground tool
results and cannot call `tasks/*`. Modern MCP removed the old core
`Tool.execution` field; task support is therefore negotiated through the
extension capability returned by `server/discover`, not through
`tools/list.execution`.

## client behavior

Importing `fastmcp-tasks` registers its standard client extension with FastMCP.
An ordinary `Client(mode="auto")` advertises the capability and transparently
polls a tasked `call_tool` to its final `CallToolResult`:

```python
from fastmcp import Client
import fastmcp_tasks  # noqa: F401

async with Client("http://127.0.0.1:8766/mcp", auth=token, mode="auto") as client:
    result = await client.call_tool("jarvis_run", arguments)
```

Client-only hosts can use the same wire support without installing
`clio-relay`:

```text
pip install "fastmcp-slim[client]==4.0.0b1" "fastmcp-tasks==4.0.0b1"
```

`fastmcp-tasks` currently brings its server/task-runner dependencies even in a
client-only environment, but the client path does not start Docket or a worker.
This server remains interoperable with that official client package because it
implements the same negotiated extension and standard methods.

Use `call_tool_task` when the caller wants the handle immediately:

```python
from fastmcp import Client
from fastmcp_tasks.client import call_tool_task

async with Client("http://127.0.0.1:8766/mcp", auth=token, mode="auto") as client:
    task = await call_tool_task(client, "jarvis_run", arguments)
    print(task.task_id)
    print((await task.status()).status)
```

The standard `ToolTask` surface supports `status()`, `wait()`, `result()`, and
`cancel()`. A caller that reconnects can retain the returned
`ClientCreateTaskResult` and construct a new `ToolTask` against a new client.
The durable task ID remains sufficient for direct `tasks/get`,
`tasks/update`, and `tasks/cancel` clients.

## state mapping

Relay state maps to SEP-2663 without changing relay semantics:

| Relay observation | MCP task status |
|---|---|
| queued, leased, or running | `working` |
| durable outstanding client input | `input_required` |
| succeeded | `completed` with `isError: false` |
| failed tool work | `completed` with `isError: true` |
| protocol-level execution error | `failed` |
| canceled | `cancelled` |

A tool-level failure is a completed MCP task because the tool call itself
finished and returned an error result. A JSON-RPC failure is a failed task.
Cancellation is cooperative and eventually consistent: `tasks/cancel`
acknowledges the relay cancellation request, while subsequent `tasks/get`
observes canonical relay state.

Low-level status, read, and cancellation tools, and every low-level
submission tool except the two below, are never task-negotiated
(`forbidden`): they always answer inline, whether or not the calling client
declares the tasks extension. Task augmentation is enabled for the virtual
remote-operation catalog, the JARVIS operation surface, and the two
remote-agent submission tools (`relay_submit_agent`,
`relay_submit_remote_agent`), all of which carry a durable job receipt to
project. The adapter creates a task only after relay admission succeeds;
validation errors or calls that return no relay job remain ordinary
synchronous results. Task-capable tools are `optional`, not `required`: a
client that does not declare the tasks extension keeps receiving the
ordinary inline result, exactly like the never-task-negotiated tools above;
only a client that declares it receives a task envelope. Admission gates
that envelope, not the job's speed -- an instant-settling call (the job is
already terminal by the time admission completes) still becomes a task,
terminal-at-birth: the create response reports a completed/failed status
immediately, with `completed_result` already resolved rather than left for
the first `tasks/get` (see the create-response bullet above).

## input and elicitation

Foreground guard tools may return `InputRequiredResult`. The FastMCP client
answers that request and re-enters the tool; only the leg that admits a durable
relay job becomes a task.

A remote-agent submission can opt into one post-admission message round with
`request_followup_message: true`. The relay admits the job first, then the
operation returns `InputRequiredResult` and durably projects the
`agent_message` request as `input_required`. The matching `tasks/update`
re-enters the guard with `ctx.input_responses` and `ctx.request_state`; after it
consumes the answer, ordinary relay-state projection resumes. The JARVIS lane
does not invent equivalent application input semantics.

Unknown or stale keys are ignored, partial answers are retained, repeated
answers are replay-safe, and concurrent updates use compare-and-swap retry.
Input request identities, answers, arguments, and the complete projection are
finite, depth-bounded, and size-bounded before persistence.

Imperative `await ctx.elicit(...)` is not supported inside background work. It
would hold a worker open for a client round trip. Background-capable tools use
the guard pattern: return `InputRequiredResult`, then read
`ctx.input_responses` and `ctx.request_state` on re-entry.

## retention and security limits

Task projections are immutable by identity and conflict when the same task ID
is reused with different tool semantics. The current wire library requires a
numeric `ttlMs`, so the server advertises 30 days rather than SEP-2663's
nullable unlimited value. Relay job retention remains separately governed by
relay policy.

Arguments are limited to 512 KiB of canonical UTF-8 JSON. Input rounds are
limited to 256 KiB, complete projections to 768 KiB, nesting to 64 levels, and
JSON trees to 100,000 nodes. Non-finite numbers and non-JSON values are
rejected. Dynamic tools remain bound to the exact catalog and cluster-route
revision used at dispatch. An optional `Mcp-Name` header on task-management
requests must equal the task ID.

These controls protect the relay's protocol and durable-record boundaries.
They do not expand tool authorization, grant a generated process new
credentials, or replace the execution and artifact checks of the underlying
relay operation.
