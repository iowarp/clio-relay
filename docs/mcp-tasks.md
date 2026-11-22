# MCP tasks

Relay is a standard MCP server. `run`, `install` and `serve` can take a long time, so they
support the `io.modelcontextprotocol/tasks` extension of MCP protocol
`2026-07-28`. Task support is optional: a client that does not opt in still
gets everything, through an operation id it follows with `describe`.

## with tasks

A client that declares the tasks extension and calls `run`, `install` or `serve` gets a
task back as soon as relay has accepted the work. The task id is relay's
operation id, so the same work can be followed through either.

| Task status | When |
| --- | --- |
| `working` | The job is queued or running, or the service is starting |
| `input_required` | The site asked something and is waiting for an answer |
| `completed` | The job reached a final state, whether it succeeded or failed, or the service is ready |
| `cancelled` | The work was cancelled |

`tasks/get` returns the status and a status message that carries the latest
progress, such as the step a simulation reached or the endpoint a service is
listening on. Relay suggests polling every two seconds and keeps a task for
seven days. When the work is done, `tasks/get` carries the final tool result:
the exit code, the outputs with their view descriptors, and the provenance.

A `serve` task completes as soon as the service is ready: JARVIS reports the
service running and relay's forward answers. A client that polls is woken by a
status change, not by a new status message, so the ready service is the task's
result: the operation `running`, `result.endpoint` with each port's `address`,
and `result.ready_at`. The service keeps running after that, until
`cancel(operation=<task id>)`, `serve(stop=<task id>)`, `tasks/cancel` or its
job's end;
`describe(job=<task id>)` and `serve(service=<task id>)` follow it. Questions
relay asks after the service is ready reach the person through relay's page and
`clio-relay asks`, not as `input_required` on the finished task.

### answering questions

Some sites ask for something before they continue: a one-time code, a sign-in,
or approval to spend an allocation. The task then turns `input_required`, and
`tasks/get` lists each question under `input_requests`, keyed by its ask id, as
a form or URL elicitation. Answer with `tasks/update`, giving
`input_responses` with the same keys. The task returns to `working` once every
question is answered.

### cancelling

`tasks/cancel` cancels the operation on its site. A job is cancelled through
its scheduler, and a service is stopped.

## without tasks

A client without the tasks extension calls the same tools:

- `run` waits up to `wait_s` seconds (20 by default, at most 300). If the job
  is done by then, the call returns its result. Otherwise it returns the
  operation, with its id in the result's `_meta` under
  `ai.iowarp.relay/operation`.
- `describe(job=<operation id>)` observes the operation at any later time,
  from the same client or another one.
- `run(cancel=<operation id>)` cancels it, as does `cancel(operation=<operation
  id>)`, the tool a host can name as a cancel.
- `run(resume=<operation id>)` continues an operation that was waiting for a
  person, once the question is answered.
- `serve` waits up to `wait_s` for the service to be ready and returns how to
  reach it. `serve(service=<operation id>)` observes it and
  `serve(stop=<operation id>)` or `cancel(operation=<operation id>)` stops it.

Questions reach the person in the best way the connection allows. On a
`2026-07-28` connection they are input requests on the tool call. On an older
connection whose client supports elicitation, relay elicits. Otherwise the
call returns a typed refusal that names the ask and the page where it can be
answered, and `clio-relay asks` answers it from a terminal.

## refusals

Every refusal is a tool error whose `structuredContent.refusal` carries a typed
reason and a message, so an agent can tell a missing sign-in from a policy
refusal or an unsupported verb without reading prose.