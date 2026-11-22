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
and `result.ready_at`. A poll does not wait behind relay's own look at the
site: while relay brings the service up, it answers with the latest state it
saw, so the task completes within seconds of the service being ready, not a
whole check later. The service keeps running after that, until
`cancel(operation=<task id>)`, `serve(stop=<task id>)`, `tasks/cancel` or its
job's end;
`describe(job=<task id>)` and `serve(service=<task id>)` follow it. Questions
relay asks after the service is ready reach the person through relay's page and
`clio-relay asks`, not as `input_required` on the finished task.

A service relay cannot bring to this desktop also completes the task, at once,
rather than retrying: the tool result is an error with `result.service_error`,
a typed reason and a message. `host_key_unverified` means relay could not
verify the compute node's ssh host key (it reads the node's keys from the
login node and checks them again once); `unreachable` means the node refused
the forward, for example because the job's allocation ended. The job's own end
then ends the operation as usual. Relay never reports a port as forwarded
before its own forward holds it.

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

## tools that confirm themselves

`run`, `serve` and `exec` confirm their own side effects. Each declares it in
its tool `_meta` as `io.iowarp/confirm: {"version": 1}`. A host can then skip
its own permission prompt for such a call and say so in the request's
`params._meta` with `io.iowarp/confirm: "server"`. For that call relay asks the
person exactly once, with a typed form:

- `approve`, a choice of approve or decline, and, when another of relay's
  services still runs on the site, `running_services` (keep or end them when
  the job is submitted);
- approval to spend an allocation and a package review fold into the same form;
  a sign-in stays its own ask, since it is a page, not a choice;
- `run(cancel=...)` and `serve(stop=...)` get a form of their own, and `exec`
  asks for that command on a site with no guard. `exec_read` never asks.

A declined form cancels the job before anything is submitted. A client
without form elicitation gets a `needs_approval` refusal and nothing runs.
When a flagged call only observes, or acts on answers given earlier, its
result's `_meta` carries `io.iowarp/confirm: "not_needed"`, so the host knows
no form was due.

## the job card

Each job also travels as one typed snapshot under `io.iowarp.clio-relay/job`: its
site, scheduler, phase (`planned`, `submitted`, `queued`, `running`, `ended`),
outcome, partition, account, wall time, and its components with their node
counts and state. A host can draw it as the job's card. Relay sends the whole
snapshot each time, and `revision` rises with each change, so a host keeps the
newest one per `operation_id`. The text of every message stays as it is, so a
host that ignores the snapshot loses nothing.

For a package job, the card is JARVIS's own plan of the job
(`jarvis ppl submit +plan`), made before anything is submitted: the
directives, each package with its nodes, ranks and node range, the couplings
between packages, and the batch script. Its `source` is `jarvis`. The yes is
bound to that plan: relay submits with its `plan_sha256`, and if the job JARVIS
would submit has changed since (an edited pipeline, a new wall time), JARVIS
refuses and relay shows the new plan as a second card rather than submitting
what the person did not see. While the job runs, each component's state comes
from JARVIS's record of it (`waiting`, `starting`, `running`, `done`, `failed`,
`cancelled`), with its hosts. A package not yet reviewed, a site whose JARVIS
cannot plan, and an install job show the card as relay records it, with
`source: relay`.

| Where | How |
| --- | --- |
| The confirm form | phase `planned`, in the elicitation's `params._meta`; on a `2026-07-28` connection, in the `input_required` result's `_meta` |
| Task status | each `tasks/get` result's `_meta` |
| Tool results | `structuredContent.job` on `run`, `serve` and `describe(job=...)` |
| Job lists | `describe(site=...)` lists the site's jobs under `jobs` |

## refusals

Every refusal is a tool error whose `structuredContent.refusal` carries a typed
reason and a message, so an agent can tell a missing sign-in from a policy
refusal or an unsupported verb without reading prose.