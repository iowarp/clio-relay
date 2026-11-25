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
job's end. Relay keeps watching it after the task ends, so the record and its
card reach the job's end without anyone asking (see the job card below);
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

`run`, `serve`, `exec` and `site_clean` confirm their own side effects. Each
declares it in its tool `_meta` as `io.iowarp/confirm: {"version": 1}`. A host
can then skip its own permission prompt for such a call and say so in the
request's `params._meta` with `io.iowarp/confirm: "server"`. For that call
relay asks the person exactly once, with a typed form:

- `approve`, a choice of approve or decline, and, when another of relay's
  services still runs on the site, `running_services` (keep or end them when
  the job is submitted);
- approval to spend an allocation and a package review fold into the same form;
  a sign-in stays its own ask, since it is a page, not a choice;
- `run(cancel=...)` and `serve(stop=...)` get a form of their own, and `exec`
  asks for that command on a site with no guard. `exec_read` never asks.
- `site_clean` always asks, flagged or not, naming what goes and what stays.

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
what the person did not see. That card opens with what changed: each directive
with its old and new value, each component's changed field, new couplings or
notes, and the batch script's changed lines. When JARVIS's plan changed only in
the input it seals and the card does not show, such as the environment a
software check filled in after the yes, the directives, components and script
are the same, so relay submits under the new plan without asking again and
records the step in the operation's `plan_rebinds`. While the job runs, each
component's state comes
from JARVIS's record of it (`waiting`, `starting`, `running`, `done`, `failed`,
`cancelled`), with its hosts. A package not yet reviewed, a site whose JARVIS
cannot plan, and an install job show the card as relay records it, with
`source: relay`.

A pipeline's couplings say which way data moves, producer to consumer, and a
host draws its arrows from them alone. The order of the packages is the order
JARVIS starts them in, which runs against the data: JARVIS starts a service
before the solver that feeds it, so a pvserver watching gray_scott starts
first while gray_scott feeds it. The card's components come producer first,
each with its `launch_order` (1 starts first). Relay declares the couplings to
JARVIS as the pipeline's `couplings:`. A job can name them
(`couplings: [{from, to, kind, label}]`, each end a package or a step id), and
when it names none relay takes them from the packages' contracts: a service
whose contract's `service.data_flow` is `in`, as pvserver's is, is fed by every
other step whose contract declares outputs; one whose `data_flow` is `out` feeds
them. The text says the same, as in "Start the pipeline clio_jarvis.gray_scott
feeding clio_jarvis.pvserver (clio_jarvis.pvserver starts first)". With no
couplings, the card draws no arrows and the text names the packages in launch
order, joined by "then".

A card stays current after its task ends. A `serve` task completes at
readiness, but the service's job runs on, and a host stops polling a finished
task. So relay looks at each open job itself every 30 s, unless a poll looked
in that time. When the card changes, for example when the service's job ends,
relay announces the resource `relay://jobs/{operation_id}` updated. That
resource holds the newest card as JSON, and every card names it in `resource`,
so a host reads the URI from the card (or the task's `_meta`) and does not
build it. A host that keeps a card after the task ends either subscribes to
that resource or reads it again (or `describe(job=...)`) on the announcement.

A service's card is JARVIS's plan of the job its start submits, connector and
all: the directives, components, couplings and advisories, the batch script,
and the binding. Relay plans that whole job before the ask, with the
connector's settings and the ports it chose, and the start keeps those ports,
so JARVIS seals the job the person said yes to. The per-start secrets (the
rendezvous token, the frp key and any per-port secret) are never in it: they
travel only in the staged `serve/secrets.env`, which JARVIS does not seal and
the connector deletes once read. If the job has changed by the start (a
desktop port taken meanwhile, a new path to the site), JARVIS refuses and
relay shows the new plan as a second card. A service that is not a package
job, or a serve runtime that cannot say which job it submits, keeps a card
with no script and no binding, and the confirm text says relay makes the batch
script when it starts the service.

`started_at` is when the job itself started on its nodes, not when the
operation did: the scheduler's start when the site reports one, else the
first state JARVIS wrote from inside the allocation, else the start of the
job's latest running stretch, after any install job before it. `advisories`
lists the picks JARVIS went ahead with but that the person should see, such as
a launch over TCP because UCX could not be verified on a node. Each has
JARVIS's `issue_id`, `reason_codes`, `message`, `pick`,
`consequence_if_wrong` and `remedy`. `message` is the person's line. What
JARVIS tells the agent goes in `agent_guidance`, with the read-only `probes`
JARVIS suggests: any sentence that names one of the issue's own fields, such
as "Investigate with details.probes", or JARVIS's own `agent_guidance` when
it sends `person_message`. `settled_at_launch` is true when JARVIS settles
every reason of the issue itself once the job's nodes are known (each reason's
`plan` is `collect_at_launch`), such as `hosts_not_in_graph` for a job larger
than the nodes the graph was collected on: the launch collects the facts on
its own hosts, merges them into the site graph and supersedes the advisory.
Such an advisory asks nothing of the person, so it is not in `advisories`, the
confirm's text or a changed plan's list: the card lists it under its own
`settled_at_launch`, for the agent. None of them holds the job: the person
accepts one later with `jarvis rg facts answer <issue_id> --value accept`
through relay's JARVIS exec, or overrides it with `jarvis rg net set`. The
confirm's text repeats each advisory's message.

| Where | How |
| --- | --- |
| The confirm form | phase `planned`, in the elicitation's `params._meta`; on a `2026-07-28` connection, in the `input_required` result's `_meta` |
| Task status | each `tasks/get` result's `_meta` |
| Tool results | `structuredContent.job` on `run`, `serve` and `describe(job=...)` |
| Job lists | `describe(site=...)` lists the site's jobs under `jobs` |
| Job resource | `relay://jobs/{operation_id}`, announced updated on each change |

## waiting for a service's compute steps

A `serve` task completes at readiness, and its job runs on. A job that also
computes, such as a solver beside the viewer that watches it, has a second
moment the agent waits for: its compute steps ending. The ready result names
the steps still running and the call that waits for them:
`serve(service=<operation_id>, until="steps")`.

With tasks, that call is a task of its own, `<operation_id>.steps`. It stays
`working` while a compute step runs, its status message names each step's
state, and it completes when every compute step has ended or the job has. The
completion is the wake, so the agent waits on the task rather than polling the
job's files or the scheduler. The result carries each step's state, reason,
hosts and time under `structuredContent.until`, then the operation as `serve`
shows it, with the job's outputs and its card. It is an error when a step
failed. Without tasks, the call waits up to `wait_s` (at most 300 s) and
returns the same result; `until.ended` says whether the steps have ended, and
the agent calls again while it is false.

The compute steps are the card's `compute` components. A JARVIS job's step
ends when JARVIS records it, and a job without steps ends with the job, so the
same call works on every site and access means. The task's state is read from
the operation's record, so it survives a relay restart. Cancelling the task
stops the waiting, not the job; `serve(stop=...)` stops the job.

## refusals

Every refusal is a tool error whose `structuredContent.refusal` carries a typed
reason and a message, so an agent can tell a missing sign-in from a policy
refusal or an unsupported verb without reading prose.