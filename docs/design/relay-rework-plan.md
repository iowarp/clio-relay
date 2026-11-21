# clio-relay rework: implementation plan

clio-relay becomes the one way an agent reaches a computer other than the one it runs on: a homelab node, a campus cluster, a DOE system, a cloud. It does this by aggregating the access protocols those places already offer behind one small MCP surface. CLIO holds no relay logic. It installs relay as a plugin and renders what relay sends, and any other MCP host can use the same server.

The plan has eleven workstreams. They are not ordered. The work is one goal that runs until every item under Done holds, and the human reviews the finished branches.

1. **The surface.** One MCP server with five verbs replaces today's `relay_*` tools.
2. **Sites and adapters.** A site registry, an adapter registry with a factory, a conformance kit, and seven adapters.
3. **Jobs.** A job spec that follows PSI/J, inputs addressed by content, and every long operation as an MCP task.
4. **Services.** `serve` runs a long-lived service next to a job and gives the desktop an endpoint to it.
5. **Guardrails.** A code policy per site built on JARVIS packages, a spend gate, and a warm allocation.
6. **Asking the person.** How relay asks for a choice, a sign-in, a key or a one-time code.
7. **The compute tab.** Relay's CLIO plugin: the tab, the sign-in surfaces, the site skills.
8. **Host semantics in CLIO.** Five general additions to clio-agent and gact-tui that any plugin can use.
9. **Removal.** Relay's old surface, and the `ssh_execute` tool and relay leftovers in clio-agent.
10. **clio-jarvis.** A new repository of JARVIS packages written for CLIO, which CLIO uses in place of jarvis-cd's `builtin` packages.
11. **Tapis.** The seventh adapter, on TACC's Tapis API, and TACC sites in the catalog.

## Contract

This plan is the contract for the eleven workstreams. When code and this plan disagree, change the code. If the plan turns out wrong, choose the fix that best keeps its intent, update this document in the same change, and record the decision in the ledger.

| Document | Governs |
| --- | --- |
| This plan | The surface, sites, adapters, jobs, services, guardrails, asks, the compute tab, the host semantics, the removals, clio-jarvis, Tapis |
| `docs/design/relay-rework-ledger.md` (new, in clio-relay) | This plan's decisions and acceptance evidence |
| `docs/connection-model.md` (clio-relay) | The one-link connection between a desktop relay and a site worker. The `worker` adapter keeps it |
| MCP specification, revision 2026-07-28 | Tasks and elicitation |
| PSI/J and `psij-python` 0.9.11 | The job spec and scheduler script generation |
| Tapis v3 and `tapipy` 25.4.0 | The `tapis` adapter's client, and the Authenticator, Systems, Apps, Jobs and Files calls it makes |
| `plans/remote-visualization-scope.md` (project) | The seven questions `serve` answers |
| `plans/mcp-apps-surfaces-plan.md` (project) | MCP Apps in tabs and the ParaView app, local and remote. This plan builds live visualization and secure sites on top of it |
| `plans/remote-connectors-plan.md` (project) | The Desktop OAuth redirect and the connector catalog. Not changed here |

**Base.** clio-relay `main` at `3b051d3` (v1.6.8), clio-agent `develop` at `0ed2241a0`, gact-tui `develop` at `def3ae0ab`, clio-kit `main` at `490269f`, jarvis-cd v1.8.1 (`9c917c2`). clio-jarvis is new.

Decisions use the prefix `RL` for relay and `HS` for host semantics. Qualification scenarios use `LQ`.

**Base, amended.** The rework starts from clio-relay `develop` at `034117be5`, clio-agent `develop` at `f08dbde3a`, gact-tui `develop` at `21163590c`, clio-kit `main` at `490269fb1` and clio-agent-marketplace `develop` at `dbc82afa8`. The drift from the base above is in `relay-rework-execution.md`, under "Base and drift". The integration branch is `jaimecernuda-relay-rework` in every repository.

## Amendments

The execution plan (`relay-rework-execution.md`) amends this contract. The amendments are written here so that code and contract change together. Where an amendment and the text below disagree, the amendment wins. The full wording of each is in the execution plan, under "Principle zero" and its tables.

| Id | Amends | Summary |
| --- | --- | --- |
| RL0 | Rule 1, RL1 | clio-relay is a standard MCP server. Any MCP client (Claude Code, Copilot CLI, Codex, a generic `mcp` client) gets the whole surface. Relay imports nothing from clio-agent, has no code path that only runs under CLIO, and reads only what the client advertises. CLIO's tight integration comes from HS1 to HS5 and relay's own plugin, which other hosts are free to ignore |
| Protocol baseline | RL2 | Relay is task-first on MCP `2026-07-28` with the tasks extension `io.modelcontextprotocol/tasks` (SEP-2663): task-augmented `run`, `serve` and large `files` calls, progress on the task, asks raised as `input_required` and answered through `tasks/update`, cancel through `tasks/cancel`. Earlier revisions are served without tasks on the RL2a fallback. The exact SDK and extension revisions in use are recorded in the ledger under W1 |
| RL2a | RL2 | Without the tasks extension, a long call returns at once with a relay handle in text and structured content, never dressed up as a task result. The five verbs' own schemas finish the work from a fresh connection: `describe` with `site` or `job`, `run` with `cancel`, `files` with `job` or `transfer`, `serve` with `service` or `stop`. One durable record per operation serves both paths. A remote HTTP relay's `serve` result gives `clio-relay connect <handle>` for the desktop |
| RL25a | RL25, RL27, RL28 | Without form elicitation, nothing is submitted. The call ends with `needs_approval`, an ask id and exactly what is asked. A person answers on relay's approval page (loopback over stdio, relay's own origin with an operator credential over HTTP; GET changes nothing; approval is a CSRF-protected POST bound to operation, hash and charge) or with `clio-relay asks answer <id>`. An approved ask submits at most once, across retries and restarts |
| HS1a | HS1 | Relay sends A2UI only on a request whose declared client capabilities include the A2UI v0.9 or v0.9.1 extension, always beside the full text result. Never inferred from the client name, never cached per process |
| HS4a | HS4, RL30 | Relay takes a grant file per provider through its own setting (`--grant-file <provider>=<path>` or `CLIO_RELAY_GRANT_FILE_<PROVIDER>`), re-read on change. The mapping from `CLIO_ACCOUNT_<PROVIDER>_FILE` lives only in the launch configuration of relay's `plugin.json` |
| Tab actions | RL32, HS2 | Compute tab actions are tools that never appear in `tools/list`. Relay accepts one only with a single-use action token bound to the surface and action it rendered, carried only in A2UI payloads |
| Guarded sites before W13 | RL24 | Until the guard lands, the new surface refuses any guarded site with `guard_unavailable` |
| LQ31a | LQ31 | Boundary check on relay: in a clean venv with no clio-agent and no `CLIO_*` host variable, relay installs from its wheel and completes LQ41. Nothing in `src/clio_relay` imports a host runtime or reads a host's `CLIO_*` variables. `CLIO_RELAY_*` is relay's own settings prefix and is allowed |
| LQ41 | Qualification | A generic client on the official `mcp` Python SDK drives relay end to end over stdio and Streamable HTTP, with form and URL elicitation, and no CLIO process on the machine |
| LQ42 | Qualification | The same client without tasks and elicitation finishes every long operation through the RL2a handle path, and the RL25a approval rules hold |
| LQ43 | Qualification | The task path end to end: task-augmented `run`, progress on `tasks/get`, the spend gate as `input_required` answered through `tasks/update`, `tasks/cancel`, the result read again on a fresh connection, the same job visible through `describe` |
| LQ-L18 | Qualification | A person adds relay to a third-party agent host with its documented `mcp add` command and runs "submit hostname to Ares and tell me the node" |

## Rules

The megachange rules carry over: delete and replace, the model decides, no silent fallback, one mechanism per concept, failures are legible and tell the user what to do, evidence over claims. Six more apply here:

1. **Relay logic lives in clio-relay.** No file in clio-agent or gact-tui names relay, a site, a scheduler or an adapter. What CLIO gains is stated as a semantic any plugin can use.
2. **Relay aggregates and does not reimplement.** Each adapter drives the protocol's own client: the system `ssh`, the Globus SDK, the IRI Facility API, SkyPilot, `tapipy`, frp, `psij-python`.
3. **Other systems are references.** hpc-bridge, OpenResearch `orx` and Academy inform the semantics. None is a dependency.
4. **A job never needs a held connection.** Relay submits, records and polls. Closing the laptop loses nothing.
5. **Secrets stay out of the model and out of CLIO's records.** A password, a one-time code or a key goes from the person to relay and nowhere else.
6. **Guardrails are enforced in relay**, per site, whatever the agent asks for.

## Out of scope

Do not implement, stub or reserve fields for these:

- CLIO's Infrastructure tab and its SSH targets. They stay as they are. Plain `ssh host command` from the agent's shell stays the simple path, and relay does not wrap it.
- Operating a public rendezvous host for users. Relay works with any rendezvous host. Running one for the community is owner work.
- Showing relay's surfaces on a host that has no A2UI. Other hosts use relay through its tools and through MCP elicitation.
- A2UI 1.0. CLIO speaks v0.9.1, and relay sends v0.9.1.
- Sharing one site entry or one worker between several people.
- A Kubernetes adapter of its own. Kubernetes is reached through `sky`.
- Changes to jarvis-cd or its `builtin` packages. VIGIL consumes them. jarvis-cd stays the runtime, and CLIO's packages go in clio-jarvis.
- In-transit streaming from a simulation to ParaView. The data path is files on the site's filesystem.
- Inference through Tapis (Pods, FlexServ), and Tapis Actors, Workflows and Streams. The `tapis` adapter does jobs and files.

## Current state

Read at the base.

**clio-relay.**

- 191,546 lines of Python in one flat package. The largest areas by module prefix are `cli` (22,464), `queue` (21,205), `bootstrap` (17,740), `endpoint` (11,595), `service` (10,382), `session` (10,319), `mcp` (10,129), `remote` (9,323), `jarvis` (9,265) and `frp` (2,225).
- Three roles: `desktop`, `worker` and `relay-host`. A connection is a desktop relay and a cluster relay joined by one persistent link. Both sides dial out to a relay host over frp (STCP, with WebSocket or raw TCP). XTCP is an optional direct path. SSH forwarding is the fallback.
- SSH is used once, to place the worker. After that no operation opens SSH.
- The MCP surface is 45 `relay_*` tools: jobs, queue, artifacts, monitors, retention, gateway sessions, JARVIS calls.
- Remote MCP federation runs each call as a queued `mcp_call` job that launches the server fresh. A server that keeps state between calls cannot be used this way.
- Scheduler-backed services exist as gateway sessions with a `ServiceRuntimeSpec`: a JARVIS package launches the service, relay watches readiness, and the desktop gets a loopback port (`desktop_bind_addr`, `desktop_bind_port`) over `frp-stcp-wss`. Detach, attach and resume are supported.
- `browser_gateway.py` (917 lines with its errors module) is a loopback proxy that gives a sandboxed viewer a short-lived capability URL. It carries GET, POST and server-sent events, and no WebSocket.
- `scheduler_providers/` has a registry and one provider, Slurm.
- The MCP tasks extension is implemented (`fastmcp-tasks`).
- Relay already ships a JARVIS repository of its own, `jarvis-packages/clio_relay`, with three packages: `remote_agent`, `mcp_call` and `bounded_command`.

**jarvis-cd.**

- A site adds a package repository with `jarvis repo add <path>`, and its packages and pipeline indexes become available (`docs/package_dev_guide.md`).
- In `builtin`, `xcompact3d` builds 2DECOMP&FFT 2.0.4 with the ADIOS2 backend and types its output as `adios2-bp`. `gray_scott` and `gray_scott_paraview` exist. `builtin.paraview` is the service VIGIL uses.
- These packages were written for relay 1.x. None declares how its output is opened in a view.

**clio-agent.**

- MCP tasks from a server are handled by `gact/external_tasks.py` and `gact/external_task_inputs.py`. An input request inside a task becomes a question.
- Elicitation is supported in both modes (`tools/mcp_handlers.py`, `gact/elicitation_schema.py`). A URL elicitation is accepted only when its scheme is `https` and its origin is in `tools.mcp.elicitation.url_trusted_origins`. With an empty list every URL is refused.
- gact-tui shows a URL elicitation as a question with the full URL and an "Open link" button, which opens the system browser (`question-interaction-forms.tsx`).
- Neither questions nor A2UI have a secret field. A value typed into either is recorded.
- A plugin can ship A2UI catalogs under `ai.iowarp.clio/a2ui/<name>/` (`gact/a2ui_catalogs/plugin.py`) and bind to saved connections with `connections`. It cannot declare a tab or an account.
- An A2UI surface can carry a sign-in button. The action `data_source/login/<provider>` is accepted for `github`, `google_drive`, `globus` and `ndp` (`gact/a2ui_actions/source_login.py`). The click is recorded, and the client's own private sign-in UI does the sign-in.
- CLIO does not read `application/a2ui+json` from an MCP result. Surfaces are created only by the agent through `gact/a2ui_producer/`.
- The Globus sign-in requests the transfer scope (`TRANSFER_SCOPE` in `gact/storage/globus_consent.py`) and `offline_access`.
- `runtime/github_cli.py` gives the managed `gh` one account grant as `GH_TOKEN` for each run. No general way exists to hand an account grant to a plugin's MCP server.
- The agent has an `ssh_execute` tool (`gact/ssh_tools.py`). Its docstring gives the reason it exists: a direct `ssh` in the shell can be blocked by the shell's network fence.
- On the Codex fence on Linux and Windows, a child's traffic is forced through CLIO's HTTP proxy (`_activate_codex` in `runtime/sandbox.py`). The proxy (`runtime/net_chokepoint.py`) tunnels `CONNECT host:port` to any port and records each connection as a `net.egress` event.
- Connectors exist (`catalogs/connectors/`, `src/clio_agent/connectors/`). An entry declares what it needs from the person as an address, a sign-in, a secret or a value. A secret is kept in the credential store and is never typed into a question. A plugin can follow a connector.

**clio-kit.**

- The ParaView server is 3.1.0 (`mcp-servers/paraview`). It is a front that CLIO starts and a node half that runs inside ParaView's Python on the machine that has the data, reached over the system `ssh`. The node half attaches to a `pvserver` that is already listening on that machine.

## Design: the surface

**RL1. One MCP server, five verbs.** Every verb takes a `site`.

| Verb | What it does |
| --- | --- |
| `describe` | With no site, lists the sites and their status. With a site, reports its adapters and which verb each serves, the scheduler, queues, accounts, limits, policy, reviewed packages, defaults and the linked skill |
| `run` | Submits a job from a job spec |
| `exec` | Runs one short command and returns bounded output |
| `files` | Lists, reads, writes and transfers files, between the desktop and a site or between two sites |
| `serve` | Starts a long-lived service on a site and returns an endpoint, issues access to it, and stops it |

**RL2. Long work is an MCP task.** `run`, `serve` and a large `files` transfer return a task. CLIO handles it on its existing external-task path with no relay-specific code. A question that comes up inside a task is the task's input request. A client without the tasks extension gets the handle path in Amendment RL2a.

**RL3. The old surface is deleted.** The `relay_*` tools, the queued `mcp_call` federation, and the HTTP routes that only those tools used are removed. The `clio-relay` command keeps operator commands that map onto the same verbs and onto site management.

## Design: sites

**RL4. A site entry carries everything the compute tab shows and lets the person edit.**

```yaml
- id: polaris
  name: ALCF Polaris
  address: polaris.alcf.anl.gov
  adapter: globus                      # the one in use
  identity: globus
  config: {endpoint: <facility endpoint id>}
  alternatives:                        # other ways in, switchable in the tab
    iri: {base_url: <facility IRI URL>}
    worker: {ssh_profile: polaris}
  policy: guarded
  spend: {ask: always}
  defaults: {account: <project>, queue: debug}
  skill: alcf-polaris                  # optional

- id: ares
  name: Ares
  address: ares.cs.iit.edu
  adapter: ssh
  config: {ssh_profile: ares}          # a Host from ~/.ssh/config
  policy: open
  spend: {ask: never}
  skill: null                          # the agent runs describe
```

- The person's entries live in `sites.yaml` in relay's configuration directory.
- Relay ships a catalog of known sites. It starts with the four the team can qualify on, Ares, Polaris, Aurora and DeltaAI, and the Tapis entries in RL49. Each catalog entry's address, endpoint id and IRI URL is read from the facility's own documentation when the entry is written, and the ledger records the page.
- A catalog entry is a starting point. Adding it copies it into `sites.yaml`, where the person edits it.

**RL5. Verb routing.** The adapter in use serves a verb when it implements it. Otherwise the first alternative that implements it serves it. `describe` shows the route for each verb, so nothing is routed silently.

## Design: adapters

**RL6. An adapter is a class behind a registry and a factory.** Adapters register through the Python entry point group `clio_relay.adapters`. The factory builds an adapter from a site entry. Each adapter declares a `Config` model, and its JSON Schema drives both validation and the form in the compute tab.

```python
class Adapter(Protocol):
    name: str
    Config: type[BaseModel]              # drives validation and the form

    async def describe(self) -> SiteInfo: ...
    async def run(self, spec: JobSpec) -> JobHandle: ...         # optional
    async def exec(self, command: list[str]) -> ExecResult: ...  # optional
    async def files(self) -> FileAccess: ...                     # optional
    async def serve(self, spec: ServiceSpec) -> Endpoint: ...    # optional
```

**RL7. A conformance kit.** `clio_relay.testing.conformance` is a pytest suite every adapter runs. It covers the shape of `describe`, bounded output from `exec`, a file round trip, the job lifecycle (queued, running, done, cancelled), observing a job after relay restarts, and the service lifecycle. An adapter declares the verbs it does not implement, and the suite checks that it refuses them with a typed reason.

**RL8. Adding an adapter is adding a package.** A new adapter ships as its own distribution with the entry point. Relay's core does not change. The adapter qualifies by passing the conformance kit.

**RL9. `local`.** The machine relay runs on. Jobs go to the local scheduler through `psij-python` when one exists, and run as supervised processes when none does.

**RL10. `ssh`.** Relay runs the system `ssh`, `scp` and `sftp` with the profile the person picked, so their agent, keys, jump hosts and `~/.ssh/config` apply. Relay embeds no SSH library.

- Every call is its own `ssh` process with `BatchMode=yes`. Nothing is held open, so the adapter works on Windows.
- On a site with a scheduler, relay renders the job script locally with `psij-python` (`generate_submit_script`), submits it with one `ssh` command, and polls with one `ssh` command for each poll (`get_status_command`, `parse_status_output`).
- On a host with no scheduler, the job runs detached, with its process id, output and exit code written to a job directory that later polls read.
- A site that demands interactive sign-in on every connection fails the first call with the reason `interactive_auth_required`, and the message names the `worker` adapter.

**RL11. `worker`.** The original relay as one adapter. The person signs in to the site once. Relay places an unprivileged worker, and the worker dials out to the rendezvous host over frp. Nothing uses SSH after that. The existing bootstrap, queue, leases and one-link connection are kept behind this adapter. It implements every verb. It is relay's way into a site that has multi-factor sign-in and no Globus.

**RL12. `globus`.** Written on the Globus SDK and the Globus Compute SDK. It is ours.

- A Globus Compute endpoint needs only outbound connectivity, so no link is placed.
- Where the facility runs a multi-user endpoint, as ALCF does on Polaris and Crux, the site entry names it and nothing is installed.
- Where the person must run their own endpoint, as on Aurora, relay starts it with one sign-in through the same path the `worker` adapter uses.
- The job spec's resources and attributes map onto the endpoint's user configuration (account, queue, walltime, nodes).
- `files` uses Globus Transfer when both ends have a collection.

**RL13. `iri`.** The IRI Facility API, which NERSC, ALCF, OLCF and ESnet serve, and which AmSC and Genesis adopt. Its compute interface is close to PSI/J, so the job spec maps directly. The facility's base URL is in the site entry. Where a facility accepts Globus tokens, the `globus` identity serves this adapter too.

**RL14. `sky`.** SkyPilot, covering the clouds, Kubernetes and Slurm. It uses the cloud profiles already on the machine. `run` is a SkyPilot managed job. The price SkyPilot reports feeds the spend gate, and its autostop is the idle release.

## Design: jobs

**RL15. The job spec follows PSI/J.** Field names match `psij-python` 0.9.11.

| Part | Fields |
| --- | --- |
| Job | `executable`, `arguments`, `directory`, `name`, `environment`, `inherit_environment`, standard input, output and error paths |
| Resources | `node_count`, `process_count`, `processes_per_node`, `cpu_cores_per_process`, `gpu_cores_per_process`, `exclusive_node_use`, `memory` |
| Attributes | `duration`, `queue_name`, `account`, `reservation_id`, `custom_attributes` |

Relay adds two things PSI/J does not have: the list of inputs (RL16), and on a guarded site a package reference in place of `executable` (RL24).

**RL16. Inputs are addressed by content.** Each input file is named by its SHA-256. Relay stages it once per site into a content store under the person's relay directory there. A job record lists the digests it used. A rerun stages nothing that is already present.

**RL17. The job record lives with the job, off the desktop.** It holds the spec, the digests, the scheduler's job id and each state change. A poll reads it. It survives a relay restart and a laptop that was closed for a week. The `worker` adapter keeps its existing queue as this record. The `ssh` adapter keeps a job directory. For the `tapis` adapter the record is the Tapis job: Tapis keeps the request, the scheduler's job id and the state history under the job UUID, and relay writes the spec and the digests into the job's `notes`.

**RL18. Scheduler coverage comes from `psij-python`.** Its batch executors cover Slurm, PBS, LSF and Cobalt. Relay's own `scheduler_providers/` package, which has only Slurm, is deleted, and the worker submits through `psij-python`.

## Design: services

`serve` answers the seven questions in `plans/remote-visualization-scope.md`. Each decision names the question it answers.

**RL19. A service is a job with ports.** A service spec is a job spec plus the ports the service listens on, each with a name and a protocol (`tcp`, `http`, `ws` or `mcp`), a health path, and a lifetime: bound to another job, or until stopped, with an optional idle release. Today's `ServiceRuntimeSpec` and gateway sessions become this. (Question 1: a service, not only a job.)

**RL20. frp is the rendezvous.** An frp client starts beside the service and dials the rendezvous host. The desktop side binds a loopback port to it. A WebSocket rides it unchanged. (Question 2.)

- Where compute nodes cannot dial out, the site entry names the hop: the site's worker, or an HTTP proxy.
- XTCP stays as the optional direct path.
- On an `ssh` site with no rendezvous host configured, the endpoint is one `ssh -L` forward held by the system `ssh` for the life of the endpoint. `describe` marks that endpoint as not durable on Windows.
- The rendezvous host is one setting: address, protocol and token. The person runs their own with relay's `relay-host` role, or points at one that someone hosts for them.

**RL21. A served MCP server looks like part of relay.** When a port's protocol is `mcp`, relay keeps one session to it for the life of the service and mounts its tools and resources on relay's own MCP server under `<site>_<name>_`. Tool metadata, including MCP Apps metadata, passes through unchanged, and relay sends `tools/list_changed`. A host needs nothing new to use it.

**RL22. Access to a served port.** (Question 3.)

- The desktop port binds to loopback only.
- The service must require its own secret on every port a browser reaches. Relay refuses to serve an `http` or `ws` port whose spec does not say how it is protected.
- Relay passes the service the address its ports will have on the desktop, in its environment, so the service can hand its own view a working URL.
- The secret reaches the browser only inside a tool result's UI data, which CLIO keeps off the model. It is never in `describe`, a task status or a log.
- A `tcp` port is for a program on the desktop and not for a browser. The loopback bind and the rendezvous secret protect it.
- Stopping the endpoint closes the desktop port at once.

`browser_gateway.py` is deleted. It existed because a sandboxed frame could not attach a header to an event stream. A service that checks its own secret on a loopback port does not need it.

**RL23. Lifetime, reconnection, several viewers, the end.**

- A service task reaches `working` with the endpoint in its status when the health path answers. (Superseded by video1k, ledger W1: the task completes then, with the endpoint as its result, so a polling client wakes; the service runs on until stopped.) New output is recorded as task progress. In CLIO progress alone does not start a turn (`_record_progress` in `gact/external_tasks.py`), so an agent that is asked to keep watching sets a loop with CLIO's `set_loop` and reads the progress on each wake. (Question 4.)
- The endpoint is durable. When the desktop sleeps or the link drops, relay re-binds the same loopback port on return. The service kept its state, so a view resumes where it was. (Question 5.)
- Any number of browser connections may use one endpoint. Whether they share state is the service's business. (Question 6.)
- On stop, the service gets a termination signal and a grace period to write its final artifacts. The task result lists them, and CLIO registers them as artifacts as it does for any task. (Question 7.)

## Design: guardrails

**RL24. Code policy per site.** A site is `open` or `guarded`. The person sets it. Catalog entries for facility systems default to `guarded`.

- **`open`:** the agent may run what it writes, including a batch script it generated. This fits Ares, a cloud node or a machine the person owns.
- **`guarded`:** `run` and `serve` accept only a reviewed JARVIS package, named with its version and SHA-256, plus parameters. CLIO's packages come from clio-jarvis (RL39). A free-form command is refused with a reason that says so.
- **Review.** A package becomes reviewed when the person approves it in a question that shows where its source is and its hash. Relay records the hash. A changed package needs a new review.
- **Authoring.** The agent may write a JARVIS package with a skill. It goes through the same review before it runs. This is the sanctioned way around a missing package, and it still forces the package semantics on what runs.
- **`exec` on a guarded site** takes an argument list, never a shell string, and only commands on the site's read-only list: file listing and reading, scheduler queries, module queries, quota queries, and JARVIS read commands.

JARVIS is the guardrail mechanism. It sets up the infrastructure and constrains what the agent can express. The guard covers relay. The agent's shell is covered by CLIO's permission rules, and a site with multi-factor sign-in refuses a non-interactive `ssh` anyway.

**RL25. The spend gate.** Nothing that consumes an allocation or money starts without a yes. The question shows the site, queue, account, nodes, walltime and the maximum charge. `spend.ask` in the site entry is `always`, `never`, or a threshold in node-hours. With nobody present the task waits on its input request and nothing is submitted.

**RL26. A warm allocation.** For interactive work on compute nodes, `exec` can use a held scheduler job. It goes through the spend gate and releases itself after ten idle minutes.

## Design: asking the person

Relay asks in four ways, and the kind of ask decides the way.

| Ask | Way | What the person sees in CLIO |
| --- | --- | --- |
| A choice or an approval: SSH profile, account, queue, a package review, the spend gate | MCP form elicitation | A question in the conversation or on the task |
| A sign-in CLIO owns: Globus, GitHub | A sign-in surface from relay with CLIO's own sign-in action | A card with a sign-in button. CLIO's private sign-in UI does the rest |
| A secret the person types: a site password, a one-time code, a cloud key | MCP URL elicitation to a page relay serves on loopback | A question with the address and "Open link". The page takes the secret |
| A sign-in on the provider's own page: TACC, and Globus on a host that hands relay no grant | MCP URL elicitation to the provider's `https` page | A question with the address and "Open link". The provider's page takes the password and any second factor, and relay receives only tokens |

**RL27. Form elicitation carries no secret.** The MCP specification forbids it, and CLIO records what is typed into a question.

**RL28. Relay's page.** Relay serves one page for each ask on a loopback port, at a single-use address that expires. The page posts to relay. A password or one-time code is held in memory for that sign-in only. A cloud key is stored in the operating system's keyring under relay's name. Nothing typed there is logged.

**RL29. Prompts from `ssh` reach the page.** Relay runs `ssh` with `SSH_ASKPASS` set to a helper and `SSH_ASKPASS_REQUIRE=force`. Each prompt the site sends (password, one-time code, a push confirmation) becomes one ask on the page. This is how the one sign-in of the `worker` adapter is answered, on Windows as well.

**RL30. Identities.** One sign-in unlocks several things.

| Sign-in | Unlocks |
| --- | --- |
| GitHub | Sources, `gh` and Copilot in CLIO today. Private package sources in relay |
| Globus | Storage transfer in CLIO today. `globus` sites, and `iri` sites where the facility takes Globus tokens |
| SSH profile | `ssh` sites and the one sign-in of `worker` sites |
| Cloud profile | `sky` sites |
| TACC | `tapis` sites (RL48) |

- On CLIO, relay receives the Globus and GitHub grants from CLIO (HS4) and does not sign in on its own.
- On a host that hands relay no grant, relay signs in to Globus itself with a URL elicitation to Globus Auth, as a native app with its own registered client.
- After a grant arrives or changes, relay probes each catalog site that uses that identity and marks it ready, missing an allocation, or unreachable. A Globus sign-in therefore lights up ALCF when the account has access.

## Design: the compute tab

**RL31. Relay ships a CLIO plugin.** It lives in the clio-relay repository and is published to the marketplace. It holds the server command, an A2UI catalog, the tab declaration, the account declarations and the site skills.

**RL32. The tab.** One row for each site: name, address, the adapter in use with a switch between the alternatives, the identity and its status, the policy, the defaults, the linked skill, and the count of reviewed packages. A row has Connect, Probe, Edit and Remove. "Add site" starts from the catalog or from an empty form, and the form is generated from the adapter's `Config` schema.

**RL33. Relay's surfaces use only components CLIO already renders.** No component is added to gact-tui for relay. Relay's catalog names the components it uses and carries its instructions.

**RL34. Site skills.** Relay serves each skill as an MCP resource. A skill is optional. Relay ships a set for the catalog sites, and the person can link their own. With no skill linked, the agent runs `describe` and works from what the site reports.

## Design: host semantics in CLIO

Five additions to clio-agent and gact-tui. Each is stated without naming relay, and each has a fixture plugin as its test.

**HS1. A2UI from a server.** A tool result or a resource whose content item has the MIME type `application/a2ui+json` is a surface owned by that server.

- The envelope version must be `v0.9` or `v0.9.1`. A `1.0` payload is refused with a typed reason.
- CLIO validates it against the catalogs the server's plugin declares, stores it and renders it like any other surface.
- A surface returned by a tool call appears in the conversation. A surface named by a plugin tab fills that tab.

**HS2. Actions on a server's surface go back to that server.** The action dispatcher delivers the action as `tools/call` to the tool the action names, on the owning server, with the action's context as arguments. No model turn is involved. Only a tool whose MCP Apps visibility includes `app` can be called this way. A2UI messages in the result are applied to the surface.

**HS3. A plugin can declare a tab.** The `ai.iowarp.clio` extension gains `tabs`: a title, an icon and the `a2ui://` resource that fills it. CLIO reads the resource when the tab opens and again on `notifications/resources/updated`.

**HS4. A plugin can declare the account grants its server needs.** The extension gains `accounts`: a provider and the scopes.

- CLIO writes each grant to a private file and passes the file's path in the server's environment as `CLIO_ACCOUNT_<PROVIDER>_FILE`. CLIO rewrites the file when it refreshes the token, and the server reads it when it needs it. The token is never in the environment or the arguments.
- A scope the plugin declares is added to that provider's consent. For Globus this is the Globus Compute scope (`ComputeScopes.all` in the Globus SDK) beside the transfer scope.
- When the grant is missing, the server shows a surface with the existing `data_source/login/<provider>` action, and CLIO's own sign-in runs.

**HS5. A plugin declares where its server's URL elicitations may point.** A plugin whose server CLIO launches may declare that it uses loopback asks. CLIO then accepts `http://127.0.0.1:<port>` and `http://localhost:<port>` from that server only. Every other `http` URL stays refused. The plugin may also name `https` origins, such as a provider's sign-in page. CLIO accepts those from that server only, beside the origins in `tools.mcp.elicitation.url_trusted_origins`.

## Design: removal

**RL35. In clio-relay.** Deleted: the `relay_*` tools and the routes only they used, the `mcp_call` federation with its package in `jarvis-packages/`, `browser_gateway.py` and its errors module, and `scheduler_providers/`. The `remote_agent` and `bounded_command` packages are the worker's own and stay in clio-relay. Kept and moved behind the `worker` adapter and `serve`: bootstrap, the queue and leases, the one-link connection, frp, the service runtime.

Version 1.6.8 stays installable. VIGIL uses its browser attachment with jarvis-cd's ParaView HTTP service, and it moves to the ParaView app when its owners choose.

**RL36. `ssh_execute` in clio-agent.** Plain `ssh` from the shell is the simple path, so the tool goes.

| File | Lines | Role |
| --- | --- | --- |
| `src/clio_agent/gact/ssh_tools.py` | 210 | The tool, with its approval gate |
| `src/clio_agent/gact/infrastructure/ssh_execution.py` | 161 | Paramiko executor, called only by the tool |
| `src/clio_agent/gact/infrastructure/ssh_result.py` | 63 | Result type, used only by the tool |
| `src/clio_agent/gact/ssh_recorded_presentation.py` | 33 | Re-renders recorded `ssh_execute` results. Imported by `tool_result_presentation.py` and `session_export_projection.py` |
| `src/clio_agent/gact/agents/native_tool_registry.py` | 3 | Registration |
| `tests/test_gact/test_ssh_tool_contract.py`, `tests/test_gact/test_ssh_execution.py` | 502 | Tests |
| gact-tui `tool-operation-contract.ts`, `tool-operation-outcome.ts`, `tool-kind-icons.ts` | a few | The tool's name |

- A session recorded before the removal shows its `ssh_execute` rows through the generic tool row.
- Paramiko stays. Connected storage and the Infrastructure transport use it.
- Nothing in the Infrastructure module's SSH code for remote deployment changes, in clio-agent or in gact-tui.

**RL37. The fenced shell reaches SSH through the fence.** This is what makes RL36 safe on Linux and Windows.

- clio-agent gains a small CONNECT helper that joins standard input and output to a tunnel through the fence's proxy.
- Inside the fenced shell, `ssh` resolves to a launcher that runs the real `ssh` with that helper as its `ProxyCommand`. `scp`, `sftp` and Git over SSH go the same way.
- The agent types `ssh ares hostname`. The connection is recorded as a `net.egress` event with the host and port 22.
- `ssh_execute` is not kept as a fallback. If the fence refuses the tunnel, the fence is what gets fixed.

**RL38. Relay leftovers in clio-agent.** All deleted.

- In `src/clio_agent/gact/infrastructure/drivers.py`: `RELAY_VERSION`, the `relay` managed service definition and `_relay_plan`. Relay is a plugin and places its own worker, so the Infrastructure tab no longer installs it.
- The orphaned docstring lines in `src/clio_agent/tools/gateway.py` (240 to 246) that describe `remote`, `jarvis` and `relay_ops` namespaces.
- Stale comments that point at deleted relay files, in `agent.py`, `gact/provenance_wiring.py`, `tools/mcp_connection_era.py`, `tools/mcp_discovery.py`, `errors.py`, `tools/fs_write.py` and `conf.py`.
- The `relay` pytest marker in `pyproject.toml` and the tests that carry it.
- In gact-tui, the `relay` engine icon in `infrastructure-topology.tsx`, which goes with the managed service. The Remote tools settings (`relay-settings.tsx`) are about servers added by URL and stay.

## Design: clio-jarvis

**RL39. A new repository of JARVIS packages, written for CLIO.** `clio-jarvis` is a JARVIS package repository. jarvis-cd stays the runtime and is not forked. CLIO stops using `builtin` packages, and `builtin` is left as it is for its other users.

JARVIS is for guarded sites. On the person's machine or an open site the agent deploys directly, with a container, the site's modules or a script, and no package is involved.

**RL40. What every clio-jarvis package provides.** This is the contract, and a package that misses any of it does not ship.

| Part | What it is |
| --- | --- |
| Parameters | A schema. It drives the form and the review on a guarded site |
| Resources | Nodes, processes, GPUs, walltime and queue in the job spec's terms (RL15), so the spend gate can price a run before it starts |
| Outputs | Each output's name, path and format, and a view descriptor: the ParaView reader, that reader's configuration, and whether the output is still being written |
| Progress | A record for each step written, which relay turns into task progress |
| Service ports | For a service: each port's name and protocol, the health path, how the port is protected, and what it writes when it is told to stop |
| Provenance | The package version and hash, and the digests of its inputs |
| A self-test | A small case that runs on one node in minutes and checks the outputs against their descriptors |

**RL41. The first packages.**

| Package | What it is |
| --- | --- |
| `pvserver` | Starts a stock ParaView `pvserver` as a service with one `tcp` port, on one process or under MPI. The ParaView app on the desktop connects to it |
| `gray_scott` | The ADIOS2 Gray-Scott reaction-diffusion code. It runs in minutes, which makes it the chain's quick test |
| `xcompact3d` | Xcompact3d with ADIOS2 output and the ParaView configuration that reads it. This is the first real target |

The Xcompact3d configuration already works by hand. The package commits it: the ADIOS2 XML the run uses and the view descriptor that opens its output.

**RL42. How a site gets the packages.** Relay adds clio-jarvis to the site's JARVIS with `jarvis repo add`, at a tagged release, the first time a package is needed there. On a guarded site each package is still reviewed and pinned by hash before it runs (RL24).

**RL43. The chain from a simulation to the tab.**

```
run xcompact3d          ->  output on the site's filesystem  ->  serve pvserver  ->  the ParaView app's tab
task progress: each step,                                         bound to the simulation's job
with its view descriptor
```

- `run` starts the simulation. On a guarded site it is a clio-jarvis package, and its task reports each output with its view descriptor. On an open site the agent runs the code itself and writes the descriptor.
- `serve` starts `pvserver`, bound to the simulation's job (RL19).
- Relay gives the desktop a loopback port for the `pvserver`. The ParaView app connects to it as it does to any remote ParaView, and the agent passes the descriptor to `open_view`. The view follows the output as it grows.
- An agent asked to keep watching sets a loop. On each wake it reads the tasks' progress, looks and measures through the ParaView app, and reports what changed.

**RL44. The ParaView server follows a source that is still being written.** This is a change to clio-kit's ParaView server, made here because a running simulation is what needs it.

- `open_view` takes a view descriptor: the path or path pattern, the format, the ParaView reader and that reader's configuration, and whether the source is still being written.
- For a source that is still being written, the server re-reads the reader's time list when the source grows. A view that is following the latest step moves to it. A view the person has parked on a step stays there and shows that newer steps exist.
- Each new step is reported through `ui/update-model-context` and as task progress.
- The data path is files on the site's filesystem, read as they are written. The simulation and ParaView share no connection, so a slow view never holds the simulation back.

**RL45. Offline visualization on a secure site.** On a guarded site ParaView runs only as the reviewed `pvserver` package, on a dataset that never leaves the site. The tab is the same one the MCP Apps plan delivers, connected through the port relay carries.

## Design: Tapis

At SC26, TACC offered compute access through Tapis in exchange for a demo of CLIO reaching TACC through relay. There is no TACC allocation yet. A Tapis account needs only a TACC account, and Tapis registers any Linux host it can reach over SSH as a system. The adapter is therefore built and shown on hosts the team already has, a Chameleon node and Ares, and moving to Vista changes a site entry and no code.

**RL46. `tapis`.** Written on `tapipy` 25.4.0. It is ours, under the same rules as every other adapter (RL6 to RL8). The desktop needs only HTTPS to the tenant.

| Verb | How |
| --- | --- |
| `describe` | `systems.getSystem` and `systems.checkUserCredential`: the host, the scheduler, the logical queues with their limits, whether the person's credential works, and whether relay's app is registered |
| `run` | `jobs.submitJob` against relay's app on the site's system (RL47). A poll is `jobs.getJobStatus`. A cancel is `jobs.cancelJob` |
| `files` | `files.listFiles`, `getContents`, `insert`, `mkdir`, `moveCopy` and `delete`. A transfer between two Tapis systems is `files.createTransferTask` and comes back as a task |
| `exec` | Not implemented. Refused with a typed reason, and routed to an alternative (RL5) |
| `serve` | Not implemented. Refused with a typed reason, and routed to an alternative (RL5) |

The job spec maps onto the Tapis job request.

| Job spec (RL15) | Tapis job request |
| --- | --- |
| `name` | `name` |
| `executable`, `arguments` | `parameterSet.appArgs`, read by relay's launcher (RL47) |
| `environment` | `parameterSet.envVariables` |
| `node_count` | `nodeCount` |
| `processes_per_node` times `cpu_cores_per_process` | `coresPerNode` |
| `memory` | `memoryMB` |
| `duration` | `maxMinutes`, rounded up |
| `queue_name` | `execSystemLogicalQueue` |
| `account`, `reservation_id`, `gpu_cores_per_process`, `custom_attributes` | `parameterSet.schedulerOptions`, as the scheduler's own flags. The account is `-A <account>` |
| Inputs (RL16) | `fileInputs` with a `tapislocal://exec.tapis/` source, so Tapis moves nothing |

Tapis job states map onto the task states.

| Tapis status | Task state |
| --- | --- |
| `PENDING`, `PROCESSING_INPUTS`, `STAGING_INPUTS`, `STAGING_JOB`, `SUBMITTING_JOB`, `QUEUED`, `BLOCKED`, `PAUSED` | Queued, with the Tapis status in the task's message |
| `RUNNING`, `ARCHIVING` | Running |
| `FINISHED` | Done |
| `CANCELLED` | Cancelled |
| `FAILED` | Failed, with Tapis's last message |

- Tapis runs batch jobs through Slurm only. `tapis` serves `run` on a system with Slurm, as a `BATCH` job, and on a system with no scheduler, as a `FORK` job. On any other scheduler `run` routes to the alternative.
- Relay polls at the default interval. It subscribes to no Tapis notification, because Tapis cannot reach the desktop.
- Inputs are staged once per digest into the content store on the system with `files.insert`.
- Outputs stay in the job's directory on the site. Relay sets the archive directory to the job's output directory on the same system and leaves Tapis's launch files out of it (`archiveFilter.includeLaunchFiles: false`). `files` brings back what the person asks for.
- The Tapis job UUID goes into the task result and the science log with the rest of the run's provenance.

**RL47. One app per system, owned by relay.** Relay registers one Tapis app for the person on each system with `apps.createAppVersion`, the first time `run` needs it. Its id is `clio-relay.<Tapis username>.<system id>`, because app ids are unique across a tenant. Its version is the version of relay's launcher. The agent never registers an app and never calls Tapis.

- The app uses Tapis's `ZIP` runtime. The archive is relay's launcher, a tar file that holds `tapisjob_app.sh`. Relay places it on the system with `files.insert`, and the app's `containerImage` is its absolute path. Tapis unpacks it, writes its own `tapisjob.sh`, and submits that through Slurm or forks it. A tar file is used so that the system does not need `unzip`.
- No container runtime is used. The job spec has no container field.
- On an `open` site the launcher runs the job spec's executable with its arguments in the spec's directory.
- On a `guarded` site the launcher accepts only a package reference (name, version, SHA-256) and parameters. It checks the hash on the site against the reviewed one and runs the package through JARVIS (RL24). Relay builds every job request, so a free-form executable is refused before anything reaches Tapis.
- On a `tapis` site relay stages the clio-jarvis release through Tapis Files, addressed by content, and the launcher adds it with `jarvis repo add` (RL42). No shell on the site is needed for it.

**RL48. The TACC identity.** The sign-in happens on TACC's page, and relay never sees the password.

- Relay uses the device code grant of the Tapis authenticator, which the `tacc` tenant lists among its grant types. It requests a code at `/v3/oauth2/device/code` with its client id and sends a URL elicitation to the verification address on `https://tacc.tapis.io`. The person enters the code there and signs in, with a second factor when TACC asks for one. Relay polls `/v3/oauth2/tokens` until the tokens arrive.
- Relay's loopback page (RL28) is not used for TACC. A password typed there would reach relay and then Tapis's password grant, which rule 5 forbids.
- The access token, four hours by default, and the refresh token are stored in the operating system's keyring under relay's name. Relay refreshes on its own. When the refresh token is refused, the identity shows as signed out and the next call asks again.
- Relay is a native app with one OAuth client registered in the tenant. The authenticator issues a refresh token only to a client that has a key, so the client's id and key ship with relay. A native app cannot keep that key private, and the key grants nothing without the person's sign-in.
- On CLIO, relay's plugin names `https://tacc.tapis.io` as an origin for its URL elicitations (HS5).
- **A person's own host.** Tapis needs a key to log in, and the person's own keys never leave their machine. Relay makes a key pair for that system with no passphrase, as Tapis requires. It appends the public half to `authorized_keys` on the host through the site's `ssh` alternative, with a comment that names relay and the system. It registers the pair with `systems.createUserCredential`, which checks the login, and then deletes its own copy of the private half. Removing the site removes the Tapis credential and the `authorized_keys` line.
- **A TACC host.** No key is made. TACC hosts ask for a second factor, which a registered key cannot pass, so the system uses `TMS_KEYS`: relay calls `systems.createUserCredential` with `createTmsKeys=true`, and Tapis's trust manager makes and holds the key.

**RL49. Systems and catalog entries.** A `tapis` site is one Tapis system.

- **A person's own host.** Relay registers the system as `clio-relay.<Tapis username>.<site id>` with `systems.createSystem`: a `LINUX` system with the host and port of the SSH profile, `rootDir` `/`, `canExec` true, the `ZIP` job runtime, and a job working directory under the person's relay directory there. The login name on the host goes in the credential as `loginUser`. When the host has Slurm, the system also gets `canRunBatch`, `batchScheduler: SLURM` and one logical queue for each partition that the `ssh` alternative's `describe` reports.
- Tapis connects from TACC. The host must accept SSH from the internet with a key and no second factor, and a jump host cannot be used. When Tapis cannot log in, registration fails and relay shows Tapis's reason.
- **A TACC host.** When the entry's `system_id` names a system TACC already provides, relay uses it and registers only the credential. Otherwise relay registers the person's own system for that host with `TMS_KEYS`.

```yaml
- id: chameleon-tapis
  name: Chameleon node (through Tapis)
  address: <floating IP>
  adapter: tapis
  identity: tacc
  config: {base_url: https://tacc.tapis.io, authn: pki}
  alternatives:
    ssh: {ssh_profile: chameleon}     # serves exec and serve, and installs the key
  policy: open
  spend: {ask: never}

- id: ares-tapis
  name: Ares (through Tapis)
  address: ares.cs.iit.edu
  adapter: tapis
  identity: tacc
  config: {base_url: https://tacc.tapis.io, authn: pki}
  alternatives:
    ssh: {ssh_profile: ares}
  policy: open
  spend: {ask: never}

- id: vista
  name: TACC Vista
  address: vista.tacc.utexas.edu
  adapter: tapis
  identity: tacc
  config: {base_url: https://tacc.tapis.io, authn: tms}
  alternatives:
    worker: {ssh_profile: vista}      # serves exec and serve
  policy: guarded
  spend: {ask: always}
  defaults: {account: <allocation>}

- id: stampede3                       # the same shape as vista
  address: stampede3.tacc.utexas.edu
```

`chameleon-tapis` and `ares-tapis` are the same shape: a host the person can already reach over SSH, registered in Tapis. Adding one walks the person through the SSH profile, the TACC sign-in and the system registration. Ares has Slurm, so `ares-tapis` runs `BATCH` jobs, which is the path Vista uses. A single Chameleon node runs `FORK` jobs.

**RL50. TACC in the compute tab.** No new component. The row shows the TACC identity's status and the Tapis system id. After a TACC sign-in, relay probes the catalog's TACC sites (RL30). A TACC host refuses a login from an account with no allocation there, so the credential check fails and the row reads "missing an allocation". That is the state of Vista and Stampede3 today.

## Windows

- No adapter holds an SSH connection. `ssh` makes one process for each call, `worker` uses frp, `globus`, `iri` and `tapis` are HTTPS, `sky` is SkyPilot's own client.
- The `worker` sign-in is one `ssh` run with its prompts answered on relay's page (RL29).
- The fenced shell reaches SSH through the helper (RL37).

## Defaults for the open questions

Take these and record each in the ledger.

| Question | Default |
| --- | --- |
| Server transport | stdio when CLIO launches it. Streamable HTTP when a person runs relay as a service |
| Policy for a catalog facility site | `guarded` |
| Policy for a site the person adds by hand | `open`, shown in the form before saving |
| `spend.ask` for a facility site | `always` |
| Warm allocation idle release | Ten minutes |
| Poll interval for a queued job | Thirty seconds, doubling to five minutes |
| Rendezvous host | None configured. `serve` on a site that needs one says how to set it |
| clio-jarvis version on a site | The latest tagged release when the site first needs it. Updating is a choice in the Compute tab |
| Relay's page port | Chosen by the operating system for each relay start |
| Where a cloud key is stored | The operating system's keyring |
| A2UI version relay sends | v0.9.1 |
| Site skills shipped | One for each catalog site |
| The `_relay_plan` managed service | Deleted |
| Tapis tenant | `tacc`, at `https://tacc.tapis.io` |
| Relay's app on a Tapis system | `clio-relay.<Tapis username>.<system id>`, at the launcher's version |
| Policy for `chameleon-tapis` and `ares-tapis` | `open` |
| Policy for Vista and Stampede3 | `guarded` |
| Where the Tapis tokens are stored | The operating system's keyring |

## What the ledger records

These are facts the work depends on. Each is recorded with its source before the code that relies on it is merged.

- Every caller of each `relay_*` tool in clio-agent, gact-tui, clio-kit and the marketplace.
- The facility page behind each catalog entry's address, endpoint id and IRI URL.
- The Globus Compute user-configuration keys each catalog endpoint accepts.
- The IRI Facility API version each facility serves, and its token type.
- Which OpenSSH versions honor `SSH_ASKPASS_REQUIRE`, on Windows as well.
- The result of a `CONNECT` to port 22 through the Codex managed proxy on Linux and Windows.
- How CLIO's action dispatcher is told a surface's owner, and where a server-owned surface is stored.
- Every reader of `RELAY_VERSION` and of the `relay` managed service id.
- The ParaView reader and reader configuration that open Xcompact3d's ADIOS2 output, as already used by hand, and the ADIOS2 XML of that run.
- Which `builtin` packages clio-relay 1.6.8 calls, so each has a clio-jarvis counterpart or a recorded reason it is not needed.
- The TACC documentation page behind each TACC catalog entry, and the Tapis system TACC names for Vista and Stampede3 when it names one.
- The `tapipy` operation used for each call in RL46 to RL49, read from the specs that ship in `tapipy` 25.4.0.
- The result of registering Ares and the Chameleon node in the `tacc` tenant, with Tapis's own message.
- The lifetime of the refresh token the `tacc` tenant issues to relay's client.

## Qualification

Run focused tests and `ruff` on touched paths while working. Then run these against a freshly restarted stack and record each in the ledger with commands, handles and the screenshots read. UI changes are checked as the megachange plan describes, at 1440 by 900 and 390 by 844.

Fixtures the session builds: an SSH host in a container, the same with Slurm, the same with PBS, a host whose sign-in asks for a one-time code, an frp rendezvous host, a small HTTP and WebSocket service that checks a secret, a small MCP server that keeps a counter between calls, a fixture plugin for the host semantics, and a Tapis stub. The stub serves the tenant, device code and token endpoints and the Systems, Apps, Jobs and Files endpoints that `tapipy` calls, with job states driven by the test.

| ID | Scenario | Passes when |
| --- | --- | --- |
| LQ1 | List the server's tools | Exactly five model-visible tools, `describe`, `run`, `exec`, `files` and `serve`, plus any mounted by a running service. A `tools/list` request whose `_meta` declares `org.a2ui/a2ui` also lists the Compute tab's action tools, marked `_meta.ui.visibility: ["app"]`. A client without A2UI sees exactly the five, and every action call needs its single-use action token |
| LQ2 | `describe` with no site, then for the SSH fixture | The list shows status. The site report shows the route for each verb |
| LQ3 | Install a fixture adapter package | It appears in `describe` and in the add-site form with no change to relay |
| LQ4 | The conformance kit on `local`, `ssh` and `worker` | All pass. Unimplemented verbs are refused with a typed reason |
| LQ5 | `run` on the Slurm fixture through `ssh`, then close relay, wait, reopen | The task completes from the site's job record. No SSH process lived between polls |
| LQ6 | The same job on the PBS fixture | The script and status come from `psij-python`. No scheduler code is in relay |
| LQ7 | Run a job twice with the same 200 MB input | The second run stages nothing |
| LQ8 | `run` on a host with no scheduler | The detached job's output and exit code are read by a later poll |
| LQ9 | Add the one-time-code fixture as `ssh` | The first call fails with `interactive_auth_required` and names `worker` |
| LQ10 | Add it as `worker` and connect | One question with a loopback address. The code typed on the page signs in. The worker dials the rendezvous. No SSH process remains |
| LQ11 | Search the transcript, the question records and relay's log after LQ10 | The code appears in none |
| LQ12 | `serve` the secret-checking service on the Slurm fixture | The task reaches `working` with an endpoint. The loopback port answers with the secret and refuses without it |
| LQ13 | Open a WebSocket to that endpoint, drop the link for a minute, restore it | The same port is bound again and a new connection works |
| LQ14 | `serve` the counter MCP server, call its tool three times | The tools are mounted under the site name. The counter reads 3. One session served all three |
| LQ15 | Stop a service that writes a file on termination | The task result lists the file and CLIO shows it as an artifact |
| LQ16 | A free-form `run` on a guarded site | Refused, with the reason and the way to proceed |
| LQ17 | The agent writes a JARVIS package for that site | A review question shows the source and hash. After a yes it runs. After an edit it needs a new review |
| LQ18 | `exec` on a guarded site with `rm`, then with a queue query | The first is refused. The second returns |
| LQ19 | `run` on a site with `spend.ask: always` | A question shows queue, account, nodes, walltime and maximum charge. A no submits nothing |
| LQ20 | The same with nobody answering | The task waits. The scheduler shows no job |
| LQ21 | A warm allocation left idle | It releases itself. The scheduler shows it gone |
| LQ22 | The fixture plugin returns an A2UI surface from a tool | It renders in the conversation. A `1.0` payload is refused with a typed reason |
| LQ23 | Click a button on that surface | The owning server receives `tools/call` with the action context. No model turn ran |
| LQ24 | The fixture plugin declares a tab | The tab opens with the surface and updates on a resource change |
| LQ25 | The fixture plugin declares an account | The grant file path is in the server's environment and the token is not. After a refresh the file changes |
| LQ26 | The fixture server sends a URL elicitation to its loopback page, to an `https` origin its plugin names, to another `http` host, and to an `https` origin nobody named | The first two are offered. The last two are refused |
| LQ27 | Open the Compute tab, add the SSH fixture from the form, switch its adapter | The row updates. Every control is one of CLIO's existing components |
| LQ28 | The Compute tab and its forms at 390 by 844 | They fit |
| LQ29 | From the fenced shell on Linux, `ssh <fixture> hostname` | It succeeds, and a `net.egress` event shows the host and port 22 |
| LQ30 | Search clio-agent and gact-tui for `ssh_execute`, `RELAY_VERSION`, `_relay_plan`, `relay_ops` and the `relay` marker | Nothing remains outside the ledger and this plan |
| LQ31 | Search clio-agent and gact-tui for the words relay, Slurm, PBS, Globus Compute and SkyPilot in code added by this plan | None |
| LQ32 | Search clio-relay for `relay_submit`, `mcp_call`, `browser_gateway` and `scheduler_providers` | Nothing remains |
| LQ33 | With clio-jarvis added on the Slurm fixture, `run` the `gray_scott` package at its self-test size | The task reports each step. The result lists every output with its view descriptor |
| LQ34 | Each clio-jarvis package's self-test, and a package with one contract part removed | The self-tests pass. The broken package is refused with the missing part named |
| LQ35 | The conformance kit on `tapis` against the Tapis stub | It passes. `exec` and `serve` are refused with a typed reason |
| LQ36 | `run` against the stub, restart relay in the middle of the job, poll | The task completes from the Tapis job. The job UUID is in the task result. The request named relay's app and used `tapislocal` inputs |
| LQ37 | `describe` for a `tapis` site whose alternative is the SSH fixture | The route for `exec` and `serve` shows `ssh` |
| LQ38 | A free-form `run` on a guarded `tapis` site | Refused, as LQ16. The stub received no job request |
| LQ39 | A TACC sign-in through the stub's device code endpoints, then a search of the transcript, the question records and relay's log | One question with an `https` address. No password was asked for. No token or device code appears in any of the three |
| LQ40 | Add the SSH fixture as a person's own host through `tapis`, then remove it | A new key pair is made. Its public half is in the fixture's `authorized_keys`, the stub received the credential, relay's copy of the private half is gone, and none of the person's keys was read. After the removal the credential and the line are both gone |

The session runs on the owner's machine, which reaches Ares over SSH. Ares is the real site the session drives. The session runs the Ares rows itself, and they must pass like the first table. A row that needs a sign-in only the person can give, an allocation, a paid cloud or a Windows machine is run by the person. Each row is recorded in the ledger.

| ID | Site | Needs and what to record | Run by |
| --- | --- | --- | --- |
| LQ-L1 | Ares through `ssh` | A job, a file transfer and a service. Record the scheduler and the time from submit to first poll | Session |
| LQ-L2 | Polaris through `globus` | A Globus sign-in in CLIO with the compute scope. The row turns ready with no SSH. Run a reviewed package in the debug queue through the spend gate | Person |
| LQ-L3 | Polaris through `worker` | The one sign-in with a one-time code on relay's page. Record how long the worker stays linked | Person |
| LQ-L4 | Aurora | Start the person's own Globus Compute endpoint with one sign-in, then run a reviewed package | Person |
| LQ-L5 | A facility IRI endpoint | Submit and cancel one job. Record the API version and token type | Person |
| LQ-L6 | AWS through `sky` | A key typed on relay's page lands in the keyring. The spend gate shows SkyPilot's price. The instance stops when idle | Person |
| LQ-L7 | Windows Desktop | LQ5, LQ10, LQ12 and LQ29 from Windows. Close the lid for an hour in the middle of LQ5 | Person |
| LQ-L8 | DeltaAI | One job through the adapter its entry names | Person |
| LQ-L9 | Gray-Scott into the tab, on Ares | "Run Gray-Scott and show me the field." Ares is an open site, so the agent builds and runs it directly and no package is involved. The ParaView tab opens while the run is going and follows each new step. Needs the ParaView app from the MCP Apps plan | Session |
| LQ-L10 | Xcompact3d into the tab, on Polaris | The same with the `xcompact3d` package in a guarded queue: the package review, the spend gate, then a tab that follows the run. Record the time from a step being written to it showing | Person |
| LQ-L11 | `serve` the `pvserver` package on Ares, and point the ParaView app at the loopback port relay returns | The port comes up and closes when the service stops. The tab opens as it does for any remote ParaView. Needs the ParaView app from the MCP Apps plan | Session |
| LQ-L12 | The ParaView server on a file series that a script keeps writing, one step every few seconds | The view follows the latest step with no reload. Parked on an earlier step, it stays and shows that newer steps exist. Needs the ParaView app from the MCP Apps plan, and can run on the cloud once it has landed | Session |
| LQ-L13 | Offline on Polaris: open a dataset already on the site through the reviewed `pvserver` package | The package review and the spend gate, then the tab. The dataset is not copied off the site | Person |
| LQ-L14 | `chameleon-tapis` | A TACC account and a leased Chameleon node with a floating IP. Add the site from the catalog, let relay register the system and its key, run a job, and pull the outputs back through Tapis Files. Record the Tapis job UUID and the time from submit to running | Session, after the person's TACC sign-in |
| LQ-L15 | The TACC demo, on `chameleon-tapis` | "Run Gray-Scott on Chameleon through Tapis and show me the field." Then the same with Xcompact3d. The job goes through Tapis, the outputs come back through Tapis Files, and they open in the ParaView tab. Record the run in the science log | Session, after the person's TACC sign-in |
| LQ-L16 | Vista through `tapis` | Needs the allocation. The row turns ready after the sign-in. A reviewed clio-jarvis package runs in a small queue through the spend gate | Person |
| LQ-L17 | Ares through `tapis` | Add `ares-tapis`, let relay register the system and its key, run a Slurm job as a Tapis `BATCH` job, and pull an output back through Tapis Files. Record the Tapis job UUID, the Slurm job id Tapis reports, and the time from submit to running. When Tapis cannot log in to Ares from TACC, record Tapis's reason, and LQ-L14 carries the adapter | Session, after the person's TACC sign-in |

A failing scenario in the first table, or in a row the session runs on Ares, is fixed, not deferred.

## Done

1. Every decision is implemented, and the items under Removal are gone.
2. Focused tests pass, the conformance kit passes for `local`, `ssh` and `worker`, and for `tapis` against the stub, and `ruff check` and `ruff format --check` are clean on touched paths.
3. Every scenario in the first table passes against a freshly restarted stack, each shown with commands, handles and the screenshots read. So does every row the session runs on Ares. A row the person runs says so.
4. The ledger holds every decision with its evidence and every fact listed above.
5. Relay's documentation has a page for each adapter, a page on writing an adapter, a page on guarded sites and package review, and a page on running a rendezvous host.
6. CLIO's user guide has a page on the Compute tab, and its developer guide has a page on the five host semantics.
7. The branches are pushed in clio-relay, clio-jarvis, clio-kit, clio-agent, gact-tui and clio-agent-marketplace, the submodule pins point at the pushed commits, and the draft PRs carry a summary and the evidence.

## Owner work beside this plan

| Item | Why |
| --- | --- |
| Create the empty `clio-jarvis` repository and give the session push access | A session cannot create a repository in the organization |
| Hand over the Xcompact3d configuration that already works: the ADIOS2 XML and the ParaView reader settings | RL41 commits it instead of rediscovering it |
| A TACC account, and the TACC sign-in when relay first asks for it | RL48. The session cannot sign in as the person |
| Register one OAuth client for relay in the `tacc` tenant, with one `create_client` call under a TACC token, and hand over its id and key | RL48. Registering a client needs a token, and the first token needs a person |
| A leased Chameleon node with a floating IP, and an SSH profile for it | LQ-L14 and LQ-L15 |
| Confirm that Ares accepts SSH from outside IIT with a key and no second factor, and that a key held by TACC's Tapis for your Ares account is acceptable there | LQ-L17. Tapis logs in to Ares from TACC |
| Ask TACC what "full access" covers, when they want the demo, whether the NAIRR Vista system can be opened meanwhile, and which Tapis system to use for Vista and Stampede3 | Decides when LQ-L16 can run and what its `system_id` is |

## References

These systems ground the semantics. Read them for how a thing is done, above all for how to drive Globus. None is a dependency unless an adapter uses it directly.

| System | What to take from it |
| --- | --- |
| hpc-bridge (`globus-labs/hpc-bridge`, read at `8c40246`, v0.1.27, Apache-2.0) | How to drive Globus Compute from an MCP server: one sign-in to place a user endpoint or use the facility's, the spend gate enforced in the server, idle self-release, a registry of sites |
| Globus Compute | Endpoints need only outbound connectivity. ALCF runs facility endpoints on Polaris and Crux. Aurora users run their own |
| IRI Facility API | A compute interface close to PSI/J, served by NERSC, ALCF, OLCF and ESnet |
| PSI/J (`psij-python` 0.9.11) | The job spec, and script generation for Slurm, PBS, LSF and Cobalt |
| SkyPilot | Used directly by the `sky` adapter |
| Tapis v3 (`tapipy` 25.4.0, and the authenticator read at `75e135d`) | Used directly by the `tapis` adapter. Systems, Apps, Jobs and Files, and the device code grant |
| OpenResearch `orx` | Job inputs addressed by SHA-256 and detached supervision. Its limits are the ones this design avoids: a 24-hour SSH master for two-factor sites, no Windows, status that stops when the master drops |
| Academy | A Globus-backed exchange between agents. It is request and response only, with 48-hour tokens |
| clio-relay 1.6.8 | frp with STCP, an unprivileged client, the SSH bootstrap, the durable queue. Kept as the `worker` adapter and `serve` |

## Working conventions

- Use the branch the session is given, with the same name in every repository, and write it in the ledger header. Open draft PRs against the base the session was given.
- Small Conventional Commits, with test evidence in each PR description.
- Keep runtime state in `.cloud-run/`. Stop the stack when you finish a work block.
