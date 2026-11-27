# Relay rework shrink ledger

What is left in `src/clio_relay` after the old system was removed, and why each module stays. Sizes are bytes of Python at integration after the shrink round 2 (W1 793b76d0). The reason is the module's own docstring, so it stays true as the code changes; a module whose docstring cannot say why it exists is a deletion candidate.

## Sizes

| Tree | Before the rework (70d3fc1) | Now |
| --- | --- | --- |
| src/clio_relay | 6,611,517 | 1,253,649 |
| tests | 4,324,615 | 804,421 |
| jarvis-packages | 164,701 | 0 |

## What was removed

- The old worker tree under `adapters/worker` (261 files): bootstrap, its journal, retention, session start, cleanup and recovery, the report artifacts. The worker is now `site-serve` and nine files (W8).
- The old service runtime, `frp_check`, `bounded_payload`, `pagination` and `redaction` (W8, W9).
- Relay's own JARVIS repository `jarvis-packages/` (`bounded_command`, `remote_agent`, `_jarvis_api`) and `process_containment*` (W8).
- The queue and its storage: `spool`, `storage_*` (policy, reservation ledger, snapshot, codec, file I/O) (W1).
- The job model of the old queue: `models` and its eleven `models_*` parts, `identifiers`, `job_identity`, `remote_values`, `filesystem_paths`, `dev_mode` (W1).
- The cluster file registry and its Windows ACL machinery: `cluster_config_{models,registry,io}` and five `cluster_config_windows_*` files. `cluster_config.py` is now the frp link settings only, in memory (W1).
- `command_evidence` and the `pytest_platform_partition` plugin; the `release_platform` selection is twelve lines in `tests/conftest.py` (W1).
- `examples/` (the old live-acceptance pipelines), `_contracts`, `docs/ai/` and `docs/design/relay-architecture-2026-08.md`.

## Reached only at run time

An import graph from the entry points in `pyproject.toml` does not reach these, but each is loaded by name or run as a program:

- `adapters/_supervise.py`: run as `python -m` by the local adapter's supervised jobs.
- `adapters/tapis/__main__.py`: the `python -m clio_relay.adapters.tapis` sign-in commands.
- `adapters/sky/bridge.py` and `adapters/sky/fake_sky/`: run in the sky environment's Python; the fake stands in for SkyPilot.
- `identity/github.py`, `identity/globus.py`, `identity/oauth.py`: loaded through the identity registry.
- `serve/_site/hop_proxy.py`: copied to the site and run there.
- `testing/conformance/`: the adapter conformance kit that contributors run.

## Every module

Owner is the workstream that answers for the module.

### surface/ (234,657 bytes)

| Module | Bytes | Owner | Why it stays |
| --- | --- | --- | --- |
| `surface/__init__.py` | 237 | W1 | Relay's MCP surface: the server, the operations service and the default hooks. |
| `surface/a2ui/__init__.py` | 454 | W14 | Relay's Compute page for CLIO, as A2UI surfaces (RL31-RL33, HS1-HS5). |
| `surface/a2ui/actions.py` | 19,554 | W14 | What the Compute page's actions do, each through the site store the CLI uses (RL32). |
| `surface/a2ui/adding.py` | 5,607 | W14 | The Compute page's add-endpoint surface (RL32, RL50). |
| `surface/a2ui/cards.py` | 8,899 | W14 | The Compute page as endpoint cards, for a client that renders only the Basic catalog (RL32). |
| `surface/a2ui/components.py` | 3,800 | W14 | A small builder for A2UI components, limited to relay's catalog (RL33). |
| `surface/a2ui/forms.py` | 11,618 | W14 | Forms generated from an adapter's ``Config`` JSON Schema (RL32). |
| `surface/a2ui/protocol.py` | 4,494 | W14 | What a client says about A2UI, and how relay wraps A2UI messages (HS1, HS2). |
| `surface/a2ui/surfaces.py` | 16,010 | W14 | The Compute page's names, state and shared form pieces (RL32, RL50). |
| `surface/a2ui/tab.py` | 18,136 | W14 | The Compute page: one resource, its app-only action tools, and how they run (RL31-RL33). |
| `surface/a2ui/table.py` | 10,312 | W14 | The Compute page's endpoint table, for a client that renders relay's catalog (RL32). |
| `surface/a2ui/tokens.py` | 6,528 | W14 | Single-use action tokens for the Compute page (RL32, LQ1). |
| `surface/a2ui/values.py` | 4,054 | W14 | Reading what a Compute page form sent: JSON Pointers, form fields and adapter settings. |
| `surface/changes.py` | 4,315 | W1 | Tell clients what changed, on every protocol relay speaks. |
| `surface/describing.py` | 2,994 | W1 | What ``describe`` reports for relay as a whole and for one site (LQ2, RL5, RL34). |
| `surface/elicit.py` | 3,653 | W1 | Tool results for refusals, and the ways relay puts an ask to the person. |
| `surface/endpoints.py` | 1,873 | W1 | What relay shows of a service: its endpoint without secrets, and a short status line. |
| `surface/files_approval.py` | 3,809 | W1 | On a guarded site, deleting or replacing a file waits for the person. |
| `surface/files_verb.py` | 9,654 | W1 | The ``files`` verb: small files inline, larger ones in chunks. |
| `surface/guarding.py` | 1,840 | W1 | The asks the guard's decision and the price add to an operation (RL24, RL25). |
| `surface/loopback.py` | 9,548 | W1 | Relay's own single-use pages on loopback, for the stub ask provider. |
| `surface/outputs.py` | 2,417 | W1 | What relay adds to a job's outputs before it stores them (RL43). |
| `surface/progress.py` | 1,272 | W1 | What a task says of a running job's latest step (RL43). |
| `surface/server.py` | 32,278 | W1 | Relay's MCP server: five verbs over a standard MCP server (RL1, RL2). |
| `surface/service.py` | 33,318 | W1 | The operations service: everything the verbs do, without the wire. |
| `surface/settings.py` | 2,863 | W1 | Where relay keeps its configuration and state. |
| `surface/skills.py` | 7,100 | W1 | Site skills, served as MCP resources (RL34). |
| `surface/stubs.py` | 6,274 | W1 | The hooks relay runs with when no other distribution provides one. |
| `surface/transient.py` | 1,746 | W1 | Transient inputs never outlive their job (RL16). |

### (top level) (154,682 bytes)

| Module | Bytes | Owner | Why it stays |
| --- | --- | --- | --- |
| `__init__.py` | 81 | W1 | clio-relay package. |
| `__main__.py` | 140 | W1 | `python -m clio_relay`: the same as the `clio-relay` command. |
| `cli.py` | 11,899 | W1 | ``clio-relay``: serve relay over MCP, and manage sites, asks and services. |
| `cli_asks.py` | 9,938 | W13 | ``clio-relay asks``: see and answer what relay is asking, and manage sign-ins (RL28, RL30). |
| `cluster_config.py` | 3,278 | W1 | The frp link settings for one site. |
| `config.py` | 2,101 | W1 | Process settings read from the environment. |
| `control_channel.py` | 2,588 | W8 | The held relay-to-relay link's shared types. |
| `errors.py` | 526 | W1 | Relay-specific exceptions. |
| `frp_link.py` | 32,638 | W9 | The frp process substrate shared by mode (c) and the modes (a)/(b) frp link. |
| `frp_proxy_naming.py` | 1,908 | W9 | The ONE canonical stcp/xtcp proxy name a cluster's link resolves to (clio-relay#279). |
| `frp_transport.py` | 26,188 | W9 | ``RelayTransport`` implementations for modes (a) ``brokered_tcp`` and (b) ``udp_rendezvous`` (iowarp/clio-relay#231 R5, tracked by ... |
| `frp_visitor_process_inspection.py` | 13,632 | W9 | Cross-platform process-table inspection primitives (#285). |
| `frp_visitor_reconciliation.py` | 23,634 | W9 | Stale frpc visitor reconciliation: reap prior-process orphans, sweep crumb dirs. |
| `jarvis_site.py` | 20,105 | W13 | JARVIS on a site: package archives, the package runner and clio-jarvis (RL24, RL42). |
| `relay_host.py` | 4,547 | W9 | Rendering for the dumb frps relay host. |
| `session_api.py` | 1,479 | W1 | The worker's session identity document. |

### serve/ (106,500 bytes)

| Module | Bytes | Owner | Why it stays |
| --- | --- | --- | --- |
| `serve/__init__.py` | 437 | W9 | Relay's service runtime: a service is a job with ports (RL19 to RL23). |
| `serve/_site/hop_proxy.py` | 4,490 | W9 | A CONNECT proxy on a cluster's login side, for compute nodes that cannot dial out. |
| `serve/config.py` | 9,056 | W9 | The rendezvous setting and the per-site serve settings (RL20). |
| `serve/descriptor.py` | 1,791 | W9 | The view descriptor relay passes on in task progress (RL43). |
| `serve/desktop.py` | 8,724 | W9 | The desktop side: ``clio-relay connect`` and the serve settings (RL20). |
| `serve/forward.py` | 10,634 | W9 | The desktop end of a served port: a loopback bind that comes back (RL20, RL22, RL23). |
| `serve/frp.py` | 7,286 | W9 | frpc configuration for both ends of a served port (RL20). |
| `serve/mount.py` | 7,913 | W9 | Served MCP servers, mounted on relay's own server (RL21). |
| `serve/plan.py` | 14,032 | W9 | Turning a service spec into the job relay submits, and the record it keeps (RL19). |
| `serve/provenance.py` | 3,310 | W9 | A neutral provenance block for what a job or service produced. |
| `serve/report.py` | 6,241 | W9 | What relay reads from a service's files on the site, and what it tells a person about it. |
| `serve/runtime.py` | 30,516 | W9 | The service runtime behind W1's ``ServeHook`` (RL19 to RL23). |
| `serve/store.py` | 2,070 | W9 | Where relay keeps its service plans: one private JSON file per operation. |

### adapters/ssh/ (103,011 bytes)

| Module | Bytes | Owner | Why it stays |
| --- | --- | --- | --- |
| `adapters/ssh/__init__.py` | 234 | W7 | The ``ssh`` adapter: a site reached with the person's own ``ssh`` (RL10). |
| `adapters/ssh/adapter.py` | 33,490 | W7 | The ``ssh`` adapter (RL10, RL16 to RL18): a site reached with the system ``ssh``. |
| `adapters/ssh/config.py` | 2,642 | W7 | Settings for the ``ssh`` adapter. |
| `adapters/ssh/errors.py` | 8,172 | W7 | What an ``ssh`` failure means, and what the person can do about it. |
| `adapters/ssh/files.py` | 13,288 | W7 | File access on a site over ``ssh`` (the ``files`` verb, docs/files.md). |
| `adapters/ssh/pricing.py` | 1,020 | W7 | The spend price of an ``ssh`` job (RL25), in node-hours. |
| `adapters/ssh/scripts.py` | 17,099 | W7 | The POSIX ``sh`` scripts the ``ssh`` adapter runs on a site. |
| `adapters/ssh/signals.py` | 3,320 | W7 | Passing a cancel's signal on to the command, in rendered Slurm scripts (RL18). |
| `adapters/ssh/store.py` | 3,057 | W7 | Dropping a job's spent inputs from the site's content store (RL16). |
| `adapters/ssh/transport.py` | 20,689 | W7 | The system ``ssh`` and ``scp``, one process per call (RL10). |

### asks/ (86,080 bytes)

| Module | Bytes | Owner | Why it stays |
| --- | --- | --- | --- |
| `asks/__init__.py` | 805 | W13 | Asks: relay's questions for the person, and the page that answers them (RL25a, RL27 to RL30). |
| `asks/askpass.py` | 17,040 | W13 | ``SSH_ASKPASS`` for relay: ssh's prompts go to the person, never to the agent (RL28). |
| `asks/core.py` | 16,394 | W13 | What relay's page shows and does, for the loopback server and the HTTP routes. |
| `asks/loopback.py` | 4,849 | W13 | Relay's page on loopback, for relay over stdio. |
| `asks/pages.py` | 10,665 | W13 | Relay's page: single-use, expiring addresses and the HTML behind them (RL28). |
| `asks/provider.py` | 16,239 | W13 | Relay's ask provider: W1's ``asks`` hook (RL25a, RL27 to RL30). |
| `asks/routes.py` | 4,881 | W13 | Relay's page under relay's own origin, for relay over Streamable HTTP. |
| `asks/runner.py` | 3,114 | W13 | A private event loop on its own thread, for the page and the sign-ins behind it. |
| `asks/signin.py` | 12,093 | W13 | The sign-ins behind URL asks (RL28, RL30). |

### identity/ (85,575 bytes)

| Module | Bytes | Owner | Why it stays |
| --- | --- | --- | --- |
| `identity/__init__.py` | 6,671 | W13 | The identity provider table (RL30). |
| `identity/base.py` | 5,478 | W13 | What an identity provider is, and the values it trades in (RL30). |
| `identity/device.py` | 2,915 | W13 | Following a polled sign-in, such as an OAuth device flow (RFC 8628). |
| `identity/github.py` | 5,298 | W13 | Relay's own GitHub sign-in, for a host that hands relay no grant (RL30). |
| `identity/globus.py` | 8,561 | W13 | Relay's own Globus sign-in, for a host that hands relay no grant (RL30). |
| `identity/grants.py` | 8,531 | W13 | Grant files a host keeps current for relay (HS4, HS4a). |
| `identity/oauth.py` | 7,769 | W13 | What relay's own OAuth sign-ins share: PKCE, token responses and the keyring entry. |
| `identity/probe.py` | 5,661 | W13 | Probing the sites an identity unlocks, after a sign-in or a grant change (RL30). |
| `identity/store.py` | 13,740 | W13 | Relay's own secrets: the operating system's keyring, or a private file (RL28). |
| `identity/tacc.py` | 20,951 | W13 | The TACC identity (RL48): the Tapis device code grant, kept in relay's store. |

### adapters/ (73,363 bytes)

| Module | Bytes | Owner | Why it stays |
| --- | --- | --- | --- |
| `adapters/__init__.py` | 460 | W1 | Adapters: the interface, the registry, the hooks and the ``local`` adapter. |
| `adapters/_files.py` | 5,502 | W1 | File access on the machine relay runs on. The ``local`` and fixture adapters use it. |
| `adapters/_process.py` | 5,486 | W1 | Bounded subprocess execution, shared by adapters that run local commands. |
| `adapters/_supervise.py` | 3,914 | W1 | The detached supervisor for a job on a host with no scheduler (RL9). |
| `adapters/base.py` | 10,907 | W1 | The adapter contract: everything an adapter implements and uses (RL6). |
| `adapters/hooks.py` | 10,504 | W1 | The hooks other workstreams implement: asks, the guard and serve. |
| `adapters/local.py` | 23,522 | W1 | The ``local`` adapter (RL9): the machine relay runs on. |
| `adapters/registry.py` | 3,545 | W1 | The adapter registry and factory (RL6, RL8). |
| `adapters/site_files.py` | 9,523 | W1 | ``files`` for a remote site, from six small primitives (docs/files.md). |

### adapters/worker/ (71,929 bytes)

| Module | Bytes | Owner | Why it stays |
| --- | --- | --- | --- |
| `adapters/worker/__init__.py` | 719 | W8 | The ``worker`` adapter and the relay worker it places on a site (RL11). |
| `adapters/worker/adapter.py` | 31,855 | W8 | The ``worker`` adapter (RL11): relay's own worker, placed on a site once. |
| `adapters/worker/cli.py` | 752 | W8 | The ``clio-relay worker`` command group. |
| `adapters/worker/files.py` | 2,870 | W8 | File access on the site for the ``worker`` adapter, over the worker's link. |
| `adapters/worker/frpc_unit.py` | 2,224 | W8 | Render the site's frpc proxy config with its secrets left to the environment. |
| `adapters/worker/link.py` | 10,437 | W8 | The held link to one placed worker, from relay's side. |
| `adapters/worker/site_app.py` | 3,653 | W8 | The worker's HTTP API on the site. |
| `adapters/worker/site_routes.py` | 13,361 | W8 | The worker's adapter routes: the site half of the ``worker`` adapter (RL11). |
| `adapters/worker/site_server.py` | 6,058 | W8 | The worker's site process: its API plus the frpc proxy that exposes it. |

### adapters/tapis/ (69,638 bytes)

| Module | Bytes | Owner | Why it stays |
| --- | --- | --- | --- |
| `adapters/tapis/__init__.py` | 253 | W10 | The ``tapis`` adapter: jobs and files on a Tapis system as the signed-in person. |
| `adapters/tapis/__main__.py` | 5,629 | W10 | A terminal driver for the Tapis site steps a person takes once. |
| `adapters/tapis/adapter.py` | 20,466 | W10 | The ``tapis`` adapter (RL46 to RL50): jobs and files through Tapis v3. |
| `adapters/tapis/client.py` | 6,816 | W10 | The one place relay calls `tapipy` 25.4.0. |
| `adapters/tapis/config.py` | 1,588 | W10 | Settings for the ``tapis`` adapter. |
| `adapters/tapis/files.py` | 6,752 | W10 | File access on a Tapis system through Tapis Files (RL46). |
| `adapters/tapis/launcher.py` | 3,447 | W10 | Relay's launcher archive and the job record it reads (RL47). |
| `adapters/tapis/mapping.py` | 6,779 | W10 | How a relay job spec becomes a Tapis job request, and Tapis states become task states (RL46). |
| `adapters/tapis/register.py` | 17,908 | W10 | Registering a person's host as a Tapis system (RL48, RL49). |

### adapters/globus/ (67,080 bytes)

| Module | Bytes | Owner | Why it stays |
| --- | --- | --- | --- |
| `adapters/globus/__init__.py` | 242 | W11 | The ``globus`` adapter: Globus Compute for jobs, Globus Transfer for files. |
| `adapters/globus/adapter.py` | 19,909 | W11 | The ``globus`` adapter (RL12): jobs through Globus Compute, files through Globus Transfer. |
| `adapters/globus/compute.py` | 9,042 | W11 | How the ``globus`` adapter talks to Globus Compute. |
| `adapters/globus/task.py` | 8,076 | W11 | The function relay sends to a Globus Compute endpoint. |
| `adapters/globus/tokens.py` | 7,164 | W11 | Globus credentials for the adapters that use them (``globus`` and ``iri``). |
| `adapters/globus/transfer.py` | 14,583 | W11 | ``files`` for the ``globus`` adapter, over Globus Transfer. |

### adapters/iri/ (51,631 bytes)

| Module | Bytes | Owner | Why it stays |
| --- | --- | --- | --- |
| `adapters/iri/__init__.py` | 201 | W11 | The ``iri`` adapter: the DOE IRI Facility API (RL13). |
| `adapters/iri/adapter.py` | 15,793 | W11 | The ``iri`` adapter (RL13): the DOE Integrated Research Infrastructure Facility API. |
| `adapters/iri/client.py` | 9,449 | W11 | A small client for the DOE IRI Facility API. |
| `adapters/iri/files.py` | 8,742 | W11 | ``files`` for the ``iri`` adapter, over the IRI ``/filesystem`` calls. |

### adapters/sky/ (35,531 bytes)

| Module | Bytes | Owner | Why it stays |
| --- | --- | --- | --- |
| `adapters/sky/__init__.py` | 368 | W11 | The ``sky`` adapter: SkyPilot managed jobs, run from SkyPilot's own environment (RL14). |
| `adapters/sky/adapter.py` | 19,678 | W11 | The ``sky`` adapter (RL14): SkyPilot managed jobs on clouds, Kubernetes and Slurm. |
| `adapters/sky/bridge.py` | 7,322 | W11 | Relay's bridge to SkyPilot. It runs in SkyPilot's own Python, never in relay's. |
| `adapters/sky/fake_sky/__init__.py` | 75 | W11 | Holds the stand-in `sky` package; put this directory on PYTHONPATH. |
| `adapters/sky/fake_sky/sky/__init__.py` | 2,890 | W11 | A stand-in for SkyPilot 0.14.3, for tests and offline demos. It is not SkyPilot. |
| `adapters/sky/fake_sky/sky/jobs.py` | 5,198 | W11 | ``sky.jobs`` of the stand-in: managed jobs as local shell processes. |

### testing/ (33,305 bytes)

| Module | Bytes | Owner | Why it stays |
| --- | --- | --- | --- |
| `testing/__init__.py` | 86 | W1 | Test support that ships with relay: the conformance kit and a fixture adapter. |
| `testing/conformance/__init__.py` | 23,333 | W1 | The adapter conformance kit (RL7). |
| `testing/fixture_adapter.py` | 9,886 | W1 | A fixture adapter: simulated jobs with real persistence, for tests and demos. |

### guard/ (31,783 bytes)

| Module | Bytes | Owner | Why it stays |
| --- | --- | --- | --- |
| `guard/__init__.py` | 456 | W13 | Relay's guard (RL24 to RL26). See :mod:`clio_relay.guard.policy`. |
| `guard/config.py` | 2,107 | W13 | Per-site guard settings, read from ``<config_dir>/guard.yaml``. |
| `guard/execlist.py` | 7,264 | W13 | The read-only ``exec`` list of a guarded site (RL24). |
| `guard/policy.py` | 13,443 | W13 | Relay's guard: code policy per site, package reviews, pricing and the warm allocation. |
| `guard/reviews.py` | 5,547 | W13 | Package reviews, pinned by hash (RL24). |
| `guard/warm.py` | 2,966 | W13 | The warm allocation (RL26). |

### jobs/ (31,416 bytes)

| Module | Bytes | Owner | Why it stays |
| --- | --- | --- | --- |
| `jobs/__init__.py` | 1,213 | W1 | Jobs, operations, refusals and the content store: relay's core data model. |
| `jobs/content.py` | 2,852 | W1 | A local content-addressed store (RL16). |
| `jobs/file_rules.py` | 3,721 | W1 | The ``files`` rules every adapter follows (docs/files.md). |
| `jobs/handles.py` | 5,853 | W1 | What adapters return: job handles and states, exec results, endpoints, file access. |
| `jobs/records.py` | 6,174 | W1 | Durable operation and ask records. |
| `jobs/refusals.py` | 2,152 | W1 | Typed refusal reasons shared by the surface, the adapters and the hooks. |
| `jobs/spec.py` | 9,451 | W1 | The job and service specs. |

### sites/ (14,748 bytes)

| Module | Bytes | Owner | Why it stays |
| --- | --- | --- | --- |
| `sites/__init__.py` | 289 | W1 | Sites: entries, the ``sites.yaml`` store and verb routing. |
| `sites/models.py` | 2,821 | W1 | Site entries (RL4). |
| `sites/routing.py` | 4,647 | W1 | Verb routing (RL5). |
| `sites/store.py` | 6,991 | W1 | The site store over ``sites.yaml``. The command line and the compute tab both call it. |

### catalog/ (2,720 bytes)

| Module | Bytes | Owner | Why it stays |
| --- | --- | --- | --- |
| `catalog/__init__.py` | 2,720 | W1 | The catalog of known sites and site skills (RL4, RL34). |
