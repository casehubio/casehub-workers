# CaseHub Workers

## Project Type

type: java

## Repository Role

Integration-tier collection of CaseHub worker implementations. Each module implements the `WorkerProvisioner` (and optionally `CaseChannelProvider`, `WorkerStatusListener`) SPIs from `casehub-engine-api`, enabling CaseHub cases to dispatch work to different execution runtimes — HTTP endpoints, Apache Camel routes, shell scripts, Kubernetes Jobs, Lambda functions, and more.

**Tier:** Integration (sits alongside `claudony` and `casehub-openclaw` in the build order)

**Design philosophy:** Thin wrappers — each worker module translates a CaseHub case step dispatch into the target runtime's protocol and reports back completion/failure. No domain logic lives here.

**Research doc:** `casehubio/parent` — `docs/superpowers/research/2026-06-07-desired-state-management-research.md`

## Build Commands

```bash
# Build all modules
mvn --batch-mode install

# Publish to GitHub Packages (CI only — requires GITHUB_TOKEN)
mvn --batch-mode deploy -DskipTests
```

## Module Structure

| Module | Artifact | Purpose |
|--------|----------|---------|
| `workers-http` | `casehub-workers-http` | Dispatch case steps to any HTTP/REST endpoint or webhook |
| `workers-camel` | `casehub-workers-camel` | Apache Camel route execution — 300+ connectors (Kafka, AWS, Salesforce, SAP, FTP, DB) |
| `workers-testing` | `casehub-workers-testing` | Test fixtures: NoOpWorkerProvisioner, CaptureWorkerProvisioner — **test scope only** |

## Key SPIs (from casehub-engine-api)

Workers implement these SPIs. All are defined in `casehub-engine-api` — never redefined here.

| SPI | Purpose |
|-----|---------|
| `WorkerProvisioner` | Provision/deprovision a worker instance for a case step |
| `CaseChannelProvider` | Open and manage channels for worker communication |
| `WorkerStatusListener` | Receive lifecycle events from running workers |
| `WorkerContextProvider` | Supply context to the worker at invocation time |

**`postToChannel` is 6-param** (engine#343): `(channel, from, content, MessageType, correlationId, deadline)` — never use the 3-param convenience form in production implementations.

## Worker Candidates (priority order)

Research and justification are in the workspace `HANDOFF.md` and in the parent research doc.

| Priority | Worker | Key Capability |
|----------|--------|---------------|
| 1 | `workers-http` | Any HTTP service — broadest compatibility, simplest to implement |
| 2 | `workers-camel` | 300+ enterprise connectors via Quarkus Camel |
| 3 | MCP worker | Any MCP server's tools become dispatchable workers |
| 4 | `workers-script` | Shell/Python/JS script execution |
| 5 | GitHub Actions | Trigger GH Actions workflows as case steps |
| 6 | `workers-k8s-job` | Any containerised workload via Kubernetes Job |
| 7 | Ansible | Ansible playbook execution — strategic fit with desired-state |
| 8 | AWS Lambda | FaaS dispatch |

## Key Rules

- `workers-testing` is never a compile or runtime dependency — test scope only.
- Each worker module must activate by classpath presence (`@ApplicationScoped`, no configuration required to enable).
- Workers must be stateless — all state lives in the case instance or the external system, never in the worker provisioner bean.
- `tenancyId` must be propagated through all provisioner calls — bind in Repository layer only (PP-20260520-e6a5f0).
- Workers must implement idempotent provisioning — calling `provision()` twice must be safe.

## Cross-Repo Conventions

Protocols live in `casehub/garden`. Do not write protocol files in this repo.

## Endpoint Registry Context

A `casehub-endpoints` module is planned for `casehub-platform`. Workers will reference named endpoints rather than hardcoded connection details. An `EndpointRegistry` SPI (Path-based addressing, tenant-scoped) will allow workers to resolve connection info by name. Issue filed: `casehubio/platform` (see workspace HANDOFF.md for issue number).

## Artifact Locations

| Skill | Writes to |
|-------|-----------|
| brainstorming (specs) | `docs/superpowers/specs/` |
| writing-plans (plans) | workspace `plans/` |
| handover | workspace `HANDOFF.md` |
| idea-log | workspace `IDEAS.md` |
| design-snapshot | workspace `snapshots/` |
| adr | `docs/adr/` |
| write-blog | workspace `blog/` |

## Routing

| Artifact   | Destination | Notes |
|------------|-------------|-------|
| adr        | project     | lands in `docs/adr/` |
| blog       | workspace   | staged here; published via publish-blog |
| specs      | project     | lands in `docs/superpowers/specs/` |
| plans      | workspace   | |
| handover   | workspace   | |

## Work Tracking

**Issue tracking:** enabled
**GitHub repo:** casehubio/casehub-workers

## Workspace

**Project repo:** `/Users/mdproctor/claude/casehub/workers`
**Workspace:** `/Users/mdproctor/claude/public/casehub-workers`
**Workspace type:** public

Git discipline — always use explicit paths:
```bash
git -C /Users/mdproctor/claude/public/casehub-workers ...   # workspace artifacts
git -C /Users/mdproctor/claude/casehub/workers ...          # project artifacts
```
