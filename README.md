# openSensus

[English](README.md) | [简体中文](README.zh-CN.md)

openSensus is a perception and attention layer for AI agents, and the reference
implementation of the [`sensus/0.1`](docs/sensus-protocol-v0.1.md) protocol.

Enterprise systems submit append-only Observations. openSensus projects current state,
detects evidence-backed Signals, and exposes a bounded, permission-filtered world view
through MCP — so a scheduled agent can ask one question and learn what changed, instead
of polling every source itself.

```text
GitLab / Jira / CRM
        |
        | Observation API
        v
   openSensus Runtime ---------> world projections
        ^                           |
        | openSensus MCP            | Signals + evidence
        |                           |
   Scheduled Agent <----------------+
```

openSensus does not replace source-specific MCP servers. It tells an agent where and why to
look, then hands deep inspection off: `get_evidence` returns a verified capability such
as `gitlab.merge_request.read`, which the harness maps to an installed vertical MCP.
openSensus does not proxy source credentials and does not execute source actions.

## What works in this MVP

- `POST /v1/observations` and `/v1/observations/batch`
- strict `sensus/0.1` validation with idempotency and conflict detection
- projections for entities, relations, state, events, and metrics, on SQLite or
  PostgreSQL
- late-arrival protection for current state
- principal- and classification-aware ACL filtering, including field-level entity ACLs
- correction Observations with deterministic subject projection replay
- explicit snapshot/reconciliation sessions with safe authoritative deletion
- configurable relative-change and threshold Signal rules
- bounded, cycle-safe relation graph expansion for organization rollups
- six read-only MCP tools: `observe`, `inspect`, `timeline`, `query`, `compare`, and
  `get_evidence`
- Evidence resolver hints for handing deep inspection to a vertical MCP server
- automated HTTP, storage, projection, Signal, evidence, and MCP integration tests,
  including a concurrency suite that is verified to fail without its fixes

The loop it closes:

```text
Observation
  -> authorization-aware projection
  -> correction/reconciliation
  -> configurable detection
  -> bounded graph observation
  -> MCP investigation
  -> verified evidence / vertical MCP handoff
```

## Documentation

| Document | What it covers | Languages |
| --- | --- | --- |
| [Integration Guide](docs/integration-guide.md) | Hands-on setup: run the runtime, build a Producer, connect an agent, configure rules, operate and troubleshoot | [EN](docs/integration-guide.md) · [中文](docs/integration-guide.zh-CN.md) |
| [Architecture](docs/architecture.md) | How the runtime works and why: the log/projection split, per-kind merge semantics, corrections, reconciliation, Signal detection, access control. Includes architecture and sequence diagrams | [EN](docs/architecture.md) · [中文](docs/architecture.zh-CN.md) |
| [Protocol v0.1](docs/sensus-protocol-v0.1.md) | The normative wire contract: Observation envelope, HTTP ingestion, MCP tools, conformance requirements | [EN](docs/sensus-protocol-v0.1.md) |

The protocol specification is a normative document and is maintained in English only,
so that the contract has a single authoritative wording.

## Requirements

- Node.js 22 or later
- npm

## Quick start

Install and build:

```bash
npm install
npm run build
```

Seed a GitLab review-latency scenario:

```bash
SENSUS_DB_PATH=./data/demo.db npm run seed
```

Start the ingestion API:

```bash
SENSUS_DB_PATH=./data/demo.db \
SENSUS_TENANT_ID=acme \
SENSUS_API_KEY=local-secret \
SENSUS_PRINCIPALS=role:agent,team:payments \
SENSUS_CLEARANCE=internal \
npm start
```

The API listens on `http://127.0.0.1:8787` by default. Check it with:

```bash
curl -H 'Authorization: Bearer local-secret' \
  http://127.0.0.1:8787/health
```

For local development, omit `SENSUS_API_KEY` to disable bearer authentication. The
default bind address remains localhost.

## Connect the MCP server

Build first, then configure an MCP host to launch:

```bash
SENSUS_DB_PATH=/absolute/path/to/data/demo.db \
SENSUS_TENANT_ID=acme \
node /absolute/path/to/openSensus/dist/src/mcp.js
```

Generic MCP host configuration:

```json
{
  "mcpServers": {
    "openSensus": {
      "command": "node",
      "args": ["/absolute/path/to/openSensus/dist/src/mcp.js"],
      "env": {
        "SENSUS_DB_PATH": "/absolute/path/to/data/demo.db",
        "SENSUS_TENANT_ID": "acme"
      }
    }
  }
}
```

Tenant selection is process configuration rather than a model-generated MCP
argument. This prevents an Agent from selecting another tenant through tool input.
The MCP process also receives trusted Consumer principals and a maximum clearance:

```text
SENSUS_PRINCIPALS=role:agent,team:payments
SENSUS_CLEARANCE=internal|confidential|restricted
```

An explicit `deny` wins over `allow`; an explicit `allow` requires at least one
matching principal. `inherit_from_source: true` without resolved `allow` or `deny`
metadata is fail-closed.

## Submit an Observation

```bash
curl -X POST http://127.0.0.1:8787/v1/observations \
  -H 'Authorization: Bearer local-secret' \
  -H 'Content-Type: application/json' \
  -d '{
    "spec_version": "sensus/0.1",
    "observation_id": "obs_example_1",
    "tenant_id": "acme",
    "kind": "state.observed",
    "subject": {
      "type": "software.change",
      "id": "gitlab:acme/payments-api!3812"
    },
    "occurred_at": "2026-09-16T09:10:00Z",
    "observed_at": "2026-09-16T09:10:04Z",
    "source": {
      "system": "gitlab",
      "instance": "acme-gitlab"
    },
    "data": {
      "field": "software.review_status",
      "operation": "set",
      "value": "waiting"
    }
  }'
```

## Snapshot reconciliation

Start a source snapshot:

```bash
curl -X POST http://127.0.0.1:8787/v1/syncs \
  -H 'Authorization: Bearer local-secret' \
  -H 'Content-Type: application/json' \
  -d '{
    "tenant_id": "acme",
    "sync_id": "sync_gitlab_20260917",
    "mode": "reconciliation",
    "source": { "system": "gitlab", "instance": "acme-gitlab" },
    "authoritative_deletion": true
  }'
```

Submit snapshot Observations with:

```text
Sensus-Sync-Id: sync_gitlab_20260917
```

Then complete the snapshot:

```bash
curl -X POST \
  http://127.0.0.1:8787/v1/syncs/sync_gitlab_20260917/complete \
  -H 'Authorization: Bearer local-secret' \
  -H 'Content-Type: application/json' \
  -d '{ "record_count": 1842, "cursor": "gitlab:2026-09-17T10:00:00Z" }'
```

Deletion is conservative. A missing entity is marked deleted only when the completed
snapshot is authoritative and no other source currently reports the entity as
present. If a previously deleted source record reappears, reconciliation emits an
auditable presence Observation and restores it.

## Configure Signal rules

Rules are persisted per tenant and immediately re-evaluate existing metric series:

```bash
curl -X POST http://127.0.0.1:8787/v1/signal-rules \
  -H 'Authorization: Bearer local-secret' \
  -H 'Content-Type: application/json' \
  -d '{
    "tenant_id": "acme",
    "rule": {
      "rule_id": "review_wait_slo",
      "name": "Review wait exceeds seven hours",
      "enabled": true,
      "applies_to": {
        "metric": "software.review_wait_time",
        "subject_types": ["organization.team"],
        "dimensions": {}
      },
      "condition": {
        "kind": "threshold",
        "operator": "gt",
        "value": 7,
        "for_samples": 2
      },
      "signal_type": "software.review_wait_slo_breach",
      "severity": "critical",
      "confidence": 1
    }
  }'
```

Supported conditions are absolute `threshold` and percentage-based
`relative_change`, both with consecutive-sample requirements.

## Relation graph observation

`observe` can roll up related entities while remaining bounded:

```json
{
  "scope": {
    "type": "organization.team",
    "id": "org:acme/team/payments"
  },
  "include": ["state", "changes", "signals"],
  "expand": {
    "direction": "incoming",
    "relations": ["organization.owned_by", "software.belongs_to"],
    "max_depth": 2,
    "max_nodes": 100
  }
}
```

Traversal is cycle-safe, ACL-filtered, and capped at five levels and 500 nodes.

## Development

```bash
npm run check
npm test
npm run dev            # ingestion API, reload on change
npm run mcp:dev        # stdio MCP
npm run mcp:http:dev   # Streamable HTTP MCP
```

`npm test` builds the project and runs the integration suite with Node's built-in test
runner. The storage conformance and PostgreSQL end-to-end suites skip themselves when no
database is reachable, so the command stays green without one.

Set `SENSUS_TEST_DATABASE_URL` and `SENSUS_TEST_DATABASE_URL_E2E` to run them against a
real PostgreSQL instance. See [CONTRIBUTING.md](CONTRIBUTING.md) for that and for the
lockfile rule CI enforces.

## Security

Read [SECURITY.md](SECURITY.md) before deploying this anywhere but a laptop. It states
the assumptions the security model rests on — Producers are trusted to describe their own
data, the ingestion API does not separate roles, and tenant isolation is configuration
rather than a default — so you can check whether they hold for you.

Report vulnerabilities privately, not in a public issue.

## License

MIT — see [LICENSE](LICENSE).

