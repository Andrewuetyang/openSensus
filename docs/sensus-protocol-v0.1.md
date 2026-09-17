# Sensus Protocol v0.1

Status: Draft  
Protocol identifier: `sensus/0.1`  
Last updated: 2026-09-17

## 1. Purpose

Sensus Protocol defines how operational facts enter the openSensus perception layer,
how openSensus turns those facts into a current world view, and how an AI agent observes
and investigates that world.

The protocol connects three boundaries:

```text
Enterprise systems              openSensus                       Agent
Jira / Git / CRM / ERP          Runtime                      Harness
        |                          |                            |
        | Observation API          | openSensus MCP                 |
        +------------------------->+<---------------------------+
                                   |
                         World projections
                         Metrics and signals
                         Evidence and history
```

openSensus is a perception interface. It is not an action protocol. Agents use openSensus
to understand the world and use other tools, such as operational MCP servers, to
change it.

## 2. v0.1 scope

Version 0.1 standardizes:

1. A common Observation envelope.
2. Five Observation kinds.
3. HTTP ingestion and reconciliation behavior.
4. The minimum world projections maintained by a openSensus runtime.
5. The Signal resource.
6. Six read-only MCP tools for agents.
7. Provenance, evidence, time, identity, and access metadata.

Version 0.1 does not standardize:

- a complete enterprise ontology;
- connectors for particular vendors;
- agent prompts or reasoning strategies;
- actions that modify source systems;
- a general-purpose rule language;
- a universal natural-language query language.

## 3. Roles

### 3.1 Producer

A Producer observes a source system and submits Observations. A Producer can be a
webhook handler, polling connector, CDC pipeline, batch importer, or an application
calling the ingestion API directly.

A Producer is responsible for:

- stable source identifiers;
- accurate source and occurrence timestamps;
- retrying failed submissions;
- sending source access metadata when available;
- retaining enough cursor information for reconciliation.

### 3.2 Runtime

The openSensus Runtime accepts and stores Observations, resolves references, materializes
world projections, computes metrics, detects Signals, and exposes evidence-backed
read interfaces.

### 3.3 Consumer

A Consumer observes the world through the openSensus MCP interface. In v0.1, a Consumer
is normally a scheduled agent, an interactively invoked agent, or a signal-triggered
agent.

## 4. Normative principles

The keywords MUST, MUST NOT, SHOULD, SHOULD NOT, and MAY are normative.

1. Observations are append-only facts. Accepted Observations MUST NOT be mutated.
2. Current state is a projection, not the source of truth for history.
3. Every derived value MUST be traceable to evidence.
4. Source occurrence time and openSensus observation time MUST remain distinct.
5. Producers MAY send incomplete knowledge. openSensus MUST support incremental
   enrichment.
6. Unknown types and fields MUST be preserved when they are valid JSON.
7. Read results MUST be filtered using the identity of the calling Consumer.
8. openSensus MCP tools in v0.1 MUST be read-only.

## 5. Common data types

### 5.1 EntityRef

An `EntityRef` identifies a thing in the observed world.

```json
{
  "type": "software.work_item",
  "id": "jira:acme/ENG-1024"
}
```

```typescript
interface EntityRef {
  type: string;
  id: string;
}
```

`type` MUST be a lowercase, dot-separated semantic type. `id` MUST be stable within
the tenant. Source-native IDs SHOULD use a namespace prefix:

```text
jira:acme/ENG-1024
github:acme/payments#3812
salesforce:acme/0065g00000AbCdE
hris:acme/employee-1938
org:acme/team/payments
```

openSensus does not require a canonical enterprise ID before ingestion. Equivalent
identities are joined with an `identity.same_as` relation.

### 5.2 SourceRef

```typescript
interface SourceRef {
  system: string;
  instance: string;
  record_id?: string;
  cursor?: string;
  sequence?: number;
}
```

Example:

```json
{
  "system": "github",
  "instance": "acme-github",
  "record_id": "acme/payments/pull/3812",
  "cursor": "delivery:8f7281b4",
  "sequence": 194282
}
```

`cursor` is opaque to openSensus. `sequence`, when present, orders Observations from the
same source instance. Ordering across source instances is undefined.

### 5.3 EvidenceRef

```typescript
interface EvidenceRef {
  type: "source_record" | "observation" | "query" | "artifact" | "derivation";
  ref: string;
  version?: string;
  title?: string;
  resolver?: {
    capability: string;
    arguments?: Record<string, unknown>;
  };
}
```

Evidence references MUST be resolvable by the Runtime for an authorized Consumer.
Resolution MAY return structured content, metadata, or a short-lived source URL.

`resolver` is a transport-independent hint for deeper inspection through a vertical
tool such as GitLab MCP. `capability` names the required ability rather than a
deployment-specific MCP server or tool name. The Agent Harness maps the capability to
an installed tool:

```json
{
  "type": "source_record",
  "ref": "gitlab://acme/payments/merge_requests/3812",
  "resolver": {
    "capability": "gitlab.merge_request.read",
    "arguments": {
      "project": "acme/payments",
      "merge_request_iid": 3812
    }
  }
}
```

openSensus MUST NOT place source credentials in resolver arguments.

### 5.4 AccessPolicy

```typescript
interface AccessPolicy {
  classification?: "public" | "internal" | "confidential" | "restricted";
  inherit_from_source?: boolean;
  allow?: string[];
  deny?: string[];
}
```

Principal strings are implementation-defined but SHOULD use namespaces such as
`user:alice`, `team:payments`, and `role:engineering-lead`.

Omitting `access` does not mean public access. The Runtime MUST apply the tenant's
default ingestion policy.

## 6. Observation envelope

Every ingested fact uses this envelope:

```typescript
interface Observation<T = unknown> {
  spec_version: "sensus/0.1";
  observation_id: string;
  tenant_id: string;
  kind:
    | "entity.observed"
    | "relation.observed"
    | "event.occurred"
    | "state.observed"
    | "metric.observed";
  subject: EntityRef;
  occurred_at: string;
  observed_at: string;
  source: SourceRef;
  data: T;
  evidence?: EvidenceRef[];
  access?: AccessPolicy;
  confidence?: number;
  labels?: Record<string, string>;
  trace_id?: string;
}
```

Required fields are `spec_version`, `observation_id`, `tenant_id`, `kind`, `subject`,
`occurred_at`, `observed_at`, `source`, and `data`.

Rules:

- Timestamps MUST be RFC 3339 values with an explicit offset. Because offsets are
  permitted, two Producers can describe the same instant differently; a Runtime MUST
  order timestamps by the instant they denote, not by their textual form, and SHOULD
  store them in a single canonical form (UTC).
- `confidence`, when present, MUST be between `0` and `1` inclusive.
- `observation_id` MUST be unique within a tenant.
- Repeating an `observation_id` with the same canonical payload is idempotent.
- Repeating an `observation_id` with a different payload is a conflict.
- The Runtime assigns an immutable `received_at` timestamp after acceptance.
- `occurred_at` is when the fact was true in the source world.
- `observed_at` is when the Producer observed or extracted the fact.
- `received_at` is when the Runtime accepted the fact.

## 7. Observation kinds

### 7.1 `entity.observed`

Declares that an entity exists, describes it, or reports its lifecycle status.

```typescript
interface EntityObservedData {
  name?: string;
  lifecycle?: "active" | "archived" | "deleted";
  attributes?: Record<string, unknown>;
}
```

```json
{
  "spec_version": "sensus/0.1",
  "observation_id": "obs_01K57Y6QJH4J7ACZB6DY7X2PXP",
  "tenant_id": "acme",
  "kind": "entity.observed",
  "subject": {
    "type": "software.work_item",
    "id": "jira:acme/ENG-1024"
  },
  "occurred_at": "2026-09-16T08:40:00Z",
  "observed_at": "2026-09-16T08:40:03Z",
  "source": {
    "system": "jira",
    "instance": "acme-jira",
    "record_id": "ENG-1024"
  },
  "data": {
    "name": "Support batch refunds",
    "lifecycle": "active",
    "attributes": {
      "priority": "high",
      "work_item_type": "requirement"
    }
  }
}
```

Entity Observations are partial patches. A missing attribute means unknown or
unchanged, not deleted. Producers MUST use an explicit JSON `null` to clear a known
attribute.

### 7.2 `relation.observed`

Declares the current observed status of a directed relationship.

```typescript
interface RelationObservedData {
  relation: string;
  target: EntityRef;
  status: "active" | "inactive";
  attributes?: Record<string, unknown>;
}
```

```json
{
  "spec_version": "sensus/0.1",
  "observation_id": "obs_01K57Y8C6J4TZXJFNSN4W8GD7Q",
  "tenant_id": "acme",
  "kind": "relation.observed",
  "subject": {
    "type": "software.work_item",
    "id": "jira:acme/ENG-1024"
  },
  "occurred_at": "2026-09-16T08:42:31Z",
  "observed_at": "2026-09-16T08:42:34Z",
  "source": {
    "system": "jira-github-connector",
    "instance": "acme-integrations"
  },
  "data": {
    "relation": "software.implemented_by",
    "target": {
      "type": "software.change",
      "id": "github:acme/payments#3812"
    },
    "status": "active"
  }
}
```

A relation identity is the tuple `(tenant_id, subject, relation, target, source)`.
An inactive observation closes the active relation from the observation's
`occurred_at` time.

### 7.3 `event.occurred`

Records an immutable occurrence in the subject's timeline.

```typescript
interface EventOccurredData {
  type: string;
  actor?: EntityRef;
  object?: EntityRef;
  attributes?: Record<string, unknown>;
}
```

```json
{
  "spec_version": "sensus/0.1",
  "observation_id": "obs_01K57YB5WVEQNHQN2SMQEZHZZF",
  "tenant_id": "acme",
  "kind": "event.occurred",
  "subject": {
    "type": "software.change",
    "id": "github:acme/payments#3812"
  },
  "occurred_at": "2026-09-16T09:05:00Z",
  "observed_at": "2026-09-16T09:05:02Z",
  "source": {
    "system": "github",
    "instance": "acme-github",
    "record_id": "acme/payments/pull/3812",
    "cursor": "delivery:8f7281b4"
  },
  "data": {
    "type": "software.review_requested",
    "actor": {
      "type": "person",
      "id": "github:acme/alice"
    },
    "attributes": {
      "reviewer_count": 2
    }
  },
  "evidence": [
    {
      "type": "source_record",
      "ref": "github://acme-github/acme/payments/pull/3812"
    }
  ]
}
```

Events are never retracted by mutation. A Producer corrects a bad Event with a new
`event.occurred` Observation whose `data.type` is
`sensus.observation_corrected`. Its attributes MUST identify the original
Observation and the correction disposition:

```json
{
  "data": {
    "type": "sensus.observation_corrected",
    "attributes": {
      "target_observation_id": "obs_01K57YB5WVEQNHQN2SMQEZHZZF",
      "disposition": "invalid",
      "replacement_observation_id": "obs_01K57YNEW8BT4VNJ9VN6HK5FQK"
    }
  }
}
```

`disposition` is `invalid` or `superseded`. The Runtime MUST preserve the original
Observation for audit and MUST exclude it from later projections after an `invalid`
correction takes effect. A replacement is itself a complete, separately ingested
Observation.

After accepting a correction, the Runtime MUST deterministically rebuild every
projection affected by the target Observation from the remaining valid Observation
log. A correction and its projection rebuild are one atomic transaction. Version
0.1 does not permit a correction to target another correction event.

### 7.4 `state.observed`

Reports the value of one state field at a point in time.

```typescript
interface StateObservedData {
  field: string;
  operation: "set" | "unset";
  value?: unknown;
  previous_value?: unknown;
}
```

```json
{
  "spec_version": "sensus/0.1",
  "observation_id": "obs_01K57YC9DTDN68RJE89G5RZVBJ",
  "tenant_id": "acme",
  "kind": "state.observed",
  "subject": {
    "type": "software.work_item",
    "id": "jira:acme/ENG-1024"
  },
  "occurred_at": "2026-09-16T09:10:00Z",
  "observed_at": "2026-09-16T09:10:04Z",
  "source": {
    "system": "jira",
    "instance": "acme-jira",
    "record_id": "ENG-1024"
  },
  "data": {
    "field": "software.lifecycle_stage",
    "operation": "set",
    "value": "review",
    "previous_value": "development"
  }
}
```

The current field value is selected by occurrence time, then source sequence, then
receipt time. Runtime policy MUST define source precedence when multiple sources
observe the same field.

### 7.5 `metric.observed`

Reports a numeric measurement over a point or interval.

```typescript
interface MetricObservedData {
  metric: string;
  value: number;
  unit: string;
  interval?: { from: string; to: string };
  dimensions?: Record<string, string>;
  aggregation?: "gauge" | "count" | "sum" | "average" | "min" | "max" | "p50" | "p90" | "p95" | "p99";
  calculation?: {
    method: "source" | "derived";
    definition?: string;
  };
}
```

```json
{
  "spec_version": "sensus/0.1",
  "observation_id": "obs_01K57YFAJ6M02S1NNB7BRNT7N6",
  "tenant_id": "acme",
  "kind": "metric.observed",
  "subject": {
    "type": "organization.team",
    "id": "org:acme/team/payments"
  },
  "occurred_at": "2026-09-16T10:00:00Z",
  "observed_at": "2026-09-16T10:00:05Z",
  "source": {
    "system": "sensus-runtime",
    "instance": "acme"
  },
  "data": {
    "metric": "software.review_wait_time",
    "value": 18.4,
    "unit": "hour",
    "interval": {
      "from": "2026-09-09T00:00:00Z",
      "to": "2026-09-16T00:00:00Z"
    },
    "dimensions": {
      "repository": "payments-api"
    },
    "aggregation": "average",
    "calculation": {
      "method": "derived",
      "definition": "review_completed_at - review_requested_at"
    }
  },
  "evidence": [
    {
      "type": "query",
      "ref": "sensus-query://acme/review-wait-time/7d/payments-api"
    }
  ]
}
```

## 8. Snapshots and reconciliation

Event delivery alone is insufficient because events can be missed and systems often
contain data that predates openSensus.

A conforming Producer SHOULD support this lifecycle:

```text
Initial snapshot -> incremental observations -> periodic reconciliation snapshot
```

Snapshots use the same Observation kinds. Snapshot batches include these HTTP
headers:

```text
Sensus-Sync-Id: sync_01K57Z...
```

Before submitting members, a Producer starts a sync:

```http
POST /v1/syncs
Content-Type: application/json

{
  "tenant_id": "acme",
  "sync_id": "sync_01K57Z...",
  "mode": "reconciliation",
  "source": {
    "system": "jira",
    "instance": "acme-jira"
  },
  "authoritative_deletion": true
}
```

An authoritative sync MUST be `snapshot` or `reconciliation`; an incremental sync
cannot prove absence. The Runtime counts idempotently attached Observation members
and MUST reject completion when the supplied `record_count` differs from the actual
member count.

The Producer closes a snapshot with:

```http
POST /v1/syncs/{sync_id}/complete
Content-Type: application/json

{
  "source": {
    "system": "jira",
    "instance": "acme-jira"
  },
  "completed_at": "2026-09-16T10:30:00Z",
  "record_count": 18421,
  "cursor": "jira:updated:2026-09-16T10:29:58Z"
}
```

The Runtime MUST NOT infer deletion from a missing snapshot entity unless the
Producer and tenant configuration explicitly enable authoritative snapshot deletion.
Even then, a Runtime MUST NOT globally delete an entity while another source reports
that entity as present. Missing and reappearing records are represented by auditable,
derived lifecycle Observations rather than silent projection mutation.

## 9. HTTP ingestion API

Authentication and tenant selection are implementation-specific. The authenticated
principal MUST be authorized for the supplied `tenant_id`.

### 9.1 Submit one Observation

```http
POST /v1/observations
Content-Type: application/json
```

Success response:

```json
{
  "observation_id": "obs_01K57YB5WVEQNHQN2SMQEZHZZF",
  "status": "accepted",
  "received_at": "2026-09-16T09:05:02.418Z"
}
```

An idempotent retry returns `status: "duplicate"` with HTTP 200. A conflicting
payload returns HTTP 409.

### 9.2 Submit a batch

```http
POST /v1/observations/batch
Content-Type: application/json

{
  "observations": [ ... ]
}
```

The maximum v0.1 batch size is 1,000 Observations. Each item is accepted or rejected
independently:

```json
{
  "accepted": 2,
  "rejected": 1,
  "results": [
    { "observation_id": "obs_1", "status": "accepted" },
    { "observation_id": "obs_2", "status": "duplicate" },
    {
      "observation_id": "obs_3",
      "status": "rejected",
      "error": {
        "code": "INVALID_OCCURRED_AT",
        "message": "occurred_at must be an RFC 3339 timestamp"
      }
    }
  ]
}
```

### 9.3 Ingestion status codes

| HTTP | Meaning |
| --- | --- |
| 200 | Accepted or idempotent duplicate |
| 202 | Accepted for asynchronous validation |
| 400 | Malformed request |
| 401 | Missing or invalid authentication |
| 403 | Producer cannot write to the tenant |
| 409 | Observation ID conflicts with an existing payload |
| 413 | Batch or payload too large |
| 422 | Valid JSON that violates the protocol schema |
| 429 | Rate limit exceeded |

## 10. Runtime world model

A v0.1 Runtime MUST maintain these logical projections. Their physical storage is
not standardized.

### 10.1 Entity projection

The latest known name, lifecycle, attributes, aliases, sources, and observation
watermark for each entity.

### 10.2 Relation projection

Active and historical typed edges between entities.

### 10.3 State projection

The latest authorized value of each state field plus its provenance and validity
time.

### 10.4 Timeline projection

Events and state changes ordered by `occurred_at`, with late arrivals placed at their
source occurrence time.

### 10.5 Metric series

Measurements indexed by metric, subject, interval, and dimensions.

### 10.6 Signal index

Open, acknowledged, and resolved Signals visible to a Consumer.

Projection processing is allowed to lag behind ingestion. Read responses MUST expose
a watermark so a Consumer can distinguish world state freshness from wall-clock time.

## 11. Signals

A Signal is a Runtime conclusion that a change deserves attention. A Signal is not
an input Observation and MUST NOT be presented as an unqualified source fact.

```typescript
interface Signal {
  signal_id: string;
  tenant_id: string;
  type: string;
  subject: EntityRef;
  detected_at: string;
  updated_at: string;
  status: "open" | "acknowledged" | "resolved";
  severity: "info" | "warning" | "critical";
  title: string;
  description?: string;
  confidence: number;
  change?: {
    direction: "up" | "down" | "changed";
    current?: number;
    baseline?: number;
    delta?: number;
    delta_percent?: number;
    unit?: string;
  };
  detection: {
    method: "rule" | "statistical" | "model" | "manual";
    definition: string;
    evaluated_at: string;
  };
  evidence: EvidenceRef[];
  access?: AccessPolicy;
}
```

Example:

```json
{
  "signal_id": "sig_01K5802QS2GAP67S6M84R4QYNS",
  "tenant_id": "acme",
  "type": "software.review_bottleneck",
  "subject": {
    "type": "organization.team",
    "id": "org:acme/team/payments"
  },
  "detected_at": "2026-09-16T10:05:00Z",
  "updated_at": "2026-09-16T10:05:00Z",
  "status": "open",
  "severity": "warning",
  "title": "Review wait time increased for the Payments team",
  "description": "The seven-day average increased for five consecutive days.",
  "confidence": 0.93,
  "change": {
    "direction": "up",
    "current": 7.8,
    "baseline": 3.2,
    "delta": 4.6,
    "delta_percent": 143.75,
    "unit": "hour"
  },
  "detection": {
    "method": "rule",
    "definition": "avg(review_wait_time, 7d) > 1.5 * avg(review_wait_time, previous_7d) and sample_count >= 20",
    "evaluated_at": "2026-09-16T10:05:00Z"
  },
  "evidence": [
    {
      "type": "query",
      "ref": "sensus-query://acme/review-wait-time/compare/payments"
    }
  ]
}
```

The Runtime MUST retain the detection definition and evidence used at detection time.
A later recomputation MUST NOT silently rewrite historical reasoning.

### 11.1 Configurable rules

A Runtime MAY ship a default detector, but tenant rules MUST be persisted and
versioned as configuration. The v0.1 reference runtime supports two safe structured
conditions instead of a general-purpose expression language:

```text
relative_change  percentage increase/decrease against the prior sample
threshold        gt/gte/lt/lte against an absolute value
```

Both conditions support `for_samples`, requiring consecutive matches. Changing a
rule MUST re-evaluate existing metric series. Disabling or removing a matching rule
MUST resolve or remove its active derived Signals without altering source
Observations.

## 12. openSensus MCP interface

The MCP server authenticates every Agent request as a Consumer principal. The
principal and tenant are supplied by the MCP server's connection configuration, not
by free-form tool arguments.

All tool results use this common metadata:

```typescript
interface ReadMeta {
  request_id: string;
  tenant_id: string;
  as_of: string;
  watermark: string;
  truncated: boolean;
  next_cursor?: string;
}
```

### 12.1 `observe`

Returns a bounded first view of a scope: current state, important changes, and open
Signals. This is the first tool a scheduled observer Agent SHOULD call.

Input:

```json
{
  "scope": {
    "type": "organization.team",
    "id": "org:acme/team/payments"
  },
  "window": {
    "from": "2026-09-15T10:00:00Z",
    "to": "2026-09-16T10:00:00Z"
  },
  "include": ["state", "changes", "signals"],
  "expand": {
    "direction": "incoming",
    "relations": ["organization.owned_by", "software.belongs_to"],
    "max_depth": 2,
    "max_nodes": 100
  },
  "limit": 50
}
```

Output:

```json
{
  "meta": {
    "request_id": "req_01K580...",
    "tenant_id": "acme",
    "as_of": "2026-09-16T10:06:00Z",
    "watermark": "2026-09-16T10:05:52Z",
    "truncated": false
  },
  "scope": {
    "type": "organization.team",
    "id": "org:acme/team/payments"
  },
  "state": [
    {
      "field": "software.active_work_items",
      "value": 43,
      "observed_at": "2026-09-16T10:00:05Z",
      "evidence": [{ "type": "query", "ref": "sensus-query://acme/active-work/payments" }]
    }
  ],
  "changes": [
    {
      "metric": "software.review_wait_time",
      "current": 7.8,
      "baseline": 3.2,
      "delta_percent": 143.75,
      "unit": "hour",
      "evidence": [{ "type": "query", "ref": "sensus-query://acme/review-wait-time/compare/payments" }]
    }
  ],
  "signals": [
    {
      "signal_id": "sig_01K5802QS2GAP67S6M84R4QYNS",
      "type": "software.review_bottleneck",
      "severity": "warning",
      "title": "Review wait time increased for the Payments team",
      "confidence": 0.93
    }
  ]
}
```

`observe` MUST return bounded structured data. It MUST NOT dump the entire underlying
event stream into the Agent context.

When `expand` is present, the Runtime traverses visible relations from the root and
rolls related state, changes, and Signals into the result. Traversal MUST be
cycle-safe, ACL-filtered, depth-bounded, and node-bounded. A truncated traversal MUST
set `meta.truncated`.

### 12.2 `inspect`

Returns details for one entity or Signal.

```typescript
inspect({
  ref: { kind: "entity" | "signal"; id: string },
  include?: ("state" | "relations" | "metrics" | "evidence")[]
})
```

### 12.3 `timeline`

Returns chronologically ordered events and state changes for a subject.

```typescript
timeline({
  subject: EntityRef,
  window: { from: string; to: string },
  types?: string[],
  limit?: number,
  cursor?: string
})
```

### 12.4 `query`

Filters entities or Signals using structured predicates. v0.1 does not accept an
arbitrary query language.

```typescript
query({
  resource: "entity" | "signal",
  type?: string,
  where?: Array<{
    field: string;
    op: "eq" | "neq" | "in" | "gt" | "gte" | "lt" | "lte" | "exists";
    value?: unknown;
  }>,
  order_by?: { field: string; direction: "asc" | "desc" },
  limit?: number,
  cursor?: string
})
```

### 12.5 `compare`

Compares one metric between two intervals and optionally groups the result.

```typescript
compare({
  metric: string,
  scope: EntityRef,
  current: { from: string; to: string },
  baseline: { from: string; to: string },
  aggregation?: "count" | "sum" | "average" | "min" | "max" | "p50" | "p90" | "p95" | "p99",
  group_by?: string[],
  filters?: Record<string, string>,
  limit?: number
})
```

The result MUST include current, baseline, absolute delta, percentage delta when
defined, sample counts, and Evidence references.

### 12.6 `get_evidence`

Resolves one Evidence reference and returns the smallest authorized representation
needed to verify a claim.

```typescript
get_evidence({
  evidence: EvidenceRef,
  format?: "structured" | "text"
})
```

Source credentials and unrestricted URLs MUST NOT be returned to an Agent. The
Runtime SHOULD prefer immutable snapshots or short-lived authorized links.

## 13. Agent scheduling and attention

The v0.1 reference operating mode uses a scheduled Agent:

```text
Schedule wakes Agent
        -> Agent calls observe
        -> no material change: remain silent
        -> material Signal: inspect and compare
        -> resolve evidence
        -> produce an evidence-backed conclusion
```

The schedule only wakes the Agent. Continuous perception happens in the Runtime,
which receives and projects Observations even while no Agent is running.

Signal-triggered wake-up is an optional extension. A trigger payload SHOULD contain
only the Signal ID, subject, severity, title, and MCP connection reference. The Agent
MUST retrieve current authorized details through MCP rather than trust the trigger
payload as complete context.

## 14. Access and derived data

The Runtime MUST enforce access on source data and derived data.

For a derived metric or Signal:

1. Its evidence is evaluated under the Consumer's identity.
2. Restricted evidence MUST NOT be exposed through titles, descriptions, dimensions,
   counts, or grouping keys.
3. Tenant policy decides whether partially visible aggregations are suppressed,
   coarsened, or recalculated from only visible data.
4. A response SHOULD indicate that data was filtered only when that indication does
   not itself disclose restricted information.

The reference access algorithm applies these rules:

1. Classification must not exceed the Consumer's clearance.
2. Any matching `deny` principal rejects access.
3. When `allow` is present, at least one Consumer principal must match.
4. `inherit_from_source: true` without resolved source `allow` or `deny` metadata is
   fail-closed.
5. A derived Signal is visible only when the Consumer can read every Observation in
   its evidence set.

## 15. Type profiles and extensibility

Core protocol types are deliberately small. Domain semantics are defined by profiles.

Recommended v0.1 profile namespaces:

```text
organization.*
software.*
sales.*
support.*
finance.*
manufacturing.*
```

Custom types SHOULD use a reverse-domain or vendor namespace:

```text
com.acme.risk_assessment
io.vendor.special_event
```

Consumers MUST tolerate unknown types, attributes, event fields, metrics, and
relations. A new optional field does not require a protocol version change. Removing
or changing the meaning of an existing field does.

## 16. Error model

API and MCP errors use the same logical shape:

```json
{
  "error": {
    "code": "INVALID_ARGUMENT",
    "message": "window.from must be before window.to",
    "retryable": false,
    "request_id": "req_01K580...",
    "details": {
      "field": "window.from"
    }
  }
}
```

Standard codes:

```text
INVALID_ARGUMENT
UNAUTHENTICATED
PERMISSION_DENIED
NOT_FOUND
CONFLICT
RATE_LIMITED
TEMPORARILY_UNAVAILABLE
INTERNAL
```

## 17. Minimum conformance

### 17.1 Producer conformance

A conforming Producer MUST:

- create stable, idempotent Observation IDs;
- provide valid occurrence and observation times;
- namespace source identifiers;
- retry retryable failures without changing the payload;
- preserve source access metadata when available.

### 17.2 Runtime conformance

A conforming Runtime MUST:

- validate and durably store accepted Observations;
- implement idempotency and conflict behavior;
- materialize entity, relation, state, timeline, metric, and Signal projections;
- preserve provenance and evidence;
- expose all six v0.1 MCP tools;
- return freshness watermarks;
- enforce Consumer authorization.

### 17.3 MCP server conformance

A conforming MCP server MUST:

- authenticate a Consumer principal;
- keep tenant selection outside arbitrary model-generated arguments;
- expose read-only tools in v0.1;
- bound and paginate results;
- return evidence for derived claims;
- avoid leaking credentials or inaccessible source content.

## 18. Reference end-to-end flow

```text
1. GitHub emits pull_request.review_requested.
2. Connector submits event.occurred.
3. Runtime appends the Observation.
4. Runtime updates the change timeline.
5. A later review.completed Event allows Runtime to derive review wait time.
6. Runtime stores metric.observed with evidence pointing to both Events.
7. Detector compares the recent seven days with the previous seven days.
8. Detector creates a software.review_bottleneck Signal.
9. A scheduled Agent calls observe and sees the Signal.
10. Agent calls inspect, compare, timeline, and get_evidence.
11. Agent reports a bounded conclusion and distinguishes evidence from hypothesis.
```

This flow is the minimum product validation target for openSensus v0.1.

## 19. Decisions intentionally deferred

The following decisions are deferred until the reference implementation produces
evidence that they are necessary:

- canonical cross-source entity merge algorithms;
- a rule DSL for Signal detection;
- natural-language or SQL-like world queries;
- subscriptions and push delivery;
- automatic Agent action execution;
- federated openSensus Runtime discovery;
- full bitemporal correction semantics;
- standardized embedding or vector-search behavior.
