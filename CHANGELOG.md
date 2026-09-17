# Changelog

Notable changes to openSensus. The format follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and the project aims to follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

Nothing yet.

## [0.1.0] — 2026-09-17

First release. The reference implementation of the `sensus/0.1` protocol.

### Added

- `POST /v1/observations` and `POST /v1/observations/batch`, with strict
  `sensus/0.1` validation, idempotency on `observation_id`, and conflict
  detection when an id is reused with a different payload.
- Projections for entities, relations, state, events, and metric series, with
  late-arrival protection for current state, on SQLite (`better-sqlite3`) or
  PostgreSQL (`pg`) behind one storage contract.
- Principal- and classification-aware read filtering, including field-level
  entity ACLs, with an explicit `deny` outranking `allow` and a fail-closed
  default for unresolved `inherit_from_source` policies.
- Correction Observations with deterministic subject projection replay, and
  snapshot/reconciliation syncs whose authoritative deletion is conservative:
  an entity is marked deleted only when no other source still reports it.
- Configurable `threshold` and `relative_change` Signal rules, persisted per
  tenant, re-evaluated against existing metric series on write.
- Bounded, cycle-safe relation graph expansion for organization rollups, capped
  at five levels and 500 nodes.
- Six read-only MCP tools — `observe`, `inspect`, `timeline`, `query`, `compare`,
  `get_evidence` — over stdio and Streamable HTTP. Identity is resolved per
  request on the HTTP transport, with claim-to-principal mapping, a clearance
  ceiling, and a tenant allowlist.
- Evidence resolver hints, so deep inspection can be handed to a vertical MCP
  server via a capability such as `gitlab.merge_request.read`.
- `docs/sensus-protocol-v0.1.md`, the normative wire contract, plus architecture
  and integration guides in English and Chinese.

### Known limits

Recorded in full in [§14 of the architecture guide](docs/architecture.md#14-known-limits).
The ones most likely to matter early: projection runs synchronously in the
ingestion transaction, so throughput is bounded by the write path; the batch
endpoint processes items sequentially; `listEntities` and `latestWatermark` scan
a bounded window before ACL filtering; and there is no push or subscription
delivery, so agents poll.
