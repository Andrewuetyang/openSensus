import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { observationSchema } from "../src/protocol.js";
import {
  ObservationConflictError,
  SensusStore,
} from "../src/store.js";
import { SqliteStorage } from "../src/storage-sqlite.js";
import { SensusWorld } from "../src/world.js";
import { change, gitlabScenario, team } from "./fixtures.js";

describe("SensusStore", () => {
  it("ingests idempotently and rejects conflicting payloads", () => {
    const store = new SensusStore(":memory:");
    const observation = gitlabScenario()[0]!;

    assert.equal(store.ingest(observation).status, "accepted");
    assert.equal(store.ingest(observation).status, "duplicate");

    const conflict = observationSchema.parse({
      ...observation,
      data: { ...observation.data, name: "Different" },
    });
    assert.throws(() => store.ingest(conflict), ObservationConflictError);
    store.close();
  });

  it("projects entity, relation, and state without letting late state overwrite current state", () => {
    const store = new SensusStore(":memory:");
    for (const observation of gitlabScenario()) store.ingest(observation);

    const projected = store.getEntity("acme", change);
    assert.equal(projected?.name, "Add batch refund support");
    assert.equal(
      store.getStates("acme", change)[0]?.value,
      "waiting",
    );
    assert.equal(store.getRelations("acme", change).length, 1);

    store.ingest(
      observationSchema.parse({
        spec_version: "sensus/0.1",
        observation_id: "obs_late_state",
        tenant_id: "acme",
        kind: "state.observed",
        subject: change,
        occurred_at: "2026-09-14T09:00:00Z",
        observed_at: "2026-09-16T10:00:00Z",
        source: { system: "gitlab", instance: "acme-gitlab" },
        data: {
          field: "software.review_status",
          operation: "set",
          value: "not_requested",
        },
      }),
    );
    assert.equal(store.getStates("acme", change)[0]?.value, "waiting");
    store.close();
  });

  it("detects a significant metric increase and exposes it through observe", async () => {
    const store = SqliteStorage.open(":memory:");
    const results = [];
    for (const observation of gitlabScenario()) {
      results.push(await store.ingest(observation));
    }
    const generated = results.flatMap((result) => result.generated_signals);

    assert.equal(generated.length, 1);
    const world = new SensusWorld(store, "acme");
    const view = await world.observe({
      scope: team,
      include: ["state", "changes", "signals"],
      limit: 50,
    });
    const changes = view.changes as Array<Record<string, unknown>>;
    const signals = view.signals as Array<Record<string, unknown>>;
    assert.equal(changes[0]?.metric, "software.review_wait_time");
    assert.equal(Math.round(Number(changes[0]?.delta_percent)), 144);
    assert.equal(signals[0]?.severity, "warning");

    const signal = await store.getSignal("acme", generated[0]!);
    assert.equal(signal?.status, "open");
    assert.equal(signal?.evidence.length, 2);
    await store.close();
  });

  it("compares metric windows and resolves observation evidence", async () => {
    const store = SqliteStorage.open(":memory:");
    for (const observation of gitlabScenario()) await store.ingest(observation);
    const world = new SensusWorld(store, "acme");

    const comparison = await world.compare({
      metric: "software.review_wait_time",
      scope: team,
      current: {
        from: "2026-09-10T00:00:00Z",
        to: "2026-09-16T23:59:59Z",
      },
      baseline: {
        from: "2026-09-01T00:00:00Z",
        to: "2026-09-09T23:59:59Z",
      },
      aggregation: "average",
      group_by: ["repository"],
      filters: {},
      limit: 50,
    });
    const result = (comparison.results as Array<Record<string, unknown>>)[0]!;
    assert.equal(result.current, 7.8);
    assert.equal(result.baseline, 3.2);

    const evidence = (result.evidence as Array<Record<string, string>>)[0]!;
    const resolved = await world.getEvidence({
      evidence: {
        type: "observation",
        ref: evidence.ref!,
      },
      format: "structured",
    });
    const observation = resolved.observation as Record<string, unknown>;
    assert.equal(observation.kind, "metric.observed");

    const sourceEvidence = await world.getEvidence({
      evidence: {
        type: "source_record",
        ref: "gitlab://acme/payments-api/merge_requests/3812",
      },
      format: "structured",
    });
    const resolution = sourceEvidence.resolution as Record<string, unknown>;
    assert.equal(resolution.status, "external_tool_required");
    assert.equal(resolution.capability, "gitlab.merge_request.read");

    await assert.rejects(() =>
      world.getEvidence({
        evidence: {
          type: "source_record",
          ref: "gitlab://untrusted/invented",
          resolver: { capability: "gitlab.admin" },
        },
        format: "structured",
      }),
    );
    await store.close();
  });
});
