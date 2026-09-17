import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { createConsumerContext } from "../src/access.js";
import { observationSchema, type Observation } from "../src/protocol.js";
import { SensusStore } from "../src/store.js";
import { SqliteStorage } from "../src/storage-sqlite.js";
import { SensusWorld } from "../src/world.js";
import {
  change,
  gitlabScenario,
  repository,
  team,
} from "./fixtures.js";

describe("robust runtime behavior", () => {
  it("enforces field-level entity ACLs and hides derived Signals", () => {
    const store = new SensusStore(":memory:");
    for (const observation of gitlabScenario()) store.ingest(observation);
    store.ingest(
      observationSchema.parse({
        spec_version: "sensus/0.1",
        observation_id: "obs_change_secret",
        tenant_id: "acme",
        kind: "entity.observed",
        subject: change,
        occurred_at: "2026-09-16T11:00:00Z",
        observed_at: "2026-09-16T11:00:01Z",
        source: { system: "gitlab", instance: "acme-gitlab" },
        data: { attributes: { security_risk: "critical" } },
        access: {
          classification: "confidential",
          allow: ["team:security"],
        },
      }),
    );

    const ordinary = createConsumerContext({
      principals: ["team:payments"],
      clearance: "confidential",
    });
    const security = createConsumerContext({
      principals: ["team:security"],
      clearance: "confidential",
    });
    const ordinaryView = store.getEntity("acme", change, ordinary)!;
    const securityView = store.getEntity("acme", change, security)!;
    assert.equal(ordinaryView.name, "Add batch refund support");
    assert.equal(
      (ordinaryView.attributes as Record<string, unknown>).security_risk,
      undefined,
    );
    assert.equal(
      (securityView.attributes as Record<string, unknown>).security_risk,
      "critical",
    );

    const restrictedMetrics = gitlabScenario()
      .filter((observation) => observation.kind === "metric.observed")
      .map((observation, index) =>
        observationSchema.parse({
          ...observation,
          observation_id: `obs_restricted_metric_${index}`,
          subject: repository,
          access: {
            classification: "confidential",
            allow: ["team:security"],
          },
        }),
      );
    for (const observation of restrictedMetrics) store.ingest(observation);
    assert.equal(
      store.getSignals("acme", repository, "open", ordinary).length,
      0,
    );
    assert.equal(
      store.getSignals("acme", repository, "open", security).length,
      1,
    );
    store.close();
  });

  it("invalidates a bad Observation and deterministically rebuilds state", () => {
    const store = new SensusStore(":memory:");
    const earlier = observationSchema.parse({
      spec_version: "sensus/0.1",
      observation_id: "obs_state_earlier",
      tenant_id: "acme",
      kind: "state.observed",
      subject: change,
      occurred_at: "2026-09-15T08:00:00Z",
      observed_at: "2026-09-15T08:00:01Z",
      source: { system: "gitlab", instance: "acme-gitlab" },
      data: {
        field: "software.review_status",
        operation: "set",
        value: "not_requested",
      },
    });
    const bad = gitlabScenario().find(
      (observation) => observation.observation_id === "obs_review_state",
    )!;
    store.ingest(earlier);
    store.ingest(bad);
    assert.equal(store.getStates("acme", change)[0]?.value, "waiting");

    const correction = observationSchema.parse({
      spec_version: "sensus/0.1",
      observation_id: "obs_correct_review_state",
      tenant_id: "acme",
      kind: "event.occurred",
      subject: change,
      occurred_at: "2026-09-16T12:00:00Z",
      observed_at: "2026-09-16T12:00:01Z",
      source: { system: "gitlab", instance: "acme-gitlab" },
      data: {
        type: "sensus.observation_corrected",
        attributes: {
          target_observation_id: "obs_review_state",
          disposition: "invalid",
        },
      },
    });
    store.ingest(correction);

    assert.equal(store.getStates("acme", change)[0]?.value, "not_requested");
    assert.equal(
      store.getObservationForConsumer(
        "acme",
        "obs_review_state",
        createConsumerContext({
          principals: ["role:agent"],
          clearance: "restricted",
        }),
      ),
      undefined,
    );
    assert.equal(store.getObservation("acme", "obs_review_state")?.kind, "state.observed");
    store.close();
  });

  it("performs authoritative reconciliation without deleting entities still present elsewhere", () => {
    const store = new SensusStore(":memory:");
    const entity = (id: string, sourceSystem = "gitlab"): Observation =>
      observationSchema.parse({
        spec_version: "sensus/0.1",
        observation_id: `obs_${sourceSystem}_${id}`,
        tenant_id: "acme",
        kind: "entity.observed",
        subject: { type: "software.repository", id: `gitlab:acme/${id}` },
        occurred_at: "2026-09-01T00:00:00Z",
        observed_at: "2026-09-01T00:00:01Z",
        source: { system: sourceSystem, instance: `acme-${sourceSystem}` },
        data: { name: id, lifecycle: "active" },
      });
    const repoA = entity("repo-a");
    const repoB = entity("repo-b");

    store.startSync({
      tenant_id: "acme",
      sync_id: "sync_initial",
      mode: "snapshot",
      source: { system: "gitlab", instance: "acme-gitlab" },
      authoritative_deletion: true,
    });
    store.ingest(repoA, { syncId: "sync_initial" });
    store.ingest(repoB, { syncId: "sync_initial" });
    store.completeSync("acme", "sync_initial", { record_count: 2 });

    store.startSync({
      tenant_id: "acme",
      sync_id: "sync_second",
      mode: "reconciliation",
      source: { system: "gitlab", instance: "acme-gitlab" },
      authoritative_deletion: true,
    });
    store.ingest(repoA, { syncId: "sync_second" });
    const completed = store.completeSync("acme", "sync_second", {
      record_count: 1,
    });
    assert.equal(completed.deleted_entity_count, 1);
    assert.equal(
      store.getEntity("acme", {
        type: "software.repository",
        id: "gitlab:acme/repo-b",
      })?.lifecycle,
      "deleted",
    );

    store.startSync({
      tenant_id: "acme",
      sync_id: "sync_return",
      mode: "reconciliation",
      source: { system: "gitlab", instance: "acme-gitlab" },
      authoritative_deletion: true,
    });
    store.ingest(repoA, { syncId: "sync_return" });
    store.ingest(repoB, { syncId: "sync_return" });
    store.completeSync("acme", "sync_return", { record_count: 2 });
    assert.equal(
      store.getEntity("acme", {
        type: "software.repository",
        id: "gitlab:acme/repo-b",
      })?.lifecycle,
      "active",
    );

    const shared = entity("repo-shared");
    const catalogCopy = entity("repo-shared", "service-catalog");
    store.ingest(shared);
    store.ingest(catalogCopy);
    store.startSync({
      tenant_id: "acme",
      sync_id: "sync_third",
      mode: "reconciliation",
      source: { system: "gitlab", instance: "acme-gitlab" },
      authoritative_deletion: true,
    });
    store.completeSync("acme", "sync_third", { record_count: 0 });
    assert.equal(
      store.getEntity("acme", {
        type: "software.repository",
        id: "gitlab:acme/repo-shared",
      })?.lifecycle,
      "active",
    );
    store.close();
  });

  it("supports configurable Signal rules and re-evaluates existing metrics", () => {
    const store = new SensusStore(":memory:");
    for (const observation of gitlabScenario()) store.ingest(observation);

    store.upsertSignalRule("acme", {
      rule_id: "review_wait_threshold",
      name: "Review wait exceeded seven hours",
      enabled: true,
      applies_to: {
        metric: "software.review_wait_time",
        subject_types: ["organization.team"],
        dimensions: { repository: "payments-api" },
      },
      condition: {
        kind: "threshold",
        operator: "gt",
        value: 7,
        for_samples: 1,
      },
      signal_type: "software.review_wait_slo_breach",
      severity: "critical",
      confidence: 1,
    });
    let signals = store.getSignals("acme", team, "open");
    assert.equal(signals.length, 1);
    assert.equal(signals[0]?.type, "software.review_wait_slo_breach");
    assert.equal(signals[0]?.severity, "critical");

    store.upsertSignalRule("acme", {
      ...store.listSignalRules("acme")[0],
      enabled: false,
    });
    signals = store.getSignals("acme", team, "open");
    assert.equal(signals.length, 0);
    store.close();
  });

  it("rolls observe up through a bounded relation graph", async () => {
    const store = SqliteStorage.open(":memory:");
    for (const observation of gitlabScenario()) await store.ingest(observation);
    const world = new SensusWorld(store, "acme");
    const view = await world.observe({
      scope: team,
      include: ["state", "changes", "signals"],
      limit: 100,
      expand: {
        direction: "incoming",
        max_depth: 2,
        max_nodes: 20,
      },
    });
    const graph = view.graph as {
      nodes: Array<{ type: string; id: string }>;
      edges: unknown[];
    };
    assert.deepEqual(
      graph.nodes.map((node) => node.id).sort(),
      [team.id, repository.id, change.id].sort(),
    );
    assert.equal(graph.edges.length, 2);
    const states = view.state as Array<{ subject: { id: string }; field: string }>;
    assert.ok(
      states.some(
        (state) =>
          state.subject.id === change.id &&
          state.field === "software.review_status",
      ),
    );
    await store.close();
  });
});
