import assert from "node:assert/strict";
import { afterEach, beforeEach, describe, it } from "node:test";
import { createConsumerContext, type ConsumerContext } from "../src/access.js";
import {
  CorrectionError,
  ObservationConflictError,
  SyncError,
} from "../src/store.js";
import { observationSchema, type Observation } from "../src/protocol.js";
import type { SensusStorage } from "../src/storage.js";
import { change, repository, team } from "./fixtures.js";

export interface StorageBackend {
  storage: SensusStorage;
  /** Releases whatever the backend allocated for this test. */
  dispose(): Promise<void>;
}

export interface ConformanceTarget {
  name: string;
  create(): Promise<StorageBackend>;
  /**
   * Set when `create()` returns a backend whose calls actually overlap — a
   * connection pool, typically.
   *
   * SQLite over a single connection cannot overlap anything: `better-sqlite3`
   * is synchronous, so concurrent callers serialize by construction and a
   * concurrency test would pass without testing anything. Its guarantee comes
   * from being a single writer instead, which is stronger.
   */
  concurrent?: boolean;
}

const tenant = "acme";

let sequence = 0;

/**
 * Builds a valid Observation, filling the boilerplate. `id` defaults to a
 * per-call unique value so tests that do not care about idempotency stay short.
 */
function obs(input: {
  id?: string;
  kind: Observation["kind"];
  subject: Observation["subject"];
  occurred_at: string;
  data: unknown;
  source?: { system: string; instance: string; sequence?: number };
  access?: unknown;
  evidence?: unknown[];
  observed_at?: string;
}): Observation {
  sequence += 1;
  return observationSchema.parse({
    spec_version: "sensus/0.1",
    observation_id: input.id ?? `obs_c${sequence}`,
    tenant_id: tenant,
    kind: input.kind,
    subject: input.subject,
    occurred_at: input.occurred_at,
    observed_at: input.observed_at ?? input.occurred_at,
    source: input.source ?? { system: "gitlab", instance: "acme-gitlab" },
    data: input.data,
    ...(input.access === undefined ? {} : { access: input.access }),
    ...(input.evidence === undefined ? {} : { evidence: input.evidence }),
  });
}

/**
 * Carries every principal the suite ever grants, so it can read anything a test
 * scopes to a team. Clearance alone is not enough: an `allow` list is satisfied
 * by principal membership, not by rank.
 */
const fullAccess: ConsumerContext = createConsumerContext({
  principals: ["role:agent", "team:security", "team:payments"],
  clearance: "restricted",
});

/**
 * Registers the behaviour every {@link SensusStorage} implementation must have.
 *
 * These are not smoke tests. Each one pins an invariant whose silent failure
 * would corrupt derived state: the three-clock comparison, per-field clocks,
 * deterministic rebuild, the reconciliation presence ledger, and ACL filtering
 * of both source and derived data.
 */
export function runStorageConformance(target: ConformanceTarget): void {
  describe(`storage conformance — ${target.name}`, () => {
    let backend: StorageBackend;
    let storage: SensusStorage;

    beforeEach(async () => {
      backend = await target.create();
      storage = backend.storage;
    });

    afterEach(async () => {
      await backend.dispose();
    });

    // ------------------------------------------------------------ ingestion

    it("is idempotent for an identical payload and conflicts on a different one", async () => {
      const first = obs({
        id: "obs_idem",
        kind: "entity.observed",
        subject: change,
        occurred_at: "2026-09-15T09:00:00Z",
        data: { name: "Add batch refund support", lifecycle: "active" },
      });

      assert.equal((await storage.ingest(first)).status, "accepted");
      assert.equal((await storage.ingest(first)).status, "duplicate");

      const conflicting = obs({
        id: "obs_idem",
        kind: "entity.observed",
        subject: change,
        occurred_at: "2026-09-15T09:00:00Z",
        data: { name: "Something else", lifecycle: "active" },
      });
      await assert.rejects(
        () => storage.ingest(conflicting),
        ObservationConflictError,
      );
    });

    it("treats a key-reordered payload as the same fact", async () => {
      const ordered = {
        spec_version: "sensus/0.1",
        observation_id: "obs_keyorder",
        tenant_id: tenant,
        kind: "entity.observed",
        subject: { type: "software.change", id: "gitlab:acme/payments-api!9" },
        occurred_at: "2026-09-15T09:00:00Z",
        observed_at: "2026-09-15T09:00:00Z",
        source: { system: "gitlab", instance: "acme-gitlab" },
        data: { name: "Same fact" },
      };
      const reordered = {
        data: { name: "Same fact" },
        source: { instance: "acme-gitlab", system: "gitlab" },
        observed_at: "2026-09-15T09:00:00Z",
        occurred_at: "2026-09-15T09:00:00Z",
        subject: { id: "gitlab:acme/payments-api!9", type: "software.change" },
        kind: "entity.observed",
        tenant_id: tenant,
        observation_id: "obs_keyorder",
        spec_version: "sensus/0.1",
      };

      assert.equal(
        (await storage.ingest(observationSchema.parse(ordered))).status,
        "accepted",
      );
      assert.equal(
        (await storage.ingest(observationSchema.parse(reordered))).status,
        "duplicate",
      );
    });

    // ------------------------------------------------ entity per-field clocks

    it("merges entity observations per field instead of replacing the whole entity", async () => {
      await storage.ingest(
        obs({
          id: "obs_e1",
          kind: "entity.observed",
          subject: change,
          occurred_at: "2026-09-15T09:00:00Z",
          data: { name: "First name", attributes: { size: 23 } },
        }),
      );
      // A later observation touching only one field must not erase the others.
      await storage.ingest(
        obs({
          id: "obs_e2",
          kind: "entity.observed",
          subject: change,
          occurred_at: "2026-09-15T10:00:00Z",
          data: { lifecycle: "active" },
        }),
      );

      const entity = await storage.getEntity(tenant, change, fullAccess);
      assert.equal(entity?.name, "First name");
      assert.equal(entity?.lifecycle, "active");
      assert.deepEqual(entity?.attributes, { size: 23 });
    });

    it("keeps the newer value when an older entity observation arrives late", async () => {
      await storage.ingest(
        obs({
          id: "obs_e_new",
          kind: "entity.observed",
          subject: change,
          occurred_at: "2026-09-15T10:00:00Z",
          data: { name: "New name" },
        }),
      );
      await storage.ingest(
        obs({
          id: "obs_e_old",
          kind: "entity.observed",
          subject: change,
          occurred_at: "2026-09-15T09:00:00Z",
          data: { name: "Old name" },
        }),
      );

      const entity = await storage.getEntity(tenant, change, fullAccess);
      assert.equal(entity?.name, "New name");
    });

    it("clears an attribute on an explicit null and keeps it unknown when omitted", async () => {
      await storage.ingest(
        obs({
          id: "obs_null_1",
          kind: "entity.observed",
          subject: change,
          occurred_at: "2026-09-15T09:00:00Z",
          data: { attributes: { risk: "high", owner: "payments" } },
        }),
      );
      await storage.ingest(
        obs({
          id: "obs_null_2",
          kind: "entity.observed",
          subject: change,
          occurred_at: "2026-09-15T10:00:00Z",
          data: { attributes: { risk: null } },
        }),
      );

      const entity = await storage.getEntity(tenant, change, fullAccess);
      assert.deepEqual(entity?.attributes, { owner: "payments" });
    });

    // --------------------------------------------------- state late arrivals

    it("never lets a late state observation overwrite newer state", async () => {
      await storage.ingest(
        obs({
          id: "obs_s1",
          kind: "state.observed",
          subject: change,
          occurred_at: "2026-09-15T09:05:00Z",
          data: { field: "software.review_status", operation: "set", value: "waiting" },
        }),
      );
      await storage.ingest(
        obs({
          id: "obs_s2",
          kind: "state.observed",
          subject: change,
          occurred_at: "2026-09-14T09:00:00Z",
          observed_at: "2026-09-16T10:00:00Z",
          data: {
            field: "software.review_status",
            operation: "set",
            value: "not_requested",
          },
        }),
      );

      const states = await storage.getStates(tenant, change, fullAccess);
      assert.equal(states[0]?.value, "waiting");
    });

    it("breaks a state clock tie by source sequence, then by receipt", async () => {
      const field = "software.review_status";
      await storage.ingest(
        obs({
          id: "obs_tie_a",
          kind: "state.observed",
          subject: change,
          occurred_at: "2026-09-15T09:05:00Z",
          source: { system: "gitlab", instance: "acme-gitlab", sequence: 10 },
          data: { field, operation: "set", value: "from-seq-10" },
        }),
      );
      await storage.ingest(
        obs({
          id: "obs_tie_b",
          kind: "state.observed",
          subject: change,
          occurred_at: "2026-09-15T09:05:00Z",
          source: { system: "gitlab", instance: "acme-gitlab", sequence: 11 },
          data: { field, operation: "set", value: "from-seq-11" },
        }),
      );

      const states = await storage.getStates(tenant, change, fullAccess);
      assert.equal(states[0]?.value, "from-seq-11");
    });

    it("reports an unset field as unset rather than dropping the row", async () => {
      const field = "software.review_status";
      await storage.ingest(
        obs({
          id: "obs_unset_1",
          kind: "state.observed",
          subject: change,
          occurred_at: "2026-09-15T09:05:00Z",
          data: { field, operation: "set", value: "waiting" },
        }),
      );
      await storage.ingest(
        obs({
          id: "obs_unset_2",
          kind: "state.observed",
          subject: change,
          occurred_at: "2026-09-15T10:05:00Z",
          data: { field, operation: "unset" },
        }),
      );

      const states = await storage.getStates(tenant, change, fullAccess);
      assert.equal(states.length, 1);
      assert.equal(states[0]?.is_unset, true);
      assert.equal(states[0]?.value, null);
    });

    // --------------------------------------------------------- metric series

    it("keeps metric samples as an append-only series", async () => {
      for (const [index, value] of [3.2, 7.8].entries()) {
        await storage.ingest(
          obs({
            id: `obs_m${index}`,
            kind: "metric.observed",
            subject: team,
            occurred_at: index === 0 ? "2026-09-09T00:00:00Z" : "2026-09-16T00:00:00Z",
            source: { system: "sensus-runtime", instance: tenant },
            data: {
              metric: "software.review_wait_time",
              value,
              unit: "hour",
              dimensions: { repository: "payments-api" },
              aggregation: "average",
            },
          }),
        );
      }

      const rows = await storage.metricsInWindow(
        tenant,
        team,
        "software.review_wait_time",
        "2026-09-01T00:00:00Z",
        "2026-09-30T00:00:00Z",
        {},
        fullAccess,
      );
      assert.equal(rows.length, 2);
      assert.deepEqual(rows.map((row) => row.value), [3.2, 7.8]);
    });

    it("filters metricsInWindow by dimensions and by readability", async () => {
      await storage.ingest(
        obs({
          id: "obs_dim_a",
          kind: "metric.observed",
          subject: team,
          occurred_at: "2026-09-09T00:00:00Z",
          source: { system: "sensus-runtime", instance: tenant },
          data: {
            metric: "software.review_wait_time",
            value: 1,
            unit: "hour",
            dimensions: { repository: "payments-api" },
          },
        }),
      );
      await storage.ingest(
        obs({
          id: "obs_dim_b",
          kind: "metric.observed",
          subject: team,
          occurred_at: "2026-09-09T00:00:00Z",
          source: { system: "sensus-runtime", instance: tenant },
          data: {
            metric: "software.review_wait_time",
            value: 2,
            unit: "hour",
            dimensions: { repository: "ledger-api" },
          },
        }),
      );

      const filtered = await storage.metricsInWindow(
        tenant,
        team,
        "software.review_wait_time",
        "2026-09-01T00:00:00Z",
        "2026-09-30T00:00:00Z",
        { repository: "payments-api" },
        fullAccess,
      );
      assert.deepEqual(filtered.map((row) => row.value), [1]);
    });

    // --------------------------------------------------------------- signals

    it("detects a threshold breach and resolves it when the series recovers", async () => {
      await storage.upsertSignalRule(tenant, {
        rule_id: "review_wait_threshold",
        name: "Review wait exceeded seven hours",
        enabled: true,
        applies_to: { metric: "software.review_wait_time", dimensions: {} },
        condition: { kind: "threshold", operator: "gt", value: 7, for_samples: 1 },
        signal_type: "software.review_wait_slo_breach",
        severity: "critical",
        confidence: 1,
      });

      const sample = (id: string, at: string, value: number): Observation =>
        obs({
          id,
          kind: "metric.observed",
          subject: team,
          occurred_at: at,
          source: { system: "sensus-runtime", instance: tenant },
          data: { metric: "software.review_wait_time", value, unit: "hour" },
        });

      await storage.ingest(sample("obs_sig_1", "2026-09-09T00:00:00Z", 8.5));
      let open = await storage.getSignals(tenant, team, "open", fullAccess);
      assert.equal(open.length, 1);
      assert.equal(open[0]?.type, "software.review_wait_slo_breach");
      assert.equal(open[0]?.severity, "critical");
      const firstDetectedAt = open[0]?.detected_at;

      await storage.ingest(sample("obs_sig_2", "2026-09-16T00:00:00Z", 2.0));
      open = await storage.getSignals(tenant, team, "open", fullAccess);
      assert.equal(open.length, 0);
      const resolved = await storage.getSignals(tenant, team, "resolved", fullAccess);
      assert.equal(resolved.length, 1, "a resolved Signal is retained, not deleted");

      await storage.ingest(sample("obs_sig_3", "2026-09-23T00:00:00Z", 9.5));
      open = await storage.getSignals(tenant, team, "open", fullAccess);
      assert.equal(open.length, 1);
      assert.equal(
        open[0]?.detected_at,
        firstDetectedAt,
        "re-detection preserves the original detection time",
      );
    });

    it("requires consecutive samples for a relative-change rule", async () => {
      await storage.upsertSignalRule(tenant, {
        rule_id: "spike",
        name: "Two consecutive increases",
        enabled: true,
        applies_to: { metric: "software.review_wait_time", dimensions: {} },
        condition: {
          kind: "relative_change",
          direction: "increase",
          threshold_percent: 50,
          for_samples: 2,
        },
        signal_type: "metric.spike",
        severity: "warning",
        confidence: 0.9,
      });

      const sample = (id: string, at: string, value: number): Observation =>
        obs({
          id,
          kind: "metric.observed",
          subject: team,
          occurred_at: at,
          source: { system: "sensus-runtime", instance: tenant },
          data: { metric: "software.review_wait_time", value, unit: "hour" },
        });

      // Two changes already qualify. A third point that breaks the run resolves.
      await storage.ingest(sample("obs_rc_1", "2026-09-01T00:00:00Z", 1));
      await storage.ingest(sample("obs_rc_2", "2026-09-02T00:00:00Z", 2));
      assert.equal(
        (await storage.getSignals(tenant, team, "open", fullAccess)).length,
        0,
        "one change is not enough for for_samples: 2",
      );

      await storage.ingest(sample("obs_rc_3", "2026-09-03T00:00:00Z", 4));
      assert.equal(
        (await storage.getSignals(tenant, team, "open", fullAccess)).length,
        1,
      );

      await storage.ingest(sample("obs_rc_4", "2026-09-04T00:00:00Z", 4));
      assert.equal(
        (await storage.getSignals(tenant, team, "open", fullAccess)).length,
        0,
        "a non-matching sample resolves the Signal",
      );
    });

    it("re-evaluates existing series when a rule is added, changed or removed", async () => {
      await storage.ingest(
        obs({
          id: "obs_reeval",
          kind: "metric.observed",
          subject: team,
          occurred_at: "2026-09-09T00:00:00Z",
          source: { system: "sensus-runtime", instance: tenant },
          data: { metric: "software.review_wait_time", value: 12, unit: "hour" },
        }),
      );
      assert.equal(
        (await storage.getSignals(tenant, team, "open", fullAccess)).length,
        0,
      );

      const rule = {
        rule_id: "added_later",
        name: "Added after the fact",
        enabled: true,
        applies_to: { metric: "software.review_wait_time", dimensions: {} },
        condition: { kind: "threshold", operator: "gt", value: 7, for_samples: 1 },
        signal_type: "metric.late_rule",
        severity: "warning",
        confidence: 0.9,
      };
      await storage.upsertSignalRule(tenant, rule);
      assert.equal(
        (await storage.getSignals(tenant, team, "open", fullAccess)).length,
        1,
        "adding a rule re-evaluates samples already stored",
      );

      await storage.upsertSignalRule(tenant, {
        ...rule,
        condition: { kind: "threshold", operator: "gt", value: 100, for_samples: 1 },
      });
      assert.equal(
        (await storage.getSignals(tenant, team, "open", fullAccess)).length,
        0,
        "editing a rule re-evaluates and resolves what no longer matches",
      );

      await storage.deleteSignalRule(tenant, "added_later");
      assert.equal(
        (await storage.getSignals(tenant, team, "open", fullAccess)).length,
        0,
      );
      assert.equal(
        (await storage.listSignalRules(tenant, false)).length,
        0,
        "the rule is gone",
      );
    });

    // ----------------------------------------------------------- corrections

    it("invalidates a corrected Observation and rebuilds the projection from the log", async () => {
      await storage.ingest(
        obs({
          id: "obs_corr_earlier",
          kind: "state.observed",
          subject: change,
          occurred_at: "2026-09-15T08:00:00Z",
          data: {
            field: "software.review_status",
            operation: "set",
            value: "not_requested",
          },
        }),
      );
      const bad = obs({
        id: "obs_corr_bad",
        kind: "state.observed",
        subject: change,
        occurred_at: "2026-09-15T09:05:00Z",
        data: { field: "software.review_status", operation: "set", value: "waiting" },
      });
      await storage.ingest(bad);
      assert.equal(
        (await storage.getStates(tenant, change, fullAccess))[0]?.value,
        "waiting",
      );

      await storage.ingest(
        obs({
          id: "obs_corr_fix",
          kind: "event.occurred",
          subject: change,
          occurred_at: "2026-09-16T12:00:00Z",
          data: {
            type: "sensus.observation_corrected",
            attributes: {
              target_observation_id: "obs_corr_bad",
              disposition: "invalid",
            },
          },
        }),
      );

      assert.equal(
        (await storage.getStates(tenant, change, fullAccess))[0]?.value,
        "not_requested",
        "the projection falls back to the remaining valid log",
      );
      assert.equal(
        await storage.getObservationForConsumer(tenant, "obs_corr_bad", fullAccess),
        undefined,
        "an invalidated Observation is not readable",
      );
      assert.equal(
        (await storage.getObservation(tenant, "obs_corr_bad"))?.kind,
        "state.observed",
        "the original is retained for audit",
      );
    });

    it("rejects malformed corrections", async () => {
      await storage.ingest(
        obs({
          id: "obs_corr_target",
          kind: "state.observed",
          subject: change,
          occurred_at: "2026-09-15T09:00:00Z",
          data: { field: "f", operation: "set", value: 1 },
        }),
      );

      const correction = (id: string, attributes: Record<string, unknown>) =>
        obs({
          id,
          kind: "event.occurred",
          subject: change,
          occurred_at: "2026-09-16T12:00:00Z",
          data: { type: "sensus.observation_corrected", attributes },
        });

      await assert.rejects(
        () =>
          storage.ingest(
            correction("obs_c_bad_disp", {
              target_observation_id: "obs_corr_target",
              disposition: "something-else",
            }),
          ),
        CorrectionError,
      );
      await assert.rejects(
        () =>
          storage.ingest(
            correction("obs_c_self", {
              target_observation_id: "obs_c_self",
              disposition: "invalid",
            }),
          ),
        CorrectionError,
      );
      await assert.rejects(
        () =>
          storage.ingest(
            correction("obs_c_missing", {
              target_observation_id: "obs_does_not_exist",
              disposition: "invalid",
            }),
          ),
        CorrectionError,
      );

      // A successful correction blocks a second one against the same target.
      await storage.ingest(
        correction("obs_c_first", {
          target_observation_id: "obs_corr_target",
          disposition: "invalid",
        }),
      );
      await assert.rejects(
        () =>
          storage.ingest(
            correction("obs_c_second", {
              target_observation_id: "obs_corr_target",
              disposition: "invalid",
            }),
          ),
        CorrectionError,
      );
    });

    // -------------------------------------------------------- reconciliation

    it("deletes entities missing from an authoritative snapshot", async () => {
      const entity = (id: string, sourceSystem = "gitlab") =>
        obs({
          id: `obs_${sourceSystem}_${id}`,
          kind: "entity.observed",
          subject: { type: "software.repository", id: `gitlab:acme/${id}` },
          occurred_at: "2026-09-01T00:00:00Z",
          source: { system: sourceSystem, instance: `acme-${sourceSystem}` },
          data: { name: id, lifecycle: "active" },
        });

      await storage.startSync({
        tenant_id: tenant,
        sync_id: "sync_1",
        mode: "snapshot",
        source: { system: "gitlab", instance: "acme-gitlab" },
        authoritative_deletion: true,
      });
      await storage.ingest(entity("repo-a"), { syncId: "sync_1" });
      await storage.ingest(entity("repo-b"), { syncId: "sync_1" });
      await storage.completeSync(tenant, "sync_1", { record_count: 2 });

      await storage.startSync({
        tenant_id: tenant,
        sync_id: "sync_2",
        mode: "reconciliation",
        source: { system: "gitlab", instance: "acme-gitlab" },
        authoritative_deletion: true,
      });
      await storage.ingest(entity("repo-a"), { syncId: "sync_2" });
      const completed = await storage.completeSync(tenant, "sync_2", {
        record_count: 1,
      });

      assert.equal(completed.deleted_entity_count, 1);
      const deleted = await storage.getEntity(
        tenant,
        { type: "software.repository", id: "gitlab:acme/repo-b" },
        fullAccess,
      );
      assert.equal(deleted?.lifecycle, "deleted");

      const survived = await storage.getEntity(
        tenant,
        { type: "software.repository", id: "gitlab:acme/repo-a" },
        fullAccess,
      );
      assert.equal(survived?.lifecycle, "active");
    });

    it("refuses to delete an entity another source still reports", async () => {
      const entity = (id: string, sourceSystem: string) =>
        obs({
          id: `obs_${sourceSystem}_${id}`,
          kind: "entity.observed",
          subject: { type: "software.repository", id: `gitlab:acme/${id}` },
          occurred_at: "2026-09-01T00:00:00Z",
          source: { system: sourceSystem, instance: `acme-${sourceSystem}` },
          data: { name: id, lifecycle: "active" },
        });

      await storage.ingest(entity("shared", "gitlab"));
      await storage.ingest(entity("shared", "service-catalog"));

      await storage.startSync({
        tenant_id: tenant,
        sync_id: "sync_shared",
        mode: "reconciliation",
        source: { system: "gitlab", instance: "acme-gitlab" },
        authoritative_deletion: true,
      });
      const completed = await storage.completeSync(tenant, "sync_shared", {
        record_count: 0,
      });

      assert.equal(completed.deleted_entity_count, 0);
      const still = await storage.getEntity(
        tenant,
        { type: "software.repository", id: "gitlab:acme/shared" },
        fullAccess,
      );
      assert.equal(
        still?.lifecycle,
        "active",
        "one source may not delete what another still reports",
      );
    });

    it("restores an entity when it reappears in a later snapshot", async () => {
      const repo = obs({
        id: "obs_reappear",
        kind: "entity.observed",
        subject: { type: "software.repository", id: "gitlab:acme/reappear" },
        occurred_at: "2026-09-01T00:00:00Z",
        data: { name: "reappear", lifecycle: "active" },
      });
      const target = { type: "software.repository", id: "gitlab:acme/reappear" };

      await storage.ingest(repo);
      await storage.startSync({
        tenant_id: tenant,
        sync_id: "sync_gone",
        mode: "reconciliation",
        source: { system: "gitlab", instance: "acme-gitlab" },
        authoritative_deletion: true,
      });
      await storage.completeSync(tenant, "sync_gone", { record_count: 0 });
      assert.equal((await storage.getEntity(tenant, target, fullAccess))?.lifecycle, "deleted");

      await storage.startSync({
        tenant_id: tenant,
        sync_id: "sync_back",
        mode: "reconciliation",
        source: { system: "gitlab", instance: "acme-gitlab" },
        authoritative_deletion: true,
      });
      await storage.ingest(repo, { syncId: "sync_back" });
      await storage.completeSync(tenant, "sync_back", { record_count: 1 });

      assert.equal(
        (await storage.getEntity(tenant, target, fullAccess))?.lifecycle,
        "active",
      );
    });

    it("rejects a sync whose record count disagrees with its members", async () => {
      await storage.startSync({
        tenant_id: tenant,
        sync_id: "sync_count",
        mode: "snapshot",
        source: { system: "gitlab", instance: "acme-gitlab" },
        authoritative_deletion: true,
      });
      await storage.ingest(
        obs({
          id: "obs_count_1",
          kind: "entity.observed",
          subject: repository,
          occurred_at: "2026-09-01T00:00:00Z",
          data: { name: "payments-api", lifecycle: "active" },
        }),
        { syncId: "sync_count" },
      );

      await assert.rejects(
        () => storage.completeSync(tenant, "sync_count", { record_count: 2 }),
        SyncError,
      );
    });

    it("rejects an observation whose source does not match the open sync", async () => {
      await storage.startSync({
        tenant_id: tenant,
        sync_id: "sync_src",
        mode: "snapshot",
        source: { system: "gitlab", instance: "acme-gitlab" },
        authoritative_deletion: true,
      });

      await assert.rejects(
        () =>
          storage.ingest(
            obs({
              id: "obs_src_mismatch",
              kind: "entity.observed",
              subject: repository,
              occurred_at: "2026-09-01T00:00:00Z",
              source: { system: "jira", instance: "acme-jira" },
              data: { name: "x" },
            }),
            { syncId: "sync_src" },
          ),
        SyncError,
      );
    });

    it("refuses authoritative deletion on an incremental sync", async () => {
      await assert.rejects(
        () =>
          storage.startSync({
            tenant_id: tenant,
            sync_id: "sync_inc",
            mode: "incremental",
            source: { system: "gitlab", instance: "acme-gitlab" },
            authoritative_deletion: true,
          }),
        SyncError,
      );
    });

    // ------------------------------------------------------------------ ACLs

    it("hides a restricted field but keeps the rest of the entity visible", async () => {
      await storage.ingest(
        obs({
          id: "obs_acl_public",
          kind: "entity.observed",
          subject: change,
          occurred_at: "2026-09-15T09:00:00Z",
          data: { name: "Add batch refund support" },
        }),
      );
      await storage.ingest(
        obs({
          id: "obs_acl_secret",
          kind: "entity.observed",
          subject: change,
          occurred_at: "2026-09-15T10:00:00Z",
          data: { attributes: { security_risk: "critical" } },
          access: { classification: "confidential", allow: ["team:security"] },
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

      const ordinaryView = await storage.getEntity(tenant, change, ordinary);
      assert.equal(ordinaryView?.name, "Add batch refund support");
      assert.equal(
        (ordinaryView?.attributes as Record<string, unknown>)?.security_risk,
        undefined,
      );

      const securityView = await storage.getEntity(tenant, change, security);
      assert.equal(
        (securityView?.attributes as Record<string, unknown>)?.security_risk,
        "critical",
      );
    });

    it("hides a Signal whose evidence the consumer cannot read", async () => {
      const observation = obs({
        id: "obs_acl_metric",
        kind: "metric.observed",
        subject: team,
        occurred_at: "2026-09-09T00:00:00Z",
        source: { system: "sensus-runtime", instance: tenant },
        data: {
          metric: "software.review_wait_time",
          value: 99,
          unit: "hour",
          dimensions: { repository: "payments-api" },
        },
      });
      await storage.ingest(observation);
      await storage.ingest(
        obs({
          id: "obs_acl_metric_restricted",
          kind: "metric.observed",
          subject: team,
          occurred_at: "2026-09-16T00:00:00Z",
          source: { system: "sensus-runtime", instance: tenant },
          data: {
            metric: "software.review_wait_time",
            value: 190,
            unit: "hour",
            dimensions: { repository: "payments-api" },
          },
          access: { classification: "confidential", allow: ["team:security"] },
        }),
      );

      const ordinary = createConsumerContext({
        principals: ["team:payments"],
        clearance: "confidential",
      });
      assert.equal(
        (await storage.getSignals(tenant, team, "open", ordinary)).length,
        0,
        "a Signal is visible only when all of its evidence is",
      );
    });

    it("computes the watermark over readable observations only", async () => {
      assert.equal(
        await storage.latestWatermark(tenant, fullAccess),
        new Date(0).toISOString(),
      );

      await storage.ingest(
        obs({
          id: "obs_wm_restricted",
          kind: "entity.observed",
          subject: repository,
          occurred_at: "2026-09-01T00:00:00Z",
          data: { name: "payments-api" },
          access: { classification: "restricted", allow: ["team:security"] },
        }),
      );

      const lowClearance = createConsumerContext({
        principals: ["team:payments"],
        clearance: "internal",
      });
      assert.equal(
        await storage.latestWatermark(tenant, lowClearance),
        new Date(0).toISOString(),
        "an unreadable Observation does not advance the caller's watermark",
      );
    });

    // ----------------------------------------------------------------- reads

    it("returns a timeline in occurrence order with working pagination", async () => {
      for (const [index, at] of [
        "2026-09-10T00:00:00Z",
        "2026-09-12T00:00:00Z",
        "2026-09-11T00:00:00Z",
      ].entries()) {
        await storage.ingest(
          obs({
            id: `obs_tl_${index}`,
            kind: "event.occurred",
            subject: change,
            occurred_at: at,
            data: { type: `software.event_${index}` },
          }),
        );
      }

      const page = await storage.timeline(
        tenant,
        change,
        "2026-09-01T00:00:00Z",
        "2026-09-30T00:00:00Z",
        undefined,
        2,
        0,
        fullAccess,
      );
      // Timestamps come back in the canonical UTC form the runtime stores:
      // milliseconds included, so every instant has one comparable shape.
      assert.deepEqual(
        page.items.map((item) => item.occurred_at),
        ["2026-09-10T00:00:00.000Z", "2026-09-11T00:00:00.000Z"],
        "items are ordered by occurrence time, not receipt",
      );
      assert.equal(page.truncated, true);
      assert.equal(page.nextOffset, 2);

      const next = await storage.timeline(
        tenant,
        change,
        "2026-09-01T00:00:00Z",
        "2026-09-30T00:00:00Z",
        undefined,
        2,
        2,
        fullAccess,
      );
      assert.deepEqual(
        next.items.map((item) => item.occurred_at),
        ["2026-09-12T00:00:00.000Z"],
      );
      assert.equal(next.truncated, false);
    });

    it("filters the timeline by semantic type", async () => {
      await storage.ingest(
        obs({
          id: "obs_tl_type_a",
          kind: "event.occurred",
          subject: change,
          occurred_at: "2026-09-10T00:00:00Z",
          data: { type: "software.review_requested" },
        }),
      );
      await storage.ingest(
        obs({
          id: "obs_tl_type_b",
          kind: "event.occurred",
          subject: change,
          occurred_at: "2026-09-11T00:00:00Z",
          data: { type: "software.review_completed" },
        }),
      );

      const page = await storage.timeline(
        tenant,
        change,
        "2026-09-01T00:00:00Z",
        "2026-09-30T00:00:00Z",
        ["software.review_completed"],
        10,
        0,
        fullAccess,
      );
      assert.equal(page.items.length, 1);
      assert.equal(
        (page.items[0]?.data as { type?: string })?.type,
        "software.review_completed",
      );
    });

    it("excludes a corrected event from the timeline", async () => {
      await storage.ingest(
        obs({
          id: "obs_tl_bad",
          kind: "event.occurred",
          subject: change,
          occurred_at: "2026-09-10T00:00:00Z",
          data: { type: "software.spurious_event" },
        }),
      );
      await storage.ingest(
        obs({
          id: "obs_tl_correction",
          kind: "event.occurred",
          subject: change,
          occurred_at: "2026-09-11T00:00:00Z",
          data: {
            type: "sensus.observation_corrected",
            attributes: {
              target_observation_id: "obs_tl_bad",
              disposition: "invalid",
            },
          },
        }),
      );

      const page = await storage.timeline(
        tenant,
        change,
        "2026-09-01T00:00:00Z",
        "2026-09-30T00:00:00Z",
        undefined,
        10,
        0,
        fullAccess,
      );
      assert.equal(
        page.items.filter((item) => item.observation_id === "obs_tl_bad").length,
        0,
      );
    });

    it("rolls up a bounded, cycle-safe relation graph", async () => {
      const nodes = ["a", "b", "c"];
      for (const name of nodes) {
        await storage.ingest(
          obs({
            id: `obs_graph_node_${name}`,
            kind: "entity.observed",
            subject: { type: "software.repository", id: `gitlab:acme/${name}` },
            occurred_at: "2026-09-01T00:00:00Z",
            data: { name },
          }),
        );
      }

      // a -> b -> c -> a forms a cycle.
      const edges: Array<[string, string]> = [
        ["a", "b"],
        ["b", "c"],
        ["c", "a"],
      ];
      for (const [from, to] of edges) {
        await storage.ingest(
          obs({
            id: `obs_graph_edge_${from}_${to}`,
            kind: "relation.observed",
            subject: { type: "software.repository", id: `gitlab:acme/${from}` },
            occurred_at: "2026-09-01T00:00:00Z",
            data: {
              relation: "software.depends_on",
              target: { type: "software.repository", id: `gitlab:acme/${to}` },
              status: "active",
            },
          }),
        );
      }

      const graph = await storage.traverseGraph(
        tenant,
        { type: "software.repository", id: "gitlab:acme/a" },
        { direction: "outgoing", maxDepth: 5, maxNodes: 50 },
        fullAccess,
      );
      assert.deepEqual(
        graph.nodes.map((node) => node.id).sort(),
        ["gitlab:acme/a", "gitlab:acme/b", "gitlab:acme/c"],
      );

      const bounded = await storage.traverseGraph(
        tenant,
        { type: "software.repository", id: "gitlab:acme/a" },
        { direction: "outgoing", maxDepth: 5, maxNodes: 2 },
        fullAccess,
      );
      assert.equal(bounded.nodes.length, 2);
      assert.equal(bounded.truncated, true, "hitting the node budget is reported");
    });

    it("does not traverse through an entity the consumer cannot see", async () => {
      await storage.ingest(
        obs({
          id: "obs_acl_graph_root",
          kind: "entity.observed",
          subject: { type: "software.repository", id: "gitlab:acme/visible" },
          occurred_at: "2026-09-01T00:00:00Z",
          data: { name: "visible" },
        }),
      );
      await storage.ingest(
        obs({
          id: "obs_acl_graph_hidden",
          kind: "entity.observed",
          subject: { type: "software.repository", id: "gitlab:acme/hidden" },
          occurred_at: "2026-09-01T00:00:00Z",
          data: { name: "hidden" },
          access: { classification: "restricted", allow: ["team:security"] },
        }),
      );
      await storage.ingest(
        obs({
          id: "obs_acl_graph_edge",
          kind: "relation.observed",
          subject: { type: "software.repository", id: "gitlab:acme/visible" },
          occurred_at: "2026-09-01T00:00:00Z",
          data: {
            relation: "software.depends_on",
            target: { type: "software.repository", id: "gitlab:acme/hidden" },
            status: "active",
          },
        }),
      );

      const ordinary = createConsumerContext({
        principals: ["team:payments"],
        clearance: "internal",
      });
      const graph = await storage.traverseGraph(
        tenant,
        { type: "software.repository", id: "gitlab:acme/visible" },
        { direction: "outgoing", maxDepth: 3, maxNodes: 50 },
        ordinary,
      );
      assert.deepEqual(
        graph.nodes.map((node) => node.id),
        ["gitlab:acme/visible"],
        "an invisible neighbour is skipped, not revealed",
      );
    });

    it("lists entities filtered by ACL and by type", async () => {
      await storage.ingest(
        obs({
          id: "obs_list_team",
          kind: "entity.observed",
          subject: team,
          occurred_at: "2026-09-01T00:00:00Z",
          data: { name: "Payments" },
        }),
      );
      await storage.ingest(
        obs({
          id: "obs_list_repo",
          kind: "entity.observed",
          subject: repository,
          occurred_at: "2026-09-01T00:00:00Z",
          data: { name: "payments-api" },
          access: { classification: "restricted", allow: ["team:security"] },
        }),
      );

      const all = await storage.listEntities(tenant, undefined, fullAccess);
      assert.equal(all.length, 2);

      const organizations = await storage.listEntities(
        tenant,
        "organization.team",
        fullAccess,
      );
      assert.equal(organizations.length, 1);
      assert.equal(organizations[0]?.id, team.id);

      const ordinary = createConsumerContext({
        principals: ["team:payments"],
        clearance: "internal",
      });
      const visible = await storage.listEntities(tenant, undefined, ordinary);
      assert.equal(visible.length, 1);
    });

    it("resolves evidence only when it was attached by an ingested Observation", async () => {
      const ref = "gitlab://acme/payments-api/merge_requests/3812";
      await assert.equal(
        await storage.findEvidence(tenant, ref, fullAccess),
        undefined,
        "an unattached reference is not resolvable",
      );

      await storage.ingest(
        obs({
          id: "obs_evidence",
          kind: "entity.observed",
          subject: change,
          occurred_at: "2026-09-15T09:00:00Z",
          data: { name: "Add batch refund support" },
          evidence: [
            {
              type: "source_record",
              ref,
              resolver: {
                capability: "gitlab.merge_request.read",
                arguments: { project: "acme/payments-api", merge_request_iid: 3812 },
              },
            },
          ],
        }),
      );

      const found = await storage.findEvidence(tenant, ref, fullAccess);
      assert.equal(found?.ref, ref);
      assert.equal(found?.resolver?.capability, "gitlab.merge_request.read");
    });

    it("reports the newest metric change pair per series", async () => {
      for (const [index, value] of [3.2, 5.0, 7.8].entries()) {
        await storage.ingest(
          obs({
            id: `obs_change_${index}`,
            kind: "metric.observed",
            subject: team,
            occurred_at: `2026-09-${String(9 + index).padStart(2, "0")}T00:00:00Z`,
            source: { system: "sensus-runtime", instance: tenant },
            data: {
              metric: "software.review_wait_time",
              value,
              unit: "hour",
              dimensions: { repository: "payments-api" },
            },
          }),
        );
      }

      const changes = await storage.getRecentMetricChanges(
        tenant,
        team,
        fullAccess,
      );
      assert.equal(changes.length, 1);
      assert.equal(changes[0]?.current, 7.8);
      assert.equal(changes[0]?.baseline, 5.0);
    });

    // ------------------------------------------------------------ concurrency
    //
    // These only run on a pooled backend. They are not decoration: each one
    // corresponds to a race that a read-then-write sequence loses in production
    // the moment two connectors ingest at the same time.

    if (target.concurrent) {
      /**
       * Establishes live connections before a race.
       *
       * A pool creates connections lazily, so the first burst of parallel calls
       * is staggered by connection setup and never truly overlaps — the race
       * window closes before it opens. Warming the pool first is what makes
       * these tests capable of failing.
       */
      const warmPool = async (width: number): Promise<void> => {
        await Promise.all(
          Array.from({ length: width }, () =>
            storage.latestWatermark(tenant, fullAccess),
          ),
        );
      };

      it("accepts exactly one of several simultaneous identical ingests", async () => {
        await warmPool(8);
        const shared = obs({
          id: "obs_race_same",
          kind: "entity.observed",
          subject: change,
          occurred_at: "2026-09-15T09:00:00Z",
          data: { name: "Raced", lifecycle: "active" },
        });

        const results = await Promise.all(
          Array.from({ length: 8 }, () => storage.ingest(shared)),
        );

        assert.equal(
          results.filter((result) => result.status === "accepted").length,
          1,
          "one writer wins",
        );
        assert.equal(
          results.filter((result) => result.status === "duplicate").length,
          7,
          "the losers observe a duplicate, not an error",
        );
      });

      it("reports a conflict, not an internal error, when payloads race on one id", async () => {
        await warmPool(4);
        const first = obs({
          id: "obs_race_conflict",
          kind: "entity.observed",
          subject: change,
          occurred_at: "2026-09-15T09:00:00Z",
          data: { name: "First" },
        });
        const second = obs({
          id: "obs_race_conflict",
          kind: "entity.observed",
          subject: change,
          occurred_at: "2026-09-15T09:00:00Z",
          data: { name: "Second" },
        });

        const settled = await Promise.allSettled([
          storage.ingest(first),
          storage.ingest(second),
        ]);

        const rejected = settled.filter((entry) => entry.status === "rejected");
        assert.equal(rejected.length, 1);
        assert.ok(
          (rejected[0] as PromiseRejectedResult).reason instanceof
            ObservationConflictError,
          "the losing payload must surface as a conflict",
        );
      });

      it("does not lose a field when two entity observations race", async () => {
        await warmPool(4);
        // Repeated so the interleaving is not a single lucky draw.
        for (let round = 0; round < 12; round += 1) {
          const subject = {
            type: "software.change",
            id: `gitlab:acme/race!${round}`,
          };
          const at = "2026-09-15T09:00:00Z";

          await Promise.all([
            storage.ingest(
              obs({
                id: `obs_race_left_${round}`,
                kind: "entity.observed",
                subject,
                occurred_at: at,
                data: { attributes: { left: round } },
              }),
            ),
            storage.ingest(
              obs({
                id: `obs_race_right_${round}`,
                kind: "entity.observed",
                subject,
                occurred_at: at,
                data: { attributes: { right: round } },
              }),
            ),
          ]);

          const entity = await storage.getEntity(tenant, subject, fullAccess);
          assert.deepEqual(
            entity?.attributes,
            { left: round, right: round },
            `round ${round}: both fields must survive the race`,
          );
        }
      });

      it("keeps the newer value when state observations race", async () => {
        await warmPool(4);
        const subject = {
          type: "software.change",
          id: "gitlab:acme/race-state",
        };
        const field = "software.review_status";

        await Promise.all([
          storage.ingest(
            obs({
              id: "obs_race_newer",
              kind: "state.observed",
              subject,
              occurred_at: "2026-09-15T10:00:00Z",
              data: { field, operation: "set", value: "newer" },
            }),
          ),
          storage.ingest(
            obs({
              id: "obs_race_older",
              kind: "state.observed",
              subject,
              occurred_at: "2026-09-15T09:00:00Z",
              data: { field, operation: "set", value: "older" },
            }),
          ),
        ]);

        const states = await storage.getStates(tenant, subject, fullAccess);
        assert.equal(
          states[0]?.value,
          "newer",
          "the three-clock comparison must hold under a race",
        );
      });

      it("lets only one of two simultaneous sync starts win", async () => {
        await warmPool(4);
        const start = () =>
          storage.startSync({
            tenant_id: tenant,
            sync_id: "sync_raced",
            mode: "snapshot",
            source: { system: "gitlab", instance: "acme-gitlab" },
            authoritative_deletion: true,
          });

        const settled = await Promise.allSettled([start(), start()]);
        const fulfilled = settled.filter((entry) => entry.status === "fulfilled");
        const rejected = settled.filter(
          (entry): entry is PromiseRejectedResult => entry.status === "rejected",
        );

        assert.equal(fulfilled.length, 1);
        assert.equal(rejected.length, 1);
        assert.ok(
          rejected[0]?.reason instanceof SyncError,
          "a lost start must be a SyncError, not a driver error",
        );
      });

      it("counts each sync member once when the same member is attached concurrently", async () => {
        await warmPool(6);
        await storage.startSync({
          tenant_id: tenant,
          sync_id: "sync_members",
          mode: "snapshot",
          source: { system: "gitlab", instance: "acme-gitlab" },
          authoritative_deletion: true,
        });
        const member = obs({
          id: "obs_race_member",
          kind: "entity.observed",
          subject: repository,
          occurred_at: "2026-09-01T00:00:00Z",
          data: { name: "payments-api", lifecycle: "active" },
        });

        await Promise.all(
          Array.from({ length: 4 }, () =>
            storage.ingest(member, { syncId: "sync_members" }),
          ),
        );

        const sync = await storage.getSync(tenant, "sync_members");
        assert.equal(
          sync?.actual_record_count,
          1,
          "duplicate attachments must not inflate the member count",
        );
      });
    }
  });
}
