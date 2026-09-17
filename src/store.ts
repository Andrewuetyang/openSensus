import { mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import Database from "better-sqlite3";
import {
  contentHash,
  observationSchema,
  stableId,
  type EntityRef,
  type EvidenceRef,
  type Observation,
  type Signal,
} from "./protocol.js";
import {
  canRead,
  combinePolicies,
  systemConsumer,
  type AccessPolicy,
  type ConsumerContext,
} from "./access.js";
import {
  defaultSignificantIncreaseRule,
  evaluateRule,
  ruleApplies,
  signalRuleSchema,
  type MetricPoint,
  type SignalRule,
} from "./signal-rules.js";

type JsonRecord = Record<string, unknown>;

export type IngestResult = {
  observation_id: string;
  status: "accepted" | "duplicate";
  received_at: string;
  generated_signals: string[];
};

export interface IngestOptions {
  syncId?: string;
}

export interface StartSyncInput {
  tenant_id: string;
  sync_id: string;
  mode: "snapshot" | "incremental" | "reconciliation";
  source: { system: string; instance: string };
  authoritative_deletion?: boolean;
  started_at?: string | undefined;
}

export interface CompleteSyncInput {
  completed_at?: string | undefined;
  record_count?: number | undefined;
  cursor?: string | undefined;
}

export interface GraphOptions {
  direction: "outgoing" | "incoming" | "both";
  relations?: string[];
  maxDepth: number;
  maxNodes: number;
}

export interface GraphResult {
  nodes: EntityRef[];
  edges: JsonRecord[];
  truncated: boolean;
}

export interface TimelinePage {
  items: JsonRecord[];
  truncated: boolean;
  nextOffset?: number;
}

type StoredMetric = {
  observation_id: string;
  metric: string;
  value: number;
  unit: string;
  occurred_at: string;
  interval_from: string | null;
  interval_to: string | null;
  dimensions: Record<string, string>;
  evidence: EvidenceRef[];
};

export class ObservationConflictError extends Error {
  constructor(public readonly observationId: string) {
    super(`Observation ${observationId} already exists with a different payload`);
    this.name = "ObservationConflictError";
  }
}

export class CorrectionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "CorrectionError";
  }
}

export class SyncError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "SyncError";
  }
}

export function defaultDatabasePath(): string {
  return resolve(process.env.SENSUS_DB_PATH ?? "data/sensus.db");
}

export class SensusStore {
  private readonly db: Database.Database;

  constructor(path = defaultDatabasePath()) {
    if (path !== ":memory:") {
      mkdirSync(dirname(path), { recursive: true });
    }
    this.db = new Database(path);
    this.db.pragma("foreign_keys = ON");
    if (path !== ":memory:") {
      this.db.pragma("journal_mode = WAL");
      // Without this, a second process writing the same file fails immediately
      // with SQLITE_BUSY instead of waiting for the write lock to clear. WAL
      // keeps readers unblocked either way.
      this.db.pragma("busy_timeout = 5000");
    }
    this.migrate();
  }

  close(): void {
    this.db.close();
  }

  private migrate(): void {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS observations (
        tenant_id TEXT NOT NULL,
        observation_id TEXT NOT NULL,
        spec_version TEXT NOT NULL,
        kind TEXT NOT NULL,
        subject_type TEXT NOT NULL,
        subject_id TEXT NOT NULL,
        occurred_at TEXT NOT NULL,
        observed_at TEXT NOT NULL,
        received_at TEXT NOT NULL,
        source_system TEXT NOT NULL,
        source_instance TEXT NOT NULL,
        source_record_id TEXT,
        source_cursor TEXT,
        source_sequence INTEGER,
        data_json TEXT NOT NULL,
        evidence_json TEXT NOT NULL,
        access_json TEXT,
        confidence REAL,
        labels_json TEXT,
        trace_id TEXT,
        sync_id TEXT,
        invalidated_at TEXT,
        invalidated_by TEXT,
        superseded_by TEXT,
        payload_json TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        PRIMARY KEY (tenant_id, observation_id)
      );

      CREATE INDEX IF NOT EXISTS idx_observations_subject_time
        ON observations (tenant_id, subject_type, subject_id, occurred_at DESC);
      CREATE INDEX IF NOT EXISTS idx_observations_kind_time
        ON observations (tenant_id, kind, occurred_at DESC);

      CREATE TABLE IF NOT EXISTS entities (
        tenant_id TEXT NOT NULL,
        type TEXT NOT NULL,
        id TEXT NOT NULL,
        name TEXT,
        lifecycle TEXT,
        attributes_json TEXT NOT NULL,
        clock_json TEXT NOT NULL,
        field_sources_json TEXT NOT NULL DEFAULT '{}',
        updated_at TEXT NOT NULL,
        observation_id TEXT NOT NULL,
        evidence_json TEXT NOT NULL,
        PRIMARY KEY (tenant_id, type, id)
      );

      CREATE TABLE IF NOT EXISTS relations (
        tenant_id TEXT NOT NULL,
        subject_type TEXT NOT NULL,
        subject_id TEXT NOT NULL,
        relation TEXT NOT NULL,
        target_type TEXT NOT NULL,
        target_id TEXT NOT NULL,
        source_system TEXT NOT NULL,
        source_instance TEXT NOT NULL,
        status TEXT NOT NULL,
        attributes_json TEXT NOT NULL,
        occurred_at TEXT NOT NULL,
        observation_id TEXT NOT NULL,
        evidence_json TEXT NOT NULL,
        PRIMARY KEY (
          tenant_id, subject_type, subject_id, relation,
          target_type, target_id, source_system, source_instance
        )
      );

      CREATE INDEX IF NOT EXISTS idx_relations_subject
        ON relations (tenant_id, subject_type, subject_id, status);
      CREATE INDEX IF NOT EXISTS idx_relations_target
        ON relations (tenant_id, target_type, target_id, status);

      CREATE TABLE IF NOT EXISTS states (
        tenant_id TEXT NOT NULL,
        subject_type TEXT NOT NULL,
        subject_id TEXT NOT NULL,
        field TEXT NOT NULL,
        value_json TEXT,
        is_unset INTEGER NOT NULL,
        occurred_at TEXT NOT NULL,
        observed_at TEXT NOT NULL,
        received_at TEXT NOT NULL,
        source_system TEXT NOT NULL,
        source_instance TEXT NOT NULL,
        source_sequence INTEGER,
        observation_id TEXT NOT NULL,
        evidence_json TEXT NOT NULL,
        PRIMARY KEY (tenant_id, subject_type, subject_id, field)
      );

      CREATE TABLE IF NOT EXISTS metrics (
        tenant_id TEXT NOT NULL,
        observation_id TEXT NOT NULL,
        subject_type TEXT NOT NULL,
        subject_id TEXT NOT NULL,
        metric TEXT NOT NULL,
        value REAL NOT NULL,
        unit TEXT NOT NULL,
        occurred_at TEXT NOT NULL,
        interval_from TEXT,
        interval_to TEXT,
        dimensions_json TEXT NOT NULL,
        dimensions_hash TEXT NOT NULL,
        aggregation TEXT,
        calculation_json TEXT,
        evidence_json TEXT NOT NULL,
        PRIMARY KEY (tenant_id, observation_id)
      );

      CREATE INDEX IF NOT EXISTS idx_metrics_series
        ON metrics (
          tenant_id, subject_type, subject_id, metric,
          dimensions_hash, occurred_at DESC
        );

      CREATE TABLE IF NOT EXISTS signals (
        tenant_id TEXT NOT NULL,
        signal_id TEXT NOT NULL,
        type TEXT NOT NULL,
        subject_type TEXT NOT NULL,
        subject_id TEXT NOT NULL,
        status TEXT NOT NULL,
        severity TEXT NOT NULL,
        detected_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        PRIMARY KEY (tenant_id, signal_id)
      );

      CREATE INDEX IF NOT EXISTS idx_signals_subject
        ON signals (tenant_id, subject_type, subject_id, status, updated_at DESC);

      CREATE TABLE IF NOT EXISTS signal_rules (
        tenant_id TEXT NOT NULL,
        rule_id TEXT NOT NULL,
        enabled INTEGER NOT NULL,
        metric TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (tenant_id, rule_id)
      );

      CREATE INDEX IF NOT EXISTS idx_signal_rules_metric
        ON signal_rules (tenant_id, metric, enabled);

      CREATE TABLE IF NOT EXISTS syncs (
        tenant_id TEXT NOT NULL,
        sync_id TEXT NOT NULL,
        mode TEXT NOT NULL,
        source_system TEXT NOT NULL,
        source_instance TEXT NOT NULL,
        authoritative_deletion INTEGER NOT NULL,
        status TEXT NOT NULL,
        started_at TEXT NOT NULL,
        completed_at TEXT,
        expected_record_count INTEGER,
        actual_record_count INTEGER NOT NULL DEFAULT 0,
        cursor TEXT,
        deleted_entity_count INTEGER NOT NULL DEFAULT 0,
        PRIMARY KEY (tenant_id, sync_id)
      );

      CREATE TABLE IF NOT EXISTS sync_members (
        tenant_id TEXT NOT NULL,
        sync_id TEXT NOT NULL,
        observation_id TEXT NOT NULL,
        subject_type TEXT NOT NULL,
        subject_id TEXT NOT NULL,
        kind TEXT NOT NULL,
        PRIMARY KEY (tenant_id, sync_id, observation_id)
      );

      CREATE INDEX IF NOT EXISTS idx_sync_members_subject
        ON sync_members (tenant_id, sync_id, subject_type, subject_id, kind);

      CREATE TABLE IF NOT EXISTS source_entities (
        tenant_id TEXT NOT NULL,
        source_system TEXT NOT NULL,
        source_instance TEXT NOT NULL,
        subject_type TEXT NOT NULL,
        subject_id TEXT NOT NULL,
        present INTEGER NOT NULL,
        last_sync_id TEXT,
        observed_at TEXT NOT NULL,
        PRIMARY KEY (
          tenant_id, source_system, source_instance, subject_type, subject_id
        )
      );
    `);

    this.ensureColumn("observations", "sync_id", "TEXT");
    this.ensureColumn("observations", "invalidated_at", "TEXT");
    this.ensureColumn("observations", "invalidated_by", "TEXT");
    this.ensureColumn("observations", "superseded_by", "TEXT");
    this.ensureColumn(
      "entities",
      "field_sources_json",
      "TEXT NOT NULL DEFAULT '{}'",
    );
  }

  private ensureColumn(table: string, column: string, definition: string): void {
    const columns = this.db.prepare(`PRAGMA table_info(${table})`).all() as Array<{
      name: string;
    }>;
    if (!columns.some((candidate) => candidate.name === column)) {
      this.db.exec(`ALTER TABLE ${table} ADD COLUMN ${column} ${definition}`);
    }
  }

  ingest(observation: Observation, options: IngestOptions = {}): IngestResult {
    const payloadJson = JSON.stringify(observation);
    const hash = contentHash(observation);
    const existing = this.findExistingObservation(
      observation.tenant_id,
      observation.observation_id,
    );

    if (existing) {
      return this.duplicateResult(observation, options, existing, hash);
    }

    const receivedAt = new Date().toISOString();
    const generatedSignals: string[] = [];
    const transaction = this.db.transaction(() => {
      this.db
        .prepare(
          `INSERT INTO observations (
             tenant_id, observation_id, spec_version, kind,
             subject_type, subject_id, occurred_at, observed_at, received_at,
             source_system, source_instance, source_record_id, source_cursor,
             source_sequence, data_json, evidence_json, access_json, confidence,
             labels_json, trace_id, sync_id, payload_json, content_hash
           ) VALUES (
             @tenant_id, @observation_id, @spec_version, @kind,
             @subject_type, @subject_id, @occurred_at, @observed_at, @received_at,
             @source_system, @source_instance, @source_record_id, @source_cursor,
             @source_sequence, @data_json, @evidence_json, @access_json, @confidence,
             @labels_json, @trace_id, @sync_id, @payload_json, @content_hash
           )`,
        )
        .run({
          tenant_id: observation.tenant_id,
          observation_id: observation.observation_id,
          spec_version: observation.spec_version,
          kind: observation.kind,
          subject_type: observation.subject.type,
          subject_id: observation.subject.id,
          occurred_at: observation.occurred_at,
          observed_at: observation.observed_at,
          received_at: receivedAt,
          source_system: observation.source.system,
          source_instance: observation.source.instance,
          source_record_id: observation.source.record_id ?? null,
          source_cursor: observation.source.cursor ?? null,
          source_sequence: observation.source.sequence ?? null,
          data_json: JSON.stringify(observation.data),
          evidence_json: JSON.stringify(observation.evidence ?? []),
          access_json: observation.access ? JSON.stringify(observation.access) : null,
          confidence: observation.confidence ?? null,
          labels_json: observation.labels ? JSON.stringify(observation.labels) : null,
          trace_id: observation.trace_id ?? null,
          sync_id: options.syncId ?? null,
          payload_json: payloadJson,
          content_hash: hash,
        });

      switch (observation.kind) {
        case "entity.observed":
          this.projectEntity(observation);
          break;
        case "relation.observed":
          this.projectRelation(observation);
          break;
        case "state.observed":
          this.projectState(observation, receivedAt);
          break;
        case "metric.observed": {
          this.projectMetric(observation);
          generatedSignals.push(...this.evaluateMetricSignals(observation));
          break;
        }
        case "event.occurred":
          if (observation.data.type === "sensus.observation_corrected") {
            generatedSignals.push(...this.applyCorrection(observation));
          }
          break;
      }
      if (observation.kind === "entity.observed") {
        this.trackSourceEntity(observation, options.syncId);
      }
      if (options.syncId) this.attachToSync(observation, options.syncId);
    });
    try {
      transaction();
    } catch (error) {
      if (!isSqlitePrimaryKeyViolation(error)) throw error;
      // Lost a race with another process between the existence check above and
      // the insert. The whole transaction rolled back, so nothing was projected
      // twice; re-read and answer as the duplicate or conflict this actually is
      // rather than letting a raw constraint error surface as a 500.
      const winner = this.findExistingObservation(
        observation.tenant_id,
        observation.observation_id,
      );
      if (!winner) throw error;
      return this.duplicateResult(observation, options, winner, hash);
    }

    return {
      observation_id: observation.observation_id,
      status: "accepted",
      received_at: receivedAt,
      generated_signals: generatedSignals,
    };
  }

  private findExistingObservation(
    tenantId: string,
    observationId: string,
  ): { content_hash: string; received_at: string } | undefined {
    return this.db
      .prepare(
        `SELECT content_hash, received_at
         FROM observations
         WHERE tenant_id = ? AND observation_id = ?`,
      )
      .get(tenantId, observationId) as
      | { content_hash: string; received_at: string }
      | undefined;
  }

  /**
   * The answer for an `observation_id` that is already stored: a duplicate when
   * the canonical payload matches, a conflict when it does not.
   *
   * Shared by the fast path and the lost-the-insert-race path, so both attach to
   * an open sync identically. Attaching on the duplicate path is what makes
   * re-sending a whole snapshot a valid reconciliation strategy.
   */
  private duplicateResult(
    observation: Observation,
    options: IngestOptions,
    existing: { content_hash: string; received_at: string },
    hash: string,
  ): IngestResult {
    if (existing.content_hash !== hash) {
      throw new ObservationConflictError(observation.observation_id);
    }
    if (options.syncId) {
      const attach = this.db.transaction(() => {
        if (observation.kind === "entity.observed") {
          this.trackSourceEntity(observation, options.syncId);
        }
        this.attachToSync(observation, options.syncId!);
        if (observation.kind === "entity.observed") {
          this.reassertEntityPresence(observation, options.syncId!);
        }
      });
      attach();
    }
    return {
      observation_id: observation.observation_id,
      status: "duplicate",
      received_at: existing.received_at,
      generated_signals: [],
    };
  }

  ingestMany(
    observations: Observation[],
    options: IngestOptions = {},
  ): IngestResult[] {
    return observations.map((observation) => this.ingest(observation, options));
  }

  private applyCorrection(
    correction: Extract<Observation, { kind: "event.occurred" }>,
  ): string[] {
    const attributes = correction.data.attributes ?? {};
    const targetObservationId = attributes.target_observation_id;
    const disposition = attributes.disposition;
    const replacementObservationId = attributes.replacement_observation_id;
    if (typeof targetObservationId !== "string") {
      throw new CorrectionError("Correction requires target_observation_id");
    }
    if (disposition !== "invalid" && disposition !== "superseded") {
      throw new CorrectionError("Correction disposition must be invalid or superseded");
    }
    if (
      replacementObservationId !== undefined &&
      typeof replacementObservationId !== "string"
    ) {
      throw new CorrectionError("replacement_observation_id must be a string");
    }
    if (targetObservationId === correction.observation_id) {
      throw new CorrectionError("A correction cannot target itself");
    }

    const target = this.db
      .prepare(
        `SELECT subject_type, subject_id, kind, invalidated_by
         FROM observations
         WHERE tenant_id = ? AND observation_id = ?`,
      )
      .get(correction.tenant_id, targetObservationId) as
      | {
          subject_type: string;
          subject_id: string;
          kind: string;
          invalidated_by: string | null;
        }
      | undefined;
    if (!target) {
      throw new CorrectionError(
        `Target observation ${targetObservationId} does not exist in the tenant`,
      );
    }
    if (target.kind === "event.occurred") {
      const targetPayload = this.getObservation(
        correction.tenant_id,
        targetObservationId,
      );
      if (
        targetPayload?.kind === "event.occurred" &&
        targetPayload.data.type === "sensus.observation_corrected"
      ) {
        throw new CorrectionError("Corrections of correction events are not supported");
      }
    }
    if (target.invalidated_by && target.invalidated_by !== correction.observation_id) {
      throw new CorrectionError(
        `Target observation was already corrected by ${target.invalidated_by}`,
      );
    }

    if (typeof replacementObservationId === "string") {
      const replacement = this.db
        .prepare(
          `SELECT subject_type, subject_id FROM observations
           WHERE tenant_id = ? AND observation_id = ?`,
        )
        .get(correction.tenant_id, replacementObservationId) as
        | { subject_type: string; subject_id: string }
        | undefined;
      if (
        replacement &&
        (replacement.subject_type !== target.subject_type ||
          replacement.subject_id !== target.subject_id)
      ) {
        throw new CorrectionError(
          "Replacement observation must describe the same subject as the target",
        );
      }
    }

    this.db
      .prepare(
        `UPDATE observations
         SET invalidated_at = ?, invalidated_by = ?, superseded_by = ?
         WHERE tenant_id = ? AND observation_id = ?`,
      )
      .run(
        correction.occurred_at,
        correction.observation_id,
        disposition === "superseded"
          ? (replacementObservationId ?? null)
          : null,
        correction.tenant_id,
        targetObservationId,
      );

    return this.rebuildSubject(correction.tenant_id, {
      type: target.subject_type,
      id: target.subject_id,
    });
  }

  private rebuildSubject(tenantId: string, subject: EntityRef): string[] {
    this.db
      .prepare(`DELETE FROM entities WHERE tenant_id = ? AND type = ? AND id = ?`)
      .run(tenantId, subject.type, subject.id);
    this.db
      .prepare(
        `DELETE FROM relations
         WHERE tenant_id = ? AND subject_type = ? AND subject_id = ?`,
      )
      .run(tenantId, subject.type, subject.id);
    this.db
      .prepare(
        `DELETE FROM states
         WHERE tenant_id = ? AND subject_type = ? AND subject_id = ?`,
      )
      .run(tenantId, subject.type, subject.id);
    this.db
      .prepare(
        `DELETE FROM metrics
         WHERE tenant_id = ? AND subject_type = ? AND subject_id = ?`,
      )
      .run(tenantId, subject.type, subject.id);
    this.db
      .prepare(
        `DELETE FROM signals
         WHERE tenant_id = ? AND subject_type = ? AND subject_id = ?`,
      )
      .run(tenantId, subject.type, subject.id);
    this.db
      .prepare(
        `DELETE FROM source_entities
         WHERE tenant_id = ? AND subject_type = ? AND subject_id = ?`,
      )
      .run(tenantId, subject.type, subject.id);

    const rows = this.db
      .prepare(
        `SELECT payload_json, received_at, sync_id
         FROM observations
         WHERE tenant_id = ? AND subject_type = ? AND subject_id = ?
           AND invalidated_at IS NULL
         ORDER BY occurred_at ASC, COALESCE(source_sequence, -1) ASC,
                  received_at ASC, observation_id ASC`,
      )
      .all(tenantId, subject.type, subject.id) as Array<{
      payload_json: string;
      received_at: string;
      sync_id: string | null;
    }>;
    const signals: string[] = [];
    for (const row of rows) {
      const observation = JSON.parse(row.payload_json) as Observation;
      switch (observation.kind) {
        case "entity.observed":
          this.projectEntity(observation);
          this.trackSourceEntity(observation, row.sync_id ?? undefined);
          break;
        case "relation.observed":
          this.projectRelation(observation);
          break;
        case "state.observed":
          this.projectState(observation, row.received_at);
          break;
        case "metric.observed":
          this.projectMetric(observation);
          signals.push(...this.evaluateMetricSignals(observation));
          break;
        case "event.occurred":
          break;
      }
    }
    return [...new Set(signals)];
  }

  startSync(input: StartSyncInput): JsonRecord {
    if (input.authoritative_deletion && input.mode === "incremental") {
      throw new SyncError(
        "authoritative_deletion is only valid for snapshot or reconciliation syncs",
      );
    }
    const startedAt = input.started_at ?? new Date().toISOString();
    try {
      this.db
        .prepare(
          `INSERT INTO syncs (
             tenant_id, sync_id, mode, source_system, source_instance,
             authoritative_deletion, status, started_at
           ) VALUES (?, ?, ?, ?, ?, ?, 'open', ?)`,
        )
        .run(
          input.tenant_id,
          input.sync_id,
          input.mode,
          input.source.system,
          input.source.instance,
          input.authoritative_deletion ? 1 : 0,
          startedAt,
        );
    } catch (error) {
      if (isSqliteConstraint(error)) {
        throw new SyncError(`Sync ${input.sync_id} already exists`);
      }
      throw error;
    }
    return this.getSync(input.tenant_id, input.sync_id)!;
  }

  getSync(tenantId: string, syncId: string): JsonRecord | undefined {
    const row = this.db
      .prepare(`SELECT * FROM syncs WHERE tenant_id = ? AND sync_id = ?`)
      .get(tenantId, syncId) as JsonRecord | undefined;
    if (!row) return undefined;
    return {
      tenant_id: row.tenant_id,
      sync_id: row.sync_id,
      mode: row.mode,
      source: { system: row.source_system, instance: row.source_instance },
      authoritative_deletion: Boolean(row.authoritative_deletion),
      status: row.status,
      started_at: row.started_at,
      completed_at: row.completed_at,
      expected_record_count: row.expected_record_count,
      actual_record_count: row.actual_record_count,
      cursor: row.cursor,
      deleted_entity_count: row.deleted_entity_count,
    };
  }

  completeSync(
    tenantId: string,
    syncId: string,
    input: CompleteSyncInput,
  ): JsonRecord {
    const completedAt = input.completed_at ?? new Date().toISOString();
    const transaction = this.db.transaction(() => {
      const sync = this.db
        .prepare(`SELECT * FROM syncs WHERE tenant_id = ? AND sync_id = ?`)
        .get(tenantId, syncId) as JsonRecord | undefined;
      if (!sync) throw new SyncError(`Sync ${syncId} was not found`);
      if (sync.status !== "open") {
        throw new SyncError(`Sync ${syncId} is already ${String(sync.status)}`);
      }
      const actualCount = Number(sync.actual_record_count);
      if (
        input.record_count !== undefined &&
        input.record_count !== actualCount
      ) {
        throw new SyncError(
          `Sync count mismatch: expected ${input.record_count}, received ${actualCount}`,
        );
      }

      let deletedEntityCount = 0;
      if (Boolean(sync.authoritative_deletion)) {
        const missing = this.db
          .prepare(
            `SELECT subject_type, subject_id
             FROM source_entities
             WHERE tenant_id = ? AND source_system = ? AND source_instance = ?
               AND present = 1
               AND (last_sync_id IS NULL OR last_sync_id <> ?)`,
          )
          .all(
            tenantId,
            String(sync.source_system),
            String(sync.source_instance),
            syncId,
          ) as Array<{ subject_type: string; subject_id: string }>;

        for (const subject of missing) {
          this.db
            .prepare(
              `UPDATE source_entities
               SET present = 0, last_sync_id = ?, observed_at = ?
               WHERE tenant_id = ? AND source_system = ? AND source_instance = ?
                 AND subject_type = ? AND subject_id = ?`,
            )
            .run(
              syncId,
              completedAt,
              tenantId,
              sync.source_system,
              sync.source_instance,
              subject.subject_type,
              subject.subject_id,
            );
          const remaining = this.db
            .prepare(
              `SELECT COUNT(*) AS count FROM source_entities
               WHERE tenant_id = ? AND subject_type = ? AND subject_id = ?
                 AND present = 1`,
            )
            .get(tenantId, subject.subject_type, subject.subject_id) as {
            count: number;
          };
          if (remaining.count > 0) continue;

          const previousAccess = this.latestEntityAccess(
            tenantId,
            subject,
            String(sync.source_system),
            String(sync.source_instance),
          );
          const deletion = observationSchema.parse({
            spec_version: "sensus/0.1",
            observation_id: stableId(
              "obs",
              tenantId,
              syncId,
              subject.subject_type,
              subject.subject_id,
              "authoritative-deletion",
            ),
            tenant_id: tenantId,
            kind: "entity.observed",
            subject: { type: subject.subject_type, id: subject.subject_id },
            occurred_at: completedAt,
            observed_at: completedAt,
            source: {
              system: "sensus-reconciliation",
              instance: `${String(sync.source_system)}:${String(sync.source_instance)}`,
              record_id: `${subject.subject_type}:${subject.subject_id}`,
            },
            data: { lifecycle: "deleted" },
            evidence: [
              {
                type: "derivation",
                ref: `sensus://sync/${encodeURIComponent(tenantId)}/${encodeURIComponent(syncId)}`,
                title: "Missing from authoritative source snapshot",
              },
            ],
            ...(previousAccess ? { access: previousAccess } : {}),
          });
          this.ingest(deletion);
          deletedEntityCount += 1;
        }
      }

      this.db
        .prepare(
          `UPDATE syncs
           SET status = 'completed', completed_at = ?, expected_record_count = ?,
               cursor = ?, deleted_entity_count = ?
           WHERE tenant_id = ? AND sync_id = ?`,
        )
        .run(
          completedAt,
          input.record_count ?? null,
          input.cursor ?? null,
          deletedEntityCount,
          tenantId,
          syncId,
        );
    });
    transaction();
    return this.getSync(tenantId, syncId)!;
  }

  private attachToSync(observation: Observation, syncId: string): void {
    const sync = this.db
      .prepare(
        `SELECT status, source_system, source_instance FROM syncs
         WHERE tenant_id = ? AND sync_id = ?`,
      )
      .get(observation.tenant_id, syncId) as
      | { status: string; source_system: string; source_instance: string }
      | undefined;
    if (!sync) throw new SyncError(`Sync ${syncId} was not found`);
    if (sync.status !== "open") throw new SyncError(`Sync ${syncId} is not open`);
    if (
      sync.source_system !== observation.source.system ||
      sync.source_instance !== observation.source.instance
    ) {
      throw new SyncError(
        `Observation source does not match sync source ${sync.source_system}/${sync.source_instance}`,
      );
    }
    const result = this.db
      .prepare(
        `INSERT OR IGNORE INTO sync_members (
           tenant_id, sync_id, observation_id, subject_type, subject_id, kind
         ) VALUES (?, ?, ?, ?, ?, ?)`,
      )
      .run(
        observation.tenant_id,
        syncId,
        observation.observation_id,
        observation.subject.type,
        observation.subject.id,
        observation.kind,
      );
    if (result.changes > 0) {
      this.db
        .prepare(
          `UPDATE syncs SET actual_record_count = actual_record_count + 1
           WHERE tenant_id = ? AND sync_id = ?`,
        )
        .run(observation.tenant_id, syncId);
    }
  }

  private trackSourceEntity(
    observation: Extract<Observation, { kind: "entity.observed" }>,
    syncId?: string,
  ): void {
    if (observation.source.system === "sensus-reconciliation") return;
    this.db
      .prepare(
        `INSERT INTO source_entities (
           tenant_id, source_system, source_instance, subject_type, subject_id,
           present, last_sync_id, observed_at
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT (
           tenant_id, source_system, source_instance, subject_type, subject_id
         ) DO UPDATE SET
           present = excluded.present,
           last_sync_id = COALESCE(excluded.last_sync_id, source_entities.last_sync_id),
           observed_at = excluded.observed_at`,
      )
      .run(
        observation.tenant_id,
        observation.source.system,
        observation.source.instance,
        observation.subject.type,
        observation.subject.id,
        observation.data.lifecycle === "deleted" ? 0 : 1,
        syncId ?? null,
        observation.occurred_at,
      );
  }

  private reassertEntityPresence(
    observation: Extract<Observation, { kind: "entity.observed" }>,
    syncId: string,
  ): void {
    if (observation.data.lifecycle === "deleted") return;
    const current = this.db
      .prepare(
        `SELECT lifecycle FROM entities
         WHERE tenant_id = ? AND type = ? AND id = ?`,
      )
      .get(
        observation.tenant_id,
        observation.subject.type,
        observation.subject.id,
      ) as { lifecycle: string | null } | undefined;
    if (current?.lifecycle !== "deleted") return;

    const now = new Date().toISOString();
    const reassertion = observationSchema.parse({
      spec_version: "sensus/0.1",
      observation_id: stableId(
        "obs",
        observation.tenant_id,
        syncId,
        observation.subject.type,
        observation.subject.id,
        "authoritative-presence",
      ),
      tenant_id: observation.tenant_id,
      kind: "entity.observed",
      subject: observation.subject,
      occurred_at: now,
      observed_at: now,
      source: {
        system: "sensus-reconciliation",
        instance: `${observation.source.system}:${observation.source.instance}`,
        record_id: `${observation.subject.type}:${observation.subject.id}`,
      },
      data: { lifecycle: observation.data.lifecycle ?? "active" },
      evidence: [
        {
          type: "derivation",
          ref: `sensus://sync/${encodeURIComponent(observation.tenant_id)}/${encodeURIComponent(syncId)}`,
          title: "Present in authoritative source snapshot",
        },
      ],
      ...(observation.access ? { access: observation.access } : {}),
    });
    this.ingest(reassertion);
  }

  private latestEntityAccess(
    tenantId: string,
    subject: { subject_type: string; subject_id: string },
    sourceSystem: string,
    sourceInstance: string,
  ): AccessPolicy | undefined {
    const row = this.db
      .prepare(
        `SELECT access_json FROM observations
         WHERE tenant_id = ? AND subject_type = ? AND subject_id = ?
           AND source_system = ? AND source_instance = ?
           AND kind = 'entity.observed' AND invalidated_at IS NULL
         ORDER BY occurred_at DESC, received_at DESC LIMIT 1`,
      )
      .get(
        tenantId,
        subject.subject_type,
        subject.subject_id,
        sourceSystem,
        sourceInstance,
      ) as { access_json: string | null } | undefined;
    return row ? parseJson<AccessPolicy | undefined>(row.access_json, undefined) : undefined;
  }

  getObservation(tenantId: string, observationId: string): Observation | undefined {
    const row = this.db
      .prepare(
        `SELECT payload_json FROM observations
         WHERE tenant_id = ? AND observation_id = ?`,
      )
      .get(tenantId, observationId) as { payload_json: string } | undefined;
    return row ? (JSON.parse(row.payload_json) as Observation) : undefined;
  }

  getObservationForConsumer(
    tenantId: string,
    observationId: string,
    consumer: ConsumerContext,
  ): Observation | undefined {
    const row = this.db
      .prepare(
        `SELECT payload_json, access_json, invalidated_at
         FROM observations
         WHERE tenant_id = ? AND observation_id = ?`,
      )
      .get(tenantId, observationId) as
      | {
          payload_json: string;
          access_json: string | null;
          invalidated_at: string | null;
        }
      | undefined;
    if (!row || row.invalidated_at) return undefined;
    const policy = parseJson<AccessPolicy | undefined>(row.access_json, undefined);
    return canRead(policy, consumer)
      ? (JSON.parse(row.payload_json) as Observation)
      : undefined;
  }

  getObservationAccess(
    tenantId: string,
    observationId: string,
  ): AccessPolicy | undefined {
    const row = this.db
      .prepare(
        `SELECT access_json FROM observations
         WHERE tenant_id = ? AND observation_id = ?`,
      )
      .get(tenantId, observationId) as { access_json: string | null } | undefined;
    return row ? parseJson<AccessPolicy | undefined>(row.access_json, undefined) : undefined;
  }

  canReadObservation(
    tenantId: string,
    observationId: string,
    consumer: ConsumerContext,
  ): boolean {
    const row = this.db
      .prepare(
        `SELECT access_json, invalidated_at FROM observations
         WHERE tenant_id = ? AND observation_id = ?`,
      )
      .get(tenantId, observationId) as
      | { access_json: string | null; invalidated_at: string | null }
      | undefined;
    if (!row || row.invalidated_at) return false;
    return canRead(
      parseJson<AccessPolicy | undefined>(row.access_json, undefined),
      consumer,
    );
  }

  private projectEntity(
    observation: Extract<Observation, { kind: "entity.observed" }>,
  ): void {
    const existing = this.db
      .prepare(
        `SELECT * FROM entities WHERE tenant_id = ? AND type = ? AND id = ?`,
      )
      .get(
        observation.tenant_id,
        observation.subject.type,
        observation.subject.id,
      ) as
      | {
          name: string | null;
          lifecycle: string | null;
          attributes_json: string;
          clock_json: string;
          field_sources_json: string;
          updated_at: string;
          observation_id: string;
          evidence_json: string;
        }
      | undefined;

    const attributes = parseJson<JsonRecord>(existing?.attributes_json, {});
    const clocks = parseJson<Record<string, string>>(existing?.clock_json, {});
    const fieldSources = parseJson<Record<string, string>>(
      existing?.field_sources_json,
      {},
    );
    const next = {
      name: existing?.name ?? null,
      lifecycle: existing?.lifecycle ?? null,
    };
    let changed = !existing;

    const setIfNewer = (key: string, set: () => void): void => {
      if (!clocks[key] || clocks[key] <= observation.occurred_at) {
        set();
        clocks[key] = observation.occurred_at;
        fieldSources[key] = observation.observation_id;
        changed = true;
      }
    };

    if (observation.data.name !== undefined) {
      setIfNewer("name", () => {
        next.name = observation.data.name ?? null;
      });
    }
    if (observation.data.lifecycle !== undefined) {
      setIfNewer("lifecycle", () => {
        next.lifecycle = observation.data.lifecycle ?? null;
      });
    }
    for (const [key, value] of Object.entries(observation.data.attributes ?? {})) {
      setIfNewer(`attribute:${key}`, () => {
        if (value === null) delete attributes[key];
        else attributes[key] = value;
      });
    }

    this.db
      .prepare(
        `INSERT INTO entities (
           tenant_id, type, id, name, lifecycle, attributes_json, clock_json,
           field_sources_json, updated_at, observation_id, evidence_json
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT (tenant_id, type, id) DO UPDATE SET
           name = excluded.name,
           lifecycle = excluded.lifecycle,
           attributes_json = excluded.attributes_json,
           clock_json = excluded.clock_json,
           field_sources_json = excluded.field_sources_json,
           updated_at = excluded.updated_at,
           observation_id = excluded.observation_id,
           evidence_json = excluded.evidence_json`,
      )
      .run(
        observation.tenant_id,
        observation.subject.type,
        observation.subject.id,
        next.name,
        next.lifecycle,
        JSON.stringify(attributes),
        JSON.stringify(clocks),
        JSON.stringify(fieldSources),
        maxTimestamp(existing?.updated_at, observation.occurred_at),
        changed ? observation.observation_id : existing!.observation_id,
        changed
          ? JSON.stringify(observation.evidence ?? [observationEvidence(observation)])
          : existing!.evidence_json,
      );
  }

  private projectRelation(
    observation: Extract<Observation, { kind: "relation.observed" }>,
  ): void {
    const existing = this.db
      .prepare(
        `SELECT occurred_at FROM relations
         WHERE tenant_id = ? AND subject_type = ? AND subject_id = ?
           AND relation = ? AND target_type = ? AND target_id = ?
           AND source_system = ? AND source_instance = ?`,
      )
      .get(
        observation.tenant_id,
        observation.subject.type,
        observation.subject.id,
        observation.data.relation,
        observation.data.target.type,
        observation.data.target.id,
        observation.source.system,
        observation.source.instance,
      ) as { occurred_at: string } | undefined;

    if (existing && existing.occurred_at > observation.occurred_at) return;

    this.db
      .prepare(
        `INSERT INTO relations (
           tenant_id, subject_type, subject_id, relation, target_type, target_id,
           source_system, source_instance, status, attributes_json, occurred_at,
           observation_id, evidence_json
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT (
           tenant_id, subject_type, subject_id, relation,
           target_type, target_id, source_system, source_instance
         ) DO UPDATE SET
           status = excluded.status,
           attributes_json = excluded.attributes_json,
           occurred_at = excluded.occurred_at,
           observation_id = excluded.observation_id,
           evidence_json = excluded.evidence_json`,
      )
      .run(
        observation.tenant_id,
        observation.subject.type,
        observation.subject.id,
        observation.data.relation,
        observation.data.target.type,
        observation.data.target.id,
        observation.source.system,
        observation.source.instance,
        observation.data.status,
        JSON.stringify(observation.data.attributes ?? {}),
        observation.occurred_at,
        observation.observation_id,
        JSON.stringify(observation.evidence ?? [observationEvidence(observation)]),
      );
  }

  private projectState(
    observation: Extract<Observation, { kind: "state.observed" }>,
    receivedAt: string,
  ): void {
    const existing = this.db
      .prepare(
        `SELECT occurred_at, source_sequence, received_at FROM states
         WHERE tenant_id = ? AND subject_type = ? AND subject_id = ? AND field = ?`,
      )
      .get(
        observation.tenant_id,
        observation.subject.type,
        observation.subject.id,
        observation.data.field,
      ) as
      | { occurred_at: string; source_sequence: number | null; received_at: string }
      | undefined;

    if (
      existing &&
      compareProjectionClock(
        existing,
        {
          occurred_at: observation.occurred_at,
          source_sequence: observation.source.sequence ?? null,
          received_at: receivedAt,
        },
      ) > 0
    ) {
      return;
    }

    const isUnset = observation.data.operation === "unset";
    this.db
      .prepare(
        `INSERT INTO states (
           tenant_id, subject_type, subject_id, field, value_json, is_unset,
           occurred_at, observed_at, received_at, source_system, source_instance,
           source_sequence, observation_id, evidence_json
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT (tenant_id, subject_type, subject_id, field) DO UPDATE SET
           value_json = excluded.value_json,
           is_unset = excluded.is_unset,
           occurred_at = excluded.occurred_at,
           observed_at = excluded.observed_at,
           received_at = excluded.received_at,
           source_system = excluded.source_system,
           source_instance = excluded.source_instance,
           source_sequence = excluded.source_sequence,
           observation_id = excluded.observation_id,
           evidence_json = excluded.evidence_json`,
      )
      .run(
        observation.tenant_id,
        observation.subject.type,
        observation.subject.id,
        observation.data.field,
        isUnset ? null : JSON.stringify(observation.data.value),
        isUnset ? 1 : 0,
        observation.occurred_at,
        observation.observed_at,
        receivedAt,
        observation.source.system,
        observation.source.instance,
        observation.source.sequence ?? null,
        observation.observation_id,
        JSON.stringify(observation.evidence ?? [observationEvidence(observation)]),
      );
  }

  private projectMetric(
    observation: Extract<Observation, { kind: "metric.observed" }>,
  ): void {
    const dimensions = observation.data.dimensions ?? {};
    this.db
      .prepare(
        `INSERT INTO metrics (
           tenant_id, observation_id, subject_type, subject_id, metric, value,
           unit, occurred_at, interval_from, interval_to, dimensions_json,
           dimensions_hash, aggregation, calculation_json, evidence_json
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
      )
      .run(
        observation.tenant_id,
        observation.observation_id,
        observation.subject.type,
        observation.subject.id,
        observation.data.metric,
        observation.data.value,
        observation.data.unit,
        observation.occurred_at,
        observation.data.interval?.from ?? null,
        observation.data.interval?.to ?? null,
        JSON.stringify(dimensions),
        contentHash(dimensions),
        observation.data.aggregation ?? null,
        observation.data.calculation
          ? JSON.stringify(observation.data.calculation)
          : null,
        JSON.stringify(observation.evidence ?? [observationEvidence(observation)]),
      );
  }

  private evaluateMetricSignals(
    observation: Extract<Observation, { kind: "metric.observed" }>,
  ): string[] {
    const dimensions = observation.data.dimensions ?? {};
    const dimensionsHash = contentHash(dimensions);
    const rules = this.matchingSignalRules(
      observation.tenant_id,
      observation.data.metric,
      observation.subject.type,
      dimensions,
    );
    if (!rules.length) return [];

    const requiredPoints = Math.max(
      2,
      ...rules.map((rule) => rule.condition.for_samples + 1),
    );
    const rows = this.db
      .prepare(
        `SELECT observation_id, value, unit, occurred_at, dimensions_json
         FROM metrics
         WHERE tenant_id = ? AND subject_type = ? AND subject_id = ?
           AND metric = ? AND dimensions_hash = ?
         ORDER BY occurred_at DESC, observation_id DESC
         LIMIT ?`,
      )
      .all(
        observation.tenant_id,
        observation.subject.type,
        observation.subject.id,
        observation.data.metric,
        dimensionsHash,
        requiredPoints,
      ) as Array<{
      observation_id: string;
      value: number;
      unit: string;
      occurred_at: string;
      dimensions_json: string;
    }>;
    if (rows[0]?.observation_id !== observation.observation_id) return [];

    const points: MetricPoint[] = rows.map((row) => ({
      observation_id: row.observation_id,
      value: row.value,
      unit: row.unit,
      occurred_at: row.occurred_at,
      dimensions: parseJson(row.dimensions_json, {}),
    }));
    const changedSignals: string[] = [];
    for (const rule of rules) {
      const evaluation = evaluateRule(rule, points);
      const signalId = stableId(
        "sig",
        observation.tenant_id,
        rule.rule_id,
        observation.subject.type,
        observation.subject.id,
        observation.data.metric,
        dimensionsHash,
      );
      const existing = this.getSignal(observation.tenant_id, signalId);
      const now = new Date().toISOString();

      if (!evaluation.matched) {
        if (existing?.status === "open" || existing?.status === "acknowledged") {
          this.saveSignal({ ...existing, status: "resolved", updated_at: now });
          changedSignals.push(signalId);
        }
        continue;
      }

      const evidence = evaluation.evidenceObservationIds.map((observationId) => ({
        type: "observation" as const,
        ref: observationUri(observation.tenant_id, observationId),
      }));
      const policies = evaluation.evidenceObservationIds.map((observationId) =>
        this.getObservationAccess(observation.tenant_id, observationId),
      );
      const title =
        rule.title ??
        defaultSignalTitle(
          observation.data.metric,
          rule,
          evaluation.deltaPercent,
          evaluation.current,
          observation.data.unit,
        );
      const signal: Signal = {
        signal_id: signalId,
        tenant_id: observation.tenant_id,
        type: rule.signal_type,
        subject: observation.subject,
        detected_at: existing?.detected_at ?? now,
        updated_at: now,
        status: "open",
        severity: rule.severity,
        title,
        description: `${rule.name}. ${evaluation.definition}`,
        confidence: rule.confidence,
        ...(evaluation.current === undefined
          ? {}
          : {
              change: {
                direction: evaluation.direction ?? "changed",
                current: evaluation.current,
                ...(evaluation.baseline === undefined
                  ? {}
                  : { baseline: evaluation.baseline }),
                ...(evaluation.delta === undefined ? {} : { delta: evaluation.delta }),
                ...(evaluation.deltaPercent === undefined
                  ? {}
                  : { delta_percent: evaluation.deltaPercent }),
                unit: observation.data.unit,
              },
            }),
        detection: {
          method: "rule",
          definition: `${rule.rule_id}: ${evaluation.definition}`,
          evaluated_at: now,
        },
        evidence,
        access: combinePolicies(policies),
      };
      this.saveSignal(signal);
      changedSignals.push(signalId);
    }
    return changedSignals;
  }

  private matchingSignalRules(
    tenantId: string,
    metric: string,
    subjectType: string,
    dimensions: Record<string, string>,
  ): SignalRule[] {
    const configured = this.listSignalRules(tenantId, false);
    const effective = configured.length
      ? configured
      : [defaultSignificantIncreaseRule()];
    return effective.filter((rule) =>
      ruleApplies(rule, { metric, subjectType, dimensions }),
    );
  }

  listSignalRules(tenantId: string, includeDefault = true): SignalRule[] {
    const rows = this.db
      .prepare(
        `SELECT payload_json FROM signal_rules
         WHERE tenant_id = ? ORDER BY rule_id`,
      )
      .all(tenantId) as Array<{ payload_json: string }>;
    const rules = rows.map((row) =>
      signalRuleSchema.parse(JSON.parse(row.payload_json)),
    );
    return includeDefault && rules.length === 0
      ? [defaultSignificantIncreaseRule()]
      : rules;
  }

  upsertSignalRule(tenantId: string, candidate: unknown): SignalRule {
    const rule = signalRuleSchema.parse(candidate);
    const now = new Date().toISOString();
    const transaction = this.db.transaction(() => {
      this.db
        .prepare(
          `INSERT INTO signal_rules (
             tenant_id, rule_id, enabled, metric, payload_json, created_at, updated_at
           ) VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT (tenant_id, rule_id) DO UPDATE SET
             enabled = excluded.enabled,
             metric = excluded.metric,
             payload_json = excluded.payload_json,
             updated_at = excluded.updated_at`,
        )
        .run(
          tenantId,
          rule.rule_id,
          rule.enabled ? 1 : 0,
          rule.applies_to.metric,
          JSON.stringify(rule),
          now,
          now,
        );
      this.reevaluateAllSignals(tenantId);
    });
    transaction();
    return rule;
  }

  deleteSignalRule(tenantId: string, ruleId: string): boolean {
    let changed = false;
    const transaction = this.db.transaction(() => {
      changed =
        this.db
          .prepare(`DELETE FROM signal_rules WHERE tenant_id = ? AND rule_id = ?`)
          .run(tenantId, ruleId).changes > 0;
      if (changed) this.reevaluateAllSignals(tenantId);
    });
    transaction();
    return changed;
  }

  private reevaluateAllSignals(tenantId: string): void {
    this.db.prepare(`DELETE FROM signals WHERE tenant_id = ?`).run(tenantId);
    const rows = this.db
      .prepare(
        `SELECT payload_json FROM (
           SELECT o.payload_json,
                  ROW_NUMBER() OVER (
                    PARTITION BY m.subject_type, m.subject_id, m.metric, m.dimensions_hash
                    ORDER BY m.occurred_at DESC, m.observation_id DESC
                  ) AS row_number
           FROM metrics m
           JOIN observations o
             ON o.tenant_id = m.tenant_id
            AND o.observation_id = m.observation_id
           WHERE m.tenant_id = ? AND o.invalidated_at IS NULL
         ) ranked
         WHERE row_number = 1`,
      )
      .all(tenantId) as Array<{ payload_json: string }>;
    for (const row of rows) {
      const observation = JSON.parse(row.payload_json) as Observation;
      if (observation.kind === "metric.observed") {
        this.evaluateMetricSignals(observation);
      }
    }
  }

  private saveSignal(signal: Signal): void {
    this.db
      .prepare(
        `INSERT INTO signals (
           tenant_id, signal_id, type, subject_type, subject_id, status, severity,
           detected_at, updated_at, payload_json
         ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
         ON CONFLICT (tenant_id, signal_id) DO UPDATE SET
           type = excluded.type,
           subject_type = excluded.subject_type,
           subject_id = excluded.subject_id,
           status = excluded.status,
           severity = excluded.severity,
           updated_at = excluded.updated_at,
           payload_json = excluded.payload_json`,
      )
      .run(
        signal.tenant_id,
        signal.signal_id,
        signal.type,
        signal.subject.type,
        signal.subject.id,
        signal.status,
        signal.severity,
        signal.detected_at,
        signal.updated_at,
        JSON.stringify(signal),
      );
  }

  getSignal(
    tenantId: string,
    signalId: string,
    consumer: ConsumerContext = systemConsumer,
  ): Signal | undefined {
    const row = this.db
      .prepare(
        `SELECT payload_json FROM signals WHERE tenant_id = ? AND signal_id = ?`,
      )
      .get(tenantId, signalId) as { payload_json: string } | undefined;
    if (!row) return undefined;
    const signal = JSON.parse(row.payload_json) as Signal;
    return this.canReadSignal(signal, consumer) ? signal : undefined;
  }

  private canReadSignal(signal: Signal, consumer: ConsumerContext): boolean {
    if (!canRead(signal.access, consumer)) return false;
    return signal.evidence.every((evidence) => {
      const parsed = parseObservationUri(evidence.ref);
      // A ref this runtime cannot interpret is fail-closed: there is no way to
      // prove the consumer may read what it points at, and treating it as
      // readable would make any malformed evidence visible to whoever can read
      // the Signal's own policy.
      if (!parsed) return false;
      return (
        parsed.tenantId === signal.tenant_id &&
        this.canReadObservation(signal.tenant_id, parsed.observationId, consumer)
      );
    });
  }

  getEntity(
    tenantId: string,
    subject: EntityRef,
    consumer: ConsumerContext = systemConsumer,
  ): JsonRecord | undefined {
    const row = this.db
      .prepare(
        `SELECT * FROM entities WHERE tenant_id = ? AND type = ? AND id = ?`,
      )
      .get(tenantId, subject.type, subject.id) as JsonRecord | undefined;
    if (!row) return undefined;
    const fieldSources = parseJson<Record<string, string>>(
      row.field_sources_json as string,
      {},
    );
    const clocks = parseJson<Record<string, string>>(row.clock_json as string, {});
    const isFieldVisible = (field: string): boolean => {
      const observationId = fieldSources[field];
      // Fail closed. A field with no provenance has no Observation to authorize
      // it, and falling back to the entity's latest changing observation would
      // authorize it with an Observation that never carried the value. That
      // fallback was reachable on a database upgraded from before
      // `field_sources_json` existed — every row starts at `{}` — and it
      // revealed restricted fields to consumers that could not read the
      // Observation that wrote them.
      if (!observationId) return false;
      return this.canReadObservation(tenantId, observationId, consumer);
    };
    const attributes = parseJson<JsonRecord>(row.attributes_json as string, {});
    const visibleAttributes = Object.fromEntries(
      Object.entries(attributes).filter(([key]) => isFieldVisible(`attribute:${key}`)),
    );
    const visibleUpdatedAt = Object.entries(clocks)
      .filter(([field]) => isFieldVisible(field))
      .map(([, timestamp]) => timestamp)
      .sort()
      .at(-1);
    const baseVisible = this.canReadObservation(
      tenantId,
      String(row.observation_id),
      consumer,
    );
    if (
      !baseVisible &&
      !isFieldVisible("name") &&
      !isFieldVisible("lifecycle") &&
      Object.keys(visibleAttributes).length === 0
    ) {
      return undefined;
    }
    return {
      type: row.type,
      id: row.id,
      ...(isFieldVisible("name") ? { name: row.name } : {}),
      ...(isFieldVisible("lifecycle") ? { lifecycle: row.lifecycle } : {}),
      attributes: visibleAttributes,
      updated_at: visibleUpdatedAt ?? row.updated_at,
      evidence: baseVisible ? parseJson(row.evidence_json as string, []) : [],
    };
  }

  getStates(
    tenantId: string,
    subject: EntityRef,
    consumer: ConsumerContext = systemConsumer,
  ): JsonRecord[] {
    const rows = this.db
      .prepare(
        `SELECT * FROM states
         WHERE tenant_id = ? AND subject_type = ? AND subject_id = ?
         ORDER BY field`,
      )
      .all(tenantId, subject.type, subject.id) as JsonRecord[];
    return rows
      .filter((row) =>
        this.canReadObservation(
          tenantId,
          String(row.observation_id),
          consumer,
        ),
      )
      .map((row) => ({
      field: row.field,
      value: row.is_unset ? null : parseJson(row.value_json as string, null),
      is_unset: Boolean(row.is_unset),
      occurred_at: row.occurred_at,
      observed_at: row.observed_at,
      source: { system: row.source_system, instance: row.source_instance },
      evidence: parseJson(row.evidence_json as string, []),
      }));
  }

  getRelations(
    tenantId: string,
    subject: EntityRef,
    consumer: ConsumerContext = systemConsumer,
  ): JsonRecord[] {
    const outgoing = this.db
      .prepare(
        `SELECT * FROM relations
         WHERE tenant_id = ? AND subject_type = ? AND subject_id = ? AND status = 'active'
         ORDER BY relation, target_type, target_id`,
      )
      .all(tenantId, subject.type, subject.id) as JsonRecord[];
    const incoming = this.db
      .prepare(
        `SELECT * FROM relations
         WHERE tenant_id = ? AND target_type = ? AND target_id = ? AND status = 'active'
         ORDER BY relation, subject_type, subject_id`,
      )
      .all(tenantId, subject.type, subject.id) as JsonRecord[];
    return [
      ...outgoing
        .filter((row) =>
          this.canReadObservation(
            tenantId,
            String(row.observation_id),
            consumer,
          ),
        )
        .map((row) => relationRow(row, "outgoing")),
      ...incoming
        .filter((row) =>
          this.canReadObservation(
            tenantId,
            String(row.observation_id),
            consumer,
          ),
        )
        .map((row) => relationRow(row, "incoming")),
    ];
  }

  traverseGraph(
    tenantId: string,
    root: EntityRef,
    options: GraphOptions,
    consumer: ConsumerContext = systemConsumer,
  ): GraphResult {
    const rootEntity = this.getEntity(tenantId, root, consumer);
    if (!rootEntity) return { nodes: [], edges: [], truncated: false };

    const nodes: EntityRef[] = [root];
    const edges: JsonRecord[] = [];
    const visited = new Set([entityKey(root)]);
    const edgeKeys = new Set<string>();
    const queue: Array<{ entity: EntityRef; depth: number }> = [
      { entity: root, depth: 0 },
    ];
    let truncated = false;

    while (queue.length) {
      const current = queue.shift()!;
      if (current.depth >= options.maxDepth) continue;
      const relations = this.getRelations(tenantId, current.entity, consumer).filter(
        (relation) => {
          if (
            options.relations?.length &&
            !options.relations.includes(String(relation.relation))
          ) {
            return false;
          }
          return (
            options.direction === "both" ||
            relation.direction === options.direction
          );
        },
      );

      for (const relation of relations) {
        const subject = relation.subject as EntityRef;
        const target = relation.target as EntityRef;
        const neighbor = relation.direction === "outgoing" ? target : subject;
        if (!this.getEntity(tenantId, neighbor, consumer)) continue;
        const key = entityKey(neighbor);
        if (!visited.has(key)) {
          if (nodes.length >= options.maxNodes) {
            truncated = true;
            continue;
          }
          visited.add(key);
          nodes.push(neighbor);
          queue.push({ entity: neighbor, depth: current.depth + 1 });
        }
        // Emitted only once both endpoints are in `nodes`. Pushing the edge
        // before the cap check produced results containing an edge to a node the
        // caller was never given.
        const edgeKey = `${entityKey(subject)}|${String(relation.relation)}|${entityKey(target)}`;
        if (!edgeKeys.has(edgeKey)) {
          edgeKeys.add(edgeKey);
          edges.push(relation);
        }
      }
    }

    return { nodes, edges, truncated };
  }

  getSignals(
    tenantId: string,
    subject?: EntityRef,
    status?: string,
    consumer: ConsumerContext = systemConsumer,
  ): Signal[] {
    let rows: Array<{ payload_json: string }>;
    if (subject && status) {
      rows = this.db
        .prepare(
          `SELECT payload_json FROM signals
           WHERE tenant_id = ? AND subject_type = ? AND subject_id = ? AND status = ?
           ORDER BY updated_at DESC`,
        )
        .all(tenantId, subject.type, subject.id, status) as Array<{
        payload_json: string;
      }>;
    } else if (subject) {
      rows = this.db
        .prepare(
          `SELECT payload_json FROM signals
           WHERE tenant_id = ? AND subject_type = ? AND subject_id = ?
           ORDER BY updated_at DESC`,
        )
        .all(tenantId, subject.type, subject.id) as Array<{ payload_json: string }>;
    } else if (status) {
      rows = this.db
        .prepare(
          `SELECT payload_json FROM signals
           WHERE tenant_id = ? AND status = ?
           ORDER BY updated_at DESC`,
        )
        .all(tenantId, status) as Array<{ payload_json: string }>;
    } else {
      rows = this.db
        .prepare(
          `SELECT payload_json FROM signals
           WHERE tenant_id = ? ORDER BY updated_at DESC`,
        )
        .all(tenantId) as Array<{ payload_json: string }>;
    }
    return rows
      .map((row) => JSON.parse(row.payload_json) as Signal)
      .filter((signal) => this.canReadSignal(signal, consumer));
  }

  findEvidence(
    tenantId: string,
    ref: string,
    consumer: ConsumerContext = systemConsumer,
  ): EvidenceRef | undefined {
    const rows = this.db
      .prepare(
        `SELECT observation_id, evidence_json FROM observations
         WHERE tenant_id = ? AND evidence_json LIKE ? ESCAPE '\\'
           AND invalidated_at IS NULL
         ORDER BY received_at DESC LIMIT 100`,
      )
      .all(tenantId, `%${escapeLikeFragment(ref)}%`) as Array<{
      observation_id: string;
      evidence_json: string;
    }>;
    for (const row of rows) {
      if (!this.canReadObservation(tenantId, row.observation_id, consumer)) continue;
      const evidence = parseJson<EvidenceRef[]>(row.evidence_json, []);
      const match = evidence.find((item) => item.ref === ref);
      if (match) return match;
    }
    return undefined;
  }

  getRecentMetricChanges(
    tenantId: string,
    subject: EntityRef,
    consumer: ConsumerContext = systemConsumer,
    window?: { from: string; to: string },
  ): JsonRecord[] {
    const series = this.db
      .prepare(
        `SELECT DISTINCT metric, dimensions_hash
         FROM metrics
         WHERE tenant_id = ? AND subject_type = ? AND subject_id = ?`,
      )
      .all(tenantId, subject.type, subject.id) as Array<{
      metric: string;
      dimensions_hash: string;
    }>;

    return series.flatMap(({ metric, dimensions_hash }) => {
      const rows = this.db
        .prepare(
          `SELECT * FROM metrics
           WHERE tenant_id = ? AND subject_type = ? AND subject_id = ?
             AND metric = ? AND dimensions_hash = ?
           ORDER BY occurred_at DESC, observation_id DESC LIMIT 100`,
        )
        .all(
          tenantId,
          subject.type,
          subject.id,
          metric,
          dimensions_hash,
        ) as JsonRecord[];
      const visibleRows = rows.filter(
        (row) =>
          (!window ||
            (String(row.occurred_at) >= window.from &&
              String(row.occurred_at) <= window.to)) &&
          this.canReadObservation(
            tenantId,
            String(row.observation_id),
            consumer,
          ),
      );
      if (visibleRows.length < 2) return [];
      const current = visibleRows[0]!;
      const baseline = visibleRows[1]!;
      const currentValue = Number(current.value);
      const baselineValue = Number(baseline.value);
      return [
        {
          metric,
          dimensions: parseJson(current.dimensions_json as string, {}),
          current: currentValue,
          baseline: baselineValue,
          delta: currentValue - baselineValue,
          delta_percent:
            baselineValue === 0
              ? null
              : ((currentValue - baselineValue) / baselineValue) * 100,
          unit: current.unit,
          occurred_at: current.occurred_at,
          evidence: [
            { type: "observation", ref: observationUri(tenantId, String(current.observation_id)) },
            { type: "observation", ref: observationUri(tenantId, String(baseline.observation_id)) },
          ],
        },
      ];
    });
  }

  timeline(
    tenantId: string,
    subject: EntityRef,
    from: string,
    to: string,
    types: string[] | undefined,
    limit: number,
    offset: number,
    consumer: ConsumerContext = systemConsumer,
  ): TimelinePage {
    const items: JsonRecord[] = [];
    let scanOffset = offset;
    let scanned = 0;
    const batchSize = Math.max(100, Math.min(1000, limit * 5));
    const maxScan = 10_000;

    while (scanned < maxScan) {
      const rows = this.db
        .prepare(
          `SELECT observation_id, kind, occurred_at, observed_at, source_system,
                  source_instance, data_json, evidence_json
           FROM observations
           WHERE tenant_id = ? AND subject_type = ? AND subject_id = ?
             AND occurred_at >= ? AND occurred_at <= ?
             AND kind IN ('event.occurred', 'state.observed')
             AND invalidated_at IS NULL
           ORDER BY occurred_at ASC, observation_id ASC
           LIMIT ? OFFSET ?`,
        )
        .all(
          tenantId,
          subject.type,
          subject.id,
          from,
          to,
          batchSize,
          scanOffset,
        ) as JsonRecord[];
      if (!rows.length) return { items, truncated: false };

      for (let index = 0; index < rows.length; index += 1) {
        const row = rows[index]!;
        const nextRawOffset = scanOffset + index;
        if (
          !this.canReadObservation(
            tenantId,
            String(row.observation_id),
            consumer,
          )
        ) {
          continue;
        }
        const data = parseJson<JsonRecord>(row.data_json as string, {});
        const semanticType = row.kind === "event.occurred" ? data.type : data.field;
        if (types?.length && !types.includes(String(semanticType))) continue;
        if (items.length >= limit) {
          return { items, truncated: true, nextOffset: nextRawOffset };
        }
        items.push({
          observation_id: row.observation_id,
          kind: row.kind,
          occurred_at: row.occurred_at,
          observed_at: row.observed_at,
          source: { system: row.source_system, instance: row.source_instance },
          data,
          evidence: parseJson(row.evidence_json as string, []),
        });
      }
      scanOffset += rows.length;
      scanned += rows.length;
      if (rows.length < batchSize) return { items, truncated: false };
    }

    return { items, truncated: true, nextOffset: scanOffset };
  }

  listEntities(
    tenantId: string,
    type?: string,
    consumer: ConsumerContext = systemConsumer,
  ): JsonRecord[] {
    const rows = type
      ? (this.db
          .prepare(
            `SELECT * FROM entities WHERE tenant_id = ? AND type = ? ORDER BY updated_at DESC LIMIT 1000`,
          )
          .all(tenantId, type) as JsonRecord[])
      : (this.db
          .prepare(
            `SELECT * FROM entities WHERE tenant_id = ? ORDER BY updated_at DESC LIMIT 1000`,
          )
          .all(tenantId) as JsonRecord[]);
    return rows
      .map((row) =>
        this.getEntity(
          tenantId,
          { type: String(row.type), id: String(row.id) },
          consumer,
        ),
      )
      .filter((row): row is JsonRecord => Boolean(row));
  }

  metricsInWindow(
    tenantId: string,
    subject: EntityRef,
    metric: string,
    from: string,
    to: string,
    filters: Record<string, string>,
    consumer: ConsumerContext = systemConsumer,
  ): StoredMetric[] {
    const rows = this.db
      .prepare(
        `SELECT * FROM metrics
         WHERE tenant_id = ? AND subject_type = ? AND subject_id = ?
           AND metric = ? AND occurred_at >= ? AND occurred_at <= ?
         ORDER BY occurred_at`,
      )
      .all(tenantId, subject.type, subject.id, metric, from, to) as JsonRecord[];
    return rows
      .map((row) => ({
        observation_id: String(row.observation_id),
        metric: String(row.metric),
        value: Number(row.value),
        unit: String(row.unit),
        occurred_at: String(row.occurred_at),
        interval_from: row.interval_from ? String(row.interval_from) : null,
        interval_to: row.interval_to ? String(row.interval_to) : null,
        dimensions: parseJson<Record<string, string>>(
          row.dimensions_json as string,
          {},
        ),
        evidence: parseJson<EvidenceRef[]>(row.evidence_json as string, []),
      }))
      .filter(
        (row) =>
          this.canReadObservation(tenantId, row.observation_id, consumer) &&
          Object.entries(filters).every(
            ([key, value]) => row.dimensions[key] === value,
          ),
      );
  }

  latestWatermark(
    tenantId: string,
    consumer: ConsumerContext = systemConsumer,
  ): string {
    const rows = this.db
      .prepare(
        `SELECT observation_id, received_at FROM observations
         WHERE tenant_id = ? AND invalidated_at IS NULL
         ORDER BY received_at DESC LIMIT 1000`,
      )
      .all(tenantId) as Array<{ observation_id: string; received_at: string }>;
    return (
      rows.find((row) =>
        this.canReadObservation(tenantId, row.observation_id, consumer),
      )?.received_at ?? new Date(0).toISOString()
    );
  }
}

function parseJson<T>(value: string | undefined | null, fallback: T): T {
  if (value === undefined || value === null) return fallback;
  return JSON.parse(value) as T;
}

function maxTimestamp(a: string | undefined, b: string): string {
  return !a || a < b ? b : a;
}

function compareProjectionClock(
  a: { occurred_at: string; source_sequence: number | null; received_at: string },
  b: { occurred_at: string; source_sequence: number | null; received_at: string },
): number {
  if (a.occurred_at !== b.occurred_at) return a.occurred_at > b.occurred_at ? 1 : -1;
  if (a.source_sequence !== null && b.source_sequence !== null) {
    if (a.source_sequence !== b.source_sequence) {
      return a.source_sequence > b.source_sequence ? 1 : -1;
    }
  }
  if (a.received_at === b.received_at) return 0;
  return a.received_at > b.received_at ? 1 : -1;
}

function observationUri(tenantId: string, observationId: string): string {
  return `sensus://observation/${encodeURIComponent(tenantId)}/${encodeURIComponent(observationId)}`;
}

function parseObservationUri(
  ref: string,
): { tenantId: string; observationId: string } | undefined {
  const match = ref.match(/^sensus:\/\/observation\/([^/]+)\/([^/]+)$/);
  return match
    ? {
        tenantId: decodeURIComponent(match[1]!),
        observationId: decodeURIComponent(match[2]!),
      }
    : undefined;
}

function observationEvidence(observation: Observation): EvidenceRef {
  return {
    type: "observation",
    ref: observationUri(observation.tenant_id, observation.observation_id),
  };
}

function relationRow(row: JsonRecord, direction: "incoming" | "outgoing"): JsonRecord {
  return {
    direction,
    relation: row.relation,
    subject: { type: row.subject_type, id: row.subject_id },
    target: { type: row.target_type, id: row.target_id },
    attributes: parseJson(row.attributes_json as string, {}),
    occurred_at: row.occurred_at,
    evidence: parseJson(row.evidence_json as string, []),
  };
}

/**
 * Escapes a value for use inside a `LIKE` pattern with `ESCAPE '\'`.
 *
 * The escape character itself has to be escaped first: a ref containing a
 * backslash would otherwise turn the following character into an escape
 * sequence, so `C:\reports\x` searched for `%reportsx%` and matched nothing.
 * PostgreSQL's implementation always did this; this one did not.
 */
function escapeLikeFragment(value: string): string {
  return value
    .replaceAll("\\", "\\\\")
    .replaceAll("%", "\\%")
    .replaceAll("_", "\\_");
}

function entityKey(entity: EntityRef): string {
  return `${entity.type}\u001f${entity.id}`;
}

function defaultSignalTitle(
  metric: string,
  rule: SignalRule,
  deltaPercent: number | undefined,
  current: number | undefined,
  unit: string,
): string {
  if (rule.condition.kind === "relative_change" && deltaPercent !== undefined) {
    const verb = deltaPercent >= 0 ? "increased" : "decreased";
    return `${metric} ${verb} ${Math.abs(deltaPercent).toFixed(1)}%`;
  }
  return `${metric} reached ${current ?? "an alerting value"} ${unit}`;
}

function isSqliteConstraint(error: unknown): boolean {
  return (
    error instanceof Error &&
    "code" in error &&
    typeof (error as { code?: unknown }).code === "string" &&
    String((error as { code: string }).code).startsWith("SQLITE_CONSTRAINT")
  );
}

/** The `(tenant_id, observation_id)` primary key, specifically. */
function isSqlitePrimaryKeyViolation(error: unknown): boolean {
  return (
    error instanceof Error &&
    "code" in error &&
    (error as { code?: unknown }).code === "SQLITE_CONSTRAINT_PRIMARYKEY"
  );
}
