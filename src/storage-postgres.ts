import { Pool, type PoolClient } from "pg";
import {
  combinePolicies,
  canRead,
  systemConsumer,
  type AccessPolicy,
  type ConsumerContext,
} from "./access.js";
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
  defaultSignificantIncreaseRule,
  evaluateRule,
  ruleApplies,
  signalRuleSchema,
  type MetricPoint,
  type SignalRule,
} from "./signal-rules.js";
import {
  CorrectionError,
  ObservationConflictError,
  SyncError,
  type CompleteSyncInput,
  type GraphOptions,
  type GraphResult,
  type IngestOptions,
  type IngestResult,
  type StartSyncInput,
  type TimelinePage,
} from "./store.js";
import type { JsonRecord, SensusStorage, StoredMetric } from "./storage.js";

type Row = Record<string, unknown>;

/**
 * PostgreSQL implementation of {@link SensusStorage}.
 *
 * ## Why every timestamp column is TEXT
 *
 * Timestamps stay ISO 8601 strings rather than becoming `timestamptz`, because
 * the contract is defined by SQLite's behaviour: ordering, the three-clock
 * comparison and the replay order all compare these values. Handing them to
 * Postgres as timestamps would introduce a normalization step — UTC conversion,
 * microsecond rounding — and the two backends would then disagree on ties. As
 * TEXT, both compare identically and the conformance suite is meaningful.
 *
 * ## Why JSON columns are TEXT
 *
 * `canonicalJson` and `contentHash` depend on the exact serialization of a
 * payload. Storing JSON as TEXT preserves the original bytes, so a payload
 * round-trips unchanged and idempotency keeps working across restarts. A `jsonb`
 * column would reorder keys and silently change the hash.
 *
 * ## Transactions
 *
 * Every method that mutates takes a `PoolClient`, so nested work (a sync
 * completion that ingests, a correction that rebuilds) shares one transaction
 * instead of opening a second one. That is what makes Invariant 1 from
 * {@link SensusStorage} hold under composition.
 */
export class PostgresStorage implements SensusStorage {
  private readonly pool: Pool;

  constructor(connectionString: string) {
    this.pool = new Pool({
      connectionString,
      // Bounded, so a saturated or unreachable database fails a request instead
      // of queueing it forever behind the default pool of 10 with no connect
      // timeout.
      max: Number(process.env.SENSUS_PG_POOL_MAX ?? 10),
      connectionTimeoutMillis: 10_000,
      idleTimeoutMillis: 30_000,
    });
    // `pg` emits this on an idle client's error. Without a listener Node treats
    // it as an unhandled 'error' event and takes the process down.
    this.pool.on("error", (error) => {
      console.error("PostgreSQL pool error on an idle client:", error);
    });
  }

  static async open(connectionString: string): Promise<PostgresStorage> {
    const storage = new PostgresStorage(connectionString);
    await storage.migrate();
    return storage;
  }

  /**
   * Drops every table and re-migrates. Test-only.
   *
   * Named so it cannot be reached by muscle memory: this is a `DROP TABLE ...
   * CASCADE` on the production class, and a call against a real deployment
   * erases every tenant.
   */
  async resetForTesting(): Promise<void> {
    await this.pool.query(`
      DROP TABLE IF EXISTS
        sync_members, syncs, source_entities, signals, signal_rules,
        metrics, states, relations, entities, observations
      CASCADE
    `);
    await this.migrate();
  }

  async migrate(): Promise<void> {
    await this.pool.query(`
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
        confidence DOUBLE PRECISION,
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
        value DOUBLE PRECISION NOT NULL,
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
  }

  async close(): Promise<void> {
    await this.pool.end();
  }

  // ------------------------------------------------------------- transactions

  private async transaction<T>(
    work: (client: PoolClient) => Promise<T>,
  ): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query("BEGIN");
      const result = await work(client);
      await client.query("COMMIT");
      return result;
    } catch (error) {
      await client.query("ROLLBACK").catch(() => undefined);
      throw error;
    } finally {
      client.release();
    }
  }

  private async rows(client: PoolClient | Pool, sql: string, values: unknown[]): Promise<Row[]> {
    const result = await client.query(sql, values);
    return result.rows as Row[];
  }

  // ---------------------------------------------------------------- ingestion

  async ingest(
    observation: Observation,
    options: IngestOptions = {},
  ): Promise<IngestResult> {
    try {
      return await this.transaction((client) =>
        this.ingestWith(client, observation, options),
      );
    } catch (error) {
      if (!isUniqueViolation(error)) throw error;
      // A concurrent writer inserted this observation_id between our existence
      // check and our insert. The failed insert waited for that writer, so the
      // row is committed and visible; the transaction is aborted, so the retry
      // has to be a fresh one. It now takes the duplicate path.
      return this.transaction((client) =>
        this.ingestWith(client, observation, options),
      );
    }
  }

  /**
   * Serializes projection work for one subject.
   *
   * `projectEntity` and `projectState` read the current row, merge in JavaScript
   * and write it back, so two concurrent writers would lose one of the updates.
   * A transaction-scoped advisory lock makes the whole read-merge-write atomic
   * per subject while leaving different subjects concurrent — which is exactly
   * the unit the projection logic assumes.
   *
   * SQLite needs none of this: one writer at a time is a stronger guarantee.
   */
  private async lockSubject(
    client: PoolClient,
    tenantId: string,
    subject: EntityRef,
  ): Promise<void> {
    await client.query("SELECT pg_advisory_xact_lock(hashtext($1), hashtext($2))", [
      tenantId,
      `${subject.type}\u001f${subject.id}`,
    ]);
  }

  async ingestMany(
    observations: Observation[],
    options: IngestOptions = {},
  ): Promise<IngestResult[]> {
    const results: IngestResult[] = [];
    for (const observation of observations) {
      results.push(await this.ingest(observation, options));
    }
    return results;
  }

  private async ingestWith(
    client: PoolClient,
    observation: Observation,
    options: IngestOptions,
  ): Promise<IngestResult> {
    const payloadJson = JSON.stringify(observation);
    const hash = contentHash(observation);
    const existing = (
      await this.rows(
        client,
        `SELECT content_hash, received_at FROM observations
         WHERE tenant_id = $1 AND observation_id = $2`,
        [observation.tenant_id, observation.observation_id],
      )
    )[0] as { content_hash: string; received_at: string } | undefined;

    if (existing) {
      if (existing.content_hash !== hash) {
        throw new ObservationConflictError(observation.observation_id);
      }
      if (options.syncId) {
        if (observation.kind === "entity.observed") {
          await this.trackSourceEntity(client, observation, options.syncId);
        }
        await this.attachToSync(client, observation, options.syncId);
        if (observation.kind === "entity.observed") {
          await this.reassertEntityPresence(client, observation, options.syncId);
        }
      }
      return {
        observation_id: observation.observation_id,
        status: "duplicate",
        received_at: existing.received_at,
        generated_signals: [],
      };
    }

    const receivedAt = new Date().toISOString();
    const generatedSignals: string[] = [];

    // Held until the transaction ends, so the projection below cannot interleave
    // with another writer touching the same subject.
    await this.lockSubject(client, observation.tenant_id, observation.subject);

    await client.query(
      `INSERT INTO observations (
         tenant_id, observation_id, spec_version, kind,
         subject_type, subject_id, occurred_at, observed_at, received_at,
         source_system, source_instance, source_record_id, source_cursor,
         source_sequence, data_json, evidence_json, access_json, confidence,
         labels_json, trace_id, sync_id, payload_json, content_hash
       ) VALUES (
         $1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,
         $19,$20,$21,$22,$23
       )`,
      [
        observation.tenant_id,
        observation.observation_id,
        observation.spec_version,
        observation.kind,
        observation.subject.type,
        observation.subject.id,
        observation.occurred_at,
        observation.observed_at,
        receivedAt,
        observation.source.system,
        observation.source.instance,
        observation.source.record_id ?? null,
        observation.source.cursor ?? null,
        observation.source.sequence ?? null,
        JSON.stringify(observation.data),
        JSON.stringify(observation.evidence ?? []),
        observation.access ? JSON.stringify(observation.access) : null,
        observation.confidence ?? null,
        observation.labels ? JSON.stringify(observation.labels) : null,
        observation.trace_id ?? null,
        options.syncId ?? null,
        payloadJson,
        hash,
      ],
    );

    switch (observation.kind) {
      case "entity.observed":
        await this.projectEntity(client, observation);
        break;
      case "relation.observed":
        await this.projectRelation(client, observation);
        break;
      case "state.observed":
        await this.projectState(client, observation, receivedAt);
        break;
      case "metric.observed":
        await this.projectMetric(client, observation);
        generatedSignals.push(...(await this.evaluateMetricSignals(client, observation)));
        break;
      case "event.occurred":
        if (observation.data.type === "sensus.observation_corrected") {
          generatedSignals.push(...(await this.applyCorrection(client, observation)));
        }
        break;
    }

    if (observation.kind === "entity.observed") {
      await this.trackSourceEntity(client, observation, options.syncId);
    }
    if (options.syncId) {
      await this.attachToSync(client, observation, options.syncId);
    }

    return {
      observation_id: observation.observation_id,
      status: "accepted",
      received_at: receivedAt,
      generated_signals: generatedSignals,
    };
  }

  // ---------------------------------------------------------- projections

  private async projectEntity(
    client: PoolClient,
    observation: Extract<Observation, { kind: "entity.observed" }>,
  ): Promise<void> {
    const existing = (
      await this.rows(
        client,
        `SELECT * FROM entities WHERE tenant_id = $1 AND type = $2 AND id = $3`,
        [observation.tenant_id, observation.subject.type, observation.subject.id],
      )
    )[0] as Row | undefined;

    const attributes = parseJson<JsonRecord>(existing?.attributes_json as string, {});
    const clocks = parseJson<Record<string, string>>(existing?.clock_json as string, {});
    const fieldSources = parseJson<Record<string, string>>(
      existing?.field_sources_json as string,
      {},
    );
    const next = {
      name: (existing?.name as string | null) ?? null,
      lifecycle: (existing?.lifecycle as string | null) ?? null,
    };
    let changed = !existing;

    const setIfNewer = (key: string, set: () => void): void => {
      const current = clocks[key];
      if (!current || current <= observation.occurred_at) {
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

    await client.query(
      `INSERT INTO entities (
         tenant_id, type, id, name, lifecycle, attributes_json, clock_json,
         field_sources_json, updated_at, observation_id, evidence_json
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
       ON CONFLICT (tenant_id, type, id) DO UPDATE SET
         name = EXCLUDED.name,
         lifecycle = EXCLUDED.lifecycle,
         attributes_json = EXCLUDED.attributes_json,
         clock_json = EXCLUDED.clock_json,
         field_sources_json = EXCLUDED.field_sources_json,
         updated_at = EXCLUDED.updated_at,
         observation_id = EXCLUDED.observation_id,
         evidence_json = EXCLUDED.evidence_json`,
      [
        observation.tenant_id,
        observation.subject.type,
        observation.subject.id,
        next.name,
        next.lifecycle,
        JSON.stringify(attributes),
        JSON.stringify(clocks),
        JSON.stringify(fieldSources),
        maxTimestamp(existing?.updated_at as string | undefined, observation.occurred_at),
        changed ? observation.observation_id : String(existing!.observation_id),
        changed
          ? JSON.stringify(observation.evidence ?? [observationEvidence(observation)])
          : String(existing!.evidence_json),
      ],
    );
  }

  private async projectRelation(
    client: PoolClient,
    observation: Extract<Observation, { kind: "relation.observed" }>,
  ): Promise<void> {
    const existing = (
      await this.rows(
        client,
        `SELECT occurred_at FROM relations
         WHERE tenant_id = $1 AND subject_type = $2 AND subject_id = $3
           AND relation = $4 AND target_type = $5 AND target_id = $6
           AND source_system = $7 AND source_instance = $8`,
        [
          observation.tenant_id,
          observation.subject.type,
          observation.subject.id,
          observation.data.relation,
          observation.data.target.type,
          observation.data.target.id,
          observation.source.system,
          observation.source.instance,
        ],
      )
    )[0] as { occurred_at: string } | undefined;

    if (existing && existing.occurred_at > observation.occurred_at) return;

    await client.query(
      `INSERT INTO relations (
         tenant_id, subject_type, subject_id, relation, target_type, target_id,
         source_system, source_instance, status, attributes_json, occurred_at,
         observation_id, evidence_json
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
       ON CONFLICT (
         tenant_id, subject_type, subject_id, relation,
         target_type, target_id, source_system, source_instance
       ) DO UPDATE SET
         status = EXCLUDED.status,
         attributes_json = EXCLUDED.attributes_json,
         occurred_at = EXCLUDED.occurred_at,
         observation_id = EXCLUDED.observation_id,
         evidence_json = EXCLUDED.evidence_json`,
      [
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
      ],
    );
  }

  private async projectState(
    client: PoolClient,
    observation: Extract<Observation, { kind: "state.observed" }>,
    receivedAt: string,
  ): Promise<void> {
    const existing = (
      await this.rows(
        client,
        `SELECT occurred_at, source_sequence, received_at FROM states
         WHERE tenant_id = $1 AND subject_type = $2 AND subject_id = $3 AND field = $4`,
        [
          observation.tenant_id,
          observation.subject.type,
          observation.subject.id,
          observation.data.field,
        ],
      )
    )[0] as
      | { occurred_at: string; source_sequence: number | null; received_at: string }
      | undefined;

    if (
      existing &&
      compareProjectionClock(existing, {
        occurred_at: observation.occurred_at,
        source_sequence: observation.source.sequence ?? null,
        received_at: receivedAt,
      }) > 0
    ) {
      return;
    }

    const isUnset = observation.data.operation === "unset";
    await client.query(
      `INSERT INTO states (
         tenant_id, subject_type, subject_id, field, value_json, is_unset,
         occurred_at, observed_at, received_at, source_system, source_instance,
         source_sequence, observation_id, evidence_json
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
       ON CONFLICT (tenant_id, subject_type, subject_id, field) DO UPDATE SET
         value_json = EXCLUDED.value_json,
         is_unset = EXCLUDED.is_unset,
         occurred_at = EXCLUDED.occurred_at,
         observed_at = EXCLUDED.observed_at,
         received_at = EXCLUDED.received_at,
         source_system = EXCLUDED.source_system,
         source_instance = EXCLUDED.source_instance,
         source_sequence = EXCLUDED.source_sequence,
         observation_id = EXCLUDED.observation_id,
         evidence_json = EXCLUDED.evidence_json`,
      [
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
      ],
    );
  }

  private async projectMetric(
    client: PoolClient,
    observation: Extract<Observation, { kind: "metric.observed" }>,
  ): Promise<void> {
    const dimensions = observation.data.dimensions ?? {};
    await client.query(
      `INSERT INTO metrics (
         tenant_id, observation_id, subject_type, subject_id, metric, value,
         unit, occurred_at, interval_from, interval_to, dimensions_json,
         dimensions_hash, aggregation, calculation_json, evidence_json
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
       ON CONFLICT (tenant_id, observation_id) DO NOTHING`,
      [
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
      ],
    );
  }

  // ----------------------------------------------------------- corrections

  private async applyCorrection(
    client: PoolClient,
    correction: Extract<Observation, { kind: "event.occurred" }>,
  ): Promise<string[]> {
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

    const target = (
      await this.rows(
        client,
        `SELECT subject_type, subject_id, kind, invalidated_by, payload_json
         FROM observations WHERE tenant_id = $1 AND observation_id = $2`,
        [correction.tenant_id, targetObservationId],
      )
    )[0] as
      | {
          subject_type: string;
          subject_id: string;
          kind: string;
          invalidated_by: string | null;
          payload_json: string;
        }
      | undefined;
    if (!target) {
      throw new CorrectionError(
        `Target observation ${targetObservationId} does not exist in the tenant`,
      );
    }
    if (target.kind === "event.occurred") {
      const payload = JSON.parse(target.payload_json) as Observation;
      if (
        payload.kind === "event.occurred" &&
        payload.data.type === "sensus.observation_corrected"
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
      const replacement = (
        await this.rows(
          client,
          `SELECT subject_type, subject_id FROM observations
           WHERE tenant_id = $1 AND observation_id = $2`,
          [correction.tenant_id, replacementObservationId],
        )
      )[0] as { subject_type: string; subject_id: string } | undefined;
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

    await client.query(
      `UPDATE observations
       SET invalidated_at = $1, invalidated_by = $2, superseded_by = $3
       WHERE tenant_id = $4 AND observation_id = $5`,
      [
        correction.occurred_at,
        correction.observation_id,
        disposition === "superseded" ? (replacementObservationId ?? null) : null,
        correction.tenant_id,
        targetObservationId,
      ],
    );

    return this.rebuildSubject(client, correction.tenant_id, {
      type: target.subject_type,
      id: target.subject_id,
    });
  }

  /**
   * Discards every projection for a subject and replays the remaining valid log
   * in the order {@link SensusStorage} requires. Returns regenerated Signal ids.
   */
  private async rebuildSubject(
    client: PoolClient,
    tenantId: string,
    subject: EntityRef,
  ): Promise<string[]> {
    for (const table of [
      "entities",
      "relations",
      "states",
      "metrics",
      "signals",
      "source_entities",
    ]) {
      const column = table === "entities" ? "type" : "subject_type";
      await client.query(
        `DELETE FROM ${table} WHERE tenant_id = $1 AND ${column} = $2 AND ${
          table === "entities" ? "id" : "subject_id"
        } = $3`,
        [tenantId, subject.type, subject.id],
      );
    }

    const rows = await this.rows(
      client,
      `SELECT payload_json, received_at, sync_id
       FROM observations
       WHERE tenant_id = $1 AND subject_type = $2 AND subject_id = $3
         AND invalidated_at IS NULL
       ORDER BY occurred_at ASC, COALESCE(source_sequence, -1) ASC,
                received_at ASC, observation_id ASC`,
      [tenantId, subject.type, subject.id],
    );

    const signals: string[] = [];
    for (const row of rows) {
      const observation = JSON.parse(String(row.payload_json)) as Observation;
      const syncId = (row.sync_id as string | null) ?? undefined;
      switch (observation.kind) {
        case "entity.observed":
          await this.projectEntity(client, observation);
          await this.trackSourceEntity(client, observation, syncId);
          break;
        case "relation.observed":
          await this.projectRelation(client, observation);
          break;
        case "state.observed":
          await this.projectState(client, observation, String(row.received_at));
          break;
        case "metric.observed":
          await this.projectMetric(client, observation);
          signals.push(...(await this.evaluateMetricSignals(client, observation)));
          break;
        case "event.occurred":
          break;
      }
    }
    return [...new Set(signals)];
  }

  // -------------------------------------------------------- reconciliation

  async startSync(input: StartSyncInput): Promise<JsonRecord> {
    if (input.authoritative_deletion && input.mode === "incremental") {
      throw new SyncError(
        "authoritative_deletion is only valid for snapshot or reconciliation syncs",
      );
    }
    const startedAt = input.started_at ?? new Date().toISOString();
    try {
      // Insert and let the primary key arbitrate. A read-then-insert would let
      // two concurrent starts both pass the check.
      await this.pool.query(
        `INSERT INTO syncs (
           tenant_id, sync_id, mode, source_system, source_instance,
           authoritative_deletion, status, started_at
         ) VALUES ($1,$2,$3,$4,$5,$6,'open',$7)`,
        [
          input.tenant_id,
          input.sync_id,
          input.mode,
          input.source.system,
          input.source.instance,
          input.authoritative_deletion ? 1 : 0,
          startedAt,
        ],
      );
    } catch (error) {
      if (isUniqueViolation(error)) {
        throw new SyncError(`Sync ${input.sync_id} already exists`);
      }
      throw error;
    }
    return (await this.getSync(input.tenant_id, input.sync_id))!;
  }

  async getSync(tenantId: string, syncId: string): Promise<JsonRecord | undefined> {
    const row = (
      await this.rows(
        this.pool,
        `SELECT * FROM syncs WHERE tenant_id = $1 AND sync_id = $2`,
        [tenantId, syncId],
      )
    )[0];
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

  async completeSync(
    tenantId: string,
    syncId: string,
    input: CompleteSyncInput,
  ): Promise<JsonRecord> {
    const completedAt = input.completed_at ?? new Date().toISOString();
    await this.transaction(async (client) => {
      const sync = (
        await this.rows(
          client,
          `SELECT * FROM syncs WHERE tenant_id = $1 AND sync_id = $2 FOR UPDATE`,
          [tenantId, syncId],
        )
      )[0];
      if (!sync) throw new SyncError(`Sync ${syncId} was not found`);
      if (sync.status !== "open") {
        throw new SyncError(`Sync ${syncId} is already ${String(sync.status)}`);
      }
      const actualCount = Number(sync.actual_record_count);
      if (input.record_count !== undefined && input.record_count !== actualCount) {
        throw new SyncError(
          `Sync count mismatch: expected ${input.record_count}, received ${actualCount}`,
        );
      }

      let deletedEntityCount = 0;
      if (Boolean(sync.authoritative_deletion)) {
        const missing = (await this.rows(
          client,
          `SELECT subject_type, subject_id FROM source_entities
           WHERE tenant_id = $1 AND source_system = $2 AND source_instance = $3
             AND present = 1
             AND (last_sync_id IS NULL OR last_sync_id <> $4)`,
          [tenantId, sync.source_system, sync.source_instance, syncId],
        )) as Array<{ subject_type: string; subject_id: string }>;

        for (const subject of missing) {
          await client.query(
            // Re-states the predicates the SELECT used. Under READ COMMITTED
            // another transaction can attach a presence Observation between the
            // two statements, and without this guard the sweep would still mark
            // that row absent — which can drop the remaining-present count to
            // zero and synthesize a deletion for an entity the source just
            // reported. SQLite gets this from its snapshot plus single writer.
            `UPDATE source_entities
             SET present = 0, last_sync_id = $1, observed_at = $2
             WHERE tenant_id = $3 AND source_system = $4 AND source_instance = $5
               AND subject_type = $6 AND subject_id = $7
               AND present = 1
               AND (last_sync_id IS NULL OR last_sync_id <> $1)`,
            [
              syncId,
              completedAt,
              tenantId,
              sync.source_system,
              sync.source_instance,
              subject.subject_type,
              subject.subject_id,
            ],
          );

          const remaining = (
            await this.rows(
              client,
              `SELECT COUNT(*) AS count FROM source_entities
               WHERE tenant_id = $1 AND subject_type = $2 AND subject_id = $3
                 AND present = 1`,
              [tenantId, subject.subject_type, subject.subject_id],
            )
          )[0] as { count: string };
          if (Number(remaining.count) > 0) continue;

          const previousAccess = await this.latestEntityAccess(
            client,
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
          await this.ingestWith(client, deletion, {});
          deletedEntityCount += 1;
        }
      }

      await client.query(
        `UPDATE syncs
         SET status = 'completed', completed_at = $1, expected_record_count = $2,
             cursor = $3, deleted_entity_count = $4
         WHERE tenant_id = $5 AND sync_id = $6`,
        [
          completedAt,
          input.record_count ?? null,
          input.cursor ?? null,
          deletedEntityCount,
          tenantId,
          syncId,
        ],
      );
    });
    return (await this.getSync(tenantId, syncId))!;
  }

  private async attachToSync(
    client: PoolClient,
    observation: Observation,
    syncId: string,
  ): Promise<void> {
    const sync = (
      await this.rows(
        client,
        `SELECT status, source_system, source_instance FROM syncs
         WHERE tenant_id = $1 AND sync_id = $2`,
        [observation.tenant_id, syncId],
      )
    )[0] as
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

    const inserted = await client.query(
      `INSERT INTO sync_members (
         tenant_id, sync_id, observation_id, subject_type, subject_id, kind
       ) VALUES ($1,$2,$3,$4,$5,$6)
       ON CONFLICT (tenant_id, sync_id, observation_id) DO NOTHING`,
      [
        observation.tenant_id,
        syncId,
        observation.observation_id,
        observation.subject.type,
        observation.subject.id,
        observation.kind,
      ],
    );
    if ((inserted.rowCount ?? 0) > 0) {
      // `status = 'open'` is re-checked here rather than trusting the read
      // above: a sync can complete in between, and the record_count guard in
      // completeSync has already run by then, so an unguarded increment would
      // inflate the counter of a closed sync without anything re-validating it.
      const counted = await client.query(
        `UPDATE syncs SET actual_record_count = actual_record_count + 1
         WHERE tenant_id = $1 AND sync_id = $2 AND status = 'open'`,
        [observation.tenant_id, syncId],
      );
      if ((counted.rowCount ?? 0) === 0) {
        throw new SyncError(`Sync ${syncId} is not open`);
      }
    }
  }

  private async trackSourceEntity(
    client: PoolClient,
    observation: Extract<Observation, { kind: "entity.observed" }>,
    syncId?: string,
  ): Promise<void> {
    if (observation.source.system === "sensus-reconciliation") return;
    await client.query(
      `INSERT INTO source_entities (
         tenant_id, source_system, source_instance, subject_type, subject_id,
         present, last_sync_id, observed_at
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
       ON CONFLICT (
         tenant_id, source_system, source_instance, subject_type, subject_id
       ) DO UPDATE SET
         present = EXCLUDED.present,
         last_sync_id = COALESCE(EXCLUDED.last_sync_id, source_entities.last_sync_id),
         observed_at = EXCLUDED.observed_at`,
      [
        observation.tenant_id,
        observation.source.system,
        observation.source.instance,
        observation.subject.type,
        observation.subject.id,
        observation.data.lifecycle === "deleted" ? 0 : 1,
        syncId ?? null,
        observation.occurred_at,
      ],
    );
  }

  private async reassertEntityPresence(
    client: PoolClient,
    observation: Extract<Observation, { kind: "entity.observed" }>,
    syncId: string,
  ): Promise<void> {
    if (observation.data.lifecycle === "deleted") return;
    const current = (
      await this.rows(
        client,
        `SELECT lifecycle FROM entities WHERE tenant_id = $1 AND type = $2 AND id = $3`,
        [observation.tenant_id, observation.subject.type, observation.subject.id],
      )
    )[0] as { lifecycle: string | null } | undefined;
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
    await this.ingestWith(client, reassertion, {});
  }

  private async latestEntityAccess(
    client: PoolClient,
    tenantId: string,
    subject: { subject_type: string; subject_id: string },
    sourceSystem: string,
    sourceInstance: string,
  ): Promise<AccessPolicy | undefined> {
    const row = (
      await this.rows(
        client,
        `SELECT access_json FROM observations
         WHERE tenant_id = $1 AND subject_type = $2 AND subject_id = $3
           AND source_system = $4 AND source_instance = $5
           AND kind = 'entity.observed' AND invalidated_at IS NULL
         ORDER BY occurred_at DESC, received_at DESC LIMIT 1`,
        [tenantId, subject.subject_type, subject.subject_id, sourceSystem, sourceInstance],
      )
    )[0] as { access_json: string | null } | undefined;
    return row
      ? parseJson<AccessPolicy | undefined>(row.access_json, undefined)
      : undefined;
  }

  // ------------------------------------------------------------- observations

  async getObservation(
    tenantId: string,
    observationId: string,
  ): Promise<Observation | undefined> {
    const row = (
      await this.rows(
        this.pool,
        `SELECT payload_json FROM observations WHERE tenant_id = $1 AND observation_id = $2`,
        [tenantId, observationId],
      )
    )[0] as { payload_json: string } | undefined;
    return row ? (JSON.parse(row.payload_json) as Observation) : undefined;
  }

  async getObservationForConsumer(
    tenantId: string,
    observationId: string,
    consumer: ConsumerContext,
  ): Promise<Observation | undefined> {
    const row = (
      await this.rows(
        this.pool,
        `SELECT payload_json, access_json, invalidated_at FROM observations
         WHERE tenant_id = $1 AND observation_id = $2`,
        [tenantId, observationId],
      )
    )[0] as
      | { payload_json: string; access_json: string | null; invalidated_at: string | null }
      | undefined;
    if (!row || row.invalidated_at) return undefined;
    const policy = parseJson<AccessPolicy | undefined>(row.access_json, undefined);
    return canRead(policy, consumer)
      ? (JSON.parse(row.payload_json) as Observation)
      : undefined;
  }

  async getObservationAccess(
    tenantId: string,
    observationId: string,
  ): Promise<AccessPolicy | undefined> {
    return this.getObservationAccessWith(this.pool, tenantId, observationId);
  }

  private async getObservationAccessWith(
    runner: PoolClient | Pool,
    tenantId: string,
    observationId: string,
  ): Promise<AccessPolicy | undefined> {
    const row = (
      await this.rows(
        runner,
        `SELECT access_json FROM observations WHERE tenant_id = $1 AND observation_id = $2`,
        [tenantId, observationId],
      )
    )[0] as { access_json: string | null } | undefined;
    return row
      ? parseJson<AccessPolicy | undefined>(row.access_json, undefined)
      : undefined;
  }

  async canReadObservation(
    tenantId: string,
    observationId: string,
    consumer: ConsumerContext,
  ): Promise<boolean> {
    const row = (
      await this.rows(
        this.pool,
        `SELECT access_json, invalidated_at FROM observations
         WHERE tenant_id = $1 AND observation_id = $2`,
        [tenantId, observationId],
      )
    )[0] as { access_json: string | null; invalidated_at: string | null } | undefined;
    if (!row || row.invalidated_at) return false;
    return canRead(
      parseJson<AccessPolicy | undefined>(row.access_json, undefined),
      consumer,
    );
  }

  // ------------------------------------------------------------- signal rules

  async listSignalRules(tenantId: string, includeDefault = true): Promise<SignalRule[]> {
    return this.listSignalRulesWith(this.pool, tenantId, includeDefault);
  }

  /**
   * Reads rules through a caller-supplied connection.
   *
   * Rule evaluation runs inside the transaction that wrote the rule, and a
   * pooled read would open a second connection that cannot see the uncommitted
   * row — the rule would appear to have no effect until the next ingest. SQLite
   * never had this failure mode because it has one connection.
   */
  private async listSignalRulesWith(
    runner: PoolClient | Pool,
    tenantId: string,
    includeDefault = true,
  ): Promise<SignalRule[]> {
    const rows = await this.rows(
      runner,
      `SELECT payload_json FROM signal_rules WHERE tenant_id = $1 ORDER BY rule_id`,
      [tenantId],
    );
    const rules = rows.map((row) =>
      signalRuleSchema.parse(JSON.parse(String(row.payload_json))),
    );
    return includeDefault && rules.length === 0
      ? [defaultSignificantIncreaseRule()]
      : rules;
  }

  async upsertSignalRule(tenantId: string, candidate: unknown): Promise<SignalRule> {
    const rule = signalRuleSchema.parse(candidate);
    const now = new Date().toISOString();
    await this.transaction(async (client) => {
      await client.query(
        `INSERT INTO signal_rules (
           tenant_id, rule_id, enabled, metric, payload_json, created_at, updated_at
         ) VALUES ($1,$2,$3,$4,$5,$6,$7)
         ON CONFLICT (tenant_id, rule_id) DO UPDATE SET
           enabled = EXCLUDED.enabled,
           metric = EXCLUDED.metric,
           payload_json = EXCLUDED.payload_json,
           updated_at = EXCLUDED.updated_at`,
        [tenantId, rule.rule_id, rule.enabled ? 1 : 0, rule.applies_to.metric, JSON.stringify(rule), now, now],
      );
      await this.reevaluateAllSignals(client, tenantId);
    });
    return rule;
  }

  async deleteSignalRule(tenantId: string, ruleId: string): Promise<boolean> {
    return this.transaction(async (client) => {
      const deleted = await client.query(
        `DELETE FROM signal_rules WHERE tenant_id = $1 AND rule_id = $2`,
        [tenantId, ruleId],
      );
      const changed = (deleted.rowCount ?? 0) > 0;
      if (changed) await this.reevaluateAllSignals(client, tenantId);
      return changed;
    });
  }

  private async matchingSignalRules(
    client: PoolClient,
    tenantId: string,
    metric: string,
    subjectType: string,
    dimensions: Record<string, string>,
  ): Promise<SignalRule[]> {
    const rules = await this.listSignalRulesWith(client, tenantId, false);
    const effective = rules.length ? rules : [defaultSignificantIncreaseRule()];
    return effective.filter((rule) =>
      ruleApplies(rule, { metric, subjectType, dimensions }),
    );
  }

  private async evaluateMetricSignals(
    client: PoolClient,
    observation: Extract<Observation, { kind: "metric.observed" }>,
  ): Promise<string[]> {
    const dimensions = observation.data.dimensions ?? {};
    const dimensionsHash = contentHash(dimensions);
    const rules = await this.matchingSignalRules(
      client,
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
    const rows = (await this.rows(
      client,
      `SELECT observation_id, value, unit, occurred_at, dimensions_json
       FROM metrics
       WHERE tenant_id = $1 AND subject_type = $2 AND subject_id = $3
         AND metric = $4 AND dimensions_hash = $5
       ORDER BY occurred_at DESC, observation_id DESC
       LIMIT $6`,
      [
        observation.tenant_id,
        observation.subject.type,
        observation.subject.id,
        observation.data.metric,
        dimensionsHash,
        requiredPoints,
      ],
    )) as Array<{
      observation_id: string;
      value: number;
      unit: string;
      occurred_at: string;
      dimensions_json: string;
    }>;
    if (rows[0]?.observation_id !== observation.observation_id) return [];

    const points: MetricPoint[] = rows.map((row) => ({
      observation_id: row.observation_id,
      value: Number(row.value),
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
      const existing = await this.getSignalWith(client, observation.tenant_id, signalId);
      const now = new Date().toISOString();

      if (!evaluation.matched) {
        if (existing?.status === "open" || existing?.status === "acknowledged") {
          await this.saveSignal(client, { ...existing, status: "resolved", updated_at: now });
          changedSignals.push(signalId);
        }
        continue;
      }

      const evidence = evaluation.evidenceObservationIds.map((observationId) => ({
        type: "observation" as const,
        ref: observationUri(observation.tenant_id, observationId),
      }));
      const policies: Array<AccessPolicy | undefined> = [];
      for (const observationId of evaluation.evidenceObservationIds) {
        policies.push(
          await this.getObservationAccessWith(client, observation.tenant_id, observationId),
        );
      }

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
      await this.saveSignal(client, signal);
      changedSignals.push(signalId);
    }
    return changedSignals;
  }

  /**
   * Re-derives every Signal in the tenant from the newest valid sample of each
   * series. Editing a rule must not leave conclusions that the new rule would
   * not reach.
   */
  private async reevaluateAllSignals(
    client: PoolClient,
    tenantId: string,
  ): Promise<void> {
    await client.query(`DELETE FROM signals WHERE tenant_id = $1`, [tenantId]);
    const rows = await this.rows(
      client,
      `SELECT payload_json FROM (
         SELECT o.payload_json,
                ROW_NUMBER() OVER (
                  PARTITION BY m.subject_type, m.subject_id, m.metric, m.dimensions_hash
                  ORDER BY m.occurred_at DESC, m.observation_id DESC
                ) AS rn
         FROM metrics m
         JOIN observations o
           ON o.tenant_id = m.tenant_id AND o.observation_id = m.observation_id
         WHERE m.tenant_id = $1 AND o.invalidated_at IS NULL
       ) ranked
       WHERE rn = 1`,
      [tenantId],
    );
    for (const row of rows) {
      const observation = JSON.parse(String(row.payload_json)) as Observation;
      if (observation.kind === "metric.observed") {
        await this.evaluateMetricSignals(client, observation);
      }
    }
  }

  private async saveSignal(client: PoolClient, signal: Signal): Promise<void> {
    await client.query(
      `INSERT INTO signals (
         tenant_id, signal_id, type, subject_type, subject_id, status, severity,
         detected_at, updated_at, payload_json
       ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
       ON CONFLICT (tenant_id, signal_id) DO UPDATE SET
         type = EXCLUDED.type,
         subject_type = EXCLUDED.subject_type,
         subject_id = EXCLUDED.subject_id,
         status = EXCLUDED.status,
         severity = EXCLUDED.severity,
         updated_at = EXCLUDED.updated_at,
         payload_json = EXCLUDED.payload_json`,
      [
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
      ],
    );
  }

  private async getSignalWith(
    client: PoolClient | Pool,
    tenantId: string,
    signalId: string,
  ): Promise<Signal | undefined> {
    const row = (
      await this.rows(
        client,
        `SELECT payload_json FROM signals WHERE tenant_id = $1 AND signal_id = $2`,
        [tenantId, signalId],
      )
    )[0] as { payload_json: string } | undefined;
    return row ? (JSON.parse(row.payload_json) as Signal) : undefined;
  }

  async getSignal(
    tenantId: string,
    signalId: string,
    consumer: ConsumerContext = systemConsumer,
  ): Promise<Signal | undefined> {
    const signal = await this.getSignalWith(this.pool, tenantId, signalId);
    if (!signal) return undefined;
    return (await this.canReadSignal(signal, consumer)) ? signal : undefined;
  }

  /**
   * A Signal is visible only when the consumer may read the Signal and every
   * Observation in its evidence set.
   */
  private async canReadSignal(
    signal: Signal,
    consumer: ConsumerContext,
  ): Promise<boolean> {
    if (!canRead(signal.access, consumer)) return false;
    for (const evidence of signal.evidence) {
      const parsed = parseObservationUri(evidence.ref);
      // A ref this runtime cannot interpret is fail-closed: there is no way to
      // prove the consumer may read what it points at, and treating it as
      // readable would make any malformed evidence visible to whoever can read
      // the Signal's own policy.
      if (!parsed) return false;
      if (parsed.tenantId !== signal.tenant_id) return false;
      if (!(await this.canReadObservation(signal.tenant_id, parsed.observationId, consumer))) {
        return false;
      }
    }
    return true;
  }

  async getSignals(
    tenantId: string,
    subject?: EntityRef,
    status?: string,
    consumer: ConsumerContext = systemConsumer,
  ): Promise<Signal[]> {
    const conditions = ["tenant_id = $1"];
    const values: unknown[] = [tenantId];
    if (subject) {
      values.push(subject.type, subject.id);
      conditions.push(`subject_type = $${values.length - 1} AND subject_id = $${values.length}`);
    }
    if (status) {
      values.push(status);
      conditions.push(`status = $${values.length}`);
    }
    const rows = await this.rows(
      this.pool,
      `SELECT payload_json FROM signals WHERE ${conditions.join(" AND ")} ORDER BY updated_at DESC`,
      values,
    );
    const visible: Signal[] = [];
    for (const row of rows) {
      const signal = JSON.parse(String(row.payload_json)) as Signal;
      if (await this.canReadSignal(signal, consumer)) visible.push(signal);
    }
    return visible;
  }

  // -------------------------------------------------------------- read model

  /**
   * Rebuilds an entity for one consumer, dropping fields whose originating
   * Observation is not readable. Field-level provenance is what makes a
   * partially visible entity possible.
   */
  async getEntity(
    tenantId: string,
    subject: EntityRef,
    consumer: ConsumerContext = systemConsumer,
  ): Promise<JsonRecord | undefined> {
    const row = (
      await this.rows(
        this.pool,
        `SELECT * FROM entities WHERE tenant_id = $1 AND type = $2 AND id = $3`,
        [tenantId, subject.type, subject.id],
      )
    )[0];
    if (!row) return undefined;

    const fieldSources = parseJson<Record<string, string>>(
      row.field_sources_json as string,
      {},
    );
    const clocks = parseJson<Record<string, string>>(row.clock_json as string, {});
    const isFieldVisible = async (field: string): Promise<boolean> => {
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
    const visibleAttributes: JsonRecord = {};
    for (const [key, value] of Object.entries(attributes)) {
      if (await isFieldVisible(`attribute:${key}`)) visibleAttributes[key] = value;
    }

    let visibleUpdatedAt: string | undefined;
    for (const [field, timestamp] of Object.entries(clocks)) {
      if (!(await isFieldVisible(field))) continue;
      if (!visibleUpdatedAt || timestamp > visibleUpdatedAt) visibleUpdatedAt = timestamp;
    }

    const baseVisible = await this.canReadObservation(
      tenantId,
      String(row.observation_id),
      consumer,
    );
    const nameVisible = await isFieldVisible("name");
    const lifecycleVisible = await isFieldVisible("lifecycle");
    if (
      !baseVisible &&
      !nameVisible &&
      !lifecycleVisible &&
      Object.keys(visibleAttributes).length === 0
    ) {
      return undefined;
    }

    return {
      type: row.type,
      id: row.id,
      ...(nameVisible ? { name: row.name } : {}),
      ...(lifecycleVisible ? { lifecycle: row.lifecycle } : {}),
      attributes: visibleAttributes,
      updated_at: visibleUpdatedAt ?? row.updated_at,
      evidence: baseVisible ? parseJson(row.evidence_json as string, []) : [],
    };
  }

  async getStates(
    tenantId: string,
    subject: EntityRef,
    consumer: ConsumerContext = systemConsumer,
  ): Promise<JsonRecord[]> {
    const rows = await this.rows(
      this.pool,
      `SELECT * FROM states
       WHERE tenant_id = $1 AND subject_type = $2 AND subject_id = $3
       ORDER BY field`,
      [tenantId, subject.type, subject.id],
    );
    const visible: JsonRecord[] = [];
    for (const row of rows) {
      if (!(await this.canReadObservation(tenantId, String(row.observation_id), consumer))) {
        continue;
      }
      visible.push({
        field: row.field,
        value: row.is_unset ? null : parseJson(row.value_json as string, null),
        is_unset: Boolean(row.is_unset),
        occurred_at: row.occurred_at,
        observed_at: row.observed_at,
        source: { system: row.source_system, instance: row.source_instance },
        evidence: parseJson(row.evidence_json as string, []),
      });
    }
    return visible;
  }

  async getRelations(
    tenantId: string,
    subject: EntityRef,
    consumer: ConsumerContext = systemConsumer,
  ): Promise<JsonRecord[]> {
    const outgoing = await this.rows(
      this.pool,
      `SELECT * FROM relations
       WHERE tenant_id = $1 AND subject_type = $2 AND subject_id = $3 AND status = 'active'
       ORDER BY relation, target_type, target_id`,
      [tenantId, subject.type, subject.id],
    );
    const incoming = await this.rows(
      this.pool,
      `SELECT * FROM relations
       WHERE tenant_id = $1 AND target_type = $2 AND target_id = $3 AND status = 'active'
       ORDER BY relation, subject_type, subject_id`,
      [tenantId, subject.type, subject.id],
    );

    const result: JsonRecord[] = [];
    for (const row of outgoing) {
      if (await this.canReadObservation(tenantId, String(row.observation_id), consumer)) {
        result.push(relationRow(row, "outgoing"));
      }
    }
    for (const row of incoming) {
      if (await this.canReadObservation(tenantId, String(row.observation_id), consumer)) {
        result.push(relationRow(row, "incoming"));
      }
    }
    return result;
  }

  async traverseGraph(
    tenantId: string,
    root: EntityRef,
    options: GraphOptions,
    consumer: ConsumerContext = systemConsumer,
  ): Promise<GraphResult> {
    const rootEntity = await this.getEntity(tenantId, root, consumer);
    if (!rootEntity) return { nodes: [], edges: [], truncated: false };

    const nodes: EntityRef[] = [root];
    const edges: JsonRecord[] = [];
    const visited = new Set([entityKey(root)]);
    const edgeKeys = new Set<string>();
    const queue: Array<{ entity: EntityRef; depth: number }> = [{ entity: root, depth: 0 }];
    let truncated = false;

    while (queue.length) {
      const current = queue.shift()!;
      if (current.depth >= options.maxDepth) continue;
      const relations = (
        await this.getRelations(tenantId, current.entity, consumer)
      ).filter((relation) => {
        if (
          options.relations?.length &&
          !options.relations.includes(String(relation.relation))
        ) {
          return false;
        }
        return (
          options.direction === "both" || relation.direction === options.direction
        );
      });

      for (const relation of relations) {
        const subject = relation.subject as EntityRef;
        const target = relation.target as EntityRef;
        const neighbor = relation.direction === "outgoing" ? target : subject;
        if (!(await this.getEntity(tenantId, neighbor, consumer))) continue;
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

  async findEvidence(
    tenantId: string,
    ref: string,
    consumer: ConsumerContext = systemConsumer,
  ): Promise<EvidenceRef | undefined> {
    const rows = (await this.rows(
      this.pool,
      `SELECT observation_id, evidence_json FROM observations
       WHERE tenant_id = $1 AND evidence_json LIKE $2 ESCAPE '\\'
         AND invalidated_at IS NULL
       ORDER BY received_at DESC LIMIT 100`,
      [tenantId, `%${escapeLikeFragment(ref)}%`],
    )) as Array<{ observation_id: string; evidence_json: string }>;

    for (const row of rows) {
      if (!(await this.canReadObservation(tenantId, row.observation_id, consumer))) continue;
      const evidence = parseJson<EvidenceRef[]>(row.evidence_json, []);
      const match = evidence.find((item) => item.ref === ref);
      if (match) return match;
    }
    return undefined;
  }

  async getRecentMetricChanges(
    tenantId: string,
    subject: EntityRef,
    consumer: ConsumerContext = systemConsumer,
    window?: { from: string; to: string },
  ): Promise<JsonRecord[]> {
    const series = (await this.rows(
      this.pool,
      `SELECT DISTINCT metric, dimensions_hash FROM metrics
       WHERE tenant_id = $1 AND subject_type = $2 AND subject_id = $3`,
      [tenantId, subject.type, subject.id],
    )) as Array<{ metric: string; dimensions_hash: string }>;

    const changes: JsonRecord[] = [];
    for (const { metric, dimensions_hash } of series) {
      const rows = await this.rows(
        this.pool,
        `SELECT * FROM metrics
         WHERE tenant_id = $1 AND subject_type = $2 AND subject_id = $3
           AND metric = $4 AND dimensions_hash = $5
         ORDER BY occurred_at DESC, observation_id DESC LIMIT 100`,
        [tenantId, subject.type, subject.id, metric, dimensions_hash],
      );

      const visible: Row[] = [];
      for (const row of rows) {
        if (
          window &&
          (String(row.occurred_at) < window.from || String(row.occurred_at) > window.to)
        ) {
          continue;
        }
        if (await this.canReadObservation(tenantId, String(row.observation_id), consumer)) {
          visible.push(row);
        }
      }
      if (visible.length < 2) continue;

      const current = visible[0]!;
      const baseline = visible[1]!;
      const currentValue = Number(current.value);
      const baselineValue = Number(baseline.value);
      changes.push({
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
          {
            type: "observation",
            ref: observationUri(tenantId, String(current.observation_id)),
          },
          {
            type: "observation",
            ref: observationUri(tenantId, String(baseline.observation_id)),
          },
        ],
      });
    }
    return changes;
  }

  async timeline(
    tenantId: string,
    subject: EntityRef,
    from: string,
    to: string,
    types: string[] | undefined,
    limit: number,
    offset: number,
    consumer: ConsumerContext = systemConsumer,
  ): Promise<TimelinePage> {
    const items: JsonRecord[] = [];
    let scanOffset = offset;
    let scanned = 0;
    const batchSize = Math.max(100, Math.min(1000, limit * 5));
    const maxScan = 10_000;

    while (scanned < maxScan) {
      const rows = await this.rows(
        this.pool,
        `SELECT observation_id, kind, occurred_at, observed_at, source_system,
                source_instance, data_json, evidence_json
         FROM observations
         WHERE tenant_id = $1 AND subject_type = $2 AND subject_id = $3
           AND occurred_at >= $4 AND occurred_at <= $5
           AND kind IN ('event.occurred', 'state.observed')
           AND invalidated_at IS NULL
         ORDER BY occurred_at ASC, observation_id ASC
         LIMIT $6 OFFSET $7`,
        [tenantId, subject.type, subject.id, from, to, batchSize, scanOffset],
      );
      if (!rows.length) return { items, truncated: false };

      for (let index = 0; index < rows.length; index += 1) {
        const row = rows[index]!;
        const nextRawOffset = scanOffset + index;
        if (
          !(await this.canReadObservation(tenantId, String(row.observation_id), consumer))
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

  async listEntities(
    tenantId: string,
    type?: string,
    consumer: ConsumerContext = systemConsumer,
  ): Promise<JsonRecord[]> {
    const rows = type
      ? await this.rows(
          this.pool,
          `SELECT * FROM entities WHERE tenant_id = $1 AND type = $2
           ORDER BY updated_at DESC LIMIT 1000`,
          [tenantId, type],
        )
      : await this.rows(
          this.pool,
          `SELECT * FROM entities WHERE tenant_id = $1
           ORDER BY updated_at DESC LIMIT 1000`,
          [tenantId],
        );

    const visible: JsonRecord[] = [];
    for (const row of rows) {
      const entity = await this.getEntity(
        tenantId,
        { type: String(row.type), id: String(row.id) },
        consumer,
      );
      if (entity) visible.push(entity);
    }
    return visible;
  }

  async metricsInWindow(
    tenantId: string,
    subject: EntityRef,
    metric: string,
    from: string,
    to: string,
    filters: Record<string, string>,
    consumer: ConsumerContext = systemConsumer,
  ): Promise<StoredMetric[]> {
    const rows = await this.rows(
      this.pool,
      `SELECT * FROM metrics
       WHERE tenant_id = $1 AND subject_type = $2 AND subject_id = $3
         AND metric = $4 AND occurred_at >= $5 AND occurred_at <= $6
       ORDER BY occurred_at`,
      [tenantId, subject.type, subject.id, metric, from, to],
    );

    const visible: StoredMetric[] = [];
    for (const row of rows) {
      if (!(await this.canReadObservation(tenantId, String(row.observation_id), consumer))) {
        continue;
      }
      const dimensions = parseJson<Record<string, string>>(
        row.dimensions_json as string,
        {},
      );
      if (
        !Object.entries(filters).every(([key, value]) => dimensions[key] === value)
      ) {
        continue;
      }
      visible.push({
        observation_id: String(row.observation_id),
        metric: String(row.metric),
        value: Number(row.value),
        unit: String(row.unit),
        occurred_at: String(row.occurred_at),
        interval_from: row.interval_from ? String(row.interval_from) : null,
        interval_to: row.interval_to ? String(row.interval_to) : null,
        dimensions,
        evidence: parseJson<EvidenceRef[]>(row.evidence_json as string, []),
      });
    }
    return visible;
  }

  async latestWatermark(
    tenantId: string,
    consumer: ConsumerContext = systemConsumer,
  ): Promise<string> {
    const rows = (await this.rows(
      this.pool,
      `SELECT observation_id, received_at FROM observations
       WHERE tenant_id = $1 AND invalidated_at IS NULL
       ORDER BY received_at DESC LIMIT 1000`,
      [tenantId],
    )) as Array<{ observation_id: string; received_at: string }>;

    for (const row of rows) {
      if (await this.canReadObservation(tenantId, row.observation_id, consumer)) {
        return row.received_at;
      }
    }
    return new Date(0).toISOString();
  }
}

// ------------------------------------------------------------------ helpers

function parseJson<T>(value: string | undefined | null, fallback: T): T {
  if (value === undefined || value === null) return fallback;
  return JSON.parse(value) as T;
}

function maxTimestamp(a: string | undefined, b: string): string {
  return !a || a < b ? b : a;
}

/**
 * The three-clock comparison. `occurred_at` decides; a source sequence breaks a
 * tie only when both sides have one, and receipt order is the last resort.
 */
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

function relationRow(row: Row, direction: "incoming" | "outgoing"): JsonRecord {
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

/** PostgreSQL's `unique_violation`. The signal that a concurrent writer won. */
function isUniqueViolation(error: unknown): boolean {
  return (
    typeof error === "object" &&
    error !== null &&
    "code" in error &&
    (error as { code?: unknown }).code === "23505"
  );
}

function escapeLikeFragment(value: string): string {
  return value.replaceAll("\\", "\\\\").replaceAll("%", "\\%").replaceAll("_", "\\_");
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
