import type { ConsumerContext } from "./access.js";
import type { EntityRef, EvidenceRef, Observation, Signal } from "./protocol.js";
import type { SignalRule } from "./signal-rules.js";
import type {
  CompleteSyncInput,
  GraphOptions,
  GraphResult,
  IngestOptions,
  IngestResult,
  StartSyncInput,
  TimelinePage,
} from "./store.js";

export type JsonRecord = Record<string, unknown>;

/** One stored metric sample, as the read model consumes it. */
export interface StoredMetric {
  observation_id: string;
  metric: string;
  value: number;
  unit: string;
  occurred_at: string;
  interval_from: string | null;
  interval_to: string | null;
  dimensions: Record<string, string>;
  evidence: EvidenceRef[];
}

/**
 * The storage surface a Sensus runtime needs, expressed asynchronously.
 *
 * `SensusStore` is the SQLite implementation of this contract; `PostgresStore`
 * is the other. Both are held to the same conformance suite, because the two
 * invariants below are what make corrections, reconciliation and rule changes
 * work at all. An implementation that breaks either one will still pass a naive
 * smoke test while silently corrupting derived state.
 *
 * ## Invariant 1 — projection is transactional
 *
 * `ingest` MUST persist the Observation and every projection side effect in a
 * single transaction. A reader must never observe an accepted Observation whose
 * projections have not been applied, nor a projection whose Observation was
 * rolled back.
 *
 * ## Invariant 2 — rebuild order is total and deterministic
 *
 * When projections for a subject are rebuilt (after a correction, or when rule
 * changes force re-evaluation), Observations MUST be replayed in exactly this
 * order:
 *
 * ```sql
 * ORDER BY occurred_at ASC,
 *          COALESCE(source_sequence, -1) ASC,
 *          received_at ASC,
 *          observation_id ASC
 * ```
 *
 * The trailing `observation_id` is not decoration: without it, two Observations
 * that agree on all three clocks could replay in either order, and the same log
 * would produce different projections on different runs.
 *
 * ## Method notes
 *
 * Every read takes the calling `ConsumerContext` and MUST filter with it. The
 * default is `systemConsumer`, which bypasses item ACLs; callers outside the
 * runtime's own jobs must pass a real consumer.
 */
export interface SensusStorage {
  // ---------------------------------------------------------------- ingestion

  /**
   * Appends one Observation and projects it.
   *
   * Repeating an `observation_id` with an identical canonical payload is a
   * no-op returning `status: "duplicate"`. Repeating it with a different payload
   * MUST raise `ObservationConflictError`. When `options.syncId` is set the
   * Observation is also attached to that sync, on the duplicate path too — that
   * is what makes re-sending a whole snapshot a valid reconciliation strategy.
   *
   * Callers MUST pass an Observation produced by `observationSchema`, which is
   * what normalizes producer timestamps to canonical UTC. A hand-built object
   * that skips it can carry two representations of one instant, and every
   * ordering comparison downstream is a string comparison.
   */
  ingest(observation: Observation, options?: IngestOptions): Promise<IngestResult>;

  /** Applies {@link ingest} to each item in order. Items are independent. */
  ingestMany(
    observations: Observation[],
    options?: IngestOptions,
  ): Promise<IngestResult[]>;

  // ------------------------------------------------------------ reconciliation

  /** Opens a snapshot or reconciliation session. */
  startSync(input: StartSyncInput): Promise<JsonRecord>;

  getSync(tenantId: string, syncId: string): Promise<JsonRecord | undefined>;

  /**
   * Closes a sync and, when it is authoritative, sweeps the source's presence
   * ledger for entities the snapshot omitted.
   *
   * An entity is marked deleted only when no other source still reports it as
   * present. The deletion MUST be recorded as a synthesized Observation with
   * `derivation` evidence, never as a silent projection mutation.
   *
   * Implementations MUST reject the call when `input.record_count` disagrees
   * with the number of idempotently attached members; that guard is what stops
   * a truncated snapshot from mass-deleting live entities.
   */
  completeSync(
    tenantId: string,
    syncId: string,
    input: CompleteSyncInput,
  ): Promise<JsonRecord>;

  // ------------------------------------------------------------------ rules

  listSignalRules(tenantId: string, includeDefault?: boolean): Promise<SignalRule[]>;

  /**
   * Stores a rule and re-evaluates existing metric series against it.
   *
   * Implementations MUST re-evaluate rather than wait for the next sample, and
   * MUST NOT alter source Observations while doing so.
   */
  upsertSignalRule(tenantId: string, candidate: unknown): Promise<SignalRule>;

  deleteSignalRule(tenantId: string, ruleId: string): Promise<boolean>;

  // ------------------------------------------------------------- observations

  /** Returns an Observation regardless of ACLs. Runtime-internal use. */
  getObservation(tenantId: string, observationId: string): Promise<Observation | undefined>;

  /**
   * Returns an Observation only when the consumer may read it, and only when it
   * has not been invalidated by a correction.
   */
  getObservationForConsumer(
    tenantId: string,
    observationId: string,
    consumer: ConsumerContext,
  ): Promise<Observation | undefined>;

  getObservationAccess(
    tenantId: string,
    observationId: string,
  ): Promise<unknown | undefined>;

  canReadObservation(
    tenantId: string,
    observationId: string,
    consumer: ConsumerContext,
  ): Promise<boolean>;

  // ------------------------------------------------------------------ reads

  getEntity(
    tenantId: string,
    subject: EntityRef,
    consumer?: ConsumerContext,
  ): Promise<JsonRecord | undefined>;

  getStates(
    tenantId: string,
    subject: EntityRef,
    consumer?: ConsumerContext,
  ): Promise<JsonRecord[]>;

  getRelations(
    tenantId: string,
    subject: EntityRef,
    consumer?: ConsumerContext,
  ): Promise<JsonRecord[]>;

  /**
   * Breadth-first expansion over visible relations.
   *
   * MUST be cycle-safe, ACL-filtered at every hop, and bounded by both
   * `maxDepth` and `maxNodes`. An invisible neighbour MUST be skipped entirely
   * rather than revealed as a node, and truncation MUST be reported.
   */
  traverseGraph(
    tenantId: string,
    root: EntityRef,
    options: GraphOptions,
    consumer?: ConsumerContext,
  ): Promise<GraphResult>;

  getSignal(
    tenantId: string,
    signalId: string,
    consumer?: ConsumerContext,
  ): Promise<Signal | undefined>;

  /**
   * Lists Signals visible to the consumer.
   *
   * A Signal is visible only when the consumer can read the Signal itself *and*
   * every Observation referenced by its evidence.
   */
  getSignals(
    tenantId: string,
    subject?: EntityRef,
    status?: string,
    consumer?: ConsumerContext,
  ): Promise<Signal[]>;

  findEvidence(
    tenantId: string,
    ref: string,
    consumer?: ConsumerContext,
  ): Promise<EvidenceRef | undefined>;

  getRecentMetricChanges(
    tenantId: string,
    subject: EntityRef,
    consumer?: ConsumerContext,
    window?: { from: string; to: string },
  ): Promise<JsonRecord[]>;

  timeline(
    tenantId: string,
    subject: EntityRef,
    from: string,
    to: string,
    types: string[] | undefined,
    limit: number,
    offset: number,
    consumer?: ConsumerContext,
  ): Promise<TimelinePage>;

  listEntities(
    tenantId: string,
    type?: string,
    consumer?: ConsumerContext,
  ): Promise<JsonRecord[]>;

  metricsInWindow(
    tenantId: string,
    subject: EntityRef,
    metric: string,
    from: string,
    to: string,
    filters: Record<string, string>,
    consumer?: ConsumerContext,
  ): Promise<StoredMetric[]>;

  /**
   * Newest `received_at` visible to the consumer, or the epoch when nothing is.
   *
   * Callers use this to tell "nothing happened" apart from "I am not cleared to
   * see what happened", so it MUST be computed over readable Observations only.
   *
   * Implementations MAY bound the scan. Both current ones look at the newest
   * 1000 Observations and report the newest readable one among them, so a
   * consumer cleared for none of that window gets the epoch — which reads as
   * "nothing happened". Making it exact needs an ACL-aware query.
   */
  latestWatermark(tenantId: string, consumer?: ConsumerContext): Promise<string>;

  // ----------------------------------------------------------------- teardown

  close(): Promise<void>;
}
