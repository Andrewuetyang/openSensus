import type { ConsumerContext } from "./access.js";
import type { EntityRef, EvidenceRef, Observation, Signal } from "./protocol.js";
import type { SignalRule } from "./signal-rules.js";
import { SensusStore } from "./store.js";
import type {
  CompleteSyncInput,
  GraphOptions,
  GraphResult,
  IngestOptions,
  IngestResult,
  StartSyncInput,
  TimelinePage,
} from "./store.js";
import type { JsonRecord, SensusStorage, StoredMetric } from "./storage.js";

/**
 * Adapts the synchronous SQLite store to {@link SensusStorage}.
 *
 * `better-sqlite3` is synchronous by design, so every method here resolves
 * immediately. The adapter exists because the contract is asynchronous — a
 * network-backed implementation cannot be synchronous in Node — and because
 * having both backends implement one interface is what lets a single
 * conformance suite hold them to identical behaviour.
 *
 * No logic lives here. If a behaviour needs changing, it belongs in
 * `SensusStore` or in the contract, not in this translation layer.
 */
export class SqliteStorage implements SensusStorage {
  constructor(private readonly store: SensusStore) {}

  static open(path: string): SqliteStorage {
    return new SqliteStorage(new SensusStore(path));
  }

  /** The underlying store, for callers that have not moved to the contract yet. */
  get sync(): SensusStore {
    return this.store;
  }

  async ingest(
    observation: Observation,
    options?: IngestOptions,
  ): Promise<IngestResult> {
    return this.store.ingest(observation, options);
  }

  async ingestMany(
    observations: Observation[],
    options?: IngestOptions,
  ): Promise<IngestResult[]> {
    return this.store.ingestMany(observations, options);
  }

  async startSync(input: StartSyncInput): Promise<JsonRecord> {
    return this.store.startSync(input);
  }

  async getSync(tenantId: string, syncId: string): Promise<JsonRecord | undefined> {
    return this.store.getSync(tenantId, syncId);
  }

  async completeSync(
    tenantId: string,
    syncId: string,
    input: CompleteSyncInput,
  ): Promise<JsonRecord> {
    return this.store.completeSync(tenantId, syncId, input);
  }

  async listSignalRules(
    tenantId: string,
    includeDefault = true,
  ): Promise<SignalRule[]> {
    return this.store.listSignalRules(tenantId, includeDefault);
  }

  async upsertSignalRule(
    tenantId: string,
    candidate: unknown,
  ): Promise<SignalRule> {
    return this.store.upsertSignalRule(tenantId, candidate);
  }

  async deleteSignalRule(tenantId: string, ruleId: string): Promise<boolean> {
    return this.store.deleteSignalRule(tenantId, ruleId);
  }

  async getObservation(
    tenantId: string,
    observationId: string,
  ): Promise<Observation | undefined> {
    return this.store.getObservation(tenantId, observationId);
  }

  async getObservationForConsumer(
    tenantId: string,
    observationId: string,
    consumer: ConsumerContext,
  ): Promise<Observation | undefined> {
    return this.store.getObservationForConsumer(tenantId, observationId, consumer);
  }

  async getObservationAccess(
    tenantId: string,
    observationId: string,
  ): Promise<unknown | undefined> {
    return this.store.getObservationAccess(tenantId, observationId);
  }

  async canReadObservation(
    tenantId: string,
    observationId: string,
    consumer: ConsumerContext,
  ): Promise<boolean> {
    return this.store.canReadObservation(tenantId, observationId, consumer);
  }

  async getEntity(
    tenantId: string,
    subject: EntityRef,
    consumer?: ConsumerContext,
  ): Promise<JsonRecord | undefined> {
    return this.store.getEntity(tenantId, subject, consumer);
  }

  async getStates(
    tenantId: string,
    subject: EntityRef,
    consumer?: ConsumerContext,
  ): Promise<JsonRecord[]> {
    return this.store.getStates(tenantId, subject, consumer);
  }

  async getRelations(
    tenantId: string,
    subject: EntityRef,
    consumer?: ConsumerContext,
  ): Promise<JsonRecord[]> {
    return this.store.getRelations(tenantId, subject, consumer);
  }

  async traverseGraph(
    tenantId: string,
    root: EntityRef,
    options: GraphOptions,
    consumer?: ConsumerContext,
  ): Promise<GraphResult> {
    return this.store.traverseGraph(tenantId, root, options, consumer);
  }

  async getSignal(
    tenantId: string,
    signalId: string,
    consumer?: ConsumerContext,
  ): Promise<Signal | undefined> {
    return this.store.getSignal(tenantId, signalId, consumer);
  }

  async getSignals(
    tenantId: string,
    subject?: EntityRef,
    status?: string,
    consumer?: ConsumerContext,
  ): Promise<Signal[]> {
    return this.store.getSignals(tenantId, subject, status, consumer);
  }

  async findEvidence(
    tenantId: string,
    ref: string,
    consumer?: ConsumerContext,
  ): Promise<EvidenceRef | undefined> {
    return this.store.findEvidence(tenantId, ref, consumer);
  }

  async getRecentMetricChanges(
    tenantId: string,
    subject: EntityRef,
    consumer?: ConsumerContext,
    window?: { from: string; to: string },
  ): Promise<JsonRecord[]> {
    return this.store.getRecentMetricChanges(tenantId, subject, consumer, window);
  }

  async timeline(
    tenantId: string,
    subject: EntityRef,
    from: string,
    to: string,
    types: string[] | undefined,
    limit: number,
    offset: number,
    consumer?: ConsumerContext,
  ): Promise<TimelinePage> {
    return this.store.timeline(
      tenantId,
      subject,
      from,
      to,
      types,
      limit,
      offset,
      consumer,
    );
  }

  async listEntities(
    tenantId: string,
    type?: string,
    consumer?: ConsumerContext,
  ): Promise<JsonRecord[]> {
    return this.store.listEntities(tenantId, type, consumer);
  }

  async metricsInWindow(
    tenantId: string,
    subject: EntityRef,
    metric: string,
    from: string,
    to: string,
    filters: Record<string, string>,
    consumer?: ConsumerContext,
  ): Promise<StoredMetric[]> {
    return this.store.metricsInWindow(
      tenantId,
      subject,
      metric,
      from,
      to,
      filters,
      consumer,
    );
  }

  async latestWatermark(
    tenantId: string,
    consumer?: ConsumerContext,
  ): Promise<string> {
    return this.store.latestWatermark(tenantId, consumer);
  }

  async close(): Promise<void> {
    this.store.close();
  }
}
