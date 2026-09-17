import { randomUUID } from "node:crypto";
import { z } from "zod/v4";
import {
  entityRefSchema,
  evidenceRefSchema,
  utcTimestamp,
  type EntityRef,
  type EvidenceRef,
  type Signal,
} from "./protocol.js";
import type { SensusStorage, StoredMetric } from "./storage.js";
import {
  createConsumerContext,
  type ConsumerContext,
} from "./access.js";

const intervalSchema = z
  .object({
    from: utcTimestamp,
    to: utcTimestamp,
  })
  .refine((value) => value.from <= value.to, {
    message: "from must be before or equal to to",
    path: ["from"],
  });

export const observeInputSchema = z.object({
  scope: entityRefSchema,
  window: intervalSchema.optional(),
  include: z
    .array(z.enum(["state", "changes", "signals"]))
    .default(["state", "changes", "signals"]),
  limit: z.number().int().min(1).max(200).default(50),
  expand: z
    .object({
      direction: z.enum(["outgoing", "incoming", "both"]).default("both"),
      relations: z.array(z.string()).max(50).optional(),
      max_depth: z.number().int().min(1).max(5).default(1),
      max_nodes: z.number().int().min(1).max(500).default(100),
    })
    .optional(),
});

export const inspectInputSchema = z.discriminatedUnion("kind", [
  z.object({
    kind: z.literal("entity"),
    entity: entityRefSchema,
    include: z
      .array(z.enum(["state", "relations", "metrics", "evidence"]))
      .default(["state", "relations", "metrics", "evidence"]),
  }),
  z.object({
    kind: z.literal("signal"),
    signal_id: z.string().min(1),
  }),
]);

export const timelineInputSchema = z.object({
  subject: entityRefSchema,
  window: intervalSchema,
  types: z.array(z.string()).optional(),
  limit: z.number().int().min(1).max(200).default(50),
  cursor: z.string().optional(),
});

const predicateSchema = z.object({
  field: z.string().min(1),
  op: z.enum(["eq", "neq", "in", "gt", "gte", "lt", "lte", "exists"]),
  value: z.unknown().optional(),
});

export const queryInputSchema = z.object({
  resource: z.enum(["entity", "signal"]),
  type: z.string().optional(),
  where: z.array(predicateSchema).default([]),
  order_by: z
    .object({
      field: z.string(),
      direction: z.enum(["asc", "desc"]),
    })
    .optional(),
  limit: z.number().int().min(1).max(200).default(50),
  cursor: z.string().optional(),
});

export const compareInputSchema = z.object({
  metric: z.string().min(1),
  scope: entityRefSchema,
  current: intervalSchema,
  baseline: intervalSchema,
  aggregation: z
    .enum(["count", "sum", "average", "min", "max", "p50", "p90", "p95", "p99"])
    .default("average"),
  group_by: z.array(z.string()).max(5).default([]),
  filters: z.record(z.string(), z.string()).default({}),
  limit: z.number().int().min(1).max(200).default(50),
});

export const getEvidenceInputSchema = z.object({
  evidence: evidenceRefSchema,
  format: z.enum(["structured", "text"]).default("structured"),
});

export type ObserveInput = z.infer<typeof observeInputSchema>;
export type InspectInput = z.infer<typeof inspectInputSchema>;
export type TimelineInput = z.infer<typeof timelineInputSchema>;
export type QueryInput = z.infer<typeof queryInputSchema>;
export type CompareInput = z.infer<typeof compareInputSchema>;
export type GetEvidenceInput = z.infer<typeof getEvidenceInputSchema>;

type JsonRecord = Record<string, unknown>;

export class SensusWorld {
  constructor(
    private readonly store: SensusStorage,
    private readonly tenantId: string,
    private readonly consumer: ConsumerContext = createConsumerContext({
      principals: ["role:agent"],
      clearance: "internal",
    }),
  ) {}

  async observe(rawInput: ObserveInput): Promise<JsonRecord> {
    const input = observeInputSchema.parse(rawInput);
    const entity = await this.store.getEntity(
      this.tenantId,
      input.scope,
      this.consumer,
    );
    if (!entity) {
      throw new WorldNotFoundError(
        "entity",
        `${input.scope.type}:${input.scope.id}`,
      );
    }
    const graph = input.expand
      ? await this.store.traverseGraph(
          this.tenantId,
          input.scope,
          {
            direction: input.expand.direction,
            ...(input.expand.relations
              ? { relations: input.expand.relations }
              : {}),
            maxDepth: input.expand.max_depth,
            maxNodes: input.expand.max_nodes,
          },
          this.consumer,
        )
      : { nodes: [input.scope], edges: [], truncated: false };
    const scopes = graph.nodes;

    const allStates: JsonRecord[] = [];
    if (input.include.includes("state")) {
      for (const subject of scopes) {
        const states = await this.store.getStates(
          this.tenantId,
          subject,
          this.consumer,
        );
        for (const state of states) allStates.push({ subject, ...state });
      }
    }

    const allChanges: JsonRecord[] = [];
    if (input.include.includes("changes")) {
      for (const subject of scopes) {
        const changes = await this.store.getRecentMetricChanges(
          this.tenantId,
          subject,
          this.consumer,
          input.window,
        );
        for (const change of changes) allChanges.push({ subject, ...change });
      }
    }

    const collectedSignals: Signal[] = [];
    if (input.include.includes("signals")) {
      for (const subject of scopes) {
        collectedSignals.push(
          ...(await this.store.getSignals(
            this.tenantId,
            subject,
            "open",
            this.consumer,
          )),
        );
      }
    }
    const allSignals = deduplicateSignals(collectedSignals);

    const states = allStates.slice(0, input.limit);
    const changes = allChanges.slice(0, input.limit);
    const signals = allSignals.slice(0, input.limit);
    const truncated =
      graph.truncated ||
      allStates.length > input.limit ||
      allChanges.length > input.limit ||
      allSignals.length > input.limit;

    return {
      meta: await this.meta(truncated),
      scope: input.scope,
      entity,
      graph: input.expand ? graph : undefined,
      state: states,
      changes,
      signals: signals.map(signalSummary),
    };
  }

  async inspect(rawInput: InspectInput): Promise<JsonRecord> {
    const input = inspectInputSchema.parse(rawInput);
    if (input.kind === "signal") {
      const signal = await this.store.getSignal(
        this.tenantId,
        input.signal_id,
        this.consumer,
      );
      if (!signal) throw new WorldNotFoundError("signal", input.signal_id);
      return { meta: await this.meta(false), signal };
    }

    const entity = await this.store.getEntity(
      this.tenantId,
      input.entity,
      this.consumer,
    );
    if (!entity) {
      throw new WorldNotFoundError(
        "entity",
        `${input.entity.type}:${input.entity.id}`,
      );
    }
    return {
      meta: await this.meta(false),
      entity,
      state: input.include.includes("state")
        ? await this.store.getStates(this.tenantId, input.entity, this.consumer)
        : undefined,
      relations: input.include.includes("relations")
        ? await this.store.getRelations(this.tenantId, input.entity, this.consumer)
        : undefined,
      metric_changes: input.include.includes("metrics")
        ? await this.store.getRecentMetricChanges(
            this.tenantId,
            input.entity,
            this.consumer,
          )
        : undefined,
    };
  }

  async timeline(rawInput: TimelineInput): Promise<JsonRecord> {
    const input = timelineInputSchema.parse(rawInput);
    const offset = decodeCursor(input.cursor);
    const page = await this.store.timeline(
      this.tenantId,
      input.subject,
      input.window.from,
      input.window.to,
      input.types,
      input.limit,
      offset,
      this.consumer,
    );
    return {
      meta: await this.meta(
        page.truncated,
        page.nextOffset === undefined ? undefined : encodeCursor(page.nextOffset),
      ),
      subject: input.subject,
      window: input.window,
      items: page.items,
    };
  }

  async query(rawInput: QueryInput): Promise<JsonRecord> {
    const input = queryInputSchema.parse(rawInput);
    const offset = decodeCursor(input.cursor);
    let resources: JsonRecord[] =
      input.resource === "entity"
        ? await this.store.listEntities(this.tenantId, input.type, this.consumer)
        : (
            await this.store.getSignals(
              this.tenantId,
              undefined,
              undefined,
              this.consumer,
            )
          )
            .filter((signal) => !input.type || signal.type === input.type)
            .map((signal) => signal as unknown as JsonRecord);

    resources = resources.filter((resource) =>
      input.where.every((predicate) => matchesPredicate(resource, predicate)),
    );
    if (input.order_by) {
      const { field, direction } = input.order_by;
      resources.sort((a, b) => {
        const left = getPath(a, field);
        const right = getPath(b, field);
        const result = compareValues(left, right);
        return direction === "asc" ? result : -result;
      });
    }
    const page = resources.slice(offset, offset + input.limit);
    const truncated = offset + page.length < resources.length;
    return {
      meta: await this.meta(
        truncated,
        truncated ? encodeCursor(offset + page.length) : undefined,
      ),
      resource: input.resource,
      items: page,
    };
  }

  async compare(rawInput: CompareInput): Promise<JsonRecord> {
    const input = compareInputSchema.parse(rawInput);
    const current = await this.store.metricsInWindow(
      this.tenantId,
      input.scope,
      input.metric,
      input.current.from,
      input.current.to,
      input.filters,
      this.consumer,
    );
    const baseline = await this.store.metricsInWindow(
      this.tenantId,
      input.scope,
      input.metric,
      input.baseline.from,
      input.baseline.to,
      input.filters,
      this.consumer,
    );
    const currentGroups = groupMetrics(current, input.group_by);
    const baselineGroups = groupMetrics(baseline, input.group_by);
    const keys = [...new Set([...currentGroups.keys(), ...baselineGroups.keys()])];
    const results = keys
      .map((key) => {
        const currentRows = currentGroups.get(key) ?? [];
        const baselineRows = baselineGroups.get(key) ?? [];
        const currentValue = aggregate(
          currentRows.map((row) => row.value),
          input.aggregation,
        );
        const baselineValue = aggregate(
          baselineRows.map((row) => row.value),
          input.aggregation,
        );
        const delta =
          currentValue === null || baselineValue === null
            ? null
            : currentValue - baselineValue;
        return {
          group: key === "__all__" ? {} : JSON.parse(key),
          current: currentValue,
          baseline: baselineValue,
          delta,
          delta_percent:
            delta === null || baselineValue === null || baselineValue === 0
              ? null
              : (delta / baselineValue) * 100,
          current_sample_count: currentRows.length,
          baseline_sample_count: baselineRows.length,
          unit: currentRows[0]?.unit ?? baselineRows[0]?.unit ?? null,
          evidence: [...currentRows, ...baselineRows].map((row) => ({
            type: "observation",
            ref: observationUri(this.tenantId, row.observation_id),
          })),
        };
      })
      .slice(0, input.limit);

    return {
      meta: await this.meta(keys.length > input.limit),
      metric: input.metric,
      scope: input.scope,
      aggregation: input.aggregation,
      current_window: input.current,
      baseline_window: input.baseline,
      results,
    };
  }

  async getEvidence(rawInput: GetEvidenceInput): Promise<JsonRecord> {
    const input = getEvidenceInputSchema.parse(rawInput);
    const observationMatch = input.evidence.ref.match(
      /^sensus:\/\/observation\/([^/]+)\/([^/]+)$/,
    );
    if (input.evidence.type === "observation" && observationMatch) {
      const tenantId = decodeURIComponent(observationMatch[1]!);
      const observationId = decodeURIComponent(observationMatch[2]!);
      if (tenantId !== this.tenantId) {
        throw new WorldPermissionError("Evidence belongs to another tenant");
      }
      const observation = await this.store.getObservationForConsumer(
        this.tenantId,
        observationId,
        this.consumer,
      );
      if (!observation) throw new WorldNotFoundError("observation", observationId);
      if (input.format === "text") {
        return {
          meta: await this.meta(false),
          text: `${observation.kind} for ${observation.subject.type}:${observation.subject.id} at ${observation.occurred_at}\n${JSON.stringify(observation.data, null, 2)}`,
        };
      }
      return { meta: await this.meta(false), observation };
    }

    const storedEvidence = await this.store.findEvidence(
      this.tenantId,
      input.evidence.ref,
      this.consumer,
    );
    if (!storedEvidence) {
      throw new WorldNotFoundError("evidence", input.evidence.ref);
    }

    return {
      meta: await this.meta(false),
      evidence: storedEvidence,
      resolution: storedEvidence.resolver
        ? {
            status: "external_tool_required",
            capability: storedEvidence.resolver.capability,
            arguments: storedEvidence.resolver.arguments ?? {},
          }
        : {
            status: "reference_only",
            message: "No resolver capability was supplied by the Producer.",
          },
    };
  }

  private async meta(truncated: boolean, nextCursor?: string): Promise<JsonRecord> {
    return {
      request_id: `req_${randomUUID()}`,
      tenant_id: this.tenantId,
      as_of: new Date().toISOString(),
      watermark: await this.store.latestWatermark(this.tenantId, this.consumer),
      truncated,
      ...(nextCursor ? { next_cursor: nextCursor } : {}),
    };
  }
}

export class WorldNotFoundError extends Error {
  constructor(
    public readonly resource: string,
    public readonly id: string,
  ) {
    super(`${resource} ${id} was not found`);
    this.name = "WorldNotFoundError";
  }
}

export class WorldPermissionError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "WorldPermissionError";
  }
}

function signalSummary(signal: Signal): JsonRecord {
  return {
    signal_id: signal.signal_id,
    type: signal.type,
    severity: signal.severity,
    title: signal.title,
    status: signal.status,
    confidence: signal.confidence,
    updated_at: signal.updated_at,
  };
}

function deduplicateSignals(signals: Signal[]): Signal[] {
  return [
    ...new Map(signals.map((signal) => [signal.signal_id, signal])).values(),
  ].sort((left, right) => right.updated_at.localeCompare(left.updated_at));
}

function encodeCursor(offset: number): string {
  return Buffer.from(JSON.stringify({ offset }), "utf8").toString("base64url");
}

function decodeCursor(cursor: string | undefined): number {
  if (!cursor) return 0;
  try {
    const parsed = JSON.parse(Buffer.from(cursor, "base64url").toString("utf8")) as {
      offset?: unknown;
    };
    if (typeof parsed.offset !== "number" || parsed.offset < 0) throw new Error();
    return parsed.offset;
  } catch {
    throw new Error("Invalid cursor");
  }
}

function getPath(value: JsonRecord, path: string): unknown {
  return path.split(".").reduce<unknown>((current, segment) => {
    if (current === null || typeof current !== "object") return undefined;
    return (current as JsonRecord)[segment];
  }, value);
}

function matchesPredicate(
  resource: JsonRecord,
  predicate: z.infer<typeof predicateSchema>,
): boolean {
  const actual = getPath(resource, predicate.field);
  switch (predicate.op) {
    case "exists":
      return predicate.value === false ? actual === undefined : actual !== undefined;
    case "eq":
      return actual === predicate.value;
    case "neq":
      return actual !== predicate.value;
    case "in":
      return Array.isArray(predicate.value) && predicate.value.includes(actual);
    case "gt":
      return compareValues(actual, predicate.value) > 0;
    case "gte":
      return compareValues(actual, predicate.value) >= 0;
    case "lt":
      return compareValues(actual, predicate.value) < 0;
    case "lte":
      return compareValues(actual, predicate.value) <= 0;
  }
}

function compareValues(left: unknown, right: unknown): number {
  if (left === right) return 0;
  if (left === undefined || left === null) return -1;
  if (right === undefined || right === null) return 1;
  if (typeof left === "number" && typeof right === "number") return left - right;
  return String(left).localeCompare(String(right));
}

type MetricRow = StoredMetric;

function groupMetrics(rows: MetricRow[], groupBy: string[]): Map<string, MetricRow[]> {
  const groups = new Map<string, MetricRow[]>();
  for (const row of rows) {
    const dimensions = Object.fromEntries(
      [...groupBy].sort().map((key) => [key, row.dimensions[key] ?? null]),
    );
    const key = groupBy.length ? JSON.stringify(dimensions) : "__all__";
    const group = groups.get(key) ?? [];
    group.push(row);
    groups.set(key, group);
  }
  return groups;
}

function aggregate(
  values: number[],
  method: CompareInput["aggregation"],
): number | null {
  if (!values.length) return null;
  switch (method) {
    case "count":
      return values.length;
    case "sum":
      return values.reduce((sum, value) => sum + value, 0);
    case "average":
      return values.reduce((sum, value) => sum + value, 0) / values.length;
    case "min":
      return Math.min(...values);
    case "max":
      return Math.max(...values);
    case "p50":
      return percentile(values, 0.5);
    case "p90":
      return percentile(values, 0.9);
    case "p95":
      return percentile(values, 0.95);
    case "p99":
      return percentile(values, 0.99);
  }
}

function percentile(values: number[], percentileValue: number): number {
  const sorted = [...values].sort((a, b) => a - b);
  const index = Math.min(
    sorted.length - 1,
    Math.max(0, Math.ceil(percentileValue * sorted.length) - 1),
  );
  return sorted[index]!;
}

function observationUri(tenantId: string, observationId: string): string {
  return `sensus://observation/${encodeURIComponent(tenantId)}/${encodeURIComponent(observationId)}`;
}
