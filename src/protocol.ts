import { createHash } from "node:crypto";
import { z } from "zod/v4";

export const entityRefSchema = z.object({
  type: z.string().regex(/^[a-z][a-z0-9_]*(\.[a-z0-9_]+)*$/),
  id: z.string().min(1),
});

export type EntityRef = z.infer<typeof entityRefSchema>;

/**
 * A producer-supplied instant, normalized to UTC.
 *
 * The protocol accepts any RFC 3339 value with an explicit offset, but every
 * ordering decision in the runtime — late-arrival protection, replay order,
 * `ORDER BY occurred_at` — compares timestamps as strings. Two representations
 * of the same instant only sort consistently once they share a form, so
 * `2026-01-01T10:00:00+02:00` is stored as `2026-01-01T08:00:00.000Z` rather
 * than trusted to compare correctly in whatever offset it arrived in.
 *
 * Normalizing here rather than at each comparison means projection, replay and
 * content hashing all see the canonical form. It also makes the same instant
 * sent with different offsets hash identically, so an offset change is a
 * duplicate rather than a conflict.
 */
export const utcTimestamp = z.iso
  .datetime({ offset: true })
  .transform((value) => new Date(value).toISOString());

export const sourceRefSchema = z.object({
  system: z.string().min(1),
  instance: z.string().min(1),
  record_id: z.string().min(1).optional(),
  cursor: z.string().min(1).optional(),
  sequence: z.number().int().nonnegative().optional(),
});

export const evidenceRefSchema = z.object({
  type: z.enum([
    "source_record",
    "observation",
    "query",
    "artifact",
    "derivation",
  ]),
  ref: z.string().min(1),
  version: z.string().optional(),
  title: z.string().optional(),
  resolver: z
    .object({
      capability: z.string().min(1),
      arguments: z.record(z.string(), z.unknown()).optional(),
    })
    .optional(),
});

export type EvidenceRef = z.infer<typeof evidenceRefSchema>;

export const accessPolicySchema = z.object({
  classification: z
    .enum(["public", "internal", "confidential", "restricted"])
    .optional(),
  inherit_from_source: z.boolean().optional(),
  allow: z.array(z.string()).optional(),
  deny: z.array(z.string()).optional(),
});

const entityObservedDataSchema = z.object({
  name: z.string().optional(),
  lifecycle: z.enum(["active", "archived", "deleted"]).optional(),
  attributes: z.record(z.string(), z.unknown()).optional(),
});

const relationObservedDataSchema = z.object({
  relation: z.string().min(1),
  target: entityRefSchema,
  status: z.enum(["active", "inactive"]),
  attributes: z.record(z.string(), z.unknown()).optional(),
});

const eventOccurredDataSchema = z.object({
  type: z.string().min(1),
  actor: entityRefSchema.optional(),
  object: entityRefSchema.optional(),
  attributes: z.record(z.string(), z.unknown()).optional(),
});

const stateObservedDataSchema = z
  .object({
    field: z.string().min(1),
    operation: z.enum(["set", "unset"]),
    value: z.unknown().optional(),
    previous_value: z.unknown().optional(),
  })
  .superRefine((data, context) => {
    if (data.operation === "set" && !("value" in data)) {
      context.addIssue({
        code: "custom",
        path: ["value"],
        message: "value is required when operation is set",
      });
    }
  });

const metricObservedDataSchema = z.object({
  metric: z.string().min(1),
  value: z.number().finite(),
  unit: z.string().min(1),
  interval: z
    .object({
      from: utcTimestamp,
      to: utcTimestamp,
    })
    .optional(),
  dimensions: z.record(z.string(), z.string()).optional(),
  aggregation: z
    .enum([
      "gauge",
      "count",
      "sum",
      "average",
      "min",
      "max",
      "p50",
      "p90",
      "p95",
      "p99",
    ])
    .optional(),
  calculation: z
    .object({
      method: z.enum(["source", "derived"]),
      definition: z.string().optional(),
    })
    .optional(),
});

const envelopeSchema = z.object({
  spec_version: z.literal("sensus/0.1"),
  observation_id: z.string().min(1),
  tenant_id: z.string().min(1),
  subject: entityRefSchema,
  occurred_at: utcTimestamp,
  observed_at: utcTimestamp,
  source: sourceRefSchema,
  evidence: z.array(evidenceRefSchema).optional(),
  access: accessPolicySchema.optional(),
  confidence: z.number().min(0).max(1).optional(),
  labels: z.record(z.string(), z.string()).optional(),
  trace_id: z.string().optional(),
});

export const observationSchema = z.discriminatedUnion("kind", [
  envelopeSchema.extend({
    kind: z.literal("entity.observed"),
    data: entityObservedDataSchema,
  }),
  envelopeSchema.extend({
    kind: z.literal("relation.observed"),
    data: relationObservedDataSchema,
  }),
  envelopeSchema.extend({
    kind: z.literal("event.occurred"),
    data: eventOccurredDataSchema,
  }),
  envelopeSchema.extend({
    kind: z.literal("state.observed"),
    data: stateObservedDataSchema,
  }),
  envelopeSchema.extend({
    kind: z.literal("metric.observed"),
    data: metricObservedDataSchema,
  }),
]);

export type Observation = z.infer<typeof observationSchema>;
export type ObservationKind = Observation["kind"];

export const signalSchema = z.object({
  signal_id: z.string(),
  tenant_id: z.string(),
  type: z.string(),
  subject: entityRefSchema,
  detected_at: z.string(),
  updated_at: z.string(),
  status: z.enum(["open", "acknowledged", "resolved"]),
  severity: z.enum(["info", "warning", "critical"]),
  title: z.string(),
  description: z.string().optional(),
  confidence: z.number().min(0).max(1),
  change: z
    .object({
      direction: z.enum(["up", "down", "changed"]),
      current: z.number().optional(),
      baseline: z.number().optional(),
      delta: z.number().optional(),
      delta_percent: z.number().optional(),
      unit: z.string().optional(),
    })
    .optional(),
  detection: z.object({
    method: z.enum(["rule", "statistical", "model", "manual"]),
    definition: z.string(),
    evaluated_at: z.string(),
  }),
  evidence: z.array(evidenceRefSchema),
  access: accessPolicySchema.optional(),
});

export type Signal = z.infer<typeof signalSchema>;

export function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map(canonicalJson).join(",")}]`;
  }
  if (value !== null && typeof value === "object") {
    const record = value as Record<string, unknown>;
    return `{${Object.keys(record)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalJson(record[key])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value);
}

export function contentHash(value: unknown): string {
  return createHash("sha256").update(canonicalJson(value)).digest("hex");
}

export function stableId(prefix: string, ...parts: string[]): string {
  const digest = createHash("sha256").update(parts.join("\u001f")).digest("hex");
  return `${prefix}_${digest.slice(0, 24)}`;
}

