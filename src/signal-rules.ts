import { z } from "zod/v4";

const ruleBaseSchema = z.object({
  rule_id: z.string().min(1),
  name: z.string().min(1),
  enabled: z.boolean().default(true),
  applies_to: z.object({
    metric: z.string().min(1),
    subject_types: z.array(z.string()).optional(),
    dimensions: z.record(z.string(), z.string()).default({}),
  }),
  signal_type: z.string().min(1),
  severity: z.enum(["info", "warning", "critical"]),
  confidence: z.number().min(0).max(1).default(0.9),
  title: z.string().min(1).optional(),
});

const relativeChangeConditionSchema = z.object({
  kind: z.literal("relative_change"),
  direction: z.enum(["increase", "decrease"]),
  threshold_percent: z.number().nonnegative(),
  minimum_baseline: z.number().nonnegative().default(0),
  for_samples: z.number().int().min(1).max(20).default(1),
});

const thresholdConditionSchema = z.object({
  kind: z.literal("threshold"),
  operator: z.enum(["gt", "gte", "lt", "lte"]),
  value: z.number(),
  for_samples: z.number().int().min(1).max(20).default(1),
});

export const signalRuleSchema = ruleBaseSchema.extend({
  condition: z.discriminatedUnion("kind", [
    relativeChangeConditionSchema,
    thresholdConditionSchema,
  ]),
});

export type SignalRule = z.infer<typeof signalRuleSchema>;

export interface MetricPoint {
  observation_id: string;
  value: number;
  unit: string;
  occurred_at: string;
  dimensions: Record<string, string>;
}

export interface RuleEvaluation {
  matched: boolean;
  definition: string;
  current?: number;
  baseline?: number;
  delta?: number;
  deltaPercent?: number;
  direction?: "up" | "down" | "changed";
  evidenceObservationIds: string[];
}

export function defaultSignificantIncreaseRule(): SignalRule {
  return {
    rule_id: "builtin.metric_significant_increase",
    name: "Significant metric increase",
    enabled: true,
    applies_to: { metric: "*", dimensions: {} },
    condition: {
      kind: "relative_change",
      direction: "increase",
      threshold_percent: 50,
      minimum_baseline: 0,
      for_samples: 1,
    },
    signal_type: "metric.significant_increase",
    severity: "warning",
    confidence: 0.9,
  };
}

export function ruleApplies(
  rule: SignalRule,
  input: {
    metric: string;
    subjectType: string;
    dimensions: Record<string, string>;
  },
): boolean {
  if (!rule.enabled) return false;
  if (rule.applies_to.metric !== "*" && rule.applies_to.metric !== input.metric) {
    return false;
  }
  if (
    rule.applies_to.subject_types?.length &&
    !rule.applies_to.subject_types.includes(input.subjectType)
  ) {
    return false;
  }
  return Object.entries(rule.applies_to.dimensions).every(
    ([key, value]) => input.dimensions[key] === value,
  );
}

export function evaluateRule(
  rule: SignalRule,
  newestFirst: MetricPoint[],
): RuleEvaluation {
  const condition = rule.condition;
  if (condition.kind === "relative_change") {
    const required = condition.for_samples + 1;
    const rows = newestFirst.slice(0, required);
    if (rows.length < required) {
      return {
        matched: false,
        definition: describeCondition(condition),
        evidenceObservationIds: rows.map((row) => row.observation_id),
      };
    }

    const comparisons = rows.slice(0, -1).map((current, index) => {
      const baseline = rows[index + 1]!;
      if (Math.abs(baseline.value) < condition.minimum_baseline) return false;
      if (baseline.value === 0) return false;
      const percent = ((current.value - baseline.value) / Math.abs(baseline.value)) * 100;
      return condition.direction === "increase"
        ? percent >= condition.threshold_percent
        : percent <= -condition.threshold_percent;
    });
    const current = rows[0]!;
    const baseline = rows[1]!;
    const delta = current.value - baseline.value;
    const deltaPercent = baseline.value === 0 ? undefined : (delta / Math.abs(baseline.value)) * 100;
    return {
      matched: comparisons.every(Boolean),
      definition: describeCondition(condition),
      current: current.value,
      baseline: baseline.value,
      delta,
      ...(deltaPercent === undefined ? {} : { deltaPercent }),
      direction: delta >= 0 ? "up" : "down",
      evidenceObservationIds: rows.map((row) => row.observation_id),
    };
  }

  const rows = newestFirst.slice(0, condition.for_samples);
  const matched =
    rows.length === condition.for_samples &&
    rows.every((row) => compareThreshold(row.value, condition.operator, condition.value));
  const current = rows[0];
  const previous = newestFirst[condition.for_samples];
  const delta = current && previous ? current.value - previous.value : undefined;
  const deltaPercent =
    delta !== undefined && previous && previous.value !== 0
      ? (delta / Math.abs(previous.value)) * 100
      : undefined;
  return {
    matched,
    definition: describeCondition(condition),
    ...(current ? { current: current.value } : {}),
    ...(previous ? { baseline: previous.value } : {}),
    ...(delta === undefined ? {} : { delta }),
    ...(deltaPercent === undefined ? {} : { deltaPercent }),
    ...(delta === undefined ? {} : { direction: delta >= 0 ? "up" : "down" }),
    evidenceObservationIds: rows.map((row) => row.observation_id),
  };
}

function compareThreshold(
  value: number,
  operator: "gt" | "gte" | "lt" | "lte",
  threshold: number,
): boolean {
  switch (operator) {
    case "gt":
      return value > threshold;
    case "gte":
      return value >= threshold;
    case "lt":
      return value < threshold;
    case "lte":
      return value <= threshold;
  }
}

function describeCondition(
  condition: SignalRule["condition"],
): string {
  if (condition.kind === "relative_change") {
    return `${condition.direction} >= ${condition.threshold_percent}% for ${condition.for_samples} consecutive sample(s), minimum baseline ${condition.minimum_baseline}`;
  }
  return `value ${condition.operator} ${condition.value} for ${condition.for_samples} consecutive sample(s)`;
}

