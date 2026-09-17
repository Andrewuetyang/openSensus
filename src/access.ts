import type { z } from "zod/v4";
import { accessPolicySchema } from "./protocol.js";

export const classifications = [
  "public",
  "internal",
  "confidential",
  "restricted",
] as const;

export type Classification = (typeof classifications)[number];
export type AccessPolicy = z.infer<typeof accessPolicySchema>;

export interface ConsumerContext {
  principals: ReadonlySet<string>;
  clearance: Classification;
  /** Internal runtime jobs may bypass item ACLs, but never tenant isolation. */
  system: boolean;
}

export const systemConsumer: ConsumerContext = {
  principals: new Set(["system:sensus"]),
  clearance: "restricted",
  system: true,
};

export function createConsumerContext(input?: {
  principals?: Iterable<string>;
  clearance?: Classification;
  system?: boolean;
}): ConsumerContext {
  return {
    principals: new Set(input?.principals ?? []),
    clearance: input?.clearance ?? "internal",
    system: input?.system ?? false,
  };
}

export function consumerFromEnvironment(
  environment: NodeJS.ProcessEnv = process.env,
): ConsumerContext {
  const principals = (environment.SENSUS_PRINCIPALS ?? "role:agent")
    .split(",")
    .map((value) => value.trim())
    .filter(Boolean);
  const requestedClearance = environment.SENSUS_CLEARANCE ?? "internal";
  const clearance = classifications.includes(requestedClearance as Classification)
    ? (requestedClearance as Classification)
    : "internal";
  return createConsumerContext({ principals, clearance });
}

export function canRead(
  policy: AccessPolicy | undefined,
  consumer: ConsumerContext,
): boolean {
  if (consumer.system) return true;
  const classification = policy?.classification ?? "internal";
  if (classificationRank(classification) > classificationRank(consumer.clearance)) {
    return false;
  }

  const deny = policy?.deny ?? [];
  if (deny.some((principal) => consumer.principals.has(principal))) return false;

  const allow = policy?.allow;
  if (allow !== undefined && !allow.some((principal) => consumer.principals.has(principal))) {
    return false;
  }

  // An unresolved source ACL is fail-closed. Producers that request inheritance must
  // resolve it to an explicit allow/deny set before ingestion.
  if (
    policy?.inherit_from_source === true &&
    policy.allow === undefined &&
    policy.deny === undefined
  ) {
    return false;
  }

  return true;
}

export function combinePolicies(
  policies: Array<AccessPolicy | undefined>,
): AccessPolicy {
  const classification = policies.reduce<Classification>((current, policy) => {
    const candidate = policy?.classification ?? "internal";
    return classificationRank(candidate) > classificationRank(current)
      ? candidate
      : current;
  }, "public");

  const allowLists = policies
    .map((policy) => policy?.allow)
    .filter((value): value is string[] => Boolean(value?.length));
  const allow = allowLists.length
    ? [...allowLists.slice(1).reduce<Set<string>>((intersection, list) => {
        return new Set(list.filter((principal) => intersection.has(principal)));
      }, new Set(allowLists[0]))]
    : undefined;
  const deny = [
    ...new Set(policies.flatMap((policy) => policy?.deny ?? [])),
  ];

  return {
    classification,
    ...(allow !== undefined ? { allow } : {}),
    ...(deny.length ? { deny } : {}),
    ...(policies.some((policy) => policy?.inherit_from_source)
      ? { inherit_from_source: true }
      : {}),
  };
}

export function classificationRank(classification: Classification): number {
  return classifications.indexOf(classification);
}

/** Returns the less privileged of two clearances. Used to enforce a ceiling. */
export function lowestClearance(
  left: Classification,
  right: Classification,
): Classification {
  return classificationRank(left) <= classificationRank(right) ? left : right;
}

export function isClassification(value: unknown): value is Classification {
  return (
    typeof value === "string" &&
    classifications.includes(value as Classification)
  );
}
