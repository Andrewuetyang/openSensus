import { z } from "zod/v4";
import {
  classificationRank,
  classifications,
  createConsumerContext,
  isClassification,
  lowestClearance,
  type Classification,
  type ConsumerContext,
} from "./access.js";

/**
 * A verified claim set. Producing one of these is the only thing a
 * deployment-specific verifier has to do; turning it into a ConsumerContext is
 * handled here so every deployment shares the same authorization rules.
 */
export interface VerifiedIdentity {
  subject: string;
  groups: readonly string[];
  claims: Readonly<Record<string, unknown>>;
  /** Seconds since epoch. The MCP bearer gate rejects tokens without it. */
  expiresAt?: number;
}

export interface ResolvedIdentity {
  consumer: ConsumerContext;
  /** Present only when the mapping derived a tenant from the identity. */
  tenantId?: string;
  /** The verified subject, when the resolver had one. */
  subject?: string;
  /** Seconds since epoch, when the credential expires. */
  expiresAt?: number;
}

export class UnauthenticatedError extends Error {
  constructor(message = "Missing or invalid credentials") {
    super(message);
    this.name = "UnauthenticatedError";
  }
}

export class ForbiddenIdentityError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "ForbiddenIdentityError";
  }
}

export interface IdentityRequest {
  /** Raw `Authorization` header value, when the transport supplies one. */
  authorization: string | undefined;
}

/**
 * The seam between a transport and Sensus authorization. Implementations verify
 * a credential; the returned ConsumerContext is what every read is filtered by.
 */
export interface IdentityResolver {
  resolve(request: IdentityRequest): Promise<ResolvedIdentity>;
  /**
   * True when this resolver serves a request that carries no credential.
   *
   * Nothing authorizes such a request, so entry points log it loudly. Only a
   * resolver built from an explicit `anonymous` block should set it.
   */
  readonly acceptsAnonymous?: boolean;
}

const principalSourceSchema = z.object({
  /** Claim to read. Array claims contribute one principal per element. */
  claim: z.string().min(1),
  /** Prepended to a value that `map` does not override. */
  prefix: z.string().default(""),
  /** Exact value replacements, applied before `prefix`. */
  map: z.record(z.string(), z.string()).default({}),
});

const clearanceMappingSchema = z.object({
  /** Scalar claim carrying a clearance name. Invalid values are ignored. */
  claim: z.string().optional(),
  /** Group name to clearance. The most privileged match wins, before the ceiling. */
  from_groups: z.record(z.string(), z.enum(classifications)).default({}),
  /** Used when neither the claim nor a group yields a clearance. */
  default: z.enum(classifications).default("internal"),
  /**
   * Upper bound this mapping can never exceed, whatever the identity provider
   * asserts. A misconfigured or compromised provider cannot raise it.
   */
  ceiling: z.enum(classifications).default("restricted"),
});

const tenantMappingSchema = z
  .object({
    claim: z.string().min(1).optional(),
    /**
     * Tenants a token may name. Required whenever `claim` is set, so that a
     * token can never select an arbitrary tenant.
     */
    allowed: z.array(z.string().min(1)).min(1).optional(),
  })
  .superRefine((value, context) => {
    if (value.claim !== undefined && value.allowed === undefined) {
      context.addIssue({
        code: "custom",
        path: ["allowed"],
        message: "allowed is required when tenant.claim is configured",
      });
    }
  });

export const identityMappingSchema = z.object({
  /** Claim holding the subject. Defaults to the standard `sub`. */
  subject_claim: z.string().min(1).default("sub"),
  /** Claim holding group membership. Defaults to the standard `groups`. */
  groups_claim: z.string().min(1).default("groups"),
  /** Prefix applied to the subject principal. Set to "" to omit it. */
  subject_prefix: z.string().default("user:"),
  /** Additional principals derived from other claims. */
  principals: z.array(principalSourceSchema).default([]),
  clearance: clearanceMappingSchema.prefault({}),
  tenant: tenantMappingSchema.prefault({}),
});

export type IdentityMapping = z.infer<typeof identityMappingSchema>;

export const defaultIdentityMapping: IdentityMapping =
  identityMappingSchema.parse({});

/**
 * Turns a verified claim set into the ConsumerContext Sensus authorizes
 * against. Three rules are enforced here regardless of configuration:
 *
 * 1. Clearance is capped by `clearance.ceiling`.
 * 2. A tenant taken from a token must be in `tenant.allowed`.
 * 3. `system` is never true. It bypasses every item ACL, so no external
 *    identity may ever produce it.
 */
export function mapIdentity(
  identity: VerifiedIdentity,
  mapping: IdentityMapping = defaultIdentityMapping,
): ResolvedIdentity {
  const principals = new Set<string>();

  if (mapping.subject_prefix) {
    principals.add(`${mapping.subject_prefix}${identity.subject}`);
  }

  for (const source of mapping.principals) {
    for (const value of claimValues(identity.claims, source.claim)) {
      if (!value) continue;
      principals.add(source.map[value] ?? `${source.prefix}${value}`);
    }
  }

  const clearance = lowestClearance(
    resolveClearance(identity, mapping.clearance),
    mapping.clearance.ceiling,
  );

  const tenantId = resolveTenant(identity, mapping.tenant);

  return {
    consumer: createConsumerContext({
      principals,
      clearance,
      system: false,
    }),
    ...(tenantId === undefined ? {} : { tenantId }),
    subject: identity.subject,
    ...(identity.expiresAt === undefined ? {} : { expiresAt: identity.expiresAt }),
  };
}

function resolveClearance(
  identity: VerifiedIdentity,
  mapping: IdentityMapping["clearance"],
): Classification {
  let claimed: Classification | undefined;
  if (mapping.claim) {
    const raw = identity.claims[mapping.claim];
    // An unrecognized value is ignored rather than rejected, so a malformed
    // claim falls back to `default` instead of granting something unexpected.
    if (isClassification(raw)) claimed = raw;
  }

  // The most privileged matching group wins; the caller applies the ceiling.
  let fromGroup: Classification | undefined;
  for (const [group, clearance] of Object.entries(mapping.from_groups)) {
    if (!identity.groups.includes(group)) continue;
    if (
      fromGroup === undefined ||
      classificationRank(clearance) > classificationRank(fromGroup)
    ) {
      fromGroup = clearance;
    }
  }

  return claimed ?? fromGroup ?? mapping.default;
}

function resolveTenant(
  identity: VerifiedIdentity,
  mapping: IdentityMapping["tenant"],
): string | undefined {
  if (!mapping.claim) return undefined;
  const raw = identity.claims[mapping.claim];
  if (typeof raw !== "string" || !raw) return undefined;
  const allowed = mapping.allowed ?? [];
  if (!allowed.includes(raw)) {
    throw new ForbiddenIdentityError(
      `Identity asserts tenant ${raw}, which is not in the allowed list`,
    );
  }
  return raw;
}

function claimValues(
  claims: Readonly<Record<string, unknown>>,
  claim: string,
): string[] {
  const raw = claims[claim];
  if (raw === undefined || raw === null) return [];
  const values = Array.isArray(raw) ? raw : [raw];
  return values
    .filter((value) => value !== null && value !== undefined)
    .map((value) =>
      typeof value === "string" ? value : JSON.stringify(value),
    );
}

/**
 * Shared-secret authentication. This is the behavior Sensus had before
 * identity resolvers existed, kept so existing deployments keep working and so
 * a single-tenant install needs no identity provider.
 */
export class ApiKeyResolver implements IdentityResolver {
  constructor(
    private readonly options: {
      apiKey: string;
      consumer: ConsumerContext;
      tenantId?: string;
    },
  ) {}

  async resolve(request: IdentityRequest): Promise<ResolvedIdentity> {
    if (request.authorization !== `Bearer ${this.options.apiKey}`) {
      throw new UnauthenticatedError();
    }
    return {
      consumer: this.options.consumer,
      ...(this.options.tenantId === undefined
        ? {}
        : { tenantId: this.options.tenantId }),
    };
  }
}

/** Accepts every request. Only for local development with no auth configured. */
export class AnonymousResolver implements IdentityResolver {
  readonly acceptsAnonymous = true;

  constructor(
    private readonly consumer: ConsumerContext,
    private readonly tenantId?: string,
  ) {}

  async resolve(): Promise<ResolvedIdentity> {
    return {
      consumer: this.consumer,
      ...(this.tenantId === undefined ? {} : { tenantId: this.tenantId }),
    };
  }
}

/**
 * Serves requests that carry no credential as a configured identity, and
 * delegates everything else to the real verifier.
 *
 * This is what the `anonymous` block in the identity configuration means. A
 * request that *does* carry a credential is never silently downgraded to the
 * anonymous identity — with nothing configured to verify it, an unverifiable
 * credential is a failed authentication, not an anonymous request.
 */
export class AnonymousFallbackResolver implements IdentityResolver {
  readonly acceptsAnonymous = true;

  constructor(
    private readonly identity: ResolvedIdentity,
    private readonly inner?: IdentityResolver,
  ) {}

  async resolve(request: IdentityRequest): Promise<ResolvedIdentity> {
    if (!request.authorization) return this.identity;
    if (!this.inner) throw new UnauthenticatedError();
    return this.inner.resolve(request);
  }
}

/** Reads a `Bearer` credential out of an Authorization header. */
export function bearerToken(authorization: string | undefined): string | undefined {
  if (!authorization) return undefined;
  const match = authorization.match(/^Bearer\s+(.+)$/i);
  return match?.[1]?.trim() || undefined;
}
