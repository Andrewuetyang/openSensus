import { readFileSync } from "node:fs";
import { z } from "zod/v4";
import {
  classifications,
  createConsumerContext,
  type ConsumerContext,
} from "./access.js";
import {
  AnonymousFallbackResolver,
  ApiKeyResolver,
  identityMappingSchema,
  type IdentityResolver,
} from "./identity.js";
import { OidcIdentityResolver } from "./identity-oidc.js";

const asymmetricAlgorithms = [
  "RS256",
  "RS384",
  "RS512",
  "ES256",
  "ES384",
  "ES512",
  "PS256",
  "PS384",
  "PS512",
  "EdDSA",
] as const;

const oidcConfigSchema = z.object({
  issuer: z.string().min(1),
  audience: z.string().min(1),
  /**
   * Discovered from `{issuer}/.well-known/openid-configuration` when omitted.
   * Set it explicitly for providers without a discovery document.
   */
  jwks_uri: z.string().min(1).optional(),
  algorithms: z.array(z.enum(asymmetricAlgorithms)).nonempty().optional(),
  clock_tolerance_seconds: z.number().int().nonnegative().max(300).optional(),
});

const consumerSchema = z.object({
  principals: z.array(z.string()).default(["role:agent"]),
  clearance: z.enum(classifications).default("internal"),
});

/**
 * Identity configuration, loaded from a JSON file named by `SENSUS_IDENTITY`.
 *
 * `oidc` or `api_key` selects how a credential is verified — at most one of
 * them. `anonymous` is separate and optional: it is the identity a request with
 * *no* credential is served as, and omitting it requires a credential on every
 * request.
 */
export const identityConfigSchema = z
  .object({
    oidc: oidcConfigSchema.optional(),
    /** Shared secret accepted as `Authorization: Bearer <value>`. */
    api_key: z.string().min(1).optional(),
    /**
     * Principal and clearance granted when `api_key` matches. Previously this
     * was read from `anonymous`, which conflated "the shared secret's identity"
     * with "the identity for requests carrying no credential at all".
     */
    api_key_identity: consumerSchema.optional(),
    /** Claim-to-ConsumerContext mapping. Applies to `oidc` only. */
    mapping: identityMappingSchema.prefault({}),
    /**
     * Identity assumed when a request carries no credential. Omit it to require
     * a credential on every request; a request with an unverifiable credential
     * is rejected either way.
     */
    anonymous: consumerSchema.optional(),
  })
  .superRefine((value, context) => {
    if (value.oidc && value.api_key) {
      context.addIssue({
        code: "custom",
        message: "Configure either oidc or api_key, not both",
      });
    }
    if (!value.oidc && !value.api_key && !value.anonymous) {
      context.addIssue({
        code: "custom",
        message:
          "Configure oidc, api_key, or anonymous; with none of them no request could ever be served",
      });
    }
  });

export type IdentityConfig = z.infer<typeof identityConfigSchema>;

export function loadIdentityConfig(path: string): IdentityConfig {
  const raw = readFileSync(path, "utf8");
  return identityConfigSchema.parse(JSON.parse(raw));
}

/**
 * Builds the resolver named by `SENSUS_IDENTITY`. Returns undefined when the
 * variable is unset, which leaves each entry point on its pre-identity
 * behaviour — a shared API key on the ingestion API, and trusted environment
 * configuration on stdio MCP.
 */
export function identityResolverFromEnvironment(
  environment: NodeJS.ProcessEnv = process.env,
): IdentityResolver | undefined {
  const path = environment.SENSUS_IDENTITY;
  if (!path) return undefined;
  return resolverFromConfig(loadIdentityConfig(path));
}

/**
 * The startup description of the configured identity mode, or undefined when no
 * configuration file is named. Call {@link identityResolverFromEnvironment}
 * first: that is the call that reports a malformed configuration.
 */
export function describeIdentityFromEnvironment(
  environment: NodeJS.ProcessEnv = process.env,
): string | undefined {
  const path = environment.SENSUS_IDENTITY;
  if (!path) return undefined;
  return describeIdentityConfig(loadIdentityConfig(path));
}

export function resolverFromConfig(config: IdentityConfig): IdentityResolver {
  const verifier = verifierFromConfig(config);
  if (!config.anonymous) {
    if (!verifier) {
      throw new Error("Identity config must set oidc, api_key, or anonymous");
    }
    return verifier;
  }
  return new AnonymousFallbackResolver(
    {
      consumer: consumerFrom(config.anonymous),
    },
    verifier,
  );
}

function verifierFromConfig(
  config: IdentityConfig,
): IdentityResolver | undefined {
  if (config.oidc) {
    return new OidcIdentityResolver({
      issuer: config.oidc.issuer,
      audience: config.oidc.audience,
      ...(config.oidc.jwks_uri ? { jwksUri: config.oidc.jwks_uri } : {}),
      ...(config.oidc.algorithms
        ? { algorithms: config.oidc.algorithms }
        : {}),
      ...(config.oidc.clock_tolerance_seconds === undefined
        ? {}
        : { clockToleranceSeconds: config.oidc.clock_tolerance_seconds }),
      mapping: config.mapping,
    });
  }
  if (config.api_key) {
    return new ApiKeyResolver({
      apiKey: config.api_key,
      consumer: consumerFrom(config.api_key_identity),
    });
  }
  return undefined;
}

function consumerFrom(
  identity: IdentityConfig["anonymous"],
): ConsumerContext {
  return createConsumerContext({
    principals: identity?.principals ?? ["role:agent"],
    clearance: identity?.clearance ?? "internal",
  });
}

/** A startup log line that names the mode without disclosing any secret. */
export function describeIdentityConfig(config: IdentityConfig): string {
  const parts: string[] = [];
  if (config.oidc) {
    const mapping = config.mapping;
    parts.push(
      `oidc issuer=${config.oidc.issuer}`,
      `audience=${config.oidc.audience}`,
      `clearance ceiling=${mapping.clearance.ceiling}`,
      `tenant claim=${mapping.tenant.claim ?? "none"}`,
    );
  }
  if (config.api_key) parts.push("shared api key");
  if (config.anonymous) {
    parts.push(
      `anonymous callers served as ${config.anonymous.principals.join(",") || "no principal"} ` +
        `at clearance ${config.anonymous.clearance}`,
    );
  }
  return parts.length ? parts.join("; ") : "unconfigured";
}
