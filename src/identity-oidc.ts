import {
  createRemoteJWKSet,
  jwtVerify,
  type JWSAlgorithm,
  type JWTPayload,
} from "jose";
import {
  bearerToken,
  defaultIdentityMapping,
  mapIdentity,
  UnauthenticatedError,
  type IdentityMapping,
  type IdentityRequest,
  type IdentityResolver,
  type ResolvedIdentity,
  type VerifiedIdentity,
} from "./identity.js";

/**
 * Asymmetric algorithms only. Symmetric algorithms are deliberately absent so a
 * token can never be verified against a shared secret that a caller could
 * derive from public key material.
 */
const defaultAlgorithms: readonly JWSAlgorithm[] = [
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
];

export interface OidcResolverOptions {
  /** Expected `iss`, exactly as the provider emits it. */
  issuer: string;
  /** Expected `aud`. Required: an unbound token should not be accepted. */
  audience: string;
  /**
   * JWKS endpoint. When omitted it is discovered from
   * `{issuer}/.well-known/openid-configuration` on first use.
   */
  jwksUri?: string;
  algorithms?: readonly JWSAlgorithm[];
  mapping?: IdentityMapping;
  /** Leeway in seconds applied to `exp` and `nbf`. Default 5. */
  clockToleranceSeconds?: number;
}

/**
 * Verifies an OIDC/OAuth2 JWT and maps its claims to a ConsumerContext.
 *
 * This covers the providers most enterprises already run — Entra ID, Okta,
 * Keycloak, Auth0, Google Workspace — because they all issue OIDC JWTs. Anything
 * that does not can implement {@link IdentityResolver} directly.
 */
export class OidcIdentityResolver implements IdentityResolver {
  private readonly algorithms: readonly JWSAlgorithm[];
  private readonly mapping: IdentityMapping;
  private readonly clockTolerance: number;
  private keys?: ReturnType<typeof createRemoteJWKSet>;

  constructor(private readonly options: OidcResolverOptions) {
    this.algorithms = options.algorithms ?? defaultAlgorithms;
    this.mapping = options.mapping ?? defaultIdentityMapping;
    this.clockTolerance = options.clockToleranceSeconds ?? 5;
  }

  async resolve(request: IdentityRequest): Promise<ResolvedIdentity> {
    const token = bearerToken(request.authorization);
    if (!token) throw new UnauthenticatedError();

    let payload: JWTPayload;
    try {
      const keys = await this.keyResolver();
      const verified = await jwtVerify(token, keys, {
        issuer: this.options.issuer,
        audience: this.options.audience,
        algorithms: [...this.algorithms],
        clockTolerance: this.clockTolerance,
      });
      payload = verified.payload;
    } catch (error) {
      // Every verification failure is one answer to the caller: the token is
      // not acceptable. The specific reason is not disclosed.
      throw new UnauthenticatedError(
        `Token verification failed: ${error instanceof Error ? error.message : "unknown error"}`,
      );
    }

    return mapIdentity(this.toVerifiedIdentity(payload), this.mapping);
  }

  private toVerifiedIdentity(payload: JWTPayload): VerifiedIdentity {
    const subject = payload[this.mapping.subject_claim];
    if (typeof subject !== "string" || !subject) {
      throw new UnauthenticatedError(
        `Token is missing a string ${this.mapping.subject_claim} claim`,
      );
    }

    const rawGroups = payload[this.mapping.groups_claim];
    const groups = Array.isArray(rawGroups)
      ? rawGroups.filter((group): group is string => typeof group === "string")
      : typeof rawGroups === "string"
        ? [rawGroups]
        : [];

    return {
      subject,
      groups,
      claims: payload as Record<string, unknown>,
      // The MCP bearer gate refuses tokens without an expiry, so a token that
      // does not expire cannot be served at all.
      ...(typeof payload.exp === "number" ? { expiresAt: payload.exp } : {}),
    };
  }

  private async keyResolver(): Promise<ReturnType<typeof createRemoteJWKSet>> {
    if (this.keys) return this.keys;
    const uri = this.options.jwksUri ?? (await this.discoverJwksUri());
    this.keys = createRemoteJWKSet(new URL(uri), {
      // createRemoteJWKSet caches keys and refetches on an unknown `kid`, which
      // is what makes a provider's key rotation transparent.
      cooldownDuration: 30_000,
    });
    return this.keys;
  }

  private async discoverJwksUri(): Promise<string> {
    const issuer = this.options.issuer.replace(/\/+$/, "");
    const discoveryUrl = `${issuer}/.well-known/openid-configuration`;
    const response = await fetch(discoveryUrl);
    if (!response.ok) {
      throw new Error(
        `OIDC discovery failed: ${discoveryUrl} returned ${response.status}`,
      );
    }
    const metadata = (await response.json()) as { jwks_uri?: unknown };
    if (typeof metadata.jwks_uri !== "string" || !metadata.jwks_uri) {
      throw new Error(
        `OIDC discovery failed: ${discoveryUrl} has no jwks_uri`,
      );
    }
    return metadata.jwks_uri;
  }
}
