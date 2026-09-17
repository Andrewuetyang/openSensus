import { createServer } from "node:http";
import {
  createMcpHandler,
  type AuthInfo,
  type McpServerFactory,
} from "@modelcontextprotocol/server";
import { consumerFromEnvironment, type ConsumerContext } from "./access.js";
import {
  describeIdentityFromEnvironment,
  identityResolverFromEnvironment,
} from "./identity-config.js";
import {
  ForbiddenIdentityError,
  UnauthenticatedError,
  type IdentityResolver,
  type ResolvedIdentity,
} from "./identity.js";
import { buildMcpServer } from "./mcp.js";
import { createNodeFetchBridge } from "./node-http.js";
import {
  describeStorage,
  openStorageFromEnvironment,
  selectStorage,
} from "./storage-factory.js";
import type { SensusStorage } from "./storage.js";
import { SensusWorld } from "./world.js";

/**
 * Key under which the resolved identity rides on the SDK's pass-through
 * `AuthInfo`. The handler performs no verification of its own, so this is the
 * only channel from per-request authentication into the server factory.
 */
const identityKey = "sensus.identity";

interface RequestIdentity {
  consumer: ConsumerContext;
  tenantId: string;
}

type AuthOutcome =
  | { ok: true; authInfo: AuthInfo }
  | { ok: false; response: Response };

export interface McpHttpOptions {
  store: SensusStorage;
  /** Tenant served when the identity does not name one. */
  tenantId?: string;
  /** Identity used when no resolver is configured. */
  fallbackConsumer: ConsumerContext;
  resolver?: IdentityResolver;
}

/**
 * Builds the per-request server factory. A fresh `SensusWorld` is created for
 * each request, which is what makes one MCP endpoint serve many identities:
 * every read is filtered by the consumer resolved from that request's token.
 */
export function buildRequestFactory(options: McpHttpOptions): McpServerFactory {
  return (context) => {
    const identity = identityOf(context.authInfo);
    return buildMcpServer(
      new SensusWorld(options.store, identity.tenantId, identity.consumer),
    );
  };
}

export function createSensusMcpHttpServer(options: McpHttpOptions) {
  const handler = createMcpHandler(buildRequestFactory(options));

  const authenticate = async (request: Request): Promise<AuthOutcome> => {
    if (!options.resolver) {
      return {
        ok: true,
        authInfo: toAuthInfo({
          consumer: options.fallbackConsumer,
          tenantId: requireTenant(options.tenantId),
        }),
      };
    }

    try {
      const identity = await options.resolver.resolve({
        authorization: request.headers.get("authorization") ?? undefined,
      });
      return {
        ok: true,
        authInfo: toAuthInfo({
          consumer: identity.consumer,
          tenantId: selectTenant(identity.tenantId, options.tenantId),
          ...(identity.subject === undefined ? {} : { subject: identity.subject }),
          ...(identity.expiresAt === undefined
            ? {}
            : { expiresAt: identity.expiresAt }),
        }),
      };
    } catch (error) {
      if (error instanceof UnauthenticatedError) {
        return { ok: false, response: unauthorized(error.message) };
      }
      if (error instanceof ForbiddenIdentityError) {
        return { ok: false, response: forbidden(error.message) };
      }
      throw error;
    }
  };

  const serve = createNodeFetchBridge(async (request) => {
    const outcome = await authenticate(request);
    if (!outcome.ok) return outcome.response;
    return handler.fetch(request, { authInfo: outcome.authInfo });
  });

  const server = createServer((request, response) => {
    void serve(request, response);
  });
  return { server, close: () => handler.close() };
}

/**
 * Tenant pinning. A deployment that names a tenant never serves another, even
 * if a token asserts one, so a token cannot move a request across tenants.
 */
export function selectTenant(
  identityTenant: string | undefined,
  pinnedTenant: string | undefined,
): string {
  if (pinnedTenant) {
    if (identityTenant && identityTenant !== pinnedTenant) {
      throw new ForbiddenIdentityError(
        "Identity tenant does not match the configured tenant",
      );
    }
    return pinnedTenant;
  }
  if (identityTenant) return identityTenant;
  throw new ForbiddenIdentityError(
    "No tenant could be determined from the request or the configuration",
  );
}

function requireTenant(tenantId: string | undefined): string {
  if (!tenantId) {
    throw new Error(
      "SENSUS_TENANT_ID is required when the MCP HTTP endpoint runs without an identity resolver",
    );
  }
  return tenantId;
}

function toAuthInfo(
  identity: RequestIdentity & Pick<ResolvedIdentity, "subject" | "expiresAt">,
): AuthInfo {
  return {
    token: "",
    clientId: identity.subject ?? "anonymous",
    scopes: [...identity.consumer.principals],
    ...(identity.expiresAt === undefined ? {} : { expiresAt: identity.expiresAt }),
    extra: { [identityKey]: identity },
  };
}

function identityOf(authInfo: AuthInfo | undefined): RequestIdentity {
  const value = authInfo?.extra?.[identityKey] as RequestIdentity | undefined;
  if (!value) {
    throw new Error("MCP request reached the server factory without an identity");
  }
  return value;
}

function unauthorized(message: string): Response {
  return Response.json(
    { error: { code: "UNAUTHENTICATED", message, retryable: false } },
    { status: 401, headers: { "www-authenticate": "Bearer" } },
  );
}

function forbidden(message: string): Response {
  return Response.json(
    { error: { code: "PERMISSION_DENIED", message, retryable: false } },
    { status: 403 },
  );
}

async function main(): Promise<void> {
  const tenantId = process.env.SENSUS_TENANT_ID;
  const resolver = identityResolverFromEnvironment();

  if (
    (!resolver || resolver.acceptsAnonymous) &&
    process.env.SENSUS_MCP_ALLOW_ANONYMOUS !== "true"
  ) {
    console.error(
      "Refusing to start an MCP HTTP endpoint that serves unauthenticated callers.\n" +
        (resolver
          ? "The configured identity resolves requests carrying no credential (its `anonymous` block).\n"
          : "") +
        "Set SENSUS_IDENTITY to a configuration file that requires a credential, or set " +
        "SENSUS_MCP_ALLOW_ANONYMOUS=true to accept this risk explicitly.",
    );
    process.exit(1);
  }

  const selection = selectStorage();
  const store = await openStorageFromEnvironment();
  const host = process.env.HOST ?? "127.0.0.1";
  const port = Number(process.env.MCP_PORT ?? 8788);

  const { server, close } = createSensusMcpHttpServer({
    store,
    ...(tenantId ? { tenantId } : {}),
    fallbackConsumer: consumerFromEnvironment(),
    ...(resolver ? { resolver } : {}),
  });

  server.listen(port, host, () => {
    console.error(`Sensus MCP HTTP listening on http://${host}:${port}`);
    console.error(`Storage: ${describeStorage(selection)}`);
    console.error(
      resolver
        ? resolver.acceptsAnonymous
          ? `Identity: ${describeIdentityFromEnvironment() ?? "resolver configured"} — serving requests with no credential as the configured anonymous identity (SENSUS_MCP_ALLOW_ANONYMOUS=true)`
          : `Identity: ${describeIdentityFromEnvironment() ?? "resolver configured"}; each request is authorized separately`
        : "Identity: NONE — every caller is served as the process consumer",
    );
  });

  const shutdown = (): void => {
    void close().finally(() => {
      server.close(() => {
        void store.close().finally(() => process.exit(0));
      });
    });
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

if (
  process.argv[1] &&
  import.meta.url === new URL(`file://${process.argv[1]}`).href
) {
  void main();
}
