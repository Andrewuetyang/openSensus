import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { z, ZodError } from "zod/v4";
import { observationSchema, utcTimestamp, type Observation } from "./protocol.js";
import {
  CorrectionError,
  ObservationConflictError,
  SyncError,
  type IngestResult,
} from "./store.js";
import {
  describeStorage,
  openStorageFromEnvironment,
  selectStorage,
} from "./storage-factory.js";
import type { SensusStorage } from "./storage.js";
import {
  consumerFromEnvironment,
  createConsumerContext,
  type ConsumerContext,
} from "./access.js";
import {
  AnonymousResolver,
  ApiKeyResolver,
  ForbiddenIdentityError,
  UnauthenticatedError,
  type IdentityResolver,
} from "./identity.js";
import {
  describeIdentityFromEnvironment,
  identityResolverFromEnvironment,
} from "./identity-config.js";
import { signalRuleSchema } from "./signal-rules.js";

const batchEnvelopeSchema = z.object({
  observations: z.array(z.unknown()).max(1000),
});

const startSyncSchema = z.object({
  tenant_id: z.string().min(1),
  sync_id: z.string().min(1),
  mode: z.enum(["snapshot", "incremental", "reconciliation"]),
  source: z.object({ system: z.string().min(1), instance: z.string().min(1) }),
  authoritative_deletion: z.boolean().default(false),
  started_at: utcTimestamp.optional(),
});

const completeSyncSchema = z.object({
  completed_at: utcTimestamp.optional(),
  record_count: z.number().int().nonnegative().optional(),
  cursor: z.string().optional(),
});

const signalRuleEnvelopeSchema = z.object({
  tenant_id: z.string().min(1),
  rule: signalRuleSchema,
});

/** One batch slot: either an ingest outcome or a per-item rejection. */
type BatchItem =
  | IngestResult
  | {
      observation_id: string;
      status: "rejected";
      error: Record<string, unknown>;
    };

type HttpOptions = {
  store: SensusStorage;
  /** Per-request identity. Takes precedence over `apiKey` when supplied. */
  identity?: IdentityResolver;
  apiKey?: string;
  defaultTenantId?: string;
  maxBodyBytes?: number;
  consumer?: ConsumerContext;
  /**
   * Allows a request to name its own tenant when neither the identity nor
   * `defaultTenantId` determined one — `x-sensus-tenant` on read routes, the
   * body on write routes. Off by default: with it off, the request is rejected
   * rather than served against a tenant no allowlist vouches for. Only enable
   * it behind an identity resolver that already constrains tenants through
   * `mapping.tenant.allowed`.
   */
  allowRequestTenant?: boolean;
};

export function createSensusHttpServer(options: HttpOptions) {
  const maxBodyBytes = options.maxBodyBytes ?? 10 * 1024 * 1024;
  const resolver = options.identity ?? legacyResolver(options);
  const allowRequestTenant = options.allowRequestTenant ?? false;

  return createServer(async (request, response) => {
    try {
      const auth = await authenticate(request, resolver);
      if (!auth.ok) {
        return sendError(response, auth.status, auth.code, auth.message);
      }
      const { consumer } = auth;
      // A tenant named by the identity takes precedence over the configured
      // default; a deployment that pins both rejects a mismatch upstream.
      const requestTenant = auth.tenantId ?? options.defaultTenantId;

      const url = new URL(request.url ?? "/", "http://localhost");
      if (request.method === "GET" && url.pathname === "/health") {
        return sendJson(response, 200, {
          status: "ok",
          service: "sensus",
          protocol: "sensus/0.1",
        });
      }

      if (request.method === "POST" && url.pathname === "/v1/observations") {
        const body = await readJsonBody(request, maxBodyBytes);
        const observation = observationSchema.parse(body);
        assertTenant(observation.tenant_id, requestTenant, allowRequestTenant);
        const syncId = headerValue(request, "sensus-sync-id");
        const result = await options.store.ingest(observation, {
          ...(syncId ? { syncId } : {}),
        });
        return sendJson(response, 200, result);
      }

      if (
        request.method === "POST" &&
        url.pathname === "/v1/observations/batch"
      ) {
        const body = batchEnvelopeSchema.parse(
          await readJsonBody(request, maxBodyBytes),
        );
        const syncId = headerValue(request, "sensus-sync-id");
        // Sequential rather than concurrent: each item is still accepted or
        // rejected independently, but a batch holds one write transaction at a
        // time instead of contending for the writer.
        const results: BatchItem[] = [];
        for (const [index, candidate] of body.observations.entries()) {
          try {
            const observation = observationSchema.parse(candidate);
            assertTenant(observation.tenant_id, requestTenant, allowRequestTenant);
            results.push(
              await options.store.ingest(observation, {
                ...(syncId ? { syncId } : {}),
              }),
            );
          } catch (error) {
            results.push({
              observation_id: getObservationId(candidate) ?? `batch-index:${index}`,
              status: "rejected",
              error: toProtocolError(error),
            });
          }
        }
        return sendJson(response, 200, {
          accepted: results.filter((item) => item.status !== "rejected").length,
          rejected: results.filter((item) => item.status === "rejected").length,
          results,
        });
      }

      if (request.method === "POST" && url.pathname === "/v1/syncs") {
        const input = startSyncSchema.parse(
          await readJsonBody(request, maxBodyBytes),
        );
        assertTenant(input.tenant_id, requestTenant, allowRequestTenant);
        return sendJson(response, 201, {
          sync: await options.store.startSync(input),
        });
      }

      const syncCompleteMatch = url.pathname.match(
        /^\/v1\/syncs\/([^/]+)\/complete$/,
      );
      if (request.method === "POST" && syncCompleteMatch) {
        const tenantId = resolveTenant(request, requestTenant, allowRequestTenant);
        const input = completeSyncSchema.parse(
          await readJsonBody(request, maxBodyBytes),
        );
        const syncId = decodeURIComponent(syncCompleteMatch[1]!);
        return sendJson(response, 200, {
          sync: await options.store.completeSync(tenantId, syncId, input),
        });
      }

      const syncMatch = url.pathname.match(/^\/v1\/syncs\/([^/]+)$/);
      if (request.method === "GET" && syncMatch) {
        const tenantId = resolveTenant(request, requestTenant, allowRequestTenant);
        const syncId = decodeURIComponent(syncMatch[1]!);
        const sync = await options.store.getSync(tenantId, syncId);
        if (!sync) return sendError(response, 404, "NOT_FOUND", "Sync not found");
        return sendJson(response, 200, { sync });
      }

      if (request.method === "POST" && url.pathname === "/v1/signal-rules") {
        const input = signalRuleEnvelopeSchema.parse(
          await readJsonBody(request, maxBodyBytes),
        );
        assertTenant(input.tenant_id, requestTenant, allowRequestTenant);
        return sendJson(response, 200, {
          rule: await options.store.upsertSignalRule(input.tenant_id, input.rule),
        });
      }

      if (request.method === "GET" && url.pathname === "/v1/signal-rules") {
        const tenantId = resolveTenant(request, requestTenant, allowRequestTenant);
        return sendJson(response, 200, {
          rules: await options.store.listSignalRules(tenantId),
        });
      }

      const ruleMatch = url.pathname.match(/^\/v1\/signal-rules\/([^/]+)$/);
      if (request.method === "DELETE" && ruleMatch) {
        const tenantId = resolveTenant(request, requestTenant, allowRequestTenant);
        const deleted = await options.store.deleteSignalRule(
          tenantId,
          decodeURIComponent(ruleMatch[1]!),
        );
        if (!deleted) return sendError(response, 404, "NOT_FOUND", "Rule not found");
        return sendJson(response, 200, { deleted: true });
      }

      const observationMatch = url.pathname.match(/^\/v1\/observations\/([^/]+)$/);
      if (request.method === "GET" && observationMatch) {
        const tenantId = resolveTenant(request, requestTenant, allowRequestTenant);
        const observationId = decodeURIComponent(observationMatch[1]!);
        const observation = await options.store.getObservationForConsumer(
          tenantId,
          observationId,
          consumer,
        );
        if (!observation) {
          return sendError(response, 404, "NOT_FOUND", "Observation not found");
        }
        return sendJson(response, 200, { observation });
      }

      return sendError(response, 404, "NOT_FOUND", "Route not found");
    } catch (error) {
      if (error instanceof BodyTooLargeError) {
        return sendError(response, 413, "PAYLOAD_TOO_LARGE", error.message);
      }
      if (error instanceof SyntaxError) {
        return sendError(response, 400, "INVALID_JSON", "Request body is not valid JSON");
      }
      if (error instanceof ZodError) {
        return sendJson(response, 422, { error: toProtocolError(error) });
      }
      if (error instanceof TenantMismatchError) {
        return sendJson(response, 403, { error: toProtocolError(error) });
      }
      if (error instanceof TenantRequiredError) {
        return sendJson(response, 400, { error: toProtocolError(error) });
      }
      if (error instanceof RequestTenantNotAllowedError) {
        return sendJson(response, 403, { error: toProtocolError(error) });
      }
      if (error instanceof ObservationConflictError) {
        return sendJson(response, 409, { error: toProtocolError(error) });
      }
      if (error instanceof CorrectionError || error instanceof SyncError) {
        return sendJson(response, 409, { error: toProtocolError(error) });
      }
      console.error(error);
      return sendError(
        response,
        500,
        "INTERNAL",
        "An unexpected error occurred",
        true,
      );
    }
  });
}

/**
 * Preserves the behaviour Sensus had before per-request identity existed: a
 * single shared bearer secret, or no authentication at all when none is set.
 */
function legacyResolver(options: HttpOptions): IdentityResolver {
  const consumer =
    options.consumer ??
    createConsumerContext({ principals: ["role:api"], clearance: "internal" });
  if (options.apiKey) {
    return new ApiKeyResolver({
      apiKey: options.apiKey,
      consumer,
      ...(options.defaultTenantId ? { tenantId: options.defaultTenantId } : {}),
    });
  }
  return new AnonymousResolver(consumer, options.defaultTenantId);
}

type AuthResult =
  | { ok: true; consumer: ConsumerContext; tenantId: string | undefined }
  | { ok: false; status: number; code: string; message: string };

async function authenticate(
  request: IncomingMessage,
  resolver: IdentityResolver,
): Promise<AuthResult> {
  try {
    const identity = await resolver.resolve({
      authorization: headerValue(request, "authorization"),
    });
    return { ok: true, consumer: identity.consumer, tenantId: identity.tenantId };
  } catch (error) {
    if (error instanceof UnauthenticatedError) {
      return {
        ok: false,
        status: 401,
        code: "UNAUTHENTICATED",
        message: error.message,
      };
    }
    if (error instanceof ForbiddenIdentityError) {
      return {
        ok: false,
        status: 403,
        code: "PERMISSION_DENIED",
        message: error.message,
      };
    }
    throw error;
  }
}

function headerValue(request: IncomingMessage, name: string): string | undefined {
  const value = request.headers[name];
  return Array.isArray(value) ? value[0] : value;
}

/**
 * Resolves the tenant a read route acts on.
 *
 * Order is deliberate: a tenant the runtime already determined — from the
 * identity resolver, or from `defaultTenantId` — always wins, and a request that
 * names a different one is rejected. Only when nothing determined a tenant does
 * the request get a say, and only with `allowRequestTenant` on, since a
 * caller-named tenant is authorized by nothing: a shared API key would
 * otherwise be able to read any tenant it can spell in a header.
 */
function resolveTenant(
  request: IncomingMessage,
  resolvedTenant: string | undefined,
  allowRequestTenant: boolean,
): string {
  const requestedTenant = headerValue(request, "x-sensus-tenant");
  if (resolvedTenant) {
    if (requestedTenant && requestedTenant !== resolvedTenant) {
      throw new TenantMismatchError();
    }
    return resolvedTenant;
  }
  if (requestedTenant) {
    if (allowRequestTenant) return requestedTenant;
    throw new RequestTenantNotAllowedError();
  }
  throw new TenantRequiredError();
}

/**
 * Checks a tenant named in a request body against the resolved one.
 *
 * With nothing resolved and `allowRequestTenant` off there is no allowlist to
 * check against, so the request is refused instead of trusted. That is what
 * stops an unconfigured deployment from being an open multi-tenant writer.
 */
function assertTenant(
  tenantId: string,
  resolvedTenant: string | undefined,
  allowRequestTenant: boolean,
): void {
  if (resolvedTenant) {
    if (resolvedTenant !== tenantId) throw new TenantMismatchError();
    return;
  }
  if (!allowRequestTenant) throw new RequestTenantNotAllowedError();
}

class TenantMismatchError extends Error {
  constructor() {
    super("Request tenant does not match the configured tenant");
    this.name = "TenantMismatchError";
  }
}

class TenantRequiredError extends Error {
  constructor() {
    super("x-sensus-tenant header is required");
    this.name = "TenantRequiredError";
  }
}

/**
 * Raised when a request names its own tenant and the deployment has not opted
 * into allowing that. The message is actionable on purpose: this is a
 * configuration gap, not a caller mistake.
 */
class RequestTenantNotAllowedError extends Error {
  constructor() {
    super(
      "This runtime has no tenant configured and request-named tenants are disabled. " +
        "Set SENSUS_TENANT_ID to the tenant this process serves, configure " +
        "mapping.tenant.allowed so the identity decides, or set " +
        "SENSUS_ALLOW_REQUEST_TENANT=true to trust the request (unsafe unless an " +
        "identity resolver constrains tenants).",
    );
    this.name = "RequestTenantNotAllowedError";
  }
}

async function readJsonBody(
  request: IncomingMessage,
  maxBytes: number,
): Promise<unknown> {
  let bytes = 0;
  const chunks: Buffer[] = [];
  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    bytes += buffer.byteLength;
    if (bytes > maxBytes) throw new BodyTooLargeError(maxBytes);
    chunks.push(buffer);
  }
  return JSON.parse(Buffer.concat(chunks).toString("utf8"));
}

class BodyTooLargeError extends Error {
  constructor(maxBytes: number) {
    super(`Request body exceeds ${maxBytes} bytes`);
  }
}

function getObservationId(value: unknown): string | undefined {
  if (!value || typeof value !== "object") return undefined;
  const id = (value as Record<string, unknown>).observation_id;
  return typeof id === "string" ? id : undefined;
}

function toProtocolError(error: unknown): Record<string, unknown> {
  if (error instanceof ZodError) {
    return {
      code: "INVALID_OBSERVATION",
      message: error.issues[0]?.message ?? "Observation failed validation",
      retryable: false,
      details: { issues: error.issues },
    };
  }
  if (error instanceof ObservationConflictError) {
    return {
      code: "CONFLICT",
      message: error.message,
      retryable: false,
      details: { observation_id: error.observationId },
    };
  }
  if (error instanceof CorrectionError || error instanceof SyncError) {
    return {
      code: error.name === "SyncError" ? "SYNC_CONFLICT" : "CORRECTION_CONFLICT",
      message: error.message,
      retryable: false,
    };
  }
  if (error instanceof TenantMismatchError) {
    return {
      code: "PERMISSION_DENIED",
      message: error.message,
      retryable: false,
    };
  }
  if (error instanceof TenantRequiredError) {
    return {
      code: "INVALID_ARGUMENT",
      message: error.message,
      retryable: false,
    };
  }
  if (error instanceof RequestTenantNotAllowedError) {
    return {
      code: "PERMISSION_DENIED",
      message: error.message,
      retryable: false,
    };
  }
  return {
    code: "INTERNAL",
    message: error instanceof Error ? error.message : "Unknown error",
    retryable: false,
  };
}

function sendError(
  response: ServerResponse,
  status: number,
  code: string,
  message: string,
  retryable = false,
): void {
  sendJson(response, status, { error: { code, message, retryable } });
}

function sendJson(response: ServerResponse, status: number, body: unknown): void {
  const payload = JSON.stringify(body);
  response.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "content-length": Buffer.byteLength(payload),
  });
  response.end(payload);
}

async function main(): Promise<void> {
  const selection = selectStorage();
  const store = await openStorageFromEnvironment();
  const port = Number(process.env.PORT ?? 8787);
  const host = process.env.HOST ?? "127.0.0.1";
  const identity = identityResolverFromEnvironment();
  const allowRequestTenant = process.env.SENSUS_ALLOW_REQUEST_TENANT === "true";
  const server = createSensusHttpServer({
    store,
    consumer: consumerFromEnvironment(),
    ...(identity ? { identity } : {}),
    ...(process.env.SENSUS_API_KEY
      ? { apiKey: process.env.SENSUS_API_KEY }
      : {}),
    ...(process.env.SENSUS_TENANT_ID
      ? { defaultTenantId: process.env.SENSUS_TENANT_ID }
      : {}),
    allowRequestTenant,
  });
  server.listen(port, host, () => {
    console.error(`Sensus HTTP listening on http://${host}:${port}`);
    console.error(`Storage: ${describeStorage(selection)}`);
    console.error(
      identity
        ? identity.acceptsAnonymous
          ? `Identity: ${describeIdentityFromEnvironment() ?? "resolver configured"} — and it SERVES REQUESTS WITH NO CREDENTIAL, as that anonymous identity`
          : `Identity: ${describeIdentityFromEnvironment() ?? "resolver configured"}; each request is authorized separately`
        : process.env.SENSUS_API_KEY
          ? "Identity: shared api key"
          : "Identity: NONE — authentication is disabled",
    );
    console.error(
      process.env.SENSUS_TENANT_ID
        ? `Tenant: pinned to ${process.env.SENSUS_TENANT_ID}`
        : allowRequestTenant
          ? "Tenant: request-named (SENSUS_ALLOW_REQUEST_TENANT=true) — any caller that authenticates can act on any tenant it names"
          : "Tenant: NOT configured — requests that name their own tenant are refused. Set SENSUS_TENANT_ID, or configure mapping.tenant.allowed",
    );
  });

  const shutdown = (): void => {
    server.close(() => {
      void store.close().finally(() => process.exit(0));
    });
  };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

if (process.argv[1] && import.meta.url === new URL(`file://${process.argv[1]}`).href) {
  void main();
}
